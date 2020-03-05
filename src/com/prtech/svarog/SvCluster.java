package com.prtech.svarog;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

/**
 * Class for managing the svarog cluster infrastructure. Provides basic
 * synchronization methods between multiple svarog nodes.
 * 
 * @author ristepejov
 *
 */
public class SvCluster extends SvCore implements Runnable {
	static final byte MSG_UNKNOWN = -2;
	static final byte MSG_FAIL = -1;
	static final byte MSG_SUCCESS = 0;
	static final byte MSG_HEARTBEAT = 1;
	static final byte MSG_LOCK = 2;
	static final byte MSG_AUTH_TOKEN_PUT = 3;
	static final byte MSG_AUTH_TOKEN_GET = 4;
	static final byte MSG_AUTH_TOKEN_SET = 6;
	static final byte MSG_DIRTY_OBJECT = 7;
	static final byte MSG_LOGOFF = 8;
	static final byte MSG_LOCK_RELEASE = 9;
	static final byte MSG_JOIN = 10;
	static final byte MSG_PART = 11;

	/**
	 * Timestamp of the next planned maintenance of the cluster table in the DB
	 */
	static DateTime nextMaintenance = new DateTime();

	/**
	 * Timeout interval on the socket receive
	 */
	static final int sockeReceiveTimeout = SvConf.getHeartBeatInterval() / 2;

	/**
	 * Flag to know if the current node is coordinator or worker.
	 */
	static boolean runMaintenanceThread = true;

	/**
	 * Flag to know if the current node is coordinator or worker.
	 */
	static boolean isCoordinator = false;

	/**
	 * Flag if the cluster is running
	 */
	static AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * Flag if the maintenance is in progress
	 */
	static AtomicBoolean maintenanceInProgress = new AtomicBoolean(false);

	/**
	 * Flag if the client is running
	 */
	static AtomicBoolean isActive = new AtomicBoolean(false);
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvCluster.class);

	/**
	 * Reference to the coordinator node
	 */
	static DbDataObject coordinatorNode = null;
	/**
	 * Reference to the current node descriptor. If the current node is
	 * coordinator, the references should be equal.
	 */
	static DbDataObject currentNode = null;

	static final String ipAddrDelimiter = ";";

	/**
	 * Member fields holding reference to the heart beat thread
	 */
	static private Thread heartBeatThread = null;
	static private Thread notifierThread = null;
	static Thread maintenanceThread = null;

	/**
	 * Reference to the coordinator node
	 */
	static boolean autoStartClient = true;

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system privileges.
	 * 
	 * @throws SvException
	 */
	SvCluster() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Method to generate a DBO describing the current node
	 * 
	 * @return A DbDataObject descriptor of the current node
	 */
	static DbDataObject getCurrentNodeInfo() {
		DbDataObject cNode = new DbDataObject(svCONST.OBJECT_TYPE_CLUSTER);
		cNode.setVal("join_time", new DateTime());
		cNode.setVal("part_time", SvConf.MAX_DATE);
		cNode.setVal("last_maintenance", new DateTime());
		nextMaintenance = new DateTime().plusSeconds(SvConf.getClusterMaintenanceInterval());
		cNode.setVal("next_maintenance", nextMaintenance);
		String localIp = "0.0.0.0:" + SvConf.getHeartBeatPort();
		try {
			localIp = SvUtil.getIpAdresses(true, ipAddrDelimiter);
		} catch (UnknownHostException e) {
			log4j.error("Can't get node IP Address!", e);
		}
		cNode.setVal("local_ip", localIp);
		JsonObject json = new JsonObject();
		json.addProperty("pid", ManagementFactory.getRuntimeMXBean().getName());
		json.addProperty("version", SvConf.appVersion);
		json.addProperty("build", SvConf.appBuild);
		json.addProperty("ip", localIp);
		cNode.setVal("node_info", json.toString());
		return cNode;
	}

	/**
	 * Method to perform coordinator promotion of the current node
	 * 
	 * @param svc
	 *            A reference to an SvCore (the read/write of the nodes has to
	 *            be in the same transaction)
	 * @return True if the current node was promoted to coordinator
	 * @throws SvException
	 *             any underlying exception (except object not updateable, which
	 *             would mean someone else got promoted in meantime)
	 */
	static boolean becomeCoordinator(SvCore svc) throws SvException {
		SvWriter svw = null;
		boolean success = false;
		try {
			svw = new SvWriter(svc);
			if (coordinatorNode == null) {
				coordinatorNode = new DbDataObject(svCONST.OBJECT_TYPE_CLUSTER);
				coordinatorNode.setObjectId(svCONST.CLUSTER_COORDINATOR_ID);
			}
			coordinatorNode.setValuesMap(currentNode.getValuesMap());
			svw.isInternal = true;
			svw.saveObject(coordinatorNode, true);
			success = true;
		} catch (SvException e) {
			// if the object is not update-able, another node became
			// coordinator and we should register a worker node
			if (!e.getLabelCode().equals("system.error.obj_not_updateable"))
				throw (e);
			else
				success = false;

		} finally {
			if (svw != null)
				svw.release();
		}
		return success;
	}

	/**
	 * Method to perform resigning of coordinator
	 * 
	 * @param svc
	 *            A reference to an SvCore (the read/write of the nodes has to
	 *            be in the same transaction)
	 * @return True if the current has resigned from the coordinator role
	 * @throws SvException
	 *             any underlying exception (except object not updateable, which
	 *             would mean someone else got promoted in meantime)
	 */
	static boolean resignCoordinator() throws SvException {

		SvWriter svw = null;
		SvReader svr = null;
		boolean success = false;
		try {
			if (isCoordinator) {
				svw = new SvWriter();
				svr = new SvReader(svw);
				DbCache.removeObject(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER);
				coordinatorNode = svr.getObjectById(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER, null);
				coordinatorNode.setVal("last_maintenance", DateTime.now());
				coordinatorNode.setVal("next_maintenance", DateTime.now());
				coordinatorNode.setVal("part_time", DateTime.now());
				svw.isInternal = true;
				svw.saveObject(coordinatorNode, true);
				success = true;
			} else
				success = false;
		} catch (SvException e) {
			// if the object is not update-able, another node became
			// coordinator and we should register a worker node
			if (!e.getLabelCode().equals("system.error.obj_not_updateable"))
				throw (e);
			else
				success = false;

		} finally {
			if (svw != null)
				svw.release();
			if (svr != null)
				svr.release();
		}
		return success;
	}

	/**
	 * Method to maintain the table of cluster nodes. It will first delete any
	 * old redundant records, then update the current cluster state based on the
	 * nodes which have been reporting via heartbeat in the last interval
	 */
	static void clusterListMaintenance() {
		// we are the coordinator and the Heart Beat server is running
		if (isCoordinator && SvClusterServer.isRunning.get()) {
			synchronized (SvCluster.isRunning) {
				SvReader svr = null;
				SvWriter svw = null;
				PreparedStatement ps = null;
				try {
					svr = new SvReader();
					Connection conn = svr.dbGetConn();

					// DbSearch dbs = new DbSearchCriterion("part_time",
					// DbCompareOperand.GREATER_EQUAL, DateTime.now());
					DbDataArray dba = svr.getObjects(null, svCONST.OBJECT_TYPE_CLUSTER, null, 0, 0);

					StringBuilder sbr = new StringBuilder();
					sbr.append("DELETE FROM ");
					sbr.append(SvConf.getDefaultSchema() + ".");
					sbr.append((String) SvCore.getDbt(svCONST.OBJECT_TYPE_CLUSTER).getVal("TABLE_NAME")
							+ " WHERE PKID NOT IN (");

					DbDataArray updatedList = new DbDataArray();
					for (DbDataObject node : dba.getItems()) {
						// do try to get a valid coordinator
						updatedList.addDataItem(node);
						boolean nodeRemoved = false;
						node.setVal("last_maintenance", DateTime.now());
						node.setVal("next_maintenance",
								DateTime.now().plusSeconds(SvConf.getClusterMaintenanceInterval()));

						if (!node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID)
								&& !SvClusterServer.nodeHeartBeats.containsKey(node.getObjectId())) {
							node.setVal("part_time", DateTime.now());
							nodeRemoved = true;
						}

						if (!isRunning.get() && node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID))
							node.setVal("part_time", DateTime.now());
						// if the node is not removed or it is the coordinator
						// record make sure we keep it
						if (!nodeRemoved || node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID))
							sbr.append(node.getPkid().toString() + ",");
					}

					sbr.setLength(sbr.length() - 1);
					sbr.append(")");
					conn.setAutoCommit(false);
					ps = conn.prepareStatement(sbr.toString());
					svw = new SvWriter(svr);
					ps.execute();
					svw.saveObject(updatedList, false);
					svw.dbCommit();
					nextMaintenance = new DateTime().plusSeconds(SvConf.getClusterMaintenanceInterval());

				} catch (Exception e) {
					log4j.error("Svarog cluster maintenance failed! Shutting down cluster", e);
					if (e instanceof SvException) {
						shutdown(false);
						initCluster();
					}
				} finally {
					try {
						SvCore.closeResource(ps, svCONST.systemUser);
					} catch (SvException e) {
						log4j.error("Svarog cluster maintenance failed!", e);
					}
					if (svr != null)
						svr.release();
					if (svw != null)
						svw.release();
				}
			}

		}
	}

	/**
	 * Method to initialise the Svarog Cluster. This method shall try to locate
	 * a valid coordinator node, or become one if there isn't any. If it becomes
	 * a coordinator then it will start the heart beat listener and the notifier
	 * proxy servers. If the current node doesn't manage to become a coordinator
	 * then it will start a Heart Beat client and notifier client and try to
	 * connect to the coordinator
	 * 
	 * @return True if the cluster was properly initialised.
	 */
	static boolean initCluster() {
		if (!isRunning.compareAndSet(false, true)) {
			log4j.error("Cluster is already initialised and active. Shutdown first");
			return false;
		}
		SvReader svr = null;
		SvWriter svw = null;
		isCoordinator = false;
		try {
			svr = new SvReader();
			// do try to get a valid coordinator
			// force purge of the cache to get the coordinator
			DbCache.removeObject(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER);
			coordinatorNode = svr.getObjectById(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER, null);
			currentNode = getCurrentNodeInfo();
			DateTime nextMaintenance = coordinatorNode != null ? (DateTime) coordinatorNode.getVal("next_maintenance")
					: null;
			// the next maintenance is in the past means there's no active
			// coordinator, we will try to become a coordinator
			if (nextMaintenance == null || nextMaintenance.isBeforeNow()) {
				isCoordinator = becomeCoordinator(svr);
				// lets try to refreshe the coordinator node infor
				if (!isCoordinator) {
					DbCache.removeObject(coordinatorNode.getObjectId(), coordinatorNode.getObjectType());
					coordinatorNode = null;
					log4j.warn("Coordinator record is invalid. Next maintenance in the past: "
							+ coordinatorNode.getVal("next_maintenance"));
				} else
					log4j.info("The node promoted to coordinator: " + coordinatorNode.getVal("node_info"));
			} else
				log4j.info("Valid coordinator record found: " + coordinatorNode.getVal("node_info"));

			// if the local IP including the hb port is the same ... means that
			// the coordinator record is related to this node, but maybe a core
			// dump caused the server to fail. So lets try to become coordinator
			String hbAddress = (String) coordinatorNode.getVal("local_ip");
			boolean initHb = false;
			boolean initNotif = false;
			if (isCoordinator || (!isCoordinator && SvUtil.getIpAdresses(true, ipAddrDelimiter).equals(hbAddress))) {
				initHb = SvClusterServer.initServer();
				initNotif = SvClusterNotifierProxy.initServer();
				if (initHb && initNotif) {
					isCoordinator = true;
					heartBeatThread = new Thread(new SvClusterServer());
					heartBeatThread.setName("SvClusterServerThread");
					heartBeatThread.start();
					notifierThread = new Thread(new SvClusterNotifierProxy());
					notifierThread.setName("SvClusterNotifierProxyThread");
					notifierThread.start();
					if (runMaintenanceThread) {
						maintenanceThread = new Thread(new SvCluster());
						maintenanceThread.start();
					}
					currentNode.setParentId(coordinatorNode.getObjectId());
					isRunning.set(true);
				} else {
					log4j.info("Svarog Cluster Servers initialisation failed. Initiating Cluster shutdown.");
					shutdown();
				}

			}
			if (autoStartClient) {
				initHb = SvClusterClient.initClient(hbAddress);
				initNotif = initHb ? SvClusterNotifierClient.initClient(hbAddress) : false;
				if (initHb && initNotif) {
					heartBeatThread = new Thread(new SvClusterClient());
					heartBeatThread.setName("SvClusterClientThread");
					heartBeatThread.start();
					notifierThread = new Thread(new SvClusterNotifierClient());
					notifierThread.setName("SvClusterNotifierClientThread");
					notifierThread.start();
					isRunning.set(true);
				} else {
					log4j.info("Svarog Cluster Clients initialisation failed. Initiating Cluster shutdown.");
					shutdown();
				}

			}

			// if the cluster is running and hasn't shut down, mark it as active
		} catch (Exception e) {
			log4j.error("Svarog cluster could not be initialised. Cluster not running!", e);
			shutdown();
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
		}
		isActive.set(isRunning.get());
		return isActive.get();
	}

	/**
	 * Method to shutdown the Svarog cluster. The cluster shutdown will in turn:
	 * 1. Check the current node is a cluster coordinator. 2. If we are a
	 * cluster coordinator then we shall shutdown the SvClusterServer and
	 * SvClusterNotifierProxy 3. If we are not a coordinator then we shall
	 * shutdown the SvClusterClient and SvClusterNotifierClient
	 */

	static void shutdown() {
		shutdown(true);
	}

	/**
	 * Method to shutdown the Svarog cluster. The cluster shutdown will in turn:
	 * 1. Check the current node is a cluster coordinator. 2. If we are a
	 * cluster coordinator then we shall shutdown the SvClusterServer and
	 * SvClusterNotifierProxy 3. If we are not a coordinator then we shall
	 * shutdown the SvClusterClient and SvClusterNotifierClient
	 */
	static void shutdown(boolean doMaintenance) {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.error("Svarog Cluster not running. Init first");
			return;
		}
		if (doMaintenance)
			clusterListMaintenance();
		if (!isCoordinator) {
			if (autoStartClient) {
				if (SvClusterClient.isRunning.get())
					SvClusterClient.shutdown();
				if (SvClusterNotifierClient.isRunning.get())
					SvClusterNotifierClient.shutdown();
			}
		} else {
			if (SvClusterServer.isRunning.get())
				SvClusterServer.shutdown();
			if (SvClusterNotifierProxy.isRunning.get())
				SvClusterNotifierProxy.shutdown();
		}
		if (!Thread.currentThread().equals(heartBeatThread) && heartBeatThread != null && heartBeatThread.isAlive())
			try {
				heartBeatThread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log4j.info("Interrupted heart beat thread join.", e);
			}
		if (!Thread.currentThread().equals(notifierThread) && notifierThread != null && notifierThread.isAlive())
			try {
				notifierThread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log4j.info("Interrupted notifier thread join.", e);
			}
		heartBeatThread = null;
		notifierThread = null;
		coordinatorNode = null;
		if (runMaintenanceThread)
			maintenanceThread = null;
		isCoordinator = false;
		isActive.set(false);
	}

	@Override
	public void run() {

		while (isRunning.get()) {
			try {
				Thread.sleep((SvConf.getClusterMaintenanceInterval() - 2) * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
			clusterListMaintenance();
		}

	}
}
