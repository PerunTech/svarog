package com.prtech.svarog;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.google.common.cache.LoadingCache;
import com.google.gson.JsonObject;
import com.prtech.svarog.SvCluster.DistributedLock;
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
public class SvCluster extends SvCore {
	static final byte MSG_UNKNOWN = -2;
	static final byte MSG_FAIL = -1;
	static final byte MSG_SUCCESS = 0;
	static final byte MSG_HEARTBEAT = 1;

	static final byte MSG_AUTH_TOKEN_PUT = 3;
	static final byte MSG_AUTH_TOKEN_GET = 4;
	static final byte MSG_AUTH_TOKEN_SET = 6;

	static final byte MSG_JOIN = 10;
	static final byte MSG_PART = 11;

	static final byte MSG_LOCK = 20;
	static final byte MSG_LOCK_RELEASE = 21;
	static final byte NOTE_LOCK_ACQUIRED = 22;
	static final byte NOTE_LOCK_RELEASED = 23;

	static final byte NOTE_DIRTY_OBJECT = 30;
	static final byte NOTE_LOGOFF = 31;
	static final byte NOTE_DIRTY_TILE = 32;
	static final byte NOTE_ACK = 33;

	static final String JOIN_TIME = "join_time";
	static final String PART_TIME = "part_time";
	static final String LAST_MAINTENANCE = "last_maintenance";
	static final String NEXT_MAINTENANCE = "next_maintenance";
	static final String NODE_INFO = "node_info";

	/**
	 * Class to wrap the standard locks in order to support the distributed
	 * locking of the svarog cluster. The key of the lock as well as the node
	 * which holds the lock are the needed information
	 * 
	 * @author ristepejov
	 *
	 */
	static class DistributedLock {
		public String key;
		public Long nodeId;
		public ReentrantLock lock;
		public int lockHash;

		DistributedLock(String key, Long nodeId, ReentrantLock lock, int lockHash) {
			this.nodeId = nodeId;
			this.lock = lock;
			this.key = key;
			this.lockHash = lockHash;
		}
	}

	/**
	 * Timestamp of the next planned maintenance of the cluster table in the DB
	 */
	static DateTime nextMaintenance = new DateTime();

	/**
	 * Timeout interval on the socket receive
	 */
	static final int SOCKET_RECV_TIMEOUT = SvConf.getHeartBeatInterval() / 2;

	/**
	 * Flag to know if the current node is coordinator or worker.
	 */
	private static boolean isCoordinator = false;

	/**
	 * Flag if the cluster is running
	 */
	private static final AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * Flag if the maintenance is in progress
	 */
	private static final AtomicBoolean maintenanceInProgress = new AtomicBoolean(false);

	/**
	 * Flag if the client is running
	 */
	private static AtomicBoolean isActive = new AtomicBoolean(false);
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvCluster.class);

	/**
	 * Reference to the coordinator node
	 */
	private static DbDataObject coordinatorNode = null;
	/**
	 * Reference to the current node descriptor. If the current node is
	 * coordinator, the references should be equal.
	 */
	// static DbDataObject currentNode = null;

	static final String IP_ADDR_DELIMITER = ";";

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
		cNode.setVal(JOIN_TIME, new DateTime());
		cNode.setVal(PART_TIME, SvConf.MAX_DATE);
		cNode.setVal(LAST_MAINTENANCE, new DateTime());
		nextMaintenance = new DateTime().plusSeconds(SvConf.getClusterMaintenanceInterval());
		cNode.setVal(NEXT_MAINTENANCE, nextMaintenance);
		String localIp = "0.0.0.0:" + SvConf.getHeartBeatPort();
		try {
			localIp = SvUtil.getIpAdresses(true, IP_ADDR_DELIMITER);
		} catch (UnknownHostException e) {
			log4j.error("Can't get node IP Address!", e);
		}
		cNode.setVal("local_ip", localIp);
		JsonObject json = new JsonObject();
		json.addProperty("pid", ManagementFactory.getRuntimeMXBean().getName());
		json.addProperty("version", SvConf.appVersion);
		json.addProperty("build", SvConf.appBuild);
		json.addProperty("ip", localIp);
		cNode.setVal(NODE_INFO, json.toString());
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
		boolean success = true;
		try {
			svw = new SvWriter(svc);
			if (coordinatorNode == null) {
				coordinatorNode = new DbDataObject(svCONST.OBJECT_TYPE_CLUSTER);
				coordinatorNode.setObjectId(svCONST.CLUSTER_COORDINATOR_ID);
			}
			coordinatorNode.setValuesMap(getCurrentNodeInfo().getValuesMap());
			svw.isInternal = true;
			svw.saveObject(coordinatorNode, true);
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

		// If we failed to promote lets try to refresh the coordinator node info
		if (!success) {
			DbCache.removeObject(coordinatorNode.getObjectId(), coordinatorNode.getObjectType());
			coordinatorNode = null;
			log4j.warn("Coordinator record is invalid. Next maintenance in the past: "
					+ coordinatorNode.getVal(NEXT_MAINTENANCE));
		} else
			log4j.info("The node promoted to coordinator: " + coordinatorNode.getVal(NODE_INFO));

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
				svr = new SvReader();
				svr.isInternal = true;
				svw = new SvWriter(svr);
				DbCache.removeObject(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER);
				coordinatorNode = svr.getObjectById(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER, null);
				coordinatorNode.setVal(LAST_MAINTENANCE, DateTime.now());
				coordinatorNode.setVal(NEXT_MAINTENANCE, DateTime.now());
				coordinatorNode.setVal(PART_TIME, DateTime.now());
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
	 * Method to delete all historical information from the cluster table. For
	 * nodes membership we don't really want to grow a big big table so its
	 * easier to delete everything which isn't in the list of dontDeleteIDs
	 * 
	 * @param conn
	 *            The JDBC connection to used for executing the query
	 * @param oldIds
	 *            The list of object PKIDs which we want to keep (don't delete)
	 *            in string format, comma separated
	 * @throws Exception
	 *             Throw any underlying exception
	 */
	static private void clusterListDeleteHistory(Connection conn, StringBuilder dontDeleteIDs) throws Exception {
		PreparedStatement ps = null;

		try {
			StringBuilder sbr = new StringBuilder(100);
			sbr.append("DELETE FROM ");
			sbr.append(SvConf.getDefaultSchema() + ".");
			sbr.append(
					(String) SvCore.getDbt(svCONST.OBJECT_TYPE_CLUSTER).getVal("TABLE_NAME") + " WHERE PKID NOT IN (");
			// append the ids which we should not delete
			sbr.append(dontDeleteIDs);
			sbr.setLength(sbr.length() - 1);
			sbr.append(")");
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sbr.toString());

			ps.execute();

		} finally {
			try {
				SvCore.closeResource(ps, svCONST.systemUser);
			} catch (SvException e) {
				log4j.error("Svarog cluster maintenance failed!", e);
			}
		}

	}

	/**
	 * Method to update all information related to a node in the cluster list.
	 * If the node is subject of historical date or should be removed from the
	 * list the method will return EMPTY string. If the node should not be
	 * removed the method will return the versioning id of the node.
	 * 
	 * @param node
	 *            Reference to a DbDataObject containing the node info
	 * @return EMPTY string if the node should be delete, otherwise PKID
	 */
	static private String clusterListUpdateNode(DbDataObject node) {

		boolean nodeRemoved = false;
		node.setVal(LAST_MAINTENANCE, DateTime.now());
		node.setVal(NEXT_MAINTENANCE, DateTime.now().plusSeconds(SvConf.getClusterMaintenanceInterval()));

		if (!node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID)
				&& !SvClusterServer.nodeHeartBeats.containsKey(node.getObjectId())) {
			node.setVal(PART_TIME, DateTime.now());
			nodeRemoved = true;
		}

		if (!isRunning.get() && node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID))
			node.setVal(PART_TIME, DateTime.now());
		// if the node is not removed or it is the coordinator
		// record make sure we keep it
		if (!nodeRemoved || node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID))
			return node.getPkid().toString() + ",";
		else
			return "";
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

				try {
					svr = new SvReader();
					Connection conn = svr.dbGetConn();

					DbDataArray dba = svr.getObjects(null, svCONST.OBJECT_TYPE_CLUSTER, null, 0, 0);

					StringBuilder sbr = new StringBuilder();

					DbDataArray updatedList = new DbDataArray();
					for (DbDataObject node : dba.getItems()) {
						// do try to get a valid coordinator
						updatedList.addDataItem(node);
						sbr.append(clusterListUpdateNode(node));
					}
					clusterListDeleteHistory(conn, sbr);
					svw = new SvWriter(svr);
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
					if (svr != null)
						svr.release();
					if (svw != null)
						svw.release();
				}
			}

		}
	}

	/**
	 * Method to ensure the cluster clients have started and connected to the
	 * hear beat address/port of the coordinator
	 * 
	 * @param hbAddress
	 *            String list of available heartbeat end points
	 * @return True if the client threads have started and connected with
	 *         success
	 */
	private static boolean startClients(String hbAddress) {
		boolean initHb = false;
		boolean initNotif = false;
		boolean threadStarted = false;
		initHb = SvClusterClient.initClient(hbAddress);
		initNotif = initHb ? SvClusterNotifierClient.initClient(hbAddress) : false;
		if (initHb && initNotif) {
			heartBeatThread = new Thread(new SvClusterClient());
			heartBeatThread.setName("SvClusterClientThread");
			heartBeatThread.start();
			notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.setName("SvClusterNotifierClientThread");
			notifierThread.start();
			threadStarted = true;
		} else {
			log4j.info("Svarog Cluster Clients initialisation failed. Initiating Cluster shutdown.");
			shutdown();
		}
		return initHb && initNotif && threadStarted;
	}

	/**
	 * Method to start the server threads of the cluster
	 * 
	 * @return True if the server threads have been started successfully.
	 * @throws SvException
	 */
	private static boolean startServers() throws SvException {
		boolean initHb = false;
		boolean initNotif = false;
		initHb = SvClusterServer.initServer();
		initNotif = SvClusterNotifierProxy.initServer();
		if (initHb && initNotif) {
			// promote the local locks from the old node id
			heartBeatThread = new Thread(new SvClusterServer());
			heartBeatThread.setName("SvClusterServerThread");
			heartBeatThread.start();
			notifierThread = new Thread(new SvClusterNotifierProxy());
			notifierThread.setName("SvClusterNotifierProxyThread");
			notifierThread.start();

			// finally we are the coordinator
			isCoordinator = true;
		} else {
			log4j.info("Svarog Cluster Servers initialisation failed. Initiating Cluster shutdown.");
			shutdown();
		}
		return initHb && initNotif && isCoordinator;
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
		isCoordinator = false;
		try {
			svr = new SvReader();
			svr.isInternal = true;
			// do try to get a valid coordinator
			// force purge of the cache to get the coordinator
			DbCache.removeObject(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER);
			coordinatorNode = svr.getObjectById(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER, null);
			// currentNode = getCurrentNodeInfo();
			DateTime nextMaintenance = coordinatorNode != null ? (DateTime) coordinatorNode.getVal(NEXT_MAINTENANCE)
					: null;
			// the next maintenance is in the past means there's no active
			// coordinator, we will try to become a coordinator
			if (nextMaintenance == null || nextMaintenance.isBeforeNow())
				isCoordinator = becomeCoordinator(svr);
			else
				log4j.info("Valid coordinator record found: " + coordinatorNode.getVal(NODE_INFO));

			// if the local IP including the hb port is the same ... means that
			// the coordinator record is related to this node, but maybe a core
			// dump caused the server to fail. So lets try to become coordinator
			String hbAddress = (String) coordinatorNode.getVal("local_ip");

			if (isCoordinator || (!isCoordinator && SvUtil.getIpAdresses(true, IP_ADDR_DELIMITER).equals(hbAddress)))
				isActive.set(startServers());

			// if the servers failed to start, then try to start as a client
			if (!isActive.get() && autoStartClient)
				isActive.set(startClients(hbAddress));

		} catch (Exception e) {
			log4j.error("Svarog cluster could not be initialised. Cluster not running!", e);
			shutdown();
		} finally {
			if (svr != null)
				svr.release();
		}
		return isActive.get();
	}

	/**
	 * Method to update a distributed lock. This is invoked when the Notifier
	 * client is processing lock acknowledgments and needs to update the list of
	 * distributed locks by the SvLock on the coordinator node.
	 * 
	 * @param lockHash
	 *            The hash of the lock
	 * @param lockKey
	 *            The key of the lock
	 * 
	 * @return True if the lock was updated
	 */
	static boolean updateDistributedLock(String lockKey, Integer lockHash,
			ConcurrentHashMap<String, SvCluster.DistributedLock> distLocks) {
		DistributedLock dlock = distLocks.get(lockKey);
		if (dlock != null)
			dlock.lockHash = lockHash;
		return dlock != null;

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
	 * Method to shutdown the clients if running
	 */
	static void stopClients() {
		if (autoStartClient) {
			if (SvClusterClient.isRunning.get())
				SvClusterClient.shutdown();
			if (SvClusterNotifierClient.isRunning.get())
				SvClusterNotifierClient.shutdown();
		}

	}

	/**
	 * Method to stop the servers if running
	 */
	static void stopServers() {
		if (SvClusterServer.isRunning.get())
			SvClusterServer.shutdown();
		if (SvClusterNotifierProxy.isRunning.get())
			SvClusterNotifierProxy.shutdown();
	}

	/**
	 * Join the heart beat and notifier threads so we are sure that the clients
	 * or servers have stopped
	 */
	static void joinDaemonThreads() {
		// just check if the join isn't invoked from the heartbeat or notifier.
		Thread joinHb = (!Thread.currentThread().equals(heartBeatThread) ? heartBeatThread : null);
		Thread joinNf = (!Thread.currentThread().equals(notifierThread) ? notifierThread : null);

		while ((joinHb != null && joinHb.isAlive()) || (joinNf != null && joinNf.isAlive())) {
			try {
				if (joinHb != null && joinHb.isAlive())
					joinHb.join(10);
				if (joinNf != null && joinNf.isAlive())
					joinNf.join(10);
			} catch (InterruptedException e) {
				log4j.info("Interrupted heart beat thread join.", e);
			}

		}

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
		// shut down all daemon threads
		if (isCoordinator)
			stopServers();
		else
			stopClients();

		// join the threads
		joinDaemonThreads();

		// reset the global variables
		heartBeatThread = null;
		notifierThread = null;
		coordinatorNode = null;
		isCoordinator = false;
		// notify interested parties that we shut down
		synchronized (SvCluster.isRunning) {
			isRunning.notifyAll();
		}
	}

	/**
	 * Method to release a distributed lock. This is invoked when the
	 * clusterServer processes releaseLock message. This method is also invoked
	 * by the SvLock on the coordinator node.
	 * 
	 * @param lockHash
	 *            The hash of the lock
	 * @param nodeId
	 *            The node id which acquires the lock
	 * @param nodeLocks
	 *            The map of nodes which contains locks held by node
	 * @param distributedLocks
	 *            The map of distributed nodes in the cluster to be used for
	 *            releasing the lock
	 * @return Null if the lock was NOT released, otherwise the lock key.
	 */
	static String releaseDistributedLock(Integer lockHash, long nodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks,
			ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks,
			LoadingCache<String, ReentrantLock> sysLocks) {
		boolean lockReleased = false;
		CopyOnWriteArrayList<SvCluster.DistributedLock> nodeLock = nodeLocks.get(nodeId);
		SvCluster.DistributedLock currentLock = null;
		if (nodeLock != null) {
			for (SvCluster.DistributedLock dLock : nodeLock)
				if (lockHash.equals(dLock.lockHash)) {
					currentLock = dLock;
					break;
				}
		}
		if (currentLock != null) {
			lockReleased = SvLock.releaseLock(currentLock.key, currentLock.lock, false, sysLocks);
			if (currentLock.lock.getHoldCount() == 0) {
				synchronized (distributedLocks) {
					distributedLocks.remove(currentLock.key, currentLock);
				}
				nodeLock.remove(currentLock);
			}
		} else
			log4j.error("Lock with hash " + lockHash.toString() + ", does not exist in the list of distributed locks");
		return lockReleased ? currentLock.key : null;
	}

	/**
	 * Method to acquire a distributed lock from the svarog cluster (this is
	 * server agnostic). It is called also by SvLock in order to synchronize
	 * properly the distributed locks
	 * 
	 * @param lockKey
	 *            The lock key which should be locked.
	 * @param nodeId
	 *            The id of the node which shall acquire the lock
	 * @param extendedInfo
	 *            The id of the node which already holds the lock (available
	 *            only if the lock fails)
	 * @param nodeLocks
	 *            The map of nodes which contains locks held by node
	 * @param distributedLocks
	 *            The map of distributed nodes in the cluster to be used for
	 *            releasing the lock
	 * @return Instance of re-entrant lock if the lock was acquired. Otherwise
	 *         null. If null the extendedInfo is populated with the node holding
	 *         the lock
	 */

	static ReentrantLock acquireDistributedLock(String lockKey, Long nodeId, Long[] extendedInfo,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks,
			ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks,
			LoadingCache<String, ReentrantLock> sysLocks) {
		ReentrantLock lock = null;
		SvCluster.DistributedLock dlock = null;

		synchronized (distributedLocks) {
			dlock = distributedLocks.get(lockKey);
			if (dlock != null && dlock.nodeId.equals(nodeId)) {
				// same node is locking the re-entrant lock again
				lock = SvLock.getLock(lockKey, false, 0L, sysLocks);
			} else {
				if (dlock == null) // brand new lock
				{
					lock = SvLock.getLock(lockKey, false, 0L, sysLocks);
					if (lock != null) {
						dlock = new SvCluster.DistributedLock(lockKey, nodeId, lock, lock.hashCode());
						distributedLocks.put(lockKey, dlock);
					}
				} else
					extendedInfo[0] = dlock.nodeId;

			}
		}
		updateNodeLocks(lock, dlock, nodeLocks, nodeId);
		return lock;

	}

	/**
	 * Update the map with locks per node if the lock was acquired
	 * 
	 * @param lock
	 * @param dlock
	 * @param nodeLocks
	 * @param nodeId
	 */
	static void updateNodeLocks(ReentrantLock lock, SvCluster.DistributedLock dlock,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks, long nodeId) {
		if (lock != null) {
			CopyOnWriteArrayList<SvCluster.DistributedLock> currentNode = new CopyOnWriteArrayList<SvCluster.DistributedLock>();
			CopyOnWriteArrayList<SvCluster.DistributedLock> oldNode = nodeLocks.putIfAbsent(nodeId, currentNode);
			if (oldNode != null)
				currentNode = oldNode;
			currentNode.addIfAbsent(dlock);
		}

	}

	/**
	 * Method to clean up the distributed locks acquired by a node
	 * 
	 * @param nodeId
	 *            The node for which the distributed locks shall be cleaned
	 */
	static void clusterCleanUp(Long nodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks,
			ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks) {
		{
			CopyOnWriteArrayList<SvCluster.DistributedLock> nodeLock = nodeLocks.get(nodeId);
			if (nodeLock != null) {
				for (SvCluster.DistributedLock dstLock : nodeLock) {
					SvLock.releaseLock(dstLock.key, dstLock.lock);
					synchronized (distributedLocks) {
						distributedLocks.remove(dstLock.key, dstLock);
					}
				}
				nodeLocks.remove(nodeId);
			}
		}
	}

	/**
	 * Method to migrate locks from one node to another. This is useful if the
	 * node has lost a heart beat, then re-joined with a new node ID. In this
	 * case we'll just migrate the locks from the old node id to the new one
	 * 
	 * @param nodeId
	 *            The node under which the locks shall be moved
	 * @param oldNodeId
	 *            The node from which the locks will be moved
	 * @param nodeLocks
	 *            The map fo locks which shall be used (Server or Client side)
	 */
	static void migrateLocks(Long nodeId, Long oldNodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks) {
		{
			CopyOnWriteArrayList<SvCluster.DistributedLock> nodeLock = nodeLocks.remove(oldNodeId);
			if (nodeLock != null) {
				for (SvCluster.DistributedLock dstLock : nodeLock) {
					dstLock.nodeId = nodeId;
				}
				nodeLocks.put(nodeId, nodeLock);
			}
		}
	}

	/**
	 * Method to migrate locks from one node to another. This is useful if the
	 * node has lost a heart beat, then re-joined with a new node ID. In this
	 * case we'll just migrate the locks from the old node id to the new one
	 * 
	 * @param nodeId
	 *            The node under which the locks shall be moved
	 * @param oldNodeId
	 *            The node from which the locks will be moved
	 * @param nodeLocks
	 *            The map fo locks which shall be used (Server or Client side)
	 */
	static void copyLocalLocks(Long nodeId, Long oldNodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> localNodeLocks) {
		{
			CopyOnWriteArrayList<SvCluster.DistributedLock> nodeLock = localNodeLocks.remove(oldNodeId);
			if (nodeLock != null) {
				for (SvCluster.DistributedLock dstLock : nodeLock) {
					SvLock.getDistributedLockImpl(dstLock.key, SvCluster.isCoordinator, nodeId);
				}

			}
		}
	}

	public static boolean isCoordinator() {
		return isCoordinator;
	}

	public static void setCoordinator(boolean isCoordinator) {
		SvCluster.isCoordinator = isCoordinator;
	}

	public static AtomicBoolean getIsRunning() {
		return isRunning;
	}

	public static void setIsRunning(boolean isRunning) {
		SvCluster.isRunning.set(isRunning);
	}

	public static AtomicBoolean getMaintenanceInProgress() {
		return maintenanceInProgress;
	}

	public static void setMaintenanceInProgress(boolean maintenanceInProgress) {
		SvCluster.maintenanceInProgress.set(maintenanceInProgress);
	}

	public static AtomicBoolean getIsActive() {
		if (isCoordinator)
			return isActive;
		else
			return SvClusterClient.isActive;
	}

	public static void setIsActive(AtomicBoolean isActive) {
		SvCluster.isActive = isActive;
	}

	public static DbDataObject getCoordinatorNode() {
		return coordinatorNode;
	}

	public static void setCoordinatorNode(DbDataObject coordinatorNode) {
		SvCluster.coordinatorNode = coordinatorNode;
	}

}
