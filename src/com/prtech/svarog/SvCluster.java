package com.prtech.svarog;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.google.common.cache.LoadingCache;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

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
	 * Class to wrap the standard locks in order to support the distributed locking
	 * of the svarog cluster. The key of the lock as well as the node which holds
	 * the lock are the needed information
	 * 
	 * @author ristepejov
	 *
	 */
	static class DistributedLock {
		private String key;
		private Long nodeId;
		private ReentrantLock lock;
		private int lockHash;

		public String getKey() {
			return key;
		}

		public Long getNodeId() {
			return nodeId;
		}

		public ReentrantLock getLock() {
			return lock;
		}

		public int getLockHash() {
			return lockHash;
		}

		DistributedLock(String key, Long nodeId, ReentrantLock lock, int lockHash) {
			this.nodeId = nodeId;
			this.lock = lock;
			this.key = key;
			this.lockHash = lockHash;
		}
	}

	public class SvarogHook implements Runnable {
		String executorList = null;
		String operationName = null;

		SvarogHook(String executors, String operation) {
			executorList = executors;
			operationName = operation;
		}

		@Override
		public void run() {
			if (executorList != null && !executorList.isEmpty()) {
				execSvarogHooks(executorList, operationName);
			}
		}
	}

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
	 * Flag if the client is running
	 */
	private static AtomicBoolean isActive = new AtomicBoolean(false);
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvCluster.class);

	/**
	 * Reference to the coordinator node
	 */
	private static DbDataObject coordinatorNode = null;
	/**
	 * Reference to the current node descriptor. If the current node is coordinator,
	 * the references should be equal.
	 */
	// static DbDataObject currentNode = null;

	static final String IP_ADDR_DELIMITER = ";";

	/**
	 * Member fields holding reference to the heart beat thread
	 */
	static private Thread heartBeatThread = null;
	static private Thread notifierThread = null;

	/**
	 * We need the autoStartClient flag to enable us to start cluster without
	 * clients for the purpose of unit testing
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
		cNode.setVal(NEXT_MAINTENANCE, new DateTime().plusSeconds(SvConf.getClusterMaintenanceInterval()));
		String localIp = "*:" + SvConf.getHeartBeatPort();
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
	 * @param svc A reference to an SvCore (the read/write of the nodes has to be in
	 *            the same transaction)
	 * @return True if the current node was promoted to coordinator
	 * @throws SvException any underlying exception (except object not updateable,
	 *                     which would mean someone else got promoted in meantime)
	 */
	static boolean becomeCoordinator(SvCore svc) throws SvException {
		try (SvWriter svw = new SvWriter(svc); SvReader svr = new SvReader(svc);) {
			if (coordinatorNode == null)
				coordinatorNode = svr.getObjectById(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER, null);
			if (coordinatorNode == null) {
				coordinatorNode = SvCluster.getCurrentNodeInfo();
				coordinatorNode.setObjectId(svCONST.CLUSTER_COORDINATOR_ID);
			}

			coordinatorNode.setValuesMap(getCurrentNodeInfo().getValuesMap());
			svw.isInternal = true;
			svw.saveObject(coordinatorNode, true);
			log4j.info("The node promoted to coordinator: " + coordinatorNode.getVal(NODE_INFO));
		} catch (SvException e) {
			// if the object is not update-able, another node became
			// coordinator and we should register a worker node
			if (!e.getLabelCode().equals("system.error.obj_not_updateable"))
				throw (e);
			else {
				if (coordinatorNode != null) {
					// If we failed to promote lets try to refresh the coordinator node info
					DbCache.removeObject(coordinatorNode.getObjectId(), coordinatorNode.getObjectType());
					Object nextMaintenance = coordinatorNode.getVal(NEXT_MAINTENANCE);
					coordinatorNode = null;
					log4j.warn("Coordinator record is invalid. Next maintenance in the past: "
							+ nextMaintenance.toString());
				}
			}
		}
		return coordinatorNode != null;
	}

	/**
	 * Method to perform resigning of coordinator
	 * 
	 * @param svc A reference to an SvCore (the read/write of the nodes has to be in
	 *            the same transaction)
	 * @return True if the current has resigned from the coordinator role
	 * @throws SvException any underlying exception (except object not updateable,
	 *                     which would mean someone else got promoted in meantime)
	 */
	static boolean resignCoordinator() throws SvException {

		boolean success = false;
		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {
			if (isCoordinator) {
				svr.isInternal = true;
				svw.isInternal = true;

				DbCache.removeObject(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER);
				coordinatorNode = svr.getObjectById(svCONST.CLUSTER_COORDINATOR_ID, svCONST.OBJECT_TYPE_CLUSTER, null);
				coordinatorNode.setVal(LAST_MAINTENANCE, DateTime.now());
				coordinatorNode.setVal(NEXT_MAINTENANCE, DateTime.now());
				coordinatorNode.setVal(PART_TIME, DateTime.now());
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

		}
		return success;
	}

	/**
	 * Method to ensure the cluster clients have started and connected to the hear
	 * beat address/port of the coordinator
	 * 
	 * @param hbAddress String list of available heartbeat end points
	 * @return True if the client threads have started and connected with success
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
			log4j.info("Svarog Cluster Servers initialisation failed. Initiating Cluster Server shutdown.");
			shutdown();
		}
		return initHb && initNotif && isCoordinator;
	}

	/**
	 * Method to initialise the Svarog Cluster. This method shall try to locate a
	 * valid coordinator node, or become one if there isn't any. If it becomes a
	 * coordinator then it will start the heart beat listener and the notifier proxy
	 * servers. If the current node doesn't manage to become a coordinator then it
	 * will start a Heart Beat client and notifier client and try to connect to the
	 * coordinator
	 * 
	 * @return True if the cluster was properly initialised.
	 */
	static boolean initCluster() {
		if (!isRunning.compareAndSet(false, true)) {
			log4j.error("Cluster is already initialised and active. Shutdown first");
			return false;
		} else
			log4j.info("SvCluster is starting");
		isActive.set(false);

		isCoordinator = false;
		try (SvReader svr = new SvReader();) {

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
			if (!isActive.get()) {
				log4j.info(
						"Svarog Cluster failed to start as Server and register as Cluster Coordinator. Starting Cluster Client :"
								+ Boolean.toString(autoStartClient));
				if (autoStartClient)
					isActive.set(startClients(hbAddress));
			}

		} catch (Exception e) {
			log4j.error("Svarog cluster could not be initialised. Cluster not running!", e);
			shutdown();
		}
		// if start was successful execute the startup hooks in nonblocking thread
		if (isActive.get()) {
			String startUpExec = (String) SvConf.getParam(SVAROG_STARTUP_HOOK_PROP);
			if (startUpExec != null && !startUpExec.isEmpty()) {
				execSvarogHooks(startUpExec, "start-up");
			}
		}
		return isActive.get();
	}

	/**
	 * Method to check if the cluster services are running correctly
	 */
	public static void checkCluster() {
		// TODO add sanity check of the threads
	}

	/**
	 * Method to send data via ZMQ socket and handle termination errors
	 * 
	 * @throws SvException
	 */
	public static byte[] zmqRecv(ZMQ.Socket socket, int timeOut) throws SvException {
		byte[] result = null;
		try {
			result = socket.recv(timeOut);
		} catch (ZMQException e) {
			if (e.getErrorCode() == zmq.ZError.ETERM)
				throw (new SvException(Sv.Exceptions.CLUSTER_INACTIVE, svCONST.systemUser, e));
			else
				throw (new SvException(Sv.Exceptions.CLUSTER_COMMUNICATION_ERROR, svCONST.systemUser, e));
		}
		return result;
	}

	/**
	 * Method to send data via ZMQ socket and handle termination errors
	 * 
	 * @throws SvException
	 */
	public static boolean zmqSend(ZMQ.Socket socket, byte[] data, int socketFlags) throws SvException {
		boolean result = false;
		try {
			result = socket.send(data, socketFlags);
			if (!result)
				log4j.error("Error sending message:" + Arrays.toString(data));
		} catch (ZMQException e) {
			if (e.getErrorCode() == zmq.ZError.ETERM)
				throw (new SvException(Sv.Exceptions.CLUSTER_INACTIVE, svCONST.systemUser, e));
			else
				throw (new SvException(Sv.Exceptions.CLUSTER_COMMUNICATION_ERROR, svCONST.systemUser, null, data, e));
		}
		return result;
	}

	/**
	 * Method to update a distributed lock. This is invoked when the Notifier client
	 * is processing lock acknowledgments and needs to update the list of
	 * distributed locks by the SvLock on the coordinator node.
	 * 
	 * @param lockHash The hash of the lock
	 * @param lockKey  The key of the lock
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
	 * Method to shutdown the Svarog cluster. The cluster shutdown will in turn: 1.
	 * Check the current node is a cluster coordinator. 2. If we are a cluster
	 * coordinator then we shall shutdown the SvClusterServer and
	 * SvClusterNotifierProxy 3. If we are not a coordinator then we shall shutdown
	 * the SvClusterClient and SvClusterNotifierClient
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
	 * Join the heart beat and notifier threads so we are sure that the clients or
	 * servers have stopped
	 */
	static void joinDaemonThreads() {
		// just check if the join isn't invoked from the heartbeat or notifier.
		Thread joinHb = (!Thread.currentThread().equals(heartBeatThread) ? heartBeatThread : null);
		Thread joinNf = (!Thread.currentThread().equals(notifierThread) ? notifierThread : null);

		while ((joinHb != null && joinHb.isAlive()) || (joinNf != null && joinNf.isAlive())) {
			try {
				if (joinHb != null && joinHb.isAlive())
					joinHb.join(1);
				if (joinNf != null && joinNf.isAlive())
					joinNf.join(1);
				synchronized (SvMaintenance.maintenanceSemaphore) {
					SvMaintenance.maintenanceSemaphore.wait(100);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				log4j.info("Waiting on the maintenance thread got interrupted. Retry to join  daemon threads", e);
			}

		}

	}

	/**
	 * Method to shutdown the Svarog cluster. The cluster shutdown will in turn: 1.
	 * Check the current node is a cluster coordinator. 2. If we are a cluster
	 * coordinator then we shall shutdown the SvClusterServer and
	 * SvClusterNotifierProxy 3. If we are not a coordinator then we shall shutdown
	 * the SvClusterClient and SvClusterNotifierClient
	 */
	static void shutdown(boolean doMaintenance) {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.debug("Svarog Cluster not running. Can't shutdown");
			return;
		} else
			log4j.info("SvCluster is stopping");

		if (doMaintenance)
			SvMaintenance.performMaintenance();
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
		// store the previous state before we mark the cluster as inactive
		boolean shutdownExecutors = isActive.get();
		isActive.set(false);
		// notify interested parties that we shut down
		synchronized (SvCluster.isRunning) {
			isRunning.notifyAll();
		}
		if (shutdownExecutors) {
			String shutDownExec = (String) SvConf.getParam(SVAROG_SHUTDOWN_HOOK_PROP);
			if (shutDownExec != null && !shutDownExec.isEmpty()) {
				execSvarogHooks(shutDownExec, "shut-down");
			}
		}

	}

	/**
	 * Method to execute list of start up or shut down executors loaded from
	 * svarog.properties.
	 * 
	 * @param shutDownExec List of key of svarog executors. Semicolon is the list
	 *                     separator
	 */
	private static void execSvarogHooks(String hooksExec, String operation) {
		String[] list = hooksExec.trim().split(";");
		try (SvExecManager sve = new SvExecManager();) {
			for (int i = 0; i < list.length; i++) {
				try {
					sve.execute(list[i].toUpperCase(), null, new DateTime());
					log4j.info("Executed " + operation + " executor: " + list[i]);
				} catch (Exception e) {
					log4j.info("Could not execute " + operation + " executor: " + list[i], e);
				}
			}
		} catch (SvException e) {
			log4j.info("Error executing Svarog " + operation + " hooks", e);
		}
	}

	/**
	 * Method to release a distributed lock. This is invoked when the clusterServer
	 * processes releaseLock message. This method is also invoked by the SvLock on
	 * the coordinator node.
	 * 
	 * @param lockHash         The hash of the lock
	 * @param nodeId           The node id which acquires the lock
	 * @param nodeLocks        The map of nodes which contains locks held by node
	 * @param distributedLocks The map of distributed nodes in the cluster to be
	 *                         used for releasing the lock
	 * @return Null if the lock was NOT released, otherwise the lock key.
	 */
	static String releaseDistributedLock(Integer lockHash, long nodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks,
			LoadingCache<String, ReentrantLock> sysLocks, boolean isLocal) {
		ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks = isLocal
				? SvClusterClient.localDistributedLocks
				: SvClusterServer.distributedLocks;
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
	 * Method to acquire a distributed lock from the svarog cluster (this is server
	 * agnostic). It is called also by SvLock in order to synchronize properly the
	 * distributed locks
	 * 
	 * @param lockKey      The lock key which should be locked.
	 * @param nodeId       The id of the node which shall acquire the lock
	 * @param extendedInfo The id of the node which already holds the lock
	 *                     (available only if the lock fails)
	 * @param nodeLocks    The map of nodes which contains locks held by node
	 * @param sysLocks     The cache containing all system locks
	 * @return Instance of re-entrant lock if the lock was acquired. Otherwise null.
	 *         If null the extendedInfo is populated with the node holding the lock
	 */

	static ReentrantLock acquireDistributedLock(String lockKey, Long nodeId, Long[] extendedInfo,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks,
			LoadingCache<String, ReentrantLock> sysLocks, boolean isLocal) {

		ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks = isLocal
				? SvClusterClient.localDistributedLocks
				: SvClusterServer.distributedLocks;

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
	 * @param nodeId The node for which the distributed locks shall be cleaned
	 */
	static void clusterCleanUp(Long nodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks, boolean isLocal) {
		ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks = isLocal
				? SvClusterClient.localDistributedLocks
				: SvClusterServer.distributedLocks;

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

	/**
	 * Method to migrate locks from one node to another. This is useful if the node
	 * has lost a heart beat, then re-joined with a new node ID. In this case we'll
	 * just migrate the locks from the old node id to the new one
	 * 
	 * @param nodeId    The node under which the locks shall be moved
	 * @param oldNodeId The node from which the locks will be moved
	 * @param nodeLocks The map fo locks which shall be used (Server or Client side)
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
	 * Method to migrate locks from one node to another. This is useful if the node
	 * has lost a heart beat, then re-joined with a new node ID. In this case we'll
	 * just migrate the locks from the old node id to the new one
	 * 
	 * @param nodeId    The node under which the locks shall be moved
	 * @param oldNodeId The node from which the locks will be moved
	 * @param nodeLocks The map fo locks which shall be used (Server or Client side)
	 * @throws SvException
	 */
	static void copyLocalLocks(Long nodeId, Long oldNodeId,
			ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> localNodeLocks)
			throws SvException {
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

	public static AtomicBoolean isRunning() {
		return isRunning;
	}

	public static AtomicBoolean getIsActive() {
		if (isCoordinator)
			return isActive;
		else
			return SvClusterClient.isActive;
	}

	public static DbDataObject getCoordinatorNode() {
		return coordinatorNode;
	}

	public static void setCoordinatorNode(DbDataObject coordinatorNode) {
		SvCluster.coordinatorNode = coordinatorNode;
	}

}
