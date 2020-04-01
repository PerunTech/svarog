package com.prtech.svarog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.prtech.svarog.SvCluster.DistributedLock;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DboFactory;

/**
 * Class to implement the server side services of the Svarog Cluster. The class
 * shall wrap a ZeroMQ response socket which will respond to different types of
 * messages
 * 
 * @author ristepejov
 *
 */
public class SvClusterServer implements Runnable {
	static int clusterMainentanceInterval = SvConf.getClusterMaintenanceInterval();
	/**
	 * Map of distributed locks
	 */
	static final ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks = new ConcurrentHashMap<String, SvCluster.DistributedLock>();
	/**
	 * Map of locks grouped by node. If a node dies, we remove all locks
	 */
	static final ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> nodeLocks = new ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>>();

	/**
	 * Map holding the last heart beat of each node in the cluster
	 */
	static final ConcurrentHashMap<Long, DateTime> nodeHeartBeats = new ConcurrentHashMap<Long, DateTime>();

	/**
	 * Map holding the last heart beat of each node in the cluster
	 */
	static final ConcurrentHashMap<Long, DateTime> missedHeartBeats = new ConcurrentHashMap<Long, DateTime>();

	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvClusterServer.class);

	/**
	 * The ZMQ context which is used for creating the socket.
	 */
	static private ZContext context = null;
	static ZMQ.Socket hbServerSock = null;

	/**
	 * Timestamp of the last contact with the coordinator
	 */
	static int heartBeatTimeOut = SvConf.getHeartBeatTimeOut();

	static DateTime lastGCTime = DateTime.now();

	static AtomicBoolean isRunning = new AtomicBoolean(false);

	static boolean initServer() {
		if (isRunning.get()) {
			log4j.error("Heartbeat thread is already running. Shutdown first");
			return false;
		}
		if (context == null)
			context = new ZContext();
		nodeHeartBeats.clear();
		// Socket to talk to clients
		hbServerSock = null;
		ZMQ.Socket tmpSock = context.createSocket(SocketType.REP);
		try {
			if (tmpSock.bind("tcp://*:" + SvConf.getHeartBeatPort()))
				hbServerSock = tmpSock;
		} catch (Exception e) {
			log4j.error("The node can't bind socket on port range:" + SvConf.getHeartBeatPort(), e);
		}
		if (hbServerSock == null)
			log4j.info("Heartbeat socket bind on port:" + SvConf.getHeartBeatPort());
		else
			hbServerSock.setReceiveTimeOut(SvCluster.SOCKET_RECV_TIMEOUT);
		// start the internal hearbeat client
		return (hbServerSock != null);
	}

	static public void shutdown() {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.error("Heartbeat thread is not running. Run the heartbeat thread first");
			return;
		}
		try {
			// do sleep until socket is able to close within timeout
			Thread.sleep(SvCluster.SOCKET_RECV_TIMEOUT);
		} catch (InterruptedException e) {
			log4j.error("Shutdown interrupted", e);
		}
		if (context != null) {
			context.close();
			context = null;
		}
		// notify the maintenance thread
		if (SvMaintenance.maintenanceThread != null)
			synchronized (SvMaintenance.maintenanceThread) {
				SvMaintenance.maintenanceThread.notifyAll();
			}

	}

	/**
	 * Method to handle Authentication messages between the cluster nodes and
	 * the coordinator
	 * 
	 * @param msgType
	 *            The received message type (TOKEN GET/PUT/SET)
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private ByteBuffer processAuthentication(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = null;
		switch (msgType) {
		case SvCluster.MSG_AUTH_TOKEN_GET:
			long mostSigBits = msgBuffer.getLong();
			long leastSigBits = msgBuffer.getLong();
			UUID uuid = new UUID(mostSigBits, leastSigBits);
			DbDataObject svToken = DbCache.getObject(uuid.toString(), svCONST.OBJECT_TYPE_SECURITY_LOG);
			byte[] token = null;
			if (svToken != null) {
				token = svToken.toSimpleJson().toString().getBytes(ZMQ.CHARSET);
			}
			// allocate one byte for message type, one long for node Id and the
			// rest for the token
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES + (token != null ? token.length : 0));
			if (token != null) {
				respBuffer.put(SvCluster.MSG_SUCCESS);
				respBuffer.putLong(nodeId);
				respBuffer.put(token);
			} else
				respBuffer.put(SvCluster.MSG_FAIL);
			break;
		case SvCluster.MSG_AUTH_TOKEN_PUT:
			String strToken = new String(
					Arrays.copyOfRange(msgBuffer.array(), 1 + Long.BYTES, msgBuffer.array().length), ZMQ.CHARSET);
			DbDataObject svTokenIn = new DbDataObject();
			Gson g = new Gson();
			svTokenIn.fromSimpleJson(g.fromJson(strToken, JsonObject.class));
			svTokenIn.setIsDirty(false);
			DboFactory.makeDboReadOnly(svTokenIn);
			DbCache.addObject(svTokenIn, (String) svTokenIn.getVal("session_id"));
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
			respBuffer.put(SvCluster.MSG_SUCCESS);
			respBuffer.putLong(nodeId);
			break;
		case SvCluster.MSG_AUTH_TOKEN_SET:
			long mostSBits = msgBuffer.getLong();
			long leastSBits = msgBuffer.getLong();
			UUID uuidIn = new UUID(mostSBits, leastSBits);
			DbDataObject tmpToken = DbCache.getObject(uuidIn.toString(), svCONST.OBJECT_TYPE_SECURITY_LOG);
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
			if (tmpToken != null)
				respBuffer.put(SvCluster.MSG_SUCCESS);
			else
				respBuffer.put(SvCluster.MSG_FAIL);
			respBuffer.putLong(nodeId);
			break;
		}

		return respBuffer;
	}

	/**
	 * Method to handle JOIN messages between the cluster nodes and the
	 * coordinator
	 * 
	 * @param msgType
	 *            The received message type (join or part)
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private List<ByteBuffer> processJoin(long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		List<ByteBuffer> response = new ArrayList<ByteBuffer>();
		String nodeInfo = new String(Arrays.copyOfRange(msgBuffer.array(), 1 + Long.BYTES, msgBuffer.array().length),
				ZMQ.CHARSET);
		SvWriter svw = null;
		DbDataObject node = null;
		synchronized (SvClusterServer.isRunning) {
			try {
				node = SvCluster.getCurrentNodeInfo();
				node.setVal("node_info", nodeInfo);
				Gson g = new Gson();
				JsonObject j = g.fromJson(nodeInfo, JsonObject.class);
				String remoteIp = j.has("ip") ? j.get("ip").getAsString() : "0";
				node.setVal("local_ip", remoteIp);
				svw = new SvWriter();
				svw.saveObject(node, true);
				nodeHeartBeats.put(node.getObjectId(), DateTime.now());
			} catch (SvException e) {
				log4j.error("Error registering new node in db!", e);
			} finally {
				if (svw != null)
					svw.release();
			}
		}

		if (node != null && node.getObjectId() > 0L) {
			// migrate server locks from old ID to new node id
			SvCluster.migrateLocks(node.getObjectId(), nodeId, nodeLocks);
			// promote local local locks to the server and confirm them

			respBuffer.put(SvCluster.MSG_SUCCESS);
			respBuffer.putLong(node.getObjectId());
			response.add(respBuffer);

			for (Entry<String, DistributedLock> entry : SvClusterServer.distributedLocks.entrySet()) {
				if (!entry.getValue().nodeId.equals(node.getObjectId())) {
					byte[] key = null;
					key = entry.getKey().getBytes(ZMQ.CHARSET);
					// allocate one byte for message type, one long for node Id
					// and the rest for the token
					respBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + (key != null ? key.length : 0));
					respBuffer.putLong(entry.getValue().nodeId);
					respBuffer.putInt(entry.getValue().lockHash);
					respBuffer.put(key);
					response.add(respBuffer);
				}
			}
		} else {
			respBuffer.put(SvCluster.MSG_FAIL);
			respBuffer.putLong(0L);
			response.add(respBuffer);
		}
		return response;
	}

	/**
	 * Method to handle JOIN/PART messages between the cluster nodes and the
	 * coordinator
	 * 
	 * @param msgType
	 *            The received message type (join or part)
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private ByteBuffer processPart(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		{
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
			respBuffer.put(SvCluster.MSG_SUCCESS);
			respBuffer.putLong(nodeId);
			nodeHeartBeats.remove(nodeId);
			clusterCleanUp(nodeId);
			if (SvMaintenance.maintenanceThread != null && !SvMaintenance.getMaintenanceInProgress().get())
				if (SvarogDaemon.osgiFramework != null)
					SvMaintenance.maintenanceThread.interrupt();
				else
					synchronized (SvMaintenance.maintenanceThread) {
						SvMaintenance.maintenanceThread.notifyAll();
					}
		}
		return respBuffer;
	}

	/**
	 * Method to process a standard heart beat message from a node.
	 * 
	 * @param msgType
	 *            The type of message (in this case only HEARTBEAT)
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private ByteBuffer processHeartBeat(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		if (nodeHeartBeats.containsKey(nodeId)) {
			respBuffer.put(SvCluster.MSG_SUCCESS);
			nodeHeartBeats.put(nodeId, DateTime.now());
		} else
			respBuffer.put(SvCluster.MSG_FAIL);
		respBuffer.putLong(nodeId);
		return respBuffer;
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
	 * 
	 * @return The lock key if the lock was released with success, otherwise
	 *         null
	 */
	static String releaseDistributedLock(Integer lockHash, Long nodeId) {
		return SvCluster.releaseDistributedLock(lockHash, nodeId, nodeLocks, distributedLocks, null);

	}

	/**
	 * Method to process a standard heart beat message from a node.
	 * 
	 * @param msgType
	 *            The type of message (in this case only LOCK_RELEASE)
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private ByteBuffer processReleaseLock(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		Integer lockHash = msgBuffer.getInt();

		// unlock the lock using the server lists of locks
		String lockKey = SvCluster.releaseDistributedLock(lockHash, nodeId, nodeLocks, distributedLocks, null);
		respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		if (lockKey != null) {
			respBuffer.put(SvCluster.MSG_SUCCESS);
			// send a broadcast that the lock was acquired
			SvClusterNotifierProxy.publishLockAction(SvCluster.NOTE_LOCK_RELEASED, lockHash, nodeId, lockKey);
		} else
			respBuffer.put(SvCluster.MSG_FAIL);

		respBuffer.putLong(nodeId);
		return respBuffer;
	}

	/**
	 * Method to get a distributed lock from the Cluster server
	 * 
	 * @param lockKey
	 *            The string identifier of the lock
	 * @return the hash code of the lock
	 */
	static int getDistributedLock(String lockKey, Long nodeId) {
		Long[] extendedInfo = new Long[1];
		ReentrantLock lck = SvCluster.acquireDistributedLock(lockKey, nodeId, extendedInfo, nodeLocks, distributedLocks,
				null);
		if (lck == null && log4j.isDebugEnabled())
			log4j.debug("Lock not acquired! Lock is held by node:" + Long.toString(extendedInfo[0]));
		return lck != null ? lck.hashCode() : 0;
	}

	/**
	 * Method to process a lock request message from a node.
	 * 
	 * @param msgType
	 *            The type of message (in this case only lock)
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private ByteBuffer processLock(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		String lockKey = new String(Arrays.copyOfRange(msgBuffer.array(), 1 + Long.BYTES, msgBuffer.array().length),
				ZMQ.CHARSET);
		Long[] extNodeInfo = new Long[1];

		// acquire the lock using the server side lists
		ReentrantLock lock = SvCluster.acquireDistributedLock(lockKey, nodeId, extNodeInfo, nodeLocks, distributedLocks,
				null);

		respBuffer = ByteBuffer.allocate(1 + Long.BYTES + (lock == null ? Long.BYTES : Integer.BYTES));
		if (lock == null)
			respBuffer.put(SvCluster.MSG_FAIL);
		else {
			byte lockResult = SvCluster.MSG_SUCCESS;
			Set<Long> nodes = null;
			nodes = new HashSet<Long>();
			nodes.addAll(nodeHeartBeats.keySet());
			SvClusterNotifierProxy.nodeAcks.put(lock.hashCode(), nodes);
			SvClusterNotifierProxy.publishLockAction(SvCluster.NOTE_LOCK_ACQUIRED, lock.hashCode(), nodeId, lockKey);
			if (!SvClusterNotifierProxy.waitForAck(nodes, lock.hashCode(), heartBeatTimeOut))
				lockResult = SvCluster.MSG_FAIL;

			respBuffer.put(lockResult);
		}
		// add the node id
		respBuffer.putLong(nodeId);

		if (lock == null) // if the lock has failed add the node which holds it
			respBuffer.putLong(extNodeInfo[0]);
		else {
			respBuffer.putInt(lock.hashCode());
			// send a broadcast that the lock was acquired
			// SvClusterNotifierProxy.publishLockAction(SvCluster.NOTE_LOCK_ACQUIRED,
			// lock.hashCode(), nodeId, lockKey);
		}

		return respBuffer;
	}

	/**
	 * Method to process cluster wide messages
	 * 
	 * @param msgType
	 *            The type of message
	 * @param nodeId
	 *            The node which sent the message
	 * @param msgBuffer
	 *            The rest of the message buffer
	 * @return A response message buffer
	 */
	private byte[] processMessage(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = null;
		if (log4j.isDebugEnabled())
			log4j.debug("Received message " + Integer.toString(msgType) + " from node " + nodeId);
		// if the node is not in the cluster and the message isn't join, reject
		// the message by responding FAIL
		if (!nodeHeartBeats.containsKey(nodeId)) {
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
			respBuffer.put(SvCluster.MSG_FAIL);
			respBuffer.putLong(nodeId);
		} else
			switch (msgType) {
			case SvCluster.MSG_HEARTBEAT:
				respBuffer = processHeartBeat(msgType, nodeId, msgBuffer);
				break;
			case SvCluster.MSG_PART:
				respBuffer = processPart(msgType, nodeId, msgBuffer);
				break;
			case SvCluster.MSG_AUTH_TOKEN_GET:
			case SvCluster.MSG_AUTH_TOKEN_PUT:
			case SvCluster.MSG_AUTH_TOKEN_SET:
				respBuffer = processAuthentication(msgType, nodeId, msgBuffer);
				break;
			case SvCluster.MSG_LOCK:
				respBuffer = processLock(msgType, nodeId, msgBuffer);
				break;
			case SvCluster.MSG_LOCK_RELEASE:
				respBuffer = processReleaseLock(msgType, nodeId, msgBuffer);
				break;
			default:
				respBuffer = ByteBuffer.allocate(1);
				respBuffer.put(SvCluster.MSG_UNKNOWN);
			}
		if (log4j.isDebugEnabled())
			log4j.debug("Response to node " + nodeId + " was " + respBuffer.get(0));
		return respBuffer.array();

	}

	private static void promoteLocalLocks() {

		SvClusterServer.distributedLocks.putAll(SvClusterClient.localDistributedLocks);
		if (SvClusterClient.localDistributedLocks.size() > 0) {
			if (log4j.isDebugEnabled())
				log4j.debug("Promoting locks from the local worker cache. Total locks count:"
						+ SvClusterClient.localDistributedLocks.size());

			for (Entry<Long, CopyOnWriteArrayList<DistributedLock>> entry : SvClusterClient.localNodeLocks.entrySet()) {
				CopyOnWriteArrayList<DistributedLock> myLocks = entry.getValue();
				if (myLocks != null)
					for (DistributedLock d : myLocks) {
						int lockHash = SvClusterServer.getDistributedLock(d.key, d.nodeId);
						SvClusterServer.updateDistributedLock(d.key, d.lockHash);
					}
			}
		}

	}

	@Override
	public void run() {

		if (!isRunning.compareAndSet(false, true)) {
			log4j.error("Heartbeat thread is already running. Shutdown first");
			return;
		}
		// TODO Auto-generated method stub
		// lets try to bind our heart beat socket.
		if (hbServerSock == null) {
			log4j.error("Heartbeat socket not available, ensure proper initialisation");
			return;
		}
		// make sure we promote any locks if we got promoted from Worker to
		// Coordinator
		promoteLocalLocks();

		lastGCTime = DateTime.now();
		log4j.info("Heartbeat server started");
		while (isRunning.get()) {
			byte[] msg = hbServerSock.recv(0);
			if (msg != null) {
				ByteBuffer msgBuffer = ByteBuffer.wrap(msg);
				byte msgType = msgBuffer.get();
				long nodeId = msgBuffer.getLong();
				int sockFlag;
				if (msgType == SvCluster.MSG_JOIN) {
					Iterator<ByteBuffer> iterator = processJoin(nodeId, msgBuffer).iterator();
					sockFlag = ZMQ.SNDMORE;
					while (iterator.hasNext()) {
						ByteBuffer b = iterator.next();

						if (!iterator.hasNext())
							sockFlag = 0;
						if (!hbServerSock.send(b.array(), sockFlag))
							log4j.error("Error sending message to node:" + Long.toString(nodeId));
					}
				} else {
					byte[] response = processMessage(msgType, nodeId, msgBuffer);
					if (!hbServerSock.send(response))
						log4j.error("Error sending message to node:" + Long.toString(nodeId));
				}
			} else
				clusterMaintenance();

		}
		// make sure we clear the locks on shutdown
		clearDistributedLocks();
		log4j.info("Heartbeat server shut down");
		// the result of the binding is the status

	}

	/**
	 * Method to clear all disributed locks and unlock the object in SvLocks
	 */
	private void clearDistributedLocks() {
		for (Entry<String, DistributedLock> entry : SvClusterServer.distributedLocks.entrySet())
			SvLock.releaseLock(entry.getValue().key, entry.getValue().lock);

		SvClusterServer.nodeLocks.clear();
		SvClusterServer.distributedLocks.clear();

	}

	/**
	 * Method to perform maintenance of the cluster list. The map with
	 * heartbeats from the nodes of the cluster is evaluated to check if any of
	 * the nodes hasn't sent a heart beat within the Heart Beat Timeout
	 * interval. The nodes which haven't sent a heartbeat within the timeout,
	 * are removed from the cluster and their locks are removed
	 */
	private void clusterMaintenance() {
		DateTime tsTimeout = lastGCTime.withDurationAdded(heartBeatTimeOut, 1);
		if (tsTimeout.isBeforeNow()) {
			synchronized (isRunning) {
				if (log4j.isDebugEnabled())
					log4j.debug(
							"Performing cluster cleanup. Node must have sent a heartbeat after" + tsTimeout.toString());

				for (Entry<Long, DateTime> hbs : nodeHeartBeats.entrySet()) {
					if (log4j.isDebugEnabled())
						log4j.debug("Last beat from " + hbs.getKey().toString() + " was" + hbs.getValue().toString());
					if (hbs.getValue().withDurationAdded(heartBeatTimeOut, 1).isBefore(tsTimeout)) {
						nodeHeartBeats.remove(hbs.getKey(), hbs.getValue());
						missedHeartBeats.put(hbs.getKey(), hbs.getValue());
						if (log4j.isDebugEnabled())
							log4j.debug("Removing node " + hbs.toString() + " no heart beat was sent in "
									+ heartBeatTimeOut + " miliseconds.");
					}

				}
				for (Entry<Long, DateTime> missHbs : missedHeartBeats.entrySet()) {
					if (missHbs.getValue().plusSeconds(clusterMainentanceInterval).isBeforeNow()) {
						if (log4j.isDebugEnabled())
							log4j.debug(
									"Node didn't respond within cluster maintenance interval. Locks cleanup is performed for node"
											+ missHbs.getKey().toString() + " was" + missHbs.getValue().toString());
						clusterCleanUp(missHbs.getKey());
						missedHeartBeats.remove(missHbs.getKey(), missHbs.getValue());
					}
				}

			}
			lastGCTime = DateTime.now();
		}
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
	static boolean updateDistributedLock(String lockKey, Integer lockHash) {
		return SvCluster.updateDistributedLock(lockKey, lockHash, distributedLocks);
	}

	/**
	 * Method to clean up the distributed locks acquired by a node
	 * 
	 * @param nodeId
	 *            The node for which the distributed locks shall be cleaned
	 */
	static void clusterCleanUp(Long nodeId) {
		SvCluster.clusterCleanUp(nodeId, SvClusterServer.nodeLocks, SvClusterServer.distributedLocks);
	}

}
