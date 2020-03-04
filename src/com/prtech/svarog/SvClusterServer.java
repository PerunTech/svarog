package com.prtech.svarog;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map.Entry;
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

	/**
	 * Class to wrap the standard locks in order to support the distributed
	 * locking of the svarog cluster. The key of the lock as well as the node
	 * which holds the lock are the needed information
	 * 
	 * @author ristepejov
	 *
	 */
	class DistributedLock {
		public String key;
		public Long nodeId;
		public ReentrantLock lock;

		DistributedLock(String key, Long nodeId, ReentrantLock lock) {
			this.nodeId = nodeId;
			this.lock = lock;
			this.key = key;
		}
	}

	/**
	 * Map of distributed locks
	 */
	static final ConcurrentHashMap<String, DistributedLock> distributedLocks = new ConcurrentHashMap<String, DistributedLock>();
	/**
	 * Map of locks grouped by node. If a node dies, we remove all locks
	 */
	static final ConcurrentHashMap<Long, CopyOnWriteArrayList<DistributedLock>> nodeLocks = new ConcurrentHashMap<Long, CopyOnWriteArrayList<DistributedLock>>();

	/**
	 * Map holding the last heart beat of each node in the cluster
	 */
	static final ConcurrentHashMap<Long, DateTime> nodeHeartBeats = new ConcurrentHashMap<Long, DateTime>();

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
			hbServerSock.setReceiveTimeOut(SvCluster.sockeReceiveTimeout);
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
			Thread.sleep(SvCluster.sockeReceiveTimeout);
		} catch (InterruptedException e) {
			log4j.error("Shutdown interrupted", e);
		}
		if (context != null) {
			context.close();
			context = null;
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
	private ByteBuffer processJoinPart(byte msgType, long nodeId, ByteBuffer msgBuffer) {
		ByteBuffer respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		if (SvCluster.MSG_JOIN == msgType) {

			String nodeInfo = new String(Arrays.copyOfRange(msgBuffer.array(), 1, msgBuffer.array().length),
					ZMQ.CHARSET);
			SvWriter svw = null;
			DbDataObject node = null;
			synchronized (SvCluster.isRunning) {
				try {
					node = SvCluster.getCurrentNodeInfo();
					node.setVal("node_info", nodeInfo);
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
				respBuffer.put(SvCluster.MSG_SUCCESS);
				respBuffer.putLong(node.getObjectId());
			} else {
				respBuffer.put(SvCluster.MSG_FAIL);
				respBuffer.putLong(0L);
			}
		} else {
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
			respBuffer.put(SvCluster.MSG_SUCCESS);
			respBuffer.putLong(nodeId);
			clusterCleanUp(nodeId);
			if (!SvCluster.maintenanceInProgress.get())
				SvCluster.maintenanceThread.interrupt();
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
	 * @return True if the log was acquired.
	 */
	boolean releaseDistributedLock(Integer lockHash, long nodeId) {
		boolean lockReleased = false;
		CopyOnWriteArrayList<DistributedLock> nodeLock = nodeLocks.get(nodeId);
		DistributedLock currentLock = null;
		if (nodeLock != null) {
			for (DistributedLock dLock : nodeLock)
				if (lockHash.equals(dLock.lock.hashCode())) {
					currentLock = dLock;
					break;
				}
		}
		if (currentLock != null) {
			lockReleased = SvLock.releaseLock(currentLock.key, currentLock.lock);
			if (currentLock.lock.getHoldCount() == 0) {
				synchronized (distributedLocks) {
					distributedLocks.remove(currentLock.key, currentLock);
				}
				nodeLock.remove(currentLock);
			}
		} else
			log4j.error("Lock with hash " + lockHash.toString() + ", does not exist in the list of distributed locks");
		return lockReleased;
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

		boolean lockReleased = releaseDistributedLock(lockHash, nodeId);
		respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
		if (lockReleased)
			respBuffer.put(SvCluster.MSG_SUCCESS);
		else
			respBuffer.put(SvCluster.MSG_FAIL);

		respBuffer.putLong(nodeId);
		return respBuffer;
	}

	/**
	 * Method to release a lock on the Cluster server
	 * 
	 * @param lockHash
	 *            The hash identifier of the lock
	 * @return True if the release was successful
	 */
	boolean releaseDistributedLock(int lockHash) {
		return releaseDistributedLock(lockHash, SvCluster.currentNode.getObjectId());
	}

	/**
	 * Method to get a distributed lock from the Cluster server
	 * 
	 * @param lockKey
	 *            The string identifier of the lock
	 * @return the hash code of the lock
	 */
	int getDistributedLock(String lockKey) {
		Long[] extendedInfo = new Long[1];
		ReentrantLock lck = acquireDistributedLock(lockKey, SvCluster.currentNode.getObjectId(), extendedInfo);
		if (lck == null && log4j.isDebugEnabled())
			log4j.debug("Lock not acquired! Lock is held by node:" + Long.toString(extendedInfo[0]));
		return lck != null ? lck.hashCode() : 0;
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
	 * @return Instance of re-entrant lock if the lock was acquired. Otherwise
	 *         null. If null the extendedInfo is populated with the node holding
	 *         the lock
	 */

	ReentrantLock acquireDistributedLock(String lockKey, Long nodeId, Long[] extendedInfo) {
		ReentrantLock lock = null;
		DistributedLock dlock = null;
		synchronized (distributedLocks) {
			dlock = distributedLocks.get(lockKey);
			if (dlock != null && dlock.nodeId.equals(nodeId)) {
				// same node is locking the re-entrant lock again
				lock = SvLock.getLock(lockKey, false, 0L);
			} else {
				if (dlock == null) // brand new lock
				{
					lock = SvLock.getLock(lockKey, false, 0L);
					if (lock != null) {
						dlock = new DistributedLock(lockKey, nodeId, lock);
						distributedLocks.put(lockKey, dlock);
					}
				} else {
					if (extendedInfo.length > 0)
						extendedInfo[0] = dlock.nodeId;
				}
			}
		}
		if (lock != null) {
			CopyOnWriteArrayList<DistributedLock> currentNode = new CopyOnWriteArrayList<DistributedLock>();
			CopyOnWriteArrayList<DistributedLock> oldNode = nodeLocks.putIfAbsent(nodeId, currentNode);
			if (oldNode != null)
				currentNode = oldNode;
			currentNode.addIfAbsent(dlock);
		}
		return lock;

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
		ReentrantLock lock = acquireDistributedLock(lockKey, nodeId, extNodeInfo);

		respBuffer = ByteBuffer.allocate(1 + Long.BYTES + (lock == null ? Long.BYTES : Integer.BYTES));
		if (lock == null)
			respBuffer.put(SvCluster.MSG_FAIL);
		else
			respBuffer.put(SvCluster.MSG_SUCCESS);
		// add the node id
		respBuffer.putLong(nodeId);

		if (lock == null) // if the lock has failed add the node which holds it
			respBuffer.putLong(extNodeInfo[0]);
		else
			respBuffer.putInt(lock.hashCode());
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
		// if the node is not in the cluster and the message isn't join, reject
		// the message by responding FAIL
		if (!nodeHeartBeats.containsKey(nodeId) && msgType != SvCluster.MSG_JOIN) {
			respBuffer = ByteBuffer.allocate(1 + Long.BYTES);
			respBuffer.put(SvCluster.MSG_FAIL);
			respBuffer.putLong(nodeId);
		}
		switch (msgType) {
		case SvCluster.MSG_HEARTBEAT:
			respBuffer = processHeartBeat(msgType, nodeId, msgBuffer);
			break;
		case SvCluster.MSG_JOIN:
		case SvCluster.MSG_PART:
			respBuffer = processJoinPart(msgType, nodeId, msgBuffer);
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
			log4j.debug("Received message " + Integer.toString(msgType) + " from node " + nodeId
					+ " and response message was " + respBuffer.get(0));

		return respBuffer.array();

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
		lastGCTime = DateTime.now();
		log4j.info("Heartbeat server started");
		while (isRunning.get()) {
			byte[] msg = hbServerSock.recv(0);
			if (msg != null) {
				ByteBuffer msgBuffer = ByteBuffer.wrap(msg);
				byte msgType = msgBuffer.get();
				long nodeId = msgBuffer.getLong();
				byte[] response = processMessage(msgType, nodeId, msgBuffer);
				if (!hbServerSock.send(response))
					log4j.error("Error sending message to node:" + Long.toString(nodeId));
			} else
				clusterMaintenance();

		}
		log4j.info("Heartbeat server shut down");
		// the result of the binding is the status

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
				for (Entry<Long, DateTime> hbs : nodeHeartBeats.entrySet())
					if (hbs.getValue().isBefore(tsTimeout)) {
						nodeHeartBeats.remove(hbs.getKey(), hbs.getValue());
						clusterCleanUp(hbs.getKey());
						if (log4j.isDebugEnabled())
							log4j.debug("Node hasn't send a heart beat in " + heartBeatTimeOut
									+ " miliseconds. Removing " + hbs.toString());
					}
				lastGCTime = DateTime.now();
			}
		}
	}

	/**
	 * Method to clean up the distributed locks acquired by a node
	 * 
	 * @param nodeId
	 *            The node for which the distributed locks shall be cleaned
	 */
	private void clusterCleanUp(Long nodeId) {
		{
			CopyOnWriteArrayList<DistributedLock> nodeLock = nodeLocks.get(nodeId);
			if (nodeLock != null) {
				for (DistributedLock dstLock : nodeLock) {
					SvLock.releaseLock(dstLock.key, dstLock.lock);
					synchronized (distributedLocks) {
						distributedLocks.remove(dstLock.key, dstLock);
					}
				}
				nodeLocks.remove(nodeId);
			}
		}
	}
}
