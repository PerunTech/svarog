package com.prtech.svarog;

import java.nio.ByteBuffer;
import java.util.Arrays;
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

public class SvClusterClient implements Runnable {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvClusterClient.class);
	/**
	 * The object id of the node which is represented by this client
	 */
	static long nodeId = SvCluster.getCoordinatorNode() != null ? SvCluster.getCoordinatorNode().getObjectId() : 0L;

	/**
	 * package visible variable to support turning off promotion for the purpose of
	 * executing unit tests
	 */
	static boolean forcePromotionOnShutDown = true;

	/**
	 * package visible variable to support turning off promotion for the purpose of
	 * executing unit tests
	 */
	static boolean rejoinOnFailedHeartBeat = false;

	/**
	 * Standard ZMQ context
	 */
	static private ZContext context = null;
	/**
	 * The heart beat socket
	 */
	static ZMQ.Socket hbClientSock = null;

	/**
	 * Flag if the client is running
	 */
	static final AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * Flag if the client has joined a cluster
	 */
	static final AtomicBoolean isActive = new AtomicBoolean(false);

	/**
	 * Delimiter to be used for split of the address list
	 */
	static final String ipAddrDelimiter = ";";

	/**
	 * Interval in miliseconds between two hearbeats
	 */
	static int heartBeatInterval = SvConf.getHeartBeatInterval();

	/**
	 * Timestamp of the last contact with the coordinator
	 */
	static DateTime lastContact = DateTime.now();

	/**
	 * Timeout interval in milliseconds. If no contact is established within timeout
	 * the node is dead
	 */
	static int heartBeatTimeOut = SvConf.getHeartBeatTimeOut();

	static String lastIpAddressList = null;

	/**
	 * Map of distributed locks as backup of the coordinator (this is updated when
	 * we run as a normal worker)
	 */
	static final ConcurrentHashMap<String, SvCluster.DistributedLock> localDistributedLocks = new ConcurrentHashMap<String, SvCluster.DistributedLock>();
	/**
	 * Map of locks grouped by node as backup of the coordinator (this is updated
	 * when we run as a normal worker)
	 */
	static final ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>> localNodeLocks = new ConcurrentHashMap<Long, CopyOnWriteArrayList<SvCluster.DistributedLock>>();

	/**
	 * Standard method for initialising the client. It tries to connect to the heart
	 * beat socked on any of the addresses in the list
	 * 
	 * @param ipAddressList The list of IP addresses on which the heart beat server
	 *                      is available
	 * @param hbInterval    the interval between two heartbeats
	 * 
	 * 
	 * @return true if the client was initialised correctly
	 */
	static boolean initClient(String ipAddressList) {
		if (isRunning.get()) {
			log4j.error("Heartbeat client thread is already running. Shutdown first");
			return false;
		}
		// Socket to talk to clients
		if (context == null)
			context = new ZContext();
		hbClientSock = null;
		ZMQ.Socket tmpSock = context.createSocket(SocketType.REQ);
		String[] address = ipAddressList.split(ipAddrDelimiter);
		try {
			for (String host : address) {
				boolean connected = tmpSock.connect("tcp://" + host);
				if (connected) {
					hbClientSock = tmpSock;
					log4j.info("Heart-beat client connected to host:" + host);
					lastIpAddressList = ipAddressList;
					hbClientSock.setReceiveTimeOut(heartBeatTimeOut);
					break;
				}
			}

			// send joing message
			if (log4j.isDebugEnabled())
				log4j.debug("Joining cluster with IP:" + lastIpAddressList);
			isActive.compareAndSet(false, joinCluster());
			if (!isActive.get()) {
				if (log4j.isDebugEnabled()) {
					log4j.debug("Client failed joining cluster with IP:" + lastIpAddressList);
				}
			} else
				log4j.debug("Successfully joined cluster with IP:" + lastIpAddressList);
		} catch (Exception e) {
			log4j.error("The node can't connect heart beat socket to:" + ipAddressList, e);
		}

		if (!isActive.get()) {
			context.close();
			context = null;
			hbClientSock = null;
		}
		return isActive.get();
	}

	/**
	 * Override to have default shutdown
	 */
	static public void shutdown() {
		shutdown(true);
	}

	/**
	 * Method to ensure clean exit from the cluster by sending a part message and
	 * waiting for response
	 */
	static void partCluster() {
		synchronized (hbClientSock) {
			try {
				ByteBuffer partBuffer = ByteBuffer.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG);
				partBuffer.put(SvCluster.MSG_PART);
				partBuffer.putLong(nodeId);
				byte[] msgPart = null;
				if (!SvCluster.zmqSend(hbClientSock, partBuffer.array(), 0))
					log4j.error("Error sending part message to coordinator node");

				msgPart = SvCluster.zmqRecv(hbClientSock, 0);
				if (msgPart != null)
					partBuffer = ByteBuffer.wrap(msgPart);
				if (msgPart != null && partBuffer.get() != SvCluster.MSG_SUCCESS && partBuffer.getLong() != nodeId) {
					log4j.error("Failed to perform a clean exit from the cluster");
				}
			} catch (Exception e) {
				log4j.error("Error sending part cluster message", e);
			}
		}
	}

	static void waitShutdown() {
		DateTime tsTimeout = DateTime.now().withDurationAdded(SvConf.getHeartBeatTimeOut(), 1);
		// if shut down in progress, wait to finish.
		while (tsTimeout.isAfterNow() && SvCluster.getIsActive().get()) {
			if (log4j.isDebugEnabled())
				log4j.debug("Cluster is still active, waiting for it to shutdown");

			try {
				synchronized (SvCluster.isRunning()) {
					SvCluster.isRunning().wait(heartBeatInterval);
				}
			} catch (InterruptedException e) {
				tsTimeout = DateTime.now();
				Thread.currentThread().interrupt();
				log4j.error("Heart beat thread sleep raised exception! Cluster did not shutdown", e);

			}
		}
	}

	/**
	 * Method to fail over
	 */
	private static void failOver() {
		// if the last contact was more than the timeout in the
		// past .. we consider the coordinator dead and
		// shutdown
		// this socket. we are up for new election
		if (lastContact.withDurationAdded(heartBeatTimeOut, 1).isBeforeNow()) {
			log4j.error("Heartbeat timed out. Max wait is:" + Integer.toString(heartBeatTimeOut)
					+ ". Last contact was: " + lastContact.toString());
			// don't sent part message to dead node
			shutdown(false);
			if (forcePromotionOnShutDown) {
				log4j.info("Restarting SvCluster node to force re-election of coordinator");
				SvCluster.shutdown();
				waitShutdown();
				if (!SvMaintenance.getMaintenanceInProgress().get())
					synchronized (SvMaintenance.maintenanceRunning) {
						SvMaintenance.maintenanceRunning.notifyAll();
					}

			}
		}
	}

	/**
	 * Shuts down the heart beat and closes the ZMQ context
	 */
	static void shutdown(boolean shouldPart) {
		if (shouldPart) {
			if (!isRunning.compareAndSet(true, false)) {
				log4j.warn("Client thread is not running. Can't shut down inactive client");
				return;
			}
			try {
				synchronized (isRunning) {
					isRunning.notifyAll();
				}
				// do wait for the main thread to shutdown
				while (isActive.get())
					synchronized (isActive) {
						isActive.wait();
					}

			} catch (InterruptedException e) {
				log4j.error("Heart-beat shutdown interrupted", e);
				Thread.currentThread().interrupt();
			}
			if (context != null)
				partCluster();
		} else
			isRunning.set(false);

		if (context != null) {
			context.close();
			context = null;
			log4j.info("Heartbeat client shutdown");
		}

		// notify the maintenance thread
		if (SvMaintenance.maintenanceSemaphore != null)
			synchronized (SvMaintenance.maintenanceSemaphore) {
				SvMaintenance.maintenanceSemaphore.notifyAll();
			}

	}

	/**
	 * Method to send a newly created authentication token to the coordinator
	 * 
	 * @param dboToken The token which should be sent (in DbDataObject format)
	 * @return True if the operation was successful
	 * @throws SvException
	 */
	static public boolean putToken(DbDataObject dboToken) throws SvException {
		if (dboToken == null || !isRunning.get()) {
			return false;
		}
		byte[] byteToken = dboToken.toSimpleJson().toString().getBytes(ZMQ.CHARSET);
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + byteToken.length);
			msgBuffer.put(SvCluster.MSG_AUTH_TOKEN_PUT);
			msgBuffer.putLong(nodeId);
			msgBuffer.put(byteToken);
			if (log4j.isDebugEnabled())
				log4j.debug("Put token " + dboToken.toSimpleJson().toString() + " to coordinator from destination:"
						+ Long.toString(nodeId));
			if (!SvCluster.zmqSend(hbClientSock, msgBuffer.array(), 0))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = SvCluster.zmqRecv(hbClientSock, 0);
			msgBuffer = ByteBuffer.wrap(msg);
			byte msgType = msgBuffer.get();
			return msgType == SvCluster.MSG_SUCCESS;
		}
	}

	/**
	 * Method to get a valid authentication token from the coordinator
	 * 
	 * @param token.error("Heartbeat timed out. Last contact was:
	 *                               "+lastContact.toString() String version of
	 *                               token UUID
	 * @return Returns a DbDataObject version of the token
	 * @throws SvException
	 */
	static public DbDataObject getToken(String token) throws SvException {
		if (!isRunning.get()) {
			return null;
		}
		DbDataObject dboToken = null;
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer
					.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + SvUtil.sizeof.LONG + SvUtil.sizeof.LONG);
			msgBuffer.put(SvCluster.MSG_AUTH_TOKEN_GET);
			UUID uid = UUID.fromString(token);
			msgBuffer.putLong(nodeId);
			msgBuffer.putLong(uid.getMostSignificantBits());
			msgBuffer.putLong(uid.getLeastSignificantBits());
			if (log4j.isDebugEnabled())
				log4j.debug("Send token " + token + " for validation to coordinator from destination:"
						+ Long.toString(nodeId));
			if (!SvCluster.zmqSend(hbClientSock, msgBuffer.array(), 0))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = SvCluster.zmqRecv(hbClientSock, 0);
			msgBuffer = ByteBuffer.wrap(msg);
			byte msgType = msgBuffer.get();
			if (msgType == SvCluster.MSG_SUCCESS) {
				dboToken = new DbDataObject();
				String strToken = new String(
						Arrays.copyOfRange(msgBuffer.array(), 1 + SvUtil.sizeof.LONG, msgBuffer.array().length),
						ZMQ.CHARSET);
				Gson g = new Gson();
				dboToken.fromSimpleJson(g.fromJson(strToken, JsonObject.class));
			}
			if (log4j.isDebugEnabled())
				log4j.debug("Token validated:" + (dboToken != null ? dboToken.toSimpleJson().toString() : "invalid"));
		}
		return dboToken;
	}

	/**
	 * Method to refresh the authentication token on the coordinator node. This is
	 * used to touch the objecto on the server in order to set the most recently
	 * used time
	 * 
	 * @param token String version of token UUID
	 * @return Returns a DbDataObject version of the token
	 * @throws SvException
	 */
	static public boolean refreshToken(String token) throws SvException {
		if (!isRunning.get()) {
			return false;
		}

		byte msgType;
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer
					.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + SvUtil.sizeof.LONG + SvUtil.sizeof.LONG);
			msgBuffer.put(SvCluster.MSG_AUTH_TOKEN_SET);
			UUID uid = UUID.fromString(token);
			msgBuffer.putLong(nodeId);
			msgBuffer.putLong(uid.getMostSignificantBits());
			msgBuffer.putLong(uid.getLeastSignificantBits());
			if (log4j.isDebugEnabled())
				log4j.debug("Send token " + token + " for LRU refresh to coordinator from destination:"
						+ Long.toString(nodeId));
			if (!SvCluster.zmqSend(hbClientSock, msgBuffer.array(), 0))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = SvCluster.zmqRecv(hbClientSock, 0);
			msgBuffer = ByteBuffer.wrap(msg);
			msgType = msgBuffer.get();
			if (log4j.isDebugEnabled())
				log4j.debug("Token refresh:"
						+ (msgType == SvCluster.MSG_SUCCESS ? "succeded" : "failed, lets put the session"));

			if (msgType == SvCluster.MSG_FAIL) {
				DbDataObject svToken = DbCache.getObject(token, svCONST.OBJECT_TYPE_SECURITY_LOG);
				if (SvClusterClient.putToken(svToken))
					msgType = SvCluster.MSG_SUCCESS;
			}

		}
		return msgType == SvCluster.MSG_SUCCESS;
	}

	/**
	 * Method to acquire a distributed cluster wide lock from the coordinator
	 * 
	 * @param lockKey String version of token UUID
	 * @return Returns a hashcode of the lock object
	 * @throws SvException
	 */
	static public int getLock(String lockKey) throws SvException {
		int hashCode = 0;
		if (!isRunning.get()) {
			log4j.error("ClusterClient is not running, can't acquire distributed lock!");
			return hashCode;
		}

		byte msgType;
		synchronized (hbClientSock) {
			byte[] key = null;
			key = lockKey.getBytes(ZMQ.CHARSET);
			// allocate one byte for message type, one long for node Id and the
			// rest for the token
			ByteBuffer msgBuffer = ByteBuffer
					.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + (key != null ? key.length : 0));
			msgBuffer.put(SvCluster.MSG_LOCK);
			msgBuffer.putLong(nodeId);
			msgBuffer.put(key);
			if (!SvCluster.zmqSend(hbClientSock, msgBuffer.array(), 0))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = SvCluster.zmqRecv(hbClientSock, 0);
			if (msg != null) {
				msgBuffer = ByteBuffer.wrap(msg);
				msgType = msgBuffer.get();
				if (msgType == SvCluster.MSG_SUCCESS) {
					if (msgBuffer.getLong() == nodeId)
						hashCode = msgBuffer.getInt();
					else
						log4j.warn("Lock was acquired with wrong node id.");
				} else if (log4j.isDebugEnabled())
					log4j.debug("Lock not acquired! Lock is held by node:" + Long.toString(msgBuffer.getLong()));
			}
		}
		return hashCode;
	}

	/**
	 * Method to release a distributed cluster wide lock from the coordinator
	 * 
	 * @param lockHash the hash code of the lock which was acquired with getLock
	 * @return Returns true if the lock was released
	 * @throws SvException
	 */
	static public boolean releaseLock(int lockHash) throws SvException {
		boolean result = false;
		if (!isRunning.get()) {
			return result;
		}
		byte msgType;
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + SvUtil.sizeof.INT);
			msgBuffer.put(SvCluster.MSG_LOCK_RELEASE);
			msgBuffer.putLong(nodeId);
			msgBuffer.putInt(lockHash);
			if (!SvCluster.zmqSend(hbClientSock, msgBuffer.array(), 0))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = SvCluster.zmqRecv(hbClientSock, 0);
			msgBuffer = ByteBuffer.wrap(msg);
			msgType = msgBuffer.get();
			if (msgType == SvCluster.MSG_SUCCESS && msgBuffer.getLong() == nodeId) {
				result = true;
			}
		}
		return result;
	}

	/**
	 * Method to release a distributed lock. This is invoked when the clusterServer
	 * processes releaseLock message. This method is also invoked by the SvLock on
	 * the coordinator node.
	 * 
	 * @param lockHash The hash of the lock
	 * @param nodeId   The node id which acquires the lock
	 * 
	 * @return The lock key if the lock was released with success, otherwise null
	 */
	static String releaseDistributedLock(Integer lockHash) {
		return SvCluster.releaseDistributedLock(lockHash, nodeId, localNodeLocks, SvLock.localSysLocks, true);

	}

	static String releaseDistributedLock(Integer lockHash, Long nodeId) {
		return SvCluster.releaseDistributedLock(lockHash, nodeId, localNodeLocks, SvLock.localSysLocks, true);

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
	static boolean updateDistributedLock(String lockKey, Integer lockHash) {
		return SvCluster.updateDistributedLock(lockKey, lockHash, localDistributedLocks);
	}

	/**
	 * Method to clean up the distributed locks acquired by a node
	 * 
	 * @param nodeId The node for which the distributed locks shall be cleaned
	 */
	static void clusterCleanUp(Long nodeId) {
		SvCluster.clusterCleanUp(nodeId, SvClusterClient.localNodeLocks, true);
	}

	/**
	 * Method to get a distributed lock from the Cluster server
	 * 
	 * @param lockKey The string identifier of the lock
	 * @return the hash code of the lock
	 */
	static ReentrantLock acquireDistributedLock(String lockKey, Long nodeId) {
		Long[] extendedInfo = new Long[1];
		ReentrantLock lck = SvCluster.acquireDistributedLock(lockKey, nodeId, extendedInfo, localNodeLocks,
				SvLock.localSysLocks, true);
		if (lck == null && log4j.isDebugEnabled())
			log4j.debug("Lock not acquired! Lock requested by " + nodeId.toString() + " but held by node:"
					+ Long.toString(extendedInfo[0]));
		return lck;
	}

	/**
	 * Method to process a lock sent by the coordinator on join
	 * 
	 * @param msgLock
	 */
	private static void processJoinLock(byte[] msgLock) {
		ByteBuffer lockBuffer;
		if (msgLock != null) {
			lockBuffer = ByteBuffer.wrap(msgLock);
			long locknode = lockBuffer.getLong();
			int lockHash = lockBuffer.getInt();
			String lockKey = new String(Arrays.copyOfRange(lockBuffer.array(), SvUtil.sizeof.LONG + SvUtil.sizeof.INT,
					lockBuffer.array().length), ZMQ.CHARSET);
			if (log4j.isDebugEnabled())
				log4j.debug("Receiving existing locks on join:" + lockKey + ", hash:" + Integer.toString(lockHash));

			if (SvClusterClient.acquireDistributedLock(lockKey, locknode) != null)
				SvClusterClient.updateDistributedLock(lockKey, lockHash);
			else if (log4j.isDebugEnabled())
				log4j.debug("Failed acquiring local distributed lock on join. Must be a nasty bug");
		}
	}

	/**
	 * Method to process a join message response from the server.
	 * 
	 * @param msgJoin The actual response message received
	 * @return True if the client successfully joined the cluster
	 */
	static boolean processJoin(byte[] msgJoin) {
		boolean result = true;
		ByteBuffer joinBuffer = null;
		if (msgJoin != null) {
			joinBuffer = ByteBuffer.wrap(msgJoin);
			byte respMsg = joinBuffer.get();
			if (respMsg == SvCluster.MSG_FAIL) {
				log4j.error("Failed to join cluster.");
				nodeId = 0L;
				result = false;
			} else {
				// before set new node Id lets first migrate the old
				// locks
				Long newNodeId = joinBuffer.getLong();
				SvCluster.migrateLocks(newNodeId, nodeId, localNodeLocks);
				nodeId = newNodeId;
			}
		} else {
			if (log4j.isDebugEnabled())
				log4j.debug("Coordinator didn't respond to join message!");
			result = false;
		}
		return result;

	}

	/**
	 * Method to send a join message to the cluster, supporting it with the node
	 * information
	 * 
	 * @return True if the node joined the cluster successfully
	 */
	static boolean joinCluster() {
		boolean result = true;
		synchronized (hbClientSock) {
			if (log4j.isDebugEnabled())
				log4j.debug("Joining cluster with old nodeId:" + Long.toString(nodeId));
			byte[] nodeInfo = SvCluster.getCurrentNodeInfo().getVal("node_info").toString().getBytes(ZMQ.CHARSET);
			ByteBuffer joinBuffer = ByteBuffer.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + nodeInfo.length);
			joinBuffer.put(SvCluster.MSG_JOIN);
			joinBuffer.putLong(nodeId);
			joinBuffer.put(nodeInfo);
			byte[] msgJoin = null;
			byte[] msgLock = null;
			try {
				if (SvCluster.zmqSend(hbClientSock, joinBuffer.array(), 0)) {
					msgJoin = SvCluster.zmqRecv(hbClientSock, 0);
					if (processJoin(msgJoin)) {
						// check if lock info follows the join
						while (hbClientSock.hasReceiveMore()) {
							msgLock = SvCluster.zmqRecv(hbClientSock, 0);
							processJoinLock(msgLock);
						}
					}

				}

			} catch (Exception e) {
				log4j.error("Error joining cluster:", e);
				nodeId = 0L;
				result = false;
			}
		}
		return result;
	}

	/**
	 * Method to process a heart beat message. If message is not MSG SUCCESS it will
	 * shutdown, under the flag to rejoin on failed heart beat is set. In case of
	 * rejoin flag, it will try to join cluster again
	 * 
	 * @param msg The heart beat message received
	 */
	private static void processHeartBeat(byte[] msg) {
		assert (msg != null);
		ByteBuffer msgBuffer = ByteBuffer.wrap(msg);
		byte msgType = msgBuffer.get();
		long dstNode = msgBuffer.getLong();
		if (msgType != SvCluster.MSG_SUCCESS) {
			log4j.error("Error receiving heartbeat from coordinator node");
			if (!rejoinOnFailedHeartBeat)
				shutdown(false);
			else {
				if (log4j.isDebugEnabled())
					log4j.debug("Received failed heartbeat from coordinator. Trying to re-join");
				joinCluster();
			}
		} else {
			lastContact = DateTime.now();
			if (log4j.isDebugEnabled()) {
				log4j.trace("Received heartbeat from coordinator node with destination:" + Long.toString(dstNode));
			}
		}
	}

	/**
	 * Method to send a heart beat to the server and wait for response.
	 * 
	 * @return A response message to the heart beat
	 */
	private static byte[] sendHearBeat() {
		byte[] msg = null;
		synchronized (hbClientSock) {
			try {
				ByteBuffer msgBuffer = ByteBuffer.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG);
				msgBuffer.put(SvCluster.MSG_HEARTBEAT);
				msgBuffer.putLong(nodeId);

				if (!SvCluster.zmqSend(hbClientSock, msgBuffer.array(), 0))
					log4j.error("Error sending message to coordinator node");
				msg = SvCluster.zmqRecv(hbClientSock, 0);
				if (msg == null)
					failOver();
			} catch (Exception e) {
				log4j.error("Heart beat thread sleep raised exception! Shutting down client", e);
				shutdown(false);
			}
		}
		return msg;

	}

	/**
	 * Overriden run method to perform the actual heart beat. It will send heart
	 * beat messages every heartBeatInterval, until the isRunning flag has been set
	 * to false or the socket is not available.
	 */
	@Override
	public void run() {
		if (!isRunning.compareAndSet(false, true)) {
			log4j.error("Heartbeat thread is already running. Shutdown first");
			return;
		}
		if (hbClientSock == null) {
			log4j.error("Heartbeat socket not available, ensure proper initialisation");
			return;
		}

		while (isRunning.get() && hbClientSock != null) {
			// send a heart beat message to the server
			byte[] response = sendHearBeat();
			// if response is received process is
			if (response != null)
				processHeartBeat(response);

			if (isRunning.get())
				try {
					synchronized (isRunning) {
						isRunning.wait(heartBeatInterval);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log4j.error("Heart beat thread sleep raised exception! Shutting down client", e);
					shutdown(false);
				}

		}
		// finally mark that we are not active
		isActive.set(false);
		synchronized (isActive) {
			isActive.notifyAll();
		}
	}

}
