package com.prtech.svarog;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DboFactory;

public class SvClusterClient implements Runnable {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvClusterClient.class);
	/**
	 * The object id of the node which is represented by this client
	 */
	static long nodeId = SvCluster.currentNode != null ? SvCluster.currentNode.getObjectId() : 0L;

	/**
	 * package visible variable to support turning off promotion for the purpose
	 * of executing unit tests
	 */
	static boolean forcePromotionOnShutDown = true;

	/**
	 * package visible variable to support turning off promotion for the purpose
	 * of executing unit tests
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
	static AtomicBoolean isRunning = new AtomicBoolean(false);
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
	 * Timestamp of the last contact with the coordinator
	 */
	static int heartBeatTimeOut = SvConf.getHeartBeatTimeOut();

	static String lastIpAddressList = null;

	/**
	 * Standard method for initialising the client. It tries to connect to the
	 * heart beat socked on any of the addresses in the list
	 * 
	 * @param ipAddressList
	 *            The list of IP addresses on which the heart beat server is
	 *            available
	 * @param hbInterval
	 *            the interval between two heartbeats
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
		SvWriter svw = null;
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
		} catch (Exception e) {
			log4j.error("The node can't connect heart beat socket to:" + ipAddressList, e);
		} finally {
			if (svw != null)
				svw.release();
		}
		boolean result = (hbClientSock != null);
		if (!result) {
			context.close();
			context = null;
			hbClientSock = null;
		}
		return result;
	}

	/**
	 * Override to have default shutdown
	 */
	static public void shutdown() {
		shutdown(true);
	}

	/**
	 * Shuts down the heart beat and closes the ZMQ context
	 */
	static void shutdown(boolean shouldPart) {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.error("Heartbeat thread is not running. Run the thread first");
			return;
		}

		if (context != null)
			synchronized (hbClientSock) {
				if (shouldPart) {
					try {
						ByteBuffer partBuffer = ByteBuffer.allocate(1 + Long.BYTES);
						partBuffer.put(SvCluster.MSG_PART);
						partBuffer.putLong(nodeId);
						byte[] msgPart = null;
						if (!hbClientSock.send(partBuffer.array()))
							log4j.error("Error sending part message to coordinator node");

						msgPart = hbClientSock.recv(0);
						partBuffer = ByteBuffer.wrap(msgPart);

						if (partBuffer.get() != SvCluster.MSG_SUCCESS && partBuffer.getLong() != nodeId) {
							log4j.error("Failed to perform a clean exit from the cluster");
						}
					} catch (Exception e) {
						log4j.error("Error sending part cluster message", e);
					}
				}
				context.close();
				context = null;
				log4j.info("Heartbeat client shutdown");
			}

	}

	/**
	 * Method to send a newly created authentication token to the coordinator
	 * 
	 * @param dboToken
	 *            The token which should be sent (in DbDataObject format)
	 * @return True if the operation was successful
	 */
	static public boolean putToken(DbDataObject dboToken) {
		if (dboToken == null || !isRunning.get()) {
			return false;
		}
		byte[] byteToken = dboToken.toSimpleJson().toString().getBytes(ZMQ.CHARSET);
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + byteToken.length);
			msgBuffer.put(SvCluster.MSG_AUTH_TOKEN_PUT);
			msgBuffer.putLong(nodeId);
			msgBuffer.put(byteToken);
			if (log4j.isDebugEnabled())
				log4j.debug("Put token " + dboToken.toSimpleJson().toString() + " to coordinator from destination:"
						+ Long.toString(nodeId));
			if (!hbClientSock.send(msgBuffer.array()))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = hbClientSock.recv(0);
			msgBuffer = ByteBuffer.wrap(msg);
			byte msgType = msgBuffer.get();
			return msgType == SvCluster.MSG_SUCCESS;
		}
	}

	/**
	 * Method to get a valid authentication token from the coordinator
	 * 
	 * @param token.error("Heartbeat
	 *            timed out. Last contact was: "+lastContact.toString() String
	 *            version of token UUID
	 * @return Returns a DbDataObject version of the token
	 */
	static public DbDataObject getToken(String token) {
		if (!isRunning.get()) {
			return null;
		}
		DbDataObject dboToken = null;
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + Long.BYTES + Long.BYTES);
			msgBuffer.put(SvCluster.MSG_AUTH_TOKEN_GET);
			UUID uid = UUID.fromString(token);
			msgBuffer.putLong(nodeId);
			msgBuffer.putLong(uid.getMostSignificantBits());
			msgBuffer.putLong(uid.getLeastSignificantBits());
			if (log4j.isDebugEnabled())
				log4j.debug("Send token " + token + " for validation to coordinator from destination:"
						+ Long.toString(nodeId));
			if (!hbClientSock.send(msgBuffer.array()))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = hbClientSock.recv(0);
			msgBuffer = ByteBuffer.wrap(msg);
			byte msgType = msgBuffer.get();
			if (msgType == SvCluster.MSG_SUCCESS) {
				dboToken = new DbDataObject();
				String strToken = new String(
						Arrays.copyOfRange(msgBuffer.array(), 1 + Long.BYTES, msgBuffer.array().length), ZMQ.CHARSET);
				Gson g = new Gson();
				dboToken.fromSimpleJson(g.fromJson(strToken, JsonObject.class));
			}
			if (log4j.isDebugEnabled())
				log4j.debug("Token validated:" + (dboToken != null ? dboToken.toSimpleJson().toString() : "invalid"));
		}
		return dboToken;
	}

	/**
	 * Method to refresh the authentication token on the coordinator node. This
	 * is used to touch the objecto on the server in order to set the most
	 * recently used time
	 * 
	 * @param token
	 *            String version of token UUID
	 * @return Returns a DbDataObject version of the token
	 */
	static public boolean refreshToken(String token) {
		if (!isRunning.get()) {
			return false;
		}

		byte msgType = SvCluster.MSG_FAIL;
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + Long.BYTES + Long.BYTES);
			msgBuffer.put(SvCluster.MSG_AUTH_TOKEN_SET);
			UUID uid = UUID.fromString(token);
			msgBuffer.putLong(nodeId);
			msgBuffer.putLong(uid.getMostSignificantBits());
			msgBuffer.putLong(uid.getLeastSignificantBits());
			if (log4j.isDebugEnabled())
				log4j.debug("Send token " + token + " for LRU refresh to coordinator from destination:"
						+ Long.toString(nodeId));
			if (!hbClientSock.send(msgBuffer.array()))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = hbClientSock.recv(0);
			msgBuffer = ByteBuffer.wrap(msg);
			msgType = msgBuffer.get();

			if (log4j.isDebugEnabled())
				log4j.debug("Token refresh:" + (msgType == SvCluster.MSG_SUCCESS ? "succeded" : "failed"));
		}
		return msgType == SvCluster.MSG_SUCCESS;
	}

	/**
	 * Method to acquire a distributed cluster wide lock from the coordinator
	 * 
	 * @param lockKey
	 *            String version of token UUID
	 * @return Returns a hashcode of the lock object
	 */
	static public int getLock(String lockKey) {
		int hashCode = 0;
		if (!isRunning.get()) {
			log4j.error("ClusterClient is not running, can't acquire distributed lock!");
			return hashCode;
		}

		byte msgType = SvCluster.MSG_FAIL;
		synchronized (hbClientSock) {
			byte[] key = null;
			key = lockKey.getBytes(ZMQ.CHARSET);
			// allocate one byte for message type, one long for node Id and the
			// rest for the token
			ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + (key != null ? key.length : 0));
			msgBuffer.put(SvCluster.MSG_LOCK);
			msgBuffer.putLong(nodeId);
			msgBuffer.put(key);
			if (!hbClientSock.send(msgBuffer.array()))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = hbClientSock.recv(0);
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
		return hashCode;
	}

	/**
	 * Method to release a distributed cluster wide lock from the coordinator
	 * 
	 * @param lockHash
	 *            the hash code of the lock which was acquired with getLock
	 * @return Returns true if the lock was released
	 */
	static public boolean releaseLock(int lockHash) {
		boolean result = false;
		if (!isRunning.get()) {
			return result;
		}
		byte msgType = SvCluster.MSG_FAIL;
		synchronized (hbClientSock) {
			ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + Integer.BYTES);
			msgBuffer.put(SvCluster.MSG_LOCK_RELEASE);
			msgBuffer.putLong(nodeId);
			msgBuffer.putInt(lockHash);
			if (!hbClientSock.send(msgBuffer.array()))
				log4j.error("Error sending message to coordinator node");
			byte[] msg = hbClientSock.recv(0);
			msgBuffer = ByteBuffer.wrap(msg);
			msgType = msgBuffer.get();
			if (msgType == SvCluster.MSG_SUCCESS && msgBuffer.getLong() == nodeId) {
				result = true;
			}
		}
		return result;
	}

	/**
	 * Method to send a join message to the cluster, supporting it with the node
	 * information
	 * 
	 * @return True if the node joined the cluster successfully
	 */
	boolean joinCluster() {
		synchronized (hbClientSock) {
			byte[] nodeInfo = SvCluster.currentNode.getVal("node_info").toString().getBytes(ZMQ.CHARSET);
			ByteBuffer joinBuffer = ByteBuffer.allocate(1 + nodeInfo.length);
			joinBuffer.put(SvCluster.MSG_JOIN);
			joinBuffer.put(nodeInfo);
			byte[] msgJoin = null;
			if (hbClientSock.send(joinBuffer.array())) {
				msgJoin = hbClientSock.recv(0);
				joinBuffer = ByteBuffer.wrap(msgJoin);
			} else
				joinBuffer = null;

			if (joinBuffer != null && joinBuffer.get() != SvCluster.MSG_SUCCESS) {
				log4j.error("Failed to join cluster.");
				return false;
			} else
				nodeId = joinBuffer.getLong();
		}
		return true;
	}

	/**
	 * Overriden run method to perform the actual heart beat
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
		boolean heartBeatFailed = false;
		// send joing message
		joinCluster();

		while (isRunning.get() && hbClientSock != null) {
			synchronized (hbClientSock) {
				ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES);
				msgBuffer.put(SvCluster.MSG_HEARTBEAT);
				msgBuffer.putLong(nodeId);
				byte[] msg = null;
				if (isRunning.get()) {
					if (!hbClientSock.send(msgBuffer.array()))
						log4j.error("Error sending message to coordinator node");
					msg = hbClientSock.recv(0);
				}
				if (msg != null) {
					msgBuffer = ByteBuffer.wrap(msg);
					byte msgType = msgBuffer.get();
					long dstNode = msgBuffer.getLong();
					if (msgType != SvCluster.MSG_SUCCESS) {
						log4j.error("Error receiving heartbeat from coordinator node");
						heartBeatFailed = true;
						if (!rejoinOnFailedHeartBeat)
							shutdown(false);
						else
							joinCluster();
					} else {
						lastContact = DateTime.now();
						if (log4j.isDebugEnabled()) {
							log4j.debug("Received heartbeat from coordinator node with destination:"
									+ Long.toString(dstNode));
						}
					}
				} else { // if the last contact was more than the timeout in the
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
							if (SvCluster.isRunning.get()) {
								SvCluster.shutdown();
							}
							SvCluster.initCluster();
						}
						continue;
					}
				}
			}
			try {
				Thread.sleep(heartBeatInterval);
			} catch (InterruptedException e) {
				log4j.error("Heart beat thread sleep raised exception!", e);
			}
		}
	}

}
