package com.prtech.svarog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.prtech.svarog_common.DbDataArray;

/**
 * The Notifier Proxy shall establish a XPUB/XSUB proxy on the coordinator node
 * which will be used for sharing notifications accross the nodes. Each node
 * shall publish notifications to the XSUB socket, and receive notifications
 * from other nodes on the XPUB socket.
 * 
 * @author ristepejov
 *
 */
public class SvClusterNotifierProxy implements Runnable {

	/**
	 * Set a flag to process the proxied notification on the current node
	 */
	static boolean processNotification = false;
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvClusterNotifierProxy.class);

	/**
	 * Map holding the last heart beat of each node in the cluster
	 */
	static final ConcurrentHashMap<Integer, Set<Long>> nodeAcks = new ConcurrentHashMap<Integer, Set<Long>>();

	static final int subscriberPort = SvConf.getHeartBeatPort() + 1;
	static final int publisherPort = SvConf.getHeartBeatPort() + 2;

	static private ZContext context = null;

	/**
	 * Flag if the main thread is running
	 */
	static final AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * Flag if the notifier is in Active state or shutdown
	 */
	static final AtomicBoolean isActive = new AtomicBoolean(false);

	static ZMQ.Socket pubServerSock = null;
	static ZMQ.Socket subServerSock = null;

	/**
	 * Main proxy server initalisation. It requires that the current state of the
	 * proxy is not active (isActive=false). After the active check this method will
	 * create the main ZMQ context as well as the XSUB/XPUB sockets.
	 * 
	 * @return True if the server proxy was initialised correctly
	 */
	static boolean initServer() {
		if (!isActive.compareAndSet(false, true)) {
			log4j.debug("Notifier proxy is already running. Shutdown first");
			return false;
		}
		if (context == null)
			context = new ZContext();
		// Socket to talk to clients

		ZMQ.Socket subSock = context.createSocket(SocketType.XSUB);
		ZMQ.Socket pubSock = context.createSocket(SocketType.XPUB);
		try {

			if (subSock.bind("tcp://*:" + subscriberPort))
				subServerSock = subSock;

			if (pubSock.bind("tcp://*:" + publisherPort))
				pubServerSock = pubSock;

		} catch (Exception e) {
			if (log4j.isDebugEnabled())
				log4j.debug("The node can't bind socket on ports:" + Integer.toString(subscriberPort) + ", "
						+ Integer.toString(publisherPort), e);
		}
		if (subServerSock == null)
			log4j.error("Notification subscriber socket can't bind on port:" + subscriberPort);
		else
			subServerSock.setReceiveTimeOut(SvCluster.SOCKET_RECV_TIMEOUT);

		if (pubServerSock == null)
			log4j.error("Notification publisher socket can't bind on port:" + publisherPort);

		boolean result = (subSock != null && pubSock != null);
		if (!result) {
			context.close();
			context = null;
			subServerSock = pubServerSock = null;
			isActive.set(false);
			log4j.error("Notifier proxy failed to connect/bind to required ports");
		}
		return result;

	}

	/**
	 * Overriden runnable method to execute the Notifier Proxy in a separate thread.
	 * This method basically proxies messages to provide pass through of published
	 * notifications from one node to other nodes in the cluster. If the flag
	 * process notification is set, in that case the messages are processed on the
	 * current node via the NotifierClient.processMessage method
	 */
	@Override
	public void run() {
		if (isActive.get() && !isRunning.compareAndSet(false, true)) {
			log4j.error(this.getClass().getName() + ".run() failed. Current status is active:" + isActive.get()
					+ ". Main server thread is running:" + isRunning.get());
			return;
		}

		if (subServerSock != null && pubServerSock != null) {
			try {
				byte[] subs = new byte[1];
				subs[0] = 1;
				SvCluster.zmqSend(subServerSock, subs, 0);
				boolean hasMoreMessages = false;
				boolean shouldForward = false;
				log4j.info("NotifierProxy started");
				while (isRunning.get()) {
					while (true) {
						// receive message from the subscriber
						byte[] msg = SvCluster.zmqRecv(subServerSock, 0);
						hasMoreMessages = subServerSock.hasReceiveMore();

						if (msg != null) {
							// process the notification on the server node and
							// then forward to the cluster
							if (processNotification) {
								ByteBuffer msgBuffer = ByteBuffer.wrap(msg);
								SvClusterNotifierClient.processMessage(msgBuffer);
								msgBuffer.rewind();
								shouldForward = serverProcessMsg(msgBuffer);
							}
							// publish it to the cluster and handle multiparts
							if (shouldForward) {
								synchronized (pubServerSock) {
									if (!SvCluster.zmqSend(pubServerSock, msg, hasMoreMessages ? ZMQ.SNDMORE : 0))
										log4j.error("Error sending notification to cluster!");
								}
							}

						}
						if (!hasMoreMessages) {
							break;
						}
					}
				}

			} catch (Exception e) {
				log4j.error("NotifierProxy threw exception, shutting down", e);
				isRunning.set(false);
			}
		} else
			log4j.error("Pub/Sub sockets not available, ensure proper initialisation");

		// set the active flag to false and notify the waiting threads
		isActive.set(false);
		synchronized (isActive) {
			isActive.notifyAll();
		}
	}

	/**
	 * Method to shutdown the proxy thread. If the notifier thread is running, try
	 * to set it to false. If the thread is not running just return. After that
	 * check if the server is active, if yes, wait for it until it performs a clean
	 * shutdown. After the main thread has shutdown, close the context so we can
	 * clean exit the ZMQ.proxy
	 */
	static public void shutdown() {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.error("Notifier thread is not running. Run the notifier thread first");
			return;
		}
		try {
			// do wait for the main thread to shutdown
			while (isActive.get())
				synchronized (isActive) {
					isActive.wait();
				}

		} catch (InterruptedException e) {
			log4j.error("Notifier shutdown interrupted", e);
			Thread.currentThread().interrupt();
		}
		if (context != null) {
			context.close();
			context = null;
			subServerSock = pubServerSock = null;
			log4j.info("Notifier Proxy is shut down");
		}
		// notify the maintenance thread
		if (SvMaintenance.maintenanceSemaphore != null)
			synchronized (SvMaintenance.maintenanceSemaphore) {
				SvMaintenance.maintenanceSemaphore.notifyAll();
			}

	}

	/**
	 * Method to process messages on the server side.
	 * 
	 * @param msgBuffer The message buffer to be processed.
	 * @return If the message should be proxied to the cluster or its for internal
	 *         use only
	 */
	private boolean serverProcessMsg(ByteBuffer msgBuffer) {
		// TODO Auto-generated method stub
		boolean shouldForward = true;
		byte msgType = msgBuffer.get();
		switch (msgType) {
		case SvCluster.NOTE_ACK:
			shouldForward = processAckNote(msgBuffer);
			break;
		}
		return shouldForward;
	}

	/**
	 * Method for processing acknowledgments from the coordinator
	 * 
	 * @param msgBuffer The acknowledgement buffer
	 * @return True if the message should be forwarded to the cluster
	 */
	private boolean processAckNote(ByteBuffer msgBuffer) {
		Long nodeId = msgBuffer.getLong();
		Integer ackValue = msgBuffer.getInt();
		byte ackResult = msgBuffer.get();
		if (log4j.isDebugEnabled())
			log4j.debug("Notification " + (ackResult == SvCluster.MSG_FAIL ? "DENIED" : "ACKNOWLEDGED") + " from node"
					+ Long.toString(nodeId) + ". With value:" + Integer.toString(ackValue));
		Set<Long> nodes = nodeAcks.get(ackValue);
		if (nodes != null) {
			if (ackResult == SvCluster.MSG_FAIL) {
				nodeAcks.remove(ackValue);
				synchronized (nodes) {
					nodes.notifyAll();
				}
			} else {
				nodes.remove(nodeId);
				if (nodes.size() == 0) {
					nodeAcks.remove(ackValue);
					log4j.debug("Interrupting client lock thread for lock:" + Integer.toString(ackValue) + "");
					synchronized (nodes) {
						nodes.notifyAll();
					}
				}
			}
		}
		return false;

	}

	/**
	 * Method which awaits for MSG_ACK from set of nodes.
	 * 
	 * @param nodes    The set of nodes from which we shall wait for acknowledgment
	 * @param ackValue The value which is acknowledged by the nodes
	 * @param timeout  Milliseconds to wait for ackowledgement
	 * @return true if the value has been acknowledged, false if the timeout has
	 *         been reached
	 * @throws SvException
	 */
	public static boolean waitForAck(int ackValue, long timeout) {
		boolean result = true;
		Set<Long> nodes = nodeAcks.get(ackValue);
		if (log4j.isDebugEnabled())
			log4j.debug("Waiting ack on " + nodes.toString() + ". Nodes which should respond:"
					+ Integer.toString(nodes.size()) + "");
		if (nodes != null) {
			try {
				synchronized (nodes) {
					nodes.wait(timeout);
				}
				if (log4j.isDebugEnabled())
					log4j.debug(
							"Acknowledge completed. Nodes which didn't respond:" + Integer.toString(nodes.size()) + "");
				nodeAcks.remove(ackValue);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				if (log4j.isDebugEnabled())
					log4j.debug("Thread Interrupted. Nodes left:" + nodes.size());
			}
			result = nodes.size() == 0;
		}
		return result;
	}

	/**
	 * Method to publish a lock action to the cluster. The action type is the
	 * message type which should be NOTE_LOCK_ACQUIRED or NOTE_LOCK_RELEASED
	 * 
	 * @param actionType Acquired or Relesead (SvCluster.NOTE_LOCK_ACQUIRED or
	 *                   SvCluster.NOTE_LOCK_RELEASED)
	 * @param hashCode   The hash code of the lock
	 * @param nodeId     The node which has performed the action
	 * @param lockKey    The text id of the lock
	 * @throws SvException
	 */

	static public void publishLockAction(byte actionType, int hashCode, long nodeId, String lockKey)
			throws SvException {
		synchronized (pubServerSock) {
			if (pubServerSock != null) {
				byte[] key = null;
				key = lockKey.getBytes(ZMQ.CHARSET);
				// allocate one byte for message type, one long for node Id and the
				// rest for the token
				ByteBuffer msgBuffer = ByteBuffer.allocate(
						SvUtil.sizeof.BYTE + SvUtil.sizeof.INT + SvUtil.sizeof.LONG + (key != null ? key.length : 0));
				msgBuffer.put(actionType);
				msgBuffer.putLong(nodeId);
				msgBuffer.putInt(hashCode);
				msgBuffer.put(key);
				if (log4j.isDebugEnabled())
					log4j.debug("Broadcast lock action " + Byte.toString(actionType) + " notification with key:"
							+ lockKey + " and node:" + Long.toString(nodeId));
				if (!SvCluster.zmqSend(pubServerSock, msgBuffer.array(), ZMQ.DONTWAIT))
					log4j.error("Error publishing message to cluster");
			}
		}
	}

	static public void publishDirtyArray(DbDataArray dba) throws SvException {
		synchronized (pubServerSock) {
			SvClusterNotifierClient.publishDirtyArray(dba, SvClusterNotifierProxy.pubServerSock);
		}
	}

	static public void publishDirtyTileArray(Set<SvSDITile> dba) throws SvException {
		synchronized (pubServerSock) {
			SvClusterNotifierClient.publishDirtyTileArray(dba, SvClusterNotifierProxy.pubServerSock);
		}
	}

}
