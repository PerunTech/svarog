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

	static AtomicBoolean isRunning = new AtomicBoolean(false);

	static ZMQ.Socket pubServerSock = null;
	static ZMQ.Socket subServerSock = null;
	static ZMQ.Socket localListener = null;

	static boolean initServer() {
		if (isRunning.get()) {
			log4j.error("Heartbeat thread is already running. Shutdown first");
			return false;
		}
		if (context == null)
			context = new ZContext();
		// Socket to talk to clients

		ZMQ.Socket subSock = context.createSocket(SocketType.XSUB);
		ZMQ.Socket pubSock = context.createSocket(SocketType.XPUB);
		localListener = context.createSocket(SocketType.PUB);
		try {

			if (subSock.bind("tcp://*:" + subscriberPort))
				subServerSock = subSock;

			if (pubSock.bind("tcp://*:" + publisherPort))
				pubServerSock = pubSock;

		} catch (Exception e) {
			log4j.error("The node can't bind socket on ports:" + Integer.toString(subscriberPort) + ", "
					+ Integer.toString(publisherPort), e);
		}
		if (subSock == null)
			log4j.error("Notification subscriber socket can't bind on port:" + subscriberPort);
		else
			subSock.setReceiveTimeOut(SvCluster.SOCKET_RECV_TIMEOUT);

		if (pubSock == null)
			log4j.error("Notification publisher socket can't bind on port:" + publisherPort);

		boolean result = (subSock != null && pubSock != null);
		if (!result) {
			context.close();
			context = null;
			subServerSock = pubServerSock = localListener = null;
		}
		return result;

	}

	/**
	 * If the notifier thread is running, try to set it to false. If it fails
	 * just return, otherwise close the context so we can clean exit the
	 * ZMQ.proxy
	 */
	static public void shutdown() {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.error("Notifier thread is not running. Run the notifier thread first");
			return;
		}
		try {
			// do sleep until socket is able to close within timeout
			Thread.sleep(SvCluster.SOCKET_RECV_TIMEOUT);
		} catch (InterruptedException e) {
			log4j.error("Notifier shutdown interrupted", e);
		}
		if (context != null) {
			context.close();
			context = null;
			log4j.info("Notifier Proxy is shut down");
		}

	}

	/**
	 * Method to publish a lock action to the cluster. The action type is the
	 * message type which should be NOTE_LOCK_ACQUIRED or NOTE_LOCK_RELEASED
	 * 
	 * @param actionType
	 *            Acquired or Relesead (SvCluster.NOTE_LOCK_ACQUIRED or
	 *            SvCluster.NOTE_LOCK_RELEASED)
	 * @param hashCode
	 *            The hash code of the lock
	 * @param nodeId
	 *            The node which has performed the action
	 * @param lockKey
	 *            The text id of the lock
	 */

	static public void publishLockAction(byte actionType, int hashCode, long nodeId, String lockKey) {
		if (pubServerSock != null) {
			byte[] key = null;
			key = lockKey.getBytes(ZMQ.CHARSET);
			// allocate one byte for message type, one long for node Id and the
			// rest for the token
			ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Integer.BYTES + Long.BYTES + (key != null ? key.length : 0));
			msgBuffer.put(actionType);
			msgBuffer.putLong(nodeId);
			msgBuffer.putInt(hashCode);
			msgBuffer.put(key);
			if (log4j.isDebugEnabled())
				log4j.debug("Broadcast lock action " + Byte.toString(actionType) + " notification with key:" + lockKey
						+ " and node:" + Long.toString(nodeId));
			if (!pubServerSock.send(msgBuffer.array(), ZMQ.DONTWAIT))
				log4j.error("Error publishing message to cluster");
		}
	}

	static public void publishDirtyArray(DbDataArray dba) {
		SvClusterNotifierClient.publishDirtyArray(dba, SvClusterNotifierProxy.pubServerSock);
	}

	static public void publishDirtyTileArray(List<SvSDITile> dba) {
		SvClusterNotifierClient.publishDirtyTileArray(dba, SvClusterNotifierProxy.pubServerSock);
	}

	/**
	 * Overriden runnable method to execute the Notifier Proxy in a separate
	 * thread. This method basically proxies messages to provide pass through of
	 * published notifications from one node to other nodes in the cluster. If
	 * the flag process notification is set, in that case the messages are
	 * processed on the current node via the NotifierClient.processMessage
	 * method
	 */
	@Override
	public void run() {
		if (!isRunning.compareAndSet(false, true)) {
			log4j.error("NotifierProxy is already running. Shutdown first");
			return;
		}
		if (subServerSock != null && pubServerSock != null) {
			byte[] subs = new byte[1];
			subs[0] = 1;
			subServerSock.send(subs);
			boolean hasMoreMessages = false;
			boolean shouldForward = false;
			log4j.info("NotifierProxy started");
			try {
				while (isRunning.get()) {
					while (true) {
						// receive message from the subscriber
						byte[] msg = subServerSock.recv(0);
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
							if (shouldForward)
								if (!pubServerSock.send(msg, hasMoreMessages ? ZMQ.SNDMORE : 0))
									log4j.error("Error sending notification to cluster!");

						}
						if (!hasMoreMessages) {
							break;
						}
					}
				}

			} catch (Exception e) {
				log4j.error("NotifierProxy threw exception", e);
			}
		} else
			log4j.error("Pub/Sub sockets not available, ensure proper initialisation");
	}

	/**
	 * Method to process messages on the server side.
	 * 
	 * @param msgBuffer
	 *            The message buffer to be processed.
	 * @return If the message should be proxied to the cluster or its for
	 *         internal use only
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
	 * @param msgBuffer
	 *            The acknowledgement buffer
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
					nodes.notify();
				}
			} else {
				nodes.remove(nodeId);
				if (nodes.size() == 0) {
					nodeAcks.remove(ackValue);
					log4j.debug("Interrupting client lock thread for lock:" + Integer.toString(ackValue) + "");
					synchronized (nodes) {
						nodes.notify();
					}
				}
			}
		}
		return false;

	}

	/**
	 * Method which awaits for MSG_ACK from set of nodes.
	 * 
	 * @param nodes
	 *            The set of nodes from which we shall wait for acknowledgment
	 * @param ackValue
	 *            The value which is acknowledged by the nodes
	 * @param timeout
	 *            Milliseconds to wait for ackowledgement
	 * @return true if the value has been acknowledged, false if the timeout has
	 *         been reached
	 * @throws SvException
	 */
	public static boolean waitForAck(Set<Long> nodes, int ackValue, long timeout) {
		if (log4j.isDebugEnabled())
			log4j.debug("Waiting ack on " + nodes.toString() + ". Nodes which should respond:"
					+ Integer.toString(nodes.size()) + "");
		try {
			synchronized (nodes) {
				nodes.wait(timeout);
			}
			if (log4j.isDebugEnabled())
				log4j.debug("Acknowledge completed. Nodes which didn't respond:" + Integer.toString(nodes.size()) + "");
			nodeAcks.remove(ackValue);
		} catch (InterruptedException e) {
			if (log4j.isDebugEnabled())
				log4j.debug("Thread Interrupted. Nodes left:" + nodes.size());
		}

		return nodes.size() == 0;
	}

}
