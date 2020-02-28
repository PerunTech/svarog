package com.prtech.svarog;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
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
			subSock.setReceiveTimeOut(SvCluster.sockeReceiveTimeout);

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
			Thread.sleep(SvCluster.sockeReceiveTimeout);
		} catch (InterruptedException e) {
			log4j.error("Notifier shutdown interrupted", e);
		}
		if (context != null) {
			context.close();
			context = null;
			log4j.info("Notifier Proxy is shut down");
		}

	}

	static public void publishDirtyArray(DbDataArray dba) {
		SvClusterNotifierClient.publishDirtyArray(dba, pubServerSock);
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
							}
							// publish it to the cluster and handle multiparts
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

}
