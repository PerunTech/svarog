package com.prtech.svarog;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

/**
 * 
 * Implementation of the Cluster Notifier Client. This class shall support
 * sending and receiving notifications from the notifier proxy available at the
 * coordinator node. The notifications are received in a separate thread on a
 * Subscriber socket. The sending of the notifications is done via a publisher
 * socket which runs in the current thread.
 * 
 * @author ristepejov
 *
 */
public class SvClusterNotifierClient implements Runnable {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvClusterNotifierClient.class);

	/**
	 * The coordinator notification subscriber listens on port which 1 above the
	 * hearbeat
	 */
	static int subscriberPort = SvConf.getHeartBeatPort() + 1;
	/**
	 * The coordinator notification publisher listens on port which 2 above the
	 * hearbeat
	 */
	static int publisherPort = SvConf.getHeartBeatPort() + 2;

	/**
	 * Standard ZMQ context for the pub/sub sockets.
	 */
	static private ZContext context = null;

	/**
	 * Flag if the client is running
	 */
	static AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * ZMQ publisher socket used for publishing notifications to the cluster
	 */
	static ZMQ.Socket pubServerSock = null;
	/**
	 * ZMQ subscriber socket used for listening for notifications from the
	 * cluster
	 */
	static ZMQ.Socket subServerSock = null;

	/**
	 * Delimiter used by the ip address list
	 */
	static final String ipAddrDelimiter = ";";

	/**
	 * Timeout for receiving a message on the socket.
	 */
	static final int sockeReceiveTimeout = 500;

	/**
	 * Method to initialise the notifier client. The client uses the ip address
	 * list and tries to connect the publisher and subscriber sockets to any of
	 * the available addresses. If connection is successful, returns true. The
	 * sockets connected on the next 2 ports above the heart beat port
	 * 
	 * @param ipAddressList
	 *            List of ip addresses which is registered by the coordinator
	 *            node
	 * @return True if the client connected to the coordinator successfully
	 */
	static boolean initClient(String ipAddressList) {
		if (isRunning.get()) {
			log4j.error("Heartbeat client thread is already running. Shutdown first");
			return false;
		}
		// Socket to talk to clients
		if (context == null)
			context = new ZContext();
		pubServerSock = null;
		subServerSock = null;
		ZMQ.Socket pubSock = context.createSocket(SocketType.PUB);
		ZMQ.Socket subSock = context.createSocket(SocketType.SUB);
		String[] address = ipAddressList.split(ipAddrDelimiter);

		for (String host : address) {
			String[] ip = host.split(":");
			subscriberPort = Integer.parseInt(ip[1]) + 1;
			boolean connected = pubSock.connect("tcp://" + ip[0] + ":" + subscriberPort);
			if (connected) {
				pubServerSock = pubSock;
				log4j.info("Notification publisher connected to host:" + ip[0] + ":" + subscriberPort);
			}
			publisherPort = Integer.parseInt(ip[1]) + 2;
			connected = subSock.connect("tcp://" + ip[0] + ":" + publisherPort);
			if (connected) {
				subServerSock = subSock;
				log4j.info("Notification publisher connected to host:" + ip[0] + ":" + publisherPort);
			}
			if (pubServerSock != null && subServerSock != null)
				break;
		}
		subServerSock.setReceiveTimeOut(sockeReceiveTimeout);
		return (pubServerSock != null && subServerSock != null);
	}

	/**
	 * Method to shutdown the cluster notification listener
	 */
	static public void shutdown() {
		if (!isRunning.compareAndSet(true, false)) {
			log4j.error("Notification thread is not running. Run the notifier thread first");
			return;
		}
		try {
			// do sleep until socket is able to close within timeout
			Thread.sleep(sockeReceiveTimeout);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (context != null) {
			context.close();
			context = null;
		}
	}

	/**
	 * Method to publish a dirty array notification to the other nodes in the
	 * cluster
	 * 
	 * @param objectId
	 *            The id of the dirty object
	 * @param objectTypeId
	 *            The type of the dirty object
	 */
	static public void publishDirtyArray(DbDataArray dba) {
		ByteBuffer msgBuffer = null;
		Long currentType = 0L;
		int objectCount = 0;
		int totalCount = 0;
		for (DbDataObject dbo : dba.getItems()) {
			if (currentType != dbo.getObjectType()) {
				if (objectCount > 0 && msgBuffer != null) {
					byte[] finalBytes = Arrays.copyOfRange(msgBuffer.array(), 0,
							(1 + Long.BYTES + (Long.BYTES * objectCount)));
					// send the previous buffer
					if (log4j.isDebugEnabled())
						log4j.debug("Sent dirty notification of array with ids:" + msgBuffer.toString()
								+ " to coordinator");
					if (!pubServerSock.send(finalBytes, ZMQ.DONTWAIT))
						log4j.error("Error publishing message to coordinator node");
				}
				objectCount = 0;
				currentType = dbo.getObjectType();
				msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + (Long.BYTES * (dba.size() - totalCount)));
				msgBuffer.put(SvCluster.MSG_DIRTY_OBJECT);
				msgBuffer.putLong(currentType);
			}
			msgBuffer.putLong(dbo.getObjectId());
			totalCount++;
			objectCount++;
		}
		// the array was finished so lets send the left over buffer if any
		if (objectCount > 0 && msgBuffer != null) {
			byte[] finalBytes = Arrays.copyOfRange(msgBuffer.array(), 0, (1 + Long.BYTES + (Long.BYTES * objectCount)));
			// send the previous buffer
			if (log4j.isDebugEnabled())
				log4j.debug("Sent dirty notification of array with ids:" + msgBuffer.toString() + " to coordinator");
			if (!pubServerSock.send(finalBytes, ZMQ.DONTWAIT))
				log4j.error("Error publishing message to coordinator node");
		}
	}

	/**
	 * Method to publish a dirty object notification to the other nodes in the
	 * cluster
	 * 
	 * @param objectId
	 *            The id of the dirty object
	 * @param objectTypeId
	 *            The type of the dirty object
	 */
	static public void publishDirtyObject(long objectId, long objectTypeId) {
		ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + Long.BYTES);
		msgBuffer.put(SvCluster.MSG_DIRTY_OBJECT);
		msgBuffer.putLong(objectTypeId);
		msgBuffer.putLong(objectId);
		if (log4j.isDebugEnabled())
			log4j.debug("Sent dirty notification of object with id:" + objectId + " to coordinator");
		if (!pubServerSock.send(msgBuffer.array(), ZMQ.DONTWAIT))
			log4j.error("Error publishing message to coordinator node");
	}

	/**
	 * Method to publish a logoff notification to the other nodes in the cluster
	 * 
	 * @param sessionId
	 *            String representation of the user session/token
	 */
	static public void publishLogoff(String sessionId) {
		ByteBuffer msgBuffer = ByteBuffer.allocate(1 + Long.BYTES + Long.BYTES);
		msgBuffer.put(SvCluster.MSG_LOGOFF);
		UUID uuid = UUID.fromString(sessionId);
		msgBuffer.putLong(uuid.getMostSignificantBits());
		msgBuffer.putLong(uuid.getLeastSignificantBits());
		if (log4j.isDebugEnabled())
			log4j.debug("Sent logoff notification with token:" + uuid.toString() + " to the coordinator");
		if (!pubServerSock.send(msgBuffer.array(), ZMQ.DONTWAIT))
			log4j.error("Error publishing message to coordinator node");
	}

	@Override
	public void run() {
		if (!isRunning.compareAndSet(false, true)) {
			log4j.error("Notify subscriber thread is already running. Shutdown first");
			return;
		}
		if (subServerSock != null) {
			subServerSock.subscribe(ZMQ.SUBSCRIPTION_ALL);
			log4j.info("Notify subscriber client started");
			while (isRunning.get()) {
				byte[] msg = subServerSock.recv(0);
				if (msg != null) {
					ByteBuffer msgBuffer = ByteBuffer.wrap(msg);
					processMessage(msgBuffer);
				}
			}

			log4j.info("Notify subscriber client shut down");
		} else
			log4j.error("Subscriber socket not available, ensure proper initialisation");

		// set the running flag to false
		isRunning.compareAndSet(true, false);
	}

	/**
	 * Method to process the notification messages. This method handles the
	 * DIRTY_OBJECT and LOGOFF messages. Processing DIRTY_OBJECT: the method it
	 * self calls the SvWriter.cacheCleanup to remove dirty objects from the
	 * cache. Processing LOGOFF: the method it self calls the SvSecurity.logoff
	 * to perform log-off.
	 * 
	 * @param msgBuffer
	 */
	static void processMessage(ByteBuffer msgBuffer) {
		boolean hasObjects = true;
		byte msgType = msgBuffer.get();
		int objectCount = 0;
		if (msgType == SvCluster.MSG_DIRTY_OBJECT) {
			long objectTypeId = msgBuffer.getLong();
			while (hasObjects) {
				try {
					long objectId = msgBuffer.getLong();
					if (log4j.isDebugEnabled())
						log4j.debug("Received dirty notification for object with id:" + objectId + " and type "
								+ objectTypeId);
					SvWriter.cacheCleanup(objectId, objectTypeId);
					objectCount++;
				} catch (SvException e) {
					log4j.info("Object dirty message not processed", e);
				} catch (BufferUnderflowException ex) {
					hasObjects = false;
					log4j.debug("Received dirty notification for " + objectCount + " objects of type " + objectTypeId);
				}
			}

		} else if (msgType == SvCluster.MSG_LOGOFF) {
			long mostSigBits = msgBuffer.getLong();
			long leastSigBits = msgBuffer.getLong();
			UUID uuid = new UUID(mostSigBits, leastSigBits);
			if (log4j.isDebugEnabled())
				log4j.debug("Received logoff notification with token:" + uuid.toString());
			SvSecurity svs = null;
			try {
				svs = new SvSecurity();
				svs.logoff(uuid.toString());
			} catch (SvException e) {
				log4j.info("Logoff message (for token " + uuid.toString() + ") was not processed", e);
			} finally {
				if (svs != null)
					svs.release();
			}
		}
	}

}
