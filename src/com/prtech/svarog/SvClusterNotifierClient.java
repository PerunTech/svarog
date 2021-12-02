package com.prtech.svarog;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

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
	private static final Logger log4j = SvConf.getLogger(SvClusterNotifierClient.class);

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
	 * Flag if the main client thread is running
	 */
	static final AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * Flag if the notifier is in Active state or shutdown
	 */
	static final AtomicBoolean isActive = new AtomicBoolean(false);

	/**
	 * ZMQ publisher socket used for publishing notifications to the cluster
	 */
	static ZMQ.Socket pubServerSock = null;
	/**
	 * ZMQ subscriber socket used for listening for notifications from the cluster
	 */
	static ZMQ.Socket subServerSock = null;

	/**
	 * Delimiter used by the ip address list
	 */
	static final String ipAddrDelimiter = ";";

	/**
	 * Timeout for receiving a message on the socket.
	 */
	static final int socketReceiveTimeout = 500;

	/**
	 * Method to initialise the notifier client. The client uses the ip address list
	 * and tries to connect the publisher and subscriber sockets to any of the
	 * available addresses. If connection is successful, returns true. The sockets
	 * connected on the next 2 ports above the heart beat port
	 * 
	 * @param ipAddressList List of ip addresses which is registered by the
	 *                      coordinator node
	 * @return True if the client connected to the coordinator successfully
	 */
	static boolean initClient(String ipAddressList) {
		if (!isActive.compareAndSet(false, true)) {
			log4j.debug("Notifier client is already active. Shutdown first");
			return false;
		}
		boolean initSuccess = false;
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
			initSuccess = (pubServerSock != null && subServerSock != null);

			if (initSuccess)
				break;
		}
		if (initSuccess)
			subServerSock.setReceiveTimeOut(socketReceiveTimeout);
		else {
			isActive.set(false);
			log4j.error("Notifier client failed to connect/bind to required ports");
		}
		return initSuccess;
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
			// do wait for the main thread to shutdown
			while (isActive.get())
				synchronized (isActive) {
					isActive.wait();
				}
		} catch (InterruptedException e) {
			log4j.warn("Shutdown procedure raised exception", e);
			Thread.currentThread().interrupt();
		}
		if (context != null) {
			context.close();
			context = null;
		}
		// notify the maintenance thread
		if (SvMaintenance.maintenanceSemaphore != null)
			synchronized (SvMaintenance.maintenanceSemaphore) {
				SvMaintenance.maintenanceSemaphore.notifyAll();
			}

	}

	/**
	 * Method to publish a dirty array notification to the other nodes in the
	 * cluster
	 * 
	 * @param dba The DbDataArray instance which should be published as dirty
	 * @throws SvException
	 */
	static public void publishDirtyArray(DbDataArray dba) throws SvException {
		synchronized (pubServerSock) {
			publishDirtyArray(dba, pubServerSock);
		}
	}

	/**
	 * Method to publish a dirty array notification to the other nodes in the
	 * cluster
	 * 
	 * @param dba    The DbDataArray instance which should be published as dirty
	 * @param socket The socket on which the dirty IDs should be sent (if the node
	 *               is coordinator, it shall send on the proxy socket. If the node
	 *               is worker, then it will publish on the publish socket of the
	 *               NotifierClient
	 * @throws SvException
	 * 
	 */
	static void publishDirtyArray(DbDataArray dba, ZMQ.Socket socket) throws SvException {
		if (socket != null) {
			ByteBuffer msgBuffer = null;
			Long currentType = 0L;
			int objectCount = 0;
			int totalCount = 0;
			int lastKnownBufferSize = 0;
			for (DbDataObject dbo : dba.getItems()) {
				if (currentType != dbo.getObjectType()) {
					if (objectCount > 0 && msgBuffer != null) {
						int finalByteCount = (1 + SvUtil.sizeof.LONG + (SvUtil.sizeof.LONG * objectCount));
						byte[] finalBytes = Arrays.copyOfRange(msgBuffer.array(), 0, finalByteCount);
						// send the previous buffer
						if (log4j.isDebugEnabled())
							log4j.debug("Sent dirty notification of array with ids:" + msgBuffer.toString()
									+ " to coordinator");
						if (!SvCluster.zmqSend(socket, finalBytes, ZMQ.DONTWAIT))
							log4j.error("Error publishing message to coordinator node");
					}
					objectCount = 0;
					currentType = dbo.getObjectType();
					lastKnownBufferSize = SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG
							+ (SvUtil.sizeof.LONG * (dba.size() - totalCount));
					msgBuffer = ByteBuffer.allocate(lastKnownBufferSize);
					msgBuffer.put(SvCluster.NOTE_DIRTY_OBJECT);
					msgBuffer.putLong(currentType);
				}
				assert (msgBuffer != null);
				msgBuffer.putLong(dbo.getObjectId());
				totalCount++;
				objectCount++;
			}
			// the array was finished so lets send the left over buffer if any
			if (objectCount > 0 && msgBuffer != null) {
				int finalByteCount = (1 + SvUtil.sizeof.LONG + (SvUtil.sizeof.LONG * objectCount));
				byte[] finalBytes = Arrays.copyOfRange(msgBuffer.array(), 0, finalByteCount);
				// send the previous buffer
				if (log4j.isDebugEnabled())
					log4j.trace(
							"Sent dirty notification of array with ids:" + msgBuffer.toString() + " to coordinator");
				if (!SvCluster.zmqSend(socket, finalBytes, ZMQ.DONTWAIT))
					log4j.error("Error publishing message to coordinator node");
			}
		} else if (log4j.isDebugEnabled())
			log4j.debug("Publisher socke is null! Notifier client not started!");
	}

	/**
	 * Method to publish a dirty tiles notification to the other nodes in the
	 * cluster
	 * 
	 * @param dba    The DbDataArray instance which should be published as dirty
	 * @param socket The socket on which the dirty IDs should be sent (if the node
	 *               is coordinator, it shall send on the proxy socket. If the node
	 *               is worker, then it will publish on the publish socket of the
	 *               NotifierClient
	 * @throws SvException
	 * 
	 */
	static void publishDirtyTileArray(Set<SvSDITile> tiles) throws SvException {
		synchronized (pubServerSock) {
			publishDirtyTileArray(tiles, pubServerSock);
		}
	}

	/**
	 * Method to publish a dirty tiles notification to the other nodes in the
	 * cluster
	 * 
	 * @param dba    The DbDataArray instance which should be published as dirty
	 * @param socket The socket on which the dirty IDs should be sent (if the node
	 *               is coordinator, it shall send on the proxy socket. If the node
	 *               is worker, then it will publish on the publish socket of the
	 *               NotifierClient
	 * @throws SvException
	 * 
	 */
	static void publishDirtyTileArray(Set<SvSDITile> tiles, ZMQ.Socket socket) throws SvException {
		if (socket != null) {
			ByteBuffer msgBuffer = null;
			Long currentType = 0L;
			int objectCount = 0;
			int totalCount = 0;
			for (SvSDITile tile : tiles) {
				if (currentType != tile.getTileTypeId()) {
					if (objectCount > 0 && msgBuffer != null) {
						byte[] finalBytes = null;
						if ((1 + SvUtil.sizeof.LONG + (SvUtil.sizeof.INT * 2 * objectCount)) < msgBuffer.capacity())
							finalBytes = Arrays.copyOfRange(msgBuffer.array(), 0,
									(1 + SvUtil.sizeof.LONG + (SvUtil.sizeof.INT * 2 * objectCount)));
						else
							finalBytes = msgBuffer.array();
						// send the previous buffer
						if (log4j.isDebugEnabled())
							log4j.debug("Sent dirty notification of array with ids:" + msgBuffer.toString()
									+ " to coordinator");
						if (!SvCluster.zmqSend(socket, finalBytes, ZMQ.DONTWAIT))
							log4j.error("Error publishing message to coordinator node");
					}
					objectCount = 0;
					currentType = tile.getTileTypeId();
					// every message will have the message type, the tile type
					// (long) and two integers for the cell multiplied by the
					// number of tiles
					msgBuffer = ByteBuffer.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG
							+ (SvUtil.sizeof.INT * 2 * (tiles.size() - totalCount)));
					msgBuffer.put(SvCluster.NOTE_DIRTY_TILE);
					msgBuffer.putLong(currentType);
				}
				int[] cell = new int[2];
				tile.getTilelId(cell);
				msgBuffer.putInt(cell[0]);
				msgBuffer.putInt(cell[1]);
				totalCount++;
				objectCount++;
			}
			// the array was finished so lets send the left over buffer if any
			if (objectCount > 0 && msgBuffer != null) {
				byte[] finalBytes = Arrays.copyOfRange(msgBuffer.array(), 0,
						(1 + SvUtil.sizeof.LONG + (SvUtil.sizeof.INT * 2 * objectCount)));
				// send the previous buffer
				if (log4j.isDebugEnabled())
					log4j.debug(
							"Sent dirty notification of array with ids:" + msgBuffer.toString() + " to coordinator");
				if (!SvCluster.zmqSend(socket, finalBytes, ZMQ.DONTWAIT))
					log4j.error("Error publishing message to coordinator node");
			}
		} else if (log4j.isDebugEnabled())
			log4j.debug("Publisher socke is null! Notifier client not started!");
	}

	/**
	 * Method to publish a dirty object notification to the other nodes in the
	 * cluster
	 * 
	 * @param objectId     The id of the dirty object
	 * @param objectTypeId The type of the dirty object
	 * @throws SvException
	 */
	static public void publishDirtyObject(long objectId, long objectTypeId) throws SvException {
		synchronized (pubServerSock) {
			if (pubServerSock != null) {
				ByteBuffer msgBuffer = ByteBuffer
						.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + SvUtil.sizeof.LONG);
				msgBuffer.put(SvCluster.NOTE_DIRTY_OBJECT);
				msgBuffer.putLong(objectTypeId);
				msgBuffer.putLong(objectId);
				if (log4j.isDebugEnabled())
					log4j.debug("Sent dirty notification of object with id:" + objectId + " to coordinator");
				if (!SvCluster.zmqSend(pubServerSock, msgBuffer.array(), ZMQ.DONTWAIT))
					log4j.error("Error publishing message to coordinator node");
			} else if (log4j.isDebugEnabled())
				log4j.debug("Publisher socke is null! Notifier client not started!");
		}
	}

	/**
	 * Method to publish a logoff notification to the other nodes in the cluster
	 * 
	 * @param sessionId String representation of the user session/token
	 * @throws SvException
	 */
	static public void publishLogoff(String sessionId) throws SvException {
		synchronized (pubServerSock) {
			if (pubServerSock != null) {
				ByteBuffer msgBuffer = ByteBuffer
						.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + SvUtil.sizeof.LONG);
				msgBuffer.put(SvCluster.NOTE_LOGOFF);
				UUID uuid = UUID.fromString(sessionId);
				msgBuffer.putLong(uuid.getMostSignificantBits());
				msgBuffer.putLong(uuid.getLeastSignificantBits());
				if (log4j.isDebugEnabled())
					log4j.debug("Sent logoff notification with token:" + uuid.toString() + " to the coordinator");
				if (!SvCluster.zmqSend(pubServerSock, msgBuffer.array(), ZMQ.DONTWAIT))
					log4j.error("Error publishing message to coordinator node");
			} else if (log4j.isDebugEnabled())
				log4j.debug("Publisher socke is null! Notifier client not started!");
		}
	}

	/**
	 * Method to publish a logoff notification to the other nodes in the cluster
	 * 
	 * @param sessionId String representation of the user session/token
	 * @throws SvException
	 */
	static public void publishAck(Integer ackValue, byte successMsg) throws SvException {
		synchronized (pubServerSock) {
			if (pubServerSock != null) {
				ByteBuffer msgBuffer = ByteBuffer
						.allocate(SvUtil.sizeof.BYTE + SvUtil.sizeof.LONG + SvUtil.sizeof.INT + 1);
				msgBuffer.put(SvCluster.NOTE_ACK);
				msgBuffer.putLong(SvClusterClient.nodeId);
				msgBuffer.putInt(ackValue);
				msgBuffer.put(successMsg);
				if (log4j.isDebugEnabled())
					log4j.debug(
							"Sent ack notification with value:" + Integer.toString(ackValue) + " to the coordinator");
				if (!SvCluster.zmqSend(pubServerSock, msgBuffer.array(), 0))
					log4j.error("Error publishing message to coordinator node");
			} else if (log4j.isDebugEnabled())
				log4j.debug("Publisher socket is null! Notifier client not started!");
		}
	}

	/**
	 * Runnable method for running the main Notifier client thread. It will receive
	 * messages from the subscriber socked, process them through the processMessage
	 * method, the
	 */
	@Override
	public void run() {
		if (isActive.get() && !isRunning.compareAndSet(false, true)) {
			log4j.error(this.getClass().getName() + ".run() failed. Current status is active:" + isActive.get()
					+ ". Main server thread is running:" + isRunning.get());
			return;
		}

		if (subServerSock != null) {
			subServerSock.subscribe(ZMQ.SUBSCRIPTION_ALL);
			log4j.info("Notify subscriber client started");
			while (isRunning.get()) {
				try {
					byte[] msg = SvCluster.zmqRecv(subServerSock, 0);
					if (msg != null) {
						ByteBuffer msgBuffer = ByteBuffer.wrap(msg);
						processMessage(msgBuffer);
					}
				} catch (SvException e) {
					if (e.getLabelCode().equals(Sv.Exceptions.CLUSTER_INACTIVE))
						isRunning.set(false);
					else
						log4j.error("Error processing notification", e);
				}
			}
			log4j.info("Notify subscriber client shut down");
		} else
			log4j.error("Subscriber socket not available, ensure proper initialisation");

		// set the active flag to false and notify the waiting threads
		isActive.set(false);
		synchronized (isActive) {
			isActive.notifyAll();
		}
	}

	/**
	 * Method for processing lock notification NOTE_LOCK_ACQUIRED
	 * 
	 * @param msgBuffer The buffer associated with the lock acquired message
	 * @return If the message was processed, return true
	 * @throws SvException
	 */
	private static boolean processLockAcquired(ByteBuffer msgBuffer) throws SvException {
		Long remoteNodeId = msgBuffer.getLong();
		Integer ackValue = msgBuffer.getInt();
		String lockKey = new String(Arrays.copyOfRange(msgBuffer.array(), 1 + SvUtil.sizeof.LONG + SvUtil.sizeof.INT,
				msgBuffer.array().length), ZMQ.CHARSET);
		byte lockResult = SvCluster.MSG_FAIL;
		// get the key then acquire, then update the hash
		if (!remoteNodeId.equals(SvClusterClient.nodeId)) {
			ReentrantLock lock = SvClusterClient.acquireDistributedLock(lockKey, remoteNodeId);
			if (lock != null && SvClusterClient.updateDistributedLock(lockKey, ackValue))
				lockResult = SvCluster.MSG_SUCCESS;
		} else if (SvClusterClient.updateDistributedLock(lockKey, ackValue))
			lockResult = SvCluster.MSG_SUCCESS;

		publishAck(ackValue, lockResult);
		return lockResult == SvCluster.MSG_SUCCESS;
	}

	/**
	 * Method for processing lock notification NOTE_LOCK_ACQUIRED
	 * 
	 * @param msgBuffer The buffer associated with the lock acquired message
	 * @return If the message was processed, return true
	 */
	private static boolean processLockReleased(ByteBuffer msgBuffer) {
		Long remoteNodeId = msgBuffer.getLong();
		Integer ackValue = msgBuffer.getInt();
		// get the key then acquire, then update the hash
		if (!remoteNodeId.equals(SvClusterClient.nodeId)) {
			String lock = SvClusterClient.releaseDistributedLock(ackValue, remoteNodeId);
			return lock != null;
		} else
			return true;
	}

	/**
	 * Method to process the notification messages. This method handles the
	 * DIRTY_OBJECT and LOGOFF messages. Processing DIRTY_OBJECT: the method it self
	 * calls the SvWriter.cacheCleanup to remove dirty objects from the cache.
	 * Processing LOGOFF: the method it self calls the SvSecurity.logoff to perform
	 * log-off.
	 * 
	 * @param msgBuffer
	 * @throws SvException
	 */
	static void processMessage(ByteBuffer msgBuffer) throws SvException {
		boolean hasObjects = true;
		byte msgType = msgBuffer.get();
		int objectCount = 0;
		switch (msgType) {
		case SvCluster.NOTE_DIRTY_OBJECT: {

			long objectTypeId = msgBuffer.getLong();
			while (hasObjects) {
				try {
					long objectId = msgBuffer.getLong();
					if (log4j.isDebugEnabled())
						log4j.trace("Received dirty notification for object with id:" + objectId + " and type "
								+ objectTypeId);
					SvWriter.cacheCleanup(objectId, objectTypeId);
					objectCount++;
				} catch (SvException e) {
					log4j.info("Object dirty message not processed", e);
				} catch (BufferUnderflowException ex) {
					hasObjects = false;
					if (log4j.isDebugEnabled())
						log4j.trace(
								"Received dirty notification for " + objectCount + " objects of type " + objectTypeId);
				}
			}

		}
			break;
		case SvCluster.NOTE_DIRTY_TILE: {
			long tileTypeId = msgBuffer.getLong();
			while (hasObjects) {
				try {
					int[] cell = new int[2];
					cell[0] = msgBuffer.getInt();
					cell[1] = msgBuffer.getInt();
					if (log4j.isDebugEnabled())
						log4j.trace("Received dirty notification for tile with id:" + cell + " and type " + tileTypeId);
					SvGeometry.markDirtyTile(tileTypeId, cell);

					objectCount++;
				} catch (BufferUnderflowException | SvException ex) {
					hasObjects = false;
					if (log4j.isDebugEnabled())
						log4j.trace("Received dirty notification " + msgBuffer.toString() + " objects of type "
								+ tileTypeId);
				}
			}

		}
			break;
		case SvCluster.NOTE_LOGOFF: {
			long mostSigBits = msgBuffer.getLong();
			long leastSigBits = msgBuffer.getLong();
			UUID uuid = new UUID(mostSigBits, leastSigBits);
			if (log4j.isDebugEnabled())
				log4j.debug("Received logoff notification with token:" + uuid.toString());

			try (SvSecurity svs = new SvSecurity()) {

				svs.logoff(uuid.toString());
			} catch (SvException e) {
				log4j.info("Logoff message (for token " + uuid.toString() + ") was not processed", e);
			}
		}
			break;
		case SvCluster.NOTE_LOCK_ACQUIRED: {
			processLockAcquired(msgBuffer);
		}
			break;
		case SvCluster.NOTE_LOCK_RELEASED: {
			processLockReleased(msgBuffer);
		}
			break;
		}

	}

}
