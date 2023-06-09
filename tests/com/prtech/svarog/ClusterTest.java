package com.prtech.svarog;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DboFactory;

public class ClusterTest {

	@BeforeClass
	public static void init() throws SQLException {


		SvConf.setClusterEnabled(false);
		// SvMaintenance.shutdown();
		SvClusterClient.shutdown();
		SvClusterNotifierClient.shutdown();
		SvClusterServer.shutdown();
		SvClusterNotifierProxy.shutdown();
		SvCluster.shutdown();
		SvLock.clearLocks();

	}

	public void promoteToCoordinator() {
		System.out.print("Test promoteToCoordinator");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvClusterClient.heartBeatTimeOut = 1000;
			SvClusterClient.forcePromotionOnShutDown = true;
			SvClusterNotifierProxy.processNotification = true;
			SvCluster.initCluster();
			Thread.sleep(50);
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");

			// to support the slow debug
			// SvClusterClient.heartBeatTimeOut = 5000;
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			Thread.sleep(50); // some milis to start all threads properly and we
								// don't hit the slow joiner syndrome
			String lockKey = "TEST_LOCK_SINGLE_PROMOTION";

			// validate a random token and see if the validation fails
			int lockHash = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (!ClusterTest.lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was NOT present on the Cluster after acquiring");

			Thread.sleep(2 * SvConf.getHeartBeatInterval());
			SvCluster.shutdown();
			SvConf.setClusterEnabled(true);
			DateTime tsTimeout = DateTime.now().withDurationAdded(SvConf.getHeartBeatTimeOut(), 1);
			boolean didWePromote = false;
			while (tsTimeout.isAfterNow()) {
				if (SvCluster.isCoordinator() && SvClusterServer.isRunning.get()) {
					didWePromote = true;
					break;
				}
				Thread.sleep(500);
			}

			SvConf.setClusterEnabled(false);
			// rejoin with new clients
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			Thread.sleep(500);

			if (!didWePromote)
				fail("Promotion to coordinator failed");

			if (!SvClusterClient.localDistributedLocks.containsKey(lockKey))
				fail("Lock got lost in the process of promotion and didn't reach the client");
			// return heart beat to normal for other tests
			SvClusterClient.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();

		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {
			try {
				SvCluster.resignCoordinator();
			} catch (SvException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.print("Perform cleanup");
			SvMaintenance.shutdown();
			SvClusterClient.shutdown();
			SvClusterNotifierClient.shutdown();
			SvClusterServer.shutdown();
			SvClusterNotifierProxy.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	@AfterClass
	public static void after() {
		SvConf.setClusterEnabled(true);
	}

	@Test
	public void heartbeatTest() {
		System.out.print("Test heartbeatTest");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			if (SvCluster.initCluster()) {
				String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
				SvClusterClient.initClient(ipAddressList);
				// SvClusterClient.nodeId = 666;
				Thread clientThread = new Thread(new SvClusterClient());
				// start the heart beat thread and sleep for 3 intervals
				clientThread.start();
				DateTime hbStartTime = DateTime.now();
				Thread.sleep(3 * SvConf.getHeartBeatInterval());

				if (!SvClusterClient.lastContact.isAfter(hbStartTime))
					fail("The heart beat client didn't establish contact");
			} else
				fail("Can't init cluster");
		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void localHeartbeatTest() {
		System.out.print("Test heartbeatTest");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();

			SvClusterClient.initClient("127.0.0.1:" + SvConf.getHeartBeatPort());
			// SvClusterClient.nodeId = 666;
			Thread clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();
			DateTime hbStartTime = DateTime.now();
			Thread.sleep(3 * SvConf.getHeartBeatInterval());

			if (!SvClusterClient.lastContact.isAfter(hbStartTime))
				fail("The heart beat client didn't establish contact");

		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {

			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	@Test
	public void heartbeatServerFail() {
		System.out.print("Test heartbeatServerFail");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.heartBeatTimeOut = 1000;
			SvClusterClient.heartBeatInterval = 1000;
			// we turn off the promotion to allow testing of failure
			SvClusterClient.forcePromotionOnShutDown = false;
			SvClusterClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 666;

			Thread clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();
			DateTime hbStartTime = DateTime.now();
			Thread.sleep(2 * SvConf.getHeartBeatInterval());

			SvClusterServer.shutdown();
			Thread.sleep(2 * SvConf.getHeartBeatInterval());

			if (SvClusterClient.isRunning.get())
				fail("Client didn't detect server was shutting down");
			// return heart beat to normal for other tests
			SvClusterClient.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {

			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	@Test
	public void heartbeatClientFail() {
		System.out.print("Test heartbeatClientFail");
		try {
			SvCore.initSvCore();
			SvClusterServer.heartBeatTimeOut = 1000;
			SvClusterClient.heartBeatInterval = 3000;
			SvCluster.autoStartClient = false;

			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			// we turn off the promotion to allow testing of failure
			SvClusterClient.forcePromotionOnShutDown = false;
			SvClusterClient.rejoinOnFailedHeartBeat = false;
			SvClusterClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 666;

			Thread clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();
			Thread.sleep(3 * SvClusterServer.heartBeatTimeOut);

			// server should have removed the client by now since the heart beat
			// interval was 5seconds
			if (SvClusterServer.nodeHeartBeats.containsKey(SvClusterClient.nodeId))
				fail("client was not removed as result of time out");

			Thread.sleep(3 * SvClusterServer.heartBeatTimeOut);
			if (SvClusterClient.isRunning.get())
				fail("Client should have shut down!");

			// once again start
			SvClusterServer.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
			SvClusterClient.heartBeatInterval = SvConf.getHeartBeatInterval();
			SvClusterClient.rejoinOnFailedHeartBeat = true; // set to rejoin on
			SvClusterClient.initClient(ipAddressList);

			// fail
			clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();
			Thread.sleep(SvClusterServer.heartBeatTimeOut);

			// server should have removed the client by now since the heart beat
			// interval was 5seconds
			if (!SvClusterServer.nodeHeartBeats.containsKey(SvClusterClient.nodeId))
				fail("client was removed as result of time out");

			// force removal from the cluster
			SvClusterServer.nodeHeartBeats.remove(SvClusterClient.nodeId);
			// sleep and wait for client to rejoin
			Thread.sleep(SvClusterServer.heartBeatTimeOut);
			// see if the client rejoined
			if (!SvClusterServer.nodeHeartBeats.containsKey(SvClusterClient.nodeId))
				fail("client did not rejoin after being removed");

			// return heart beat to normal for other tests
			SvClusterClient.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {

			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	
	//@Test
	public void heartbeatClientFailLockTest() {
		System.out.print("Test heartbeatClientFailLockTest");
		int lockHash2 = 0;
		int lockHash = 0;
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvClusterServer.heartBeatTimeOut = 10000;
			SvClusterClient.heartBeatInterval = 3000;
			SvClusterServer.clusterMainentanceInterval = 5;
			SvCluster.initCluster();
			Thread.sleep(5);
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			// we turn off the promotion to allow testing of failure
			SvClusterClient.forcePromotionOnShutDown = false;
			SvClusterClient.rejoinOnFailedHeartBeat = false;
			SvClusterClient.initClient(ipAddressList);

			SvClusterNotifierClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			Thread clientnotThread = new Thread(new SvClusterNotifierClient());
			clientnotThread.start();

			// SvClusterClient.nodeId = 666;

			String lockKey = "LAST_LOCK777";
			Thread clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();

			Thread.sleep(50);
			ReentrantLock l = SvClusterClient.acquireDistributedLock(lockKey, SvClusterClient.nodeId);
			lockHash = SvClusterClient.getLock(lockKey);
			SvClusterServer.heartBeatTimeOut = 1000;
			if (lockHash == 0)
				fail("cant get lock");
			Thread.sleep(3 * SvClusterServer.heartBeatTimeOut);

			// server should have removed the client by now since the heart beat
			// interval was 5seconds
			if (SvClusterServer.nodeHeartBeats.containsKey(SvClusterClient.nodeId))
				fail("client was not removed as result of time out");

			Thread.sleep(3 * SvClusterServer.heartBeatTimeOut);
			if (SvClusterClient.isRunning.get())
				fail("Client should have shut down!");

			long originalId = SvClusterClient.nodeId;
			SvClusterClient.nodeId = 777;

			// once again start
			SvClusterServer.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
			SvClusterClient.heartBeatInterval = SvConf.getHeartBeatInterval();
			SvClusterClient.rejoinOnFailedHeartBeat = true; // set to rejoin on
			SvClusterClient.initClient(ipAddressList);

			// fail
			clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();
			Thread.sleep(200);
			lockHash2 = SvClusterClient.getLock(lockKey);
			if (lockHash2 == 0)
				fail("cant get lock although the first lock should have been removed as timeout");

			// return heart beat to normal for other tests
			SvClusterClient.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {
			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	@Test
	public void ClusterLongHearBeatTest() {
		System.out.print("Test ClusterLongHeartBeat");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvClusterClient.heartBeatInterval = 500;
			SvClusterServer.heartBeatTimeOut = 2000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");

			SvClusterClient.rejoinOnFailedHeartBeat = true; // set to rejoin on
			SvClusterClient.nodeId = 0L;
			SvClusterClient.initClient(ipAddressList);
			try {
				Thread clientThread = new Thread(new SvClusterClient());
				clientThread.start();
				while (SvClusterClient.nodeId == 0L)
					Thread.sleep(200);
				Long nid = SvClusterClient.nodeId;
				Thread.sleep(4000);
				if (!nid.equals(SvClusterClient.nodeId))
					fail("nodeId has changed, due to re-join");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	@Test
	public void ClusterDirtyObjectTest() {
		System.out.print("Test ClusterDirtyObjectTest");
		SvCluster.shutdown();
		SvLock.clearLocks();
		SvSecurity svs = null;
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			// this sleep is just to give threads some time to boot.
			Thread.sleep(50);
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");

			SvClusterNotifierClient.initClient(ipAddressList);
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			// get a new token from the roles test
			String secondToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			DbDataObject dboToken = DbCache.getObject(secondToken, svCONST.OBJECT_TYPE_SECURITY_LOG);

			// send dirty notification
			SvClusterNotifierClient.publishDirtyObject(dboToken.getObjectId(), dboToken.getObjectType());

			// get the timeout
			DateTime tsTimeout = DateTime.now().withDurationAdded(SvConf.getHeartBeatTimeOut(), 1);
			// if shut down in progress, wait to finish.
			dboToken = DbCache.getObject(dboToken.getObjectId(), dboToken.getObjectType());
			while (tsTimeout.isAfterNow() && dboToken != null) {
				// sleep few milis to ensure the message passed through the
				// proxy
				Thread.sleep(200);
				dboToken = DbCache.getObject(dboToken.getObjectId(), dboToken.getObjectType());
			}
			// now check the cache again ... the token shouldn't be there
			if (dboToken != null)
				fail("Dirty object still in cache!");

			SvClusterNotifierClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (svs != null)
				svs.release();
			SvClusterNotifierClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();

		}
	}

	@Test
	public void ClusterDirtyArrayMultiTypeTest() {
		System.out.print("Test ClusterDirtyArrayMultiTypeTest");
		SvCluster.shutdown();
		SvLock.clearLocks();
		SvSecurity svs = null;
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");

			SvClusterNotifierClient.initClient(ipAddressList);
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			// now test the array clean up
			DbDataArray array = new DbDataArray();
			// get one more token
			String secondToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			DbDataObject dboToken = DbCache.getObject(secondToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
			if (dboToken != null)
				array.addDataItem(dboToken);
			else
				fail("can't get object from cache");
			

			for(int i=0; i<3; i++)
			{
				DbDataObject dbod = new DbDataObject(svCONST.OBJECT_TYPE_CONVERSATION);
				dbod.setObjectId(new Long(i+123));
				array.addDataItem(dbod);

			}
			// get one more token
			secondToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			dboToken = DbCache.getObject(secondToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
			if (dboToken != null)
				array.addDataItem(dboToken);
			else
				fail("can't get object from cache");

			for(int i=0; i<3; i++)
			{
				DbDataObject dbod = new DbDataObject(svCONST.OBJECT_TYPE_ACL);
				dbod.setObjectId(new Long(i+12355));
				array.addDataItem(dbod);

			}
			// send dirty notification
			SvClusterNotifierClient.publishDirtyArray(array);

			// sleep few milis to ensure the message passed through the proxy
			Thread.sleep(200);
			// now check the cache again ... the token shouldn't be there
			dboToken = DbCache.getObject(array.get(0).getObjectId(), array.get(0).getObjectType());
			if (dboToken != null)
				fail("Dirty object 1 still in cache!");

			dboToken = DbCache.getObject(array.get(1).getObjectId(), array.get(1).getObjectType());
			if (dboToken != null)
				fail("Dirty object 2 still in cache!");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (svs != null)
				svs.release();
			SvClusterNotifierClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	
	@Test
	public void ClusterDirtyArrayTest() {
		System.out.print("Test ClusterDirtyObjectTest");
		SvCluster.shutdown();
		SvLock.clearLocks();
		SvSecurity svs = null;
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");

			SvClusterNotifierClient.initClient(ipAddressList);
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			// now test the array clean up
			DbDataArray array = new DbDataArray();
			// get one more token
			String secondToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			DbDataObject dboToken = DbCache.getObject(secondToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
			if (dboToken != null)
				array.addDataItem(dboToken);
			else
				fail("can't get object from cache");
			// get one more token
			secondToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			dboToken = DbCache.getObject(secondToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
			if (dboToken != null)
				array.addDataItem(dboToken);
			else
				fail("can't get object from cache");

			// send dirty notification
			SvClusterNotifierClient.publishDirtyArray(array);

			// sleep few milis to ensure the message passed through the proxy
			Thread.sleep(200);
			// now check the cache again ... the token shouldn't be there
			dboToken = DbCache.getObject(array.get(0).getObjectId(), array.get(0).getObjectType());
			if (dboToken != null)
				fail("Dirty object 1 still in cache!");

			dboToken = DbCache.getObject(array.get(1).getObjectId(), array.get(1).getObjectType());
			if (dboToken != null)
				fail("Dirty object 2 still in cache!");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (svs != null)
				svs.release();
			SvClusterNotifierClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterInit() {
		System.out.print("Test ClusterInit");
		SvClusterClient.initClient("192.168.0.234");
		// SvClusterClient.nodeId = 666;
		Thread clientThread = new Thread(new SvClusterClient());
		clientThread.start();
		// sleep to let the heartbeat start
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SvClusterClient.shutdown();
		// now good init

		try {
			SvCore.initSvCore();
		} catch (SvException e1) {
			// TODO Auto-generated catch block
			fail("Can't initialise Svarog");
		}

		try {
			Thread.sleep(500);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if (!SvCluster.initCluster())
			fail("Can't init cluster");
		String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
		SvClusterClient.initClient(ipAddressList);
		// SvClusterClient.nodeId = 666;
		clientThread = new Thread(new SvClusterClient());
		clientThread.start();
		try {
			Thread.sleep(200);
			SvCluster.shutdown();
			SvLock.clearLocks();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}

	}

	@Test
	public void ClusterAuthenticationTest() {
		System.out.print("Test ClusterAuthenticationTest");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			Thread.sleep(5);
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterClient.nodeId = 666;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			// sleep to let the heartbeat start
			Thread.sleep(5);

			// validate a random token and see if the validation fails
			DbDataObject token = SvClusterClient.getToken(UUID.randomUUID().toString());
			if (token != null)
				fail("Validating random token must fail");

			// get a new token from the roles test
			String newToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			DbDataObject localToken = DbCache.getObject(newToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
			// make sure we remove it from the cache
			DbCache.removeObject(localToken.getObjectId(), newToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
			// now lets try to put it back via the cluster
			if (!SvClusterClient.putToken(localToken))
				fail("Could not put the token in the coordinator");

			// now the token should be again in the cache .. lets try to
			// validate
			token = SvClusterClient.getToken(newToken);
			if (token == null)
				fail("Validation of normal token can't fail!");

			if (!token.toSimpleJson().toString().equals(localToken.toSimpleJson().toString())) {
				fail("Token is not equal");
			}

			if (!SvClusterClient.refreshToken(newToken))
				fail("Can't refresh token");

			// make sure we remove it from the cache
			DbCache.removeObject(localToken.getObjectId(), newToken, svCONST.OBJECT_TYPE_SECURITY_LOG);

			if (SvClusterClient.refreshToken(newToken))
				fail("Refresh token was success after logoff");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterLogoffTest() {
		System.out.print("Test ClusterLogoffTest");
		try {

			SvClusterClient.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
			SvCore.initSvCore();
			SvClusterClient.shutdown(false);
			SvClusterNotifierClient.shutdown();
			SvClusterNotifierProxy.processNotification = true;
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");

			SvClusterNotifierClient.initClient(ipAddressList);
			SvClusterClient.initClient(ipAddressList);

			Thread hbThread = new Thread(new SvClusterClient());
			hbThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			// sleep to let the heartbeat start
			Thread.sleep(500);

			// validate a random token and see if the validation fails
			DbDataObject token = null;

			// get a new token from the roles test
			String newToken = SvarogRolesTest.getUserToken(false);
			// get the Dbo representation
			DbDataObject localToken = DbCache.getObject(newToken, svCONST.OBJECT_TYPE_SECURITY_LOG);

			// publish the logoff event
			SvClusterNotifierClient.publishLogoff(newToken);

			// sleep to let the logoff become effective

			int timeOut = SvClusterClient.heartBeatTimeOut;

			while (timeOut > 0 && localToken != null) {
				localToken = DbCache.getObject(newToken, svCONST.OBJECT_TYPE_SECURITY_LOG);
				Thread.sleep(200);
				timeOut -= 200;
			}

			// now the token should be invalid
			if (localToken != null)
				fail("Logoff failed!");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterClient.shutdown();
			SvClusterNotifierClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterLockAcquire() {
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			// to support the slow debug
			// SvClusterClient.heartBeatTimeOut = 50000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();
			// sleep to let the heartbeat start
			Thread.sleep(200);

			String lockKey = "TEST_LOCK_SINGLE";
			// validate a random token and see if the validation fails
			int lockHash = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (lockHash == 0)
				fail("Error acquiring lock");

			if (!lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was NOT present on the Cluster after acquiring");

			if (!SvClusterClient.releaseLock(lockHash))
				fail("Failed Lock release lock");

			if (!lockOnServer(lockKey, lockHash, false, SvClusterClient.nodeId))
				fail("Lock was present on the Cluster after release");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterSvLockAcquire() {
		System.out.print("Test ClusterSvLockAcquire");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			// to support the slow debug
			// SvClusterClient.heartBeatTimeOut = 50000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			// sleep to let the heartbeat start
			Thread.sleep(200);

			String lockKey = "TEST_LOCK_SINGLE";
			// validate a random token and see if the validation fails
			int lockHash = SvLock.getDistributedLock(lockKey);
			if (lockHash == 0)
				fail("Error acquiring lock");

			if (!lockOnServer(lockKey, lockHash, true,
					SvCluster.isCoordinator() ? SvCluster.getCoordinatorNode().getObjectId() : SvClusterClient.nodeId))
				fail("Lock was NOT present on the Cluster after acquiring");

			if (!SvLock.releaseDistributedLock(lockHash))
				fail("Failed Lock release lock");

			if (!lockOnServer(lockKey, lockHash, false,
					SvCluster.isCoordinator() ? SvCluster.getCoordinatorNode().getObjectId() : SvClusterClient.nodeId))
				fail("Lock was present on the Cluster after release");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterLockAck() {
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvClusterNotifierProxy.processNotification = true;
			// to support the slow debug
			// SvClusterClient.heartBeatTimeOut = 5000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			// sleep to let the heartbeat start
			Thread.sleep(200);

			String lockKey = "TEST_LOCK_SINGLE_ACK";
			HashSet<Long> nodes = new HashSet<Long>();
			nodes.addAll(SvClusterServer.nodeHeartBeats.keySet());
			SvClusterNotifierProxy.nodeAcks.put(999, nodes);

			SvClusterNotifierProxy.publishLockAction(SvCluster.NOTE_LOCK_ACQUIRED, 999, 666, lockKey);
			if (!SvClusterNotifierProxy.waitForAck(999, SvClusterClient.heartBeatTimeOut))
				fail("Ack failed");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterClient.shutdown();
			SvClusterNotifierClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
			SvLock.clearLocks();
		}

	}

	@Test
	public void ClusterImplLockAcquire() {
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;

			// to support the slow debug
			// SvClusterClient.heartBeatTimeOut = 50000;
			// SvClusterClient.heartBeatInterval = 3000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			String lockKey = "TEST_LOCK_SINGLE";
			// validate a random token and see if the validation fails
			Thread.sleep(50);

			int lockHash = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (lockHash == 0)
				fail("Error acquiring lock");

			if (!lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was NOT present on the Cluster after acquiring");

			if (!SvLock.releaseDistributedLockImpl(lockHash, false, SvClusterClient.nodeId))
				fail("Failed Lock release lock");

			if (!lockOnServer(lockKey, lockHash, false, SvClusterClient.nodeId))
				fail("Lock was present on the Cluster after release");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void clusterLockMigrationTest() {
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;

			// to support the slow debug
			SvClusterClient.heartBeatTimeOut = 500;
			SvClusterServer.heartBeatTimeOut = 500;
			SvClusterClient.heartBeatInterval = 1500;
			SvClusterClient.rejoinOnFailedHeartBeat = true;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();

			String lockKey = "TEST_LOCK_MIGRATION";
			// validate a random token and see if the validation fails
			Thread.sleep(50);

			int lockHash = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (lockHash == 0)
				fail("Error acquiring lock");

			Thread.sleep(50);
			Long oldNode = SvClusterClient.nodeId;
			if (!lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was NOT present on the Cluster after acquiring");

			while (SvClusterClient.nodeId == oldNode) {
				Thread.sleep(200);
			}

			if (!lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was not present on the Cluster after rejoin");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	static boolean lockOnServer(String lockKey, int lockHash, boolean present, long nodeId) {
		boolean lockFound = false;
		boolean result = false;
		SvCluster.DistributedLock dld = SvClusterServer.distributedLocks.get(lockKey);
		if (dld != null && dld.getNodeId().equals(nodeId))
			lockFound = true;

		result = !(present ^ lockFound);
		if (result) {
			lockFound = false;
			if (SvClusterServer.nodeLocks.containsKey(nodeId)) {
				CopyOnWriteArrayList<SvCluster.DistributedLock> dlocks = SvClusterServer.nodeLocks.get(nodeId);
				if (dlocks != null)
					for (SvCluster.DistributedLock d : dlocks)
						if (d.getLock().hashCode() == lockHash)
							lockFound = true;
			}
			result = !(present ^ lockFound);
		}
		return result;
	}

	@Test
	public void ClusterCompetingLockTest() {
		System.out.print("Test ClusterCompetingLockTest");
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvClusterClient.heartBeatTimeOut = 50000;
			SvClusterClient.heartBeatInterval = 50000;
			SvClusterServer.heartBeatTimeOut = 50000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();
			// sleep to let the heartbeat start
			Thread.sleep(200);

			String lockKey = "TEST_LOCK";
			// validate a random token and see if the validation fails
			// SvClusterClient.nodeId = 888;
			ReentrantLock l = SvClusterClient.acquireDistributedLock(lockKey, SvClusterClient.nodeId);
			int lockHash = SvClusterClient.getLock(lockKey);
			if (lockHash == 0)
				fail("Lock acquiring failed");

			if (!lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was present NOT on the Cluster after acquire");

			Long oldNodeId = SvClusterClient.nodeId;
			SvClusterClient.nodeId = 777;
			SvClusterClient.joinCluster();
			int lockFail = SvClusterClient.getLock(lockKey);
			if (lockFail != 0)
				fail("Lock acquired from competing node");

			if (!lockOnServer(lockKey, lockHash, true, oldNodeId))
				fail("Lock was NOT present on the Cluster after competing acquire");

			if (!lockOnServer(lockKey, lockHash, false, 777L))
				fail("Lock was present at the competing node on the Cluster after competing acquire");

			if (SvClusterClient.releaseLock(lockHash))
				fail("Lock released from wrong node!");

			if (!lockOnServer(lockKey, lockHash, true, oldNodeId))
				fail("Lock was NOT present on the Cluster after release from wrong node");

			if (!lockOnServer(lockKey, lockHash, false, 777L))
				fail("Lock was NOT absent for competing node");

			// back to the original node id
			SvClusterClient.nodeId = oldNodeId;
			if (!SvClusterClient.releaseLock(lockHash))
				fail("Lock can't be released from original node!");

			if (!lockOnServer(lockKey, lockHash, false, oldNodeId))
				fail("Lock was present on the Cluster after release");
			SvClusterClient.heartBeatTimeOut = SvConf.getHeartBeatTimeOut();
			SvClusterClient.heartBeatInterval = SvConf.getHeartBeatInterval();
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterReentrantLockTest() {
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			// SvClusterClient.heartBeatTimeOut = 50000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();
			Thread.sleep(200);

			String lockKey = "TEST_LOCK_123";
			// validate a random token and see if the validation fails
			int lockHash = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (lockHash == 0)
				fail("Error acquiring lock");

			int lockHash2 = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (lockHash2 != lockHash)
				fail("Lock is not re-entrant");

			Thread.sleep(200);
			if (!lockOnServer(lockKey, lockHash, true, SvClusterClient.nodeId))
				fail("Lock was present NOT on the Cluster after acquire");

			if (!SvClusterClient.releaseLock(lockHash2))
				fail("Failed to release lock");

			if (!SvClusterClient.releaseLock(lockHash))
				fail("Failed Lock release lock final");

			if (!lockOnServer(lockKey, lockHash, false, SvClusterClient.nodeId))
				fail("Lock was present on the Cluster after release");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void lockPersistsAfterJoin() {
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			// SvClusterClient.heartBeatTimeOut = 50000;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			SvClusterNotifierProxy.processNotification = true;
			SvClusterNotifierClient.initClient(ipAddressList);
			// SvClusterClient.nodeId = 999;
			// SvLock.isPreemtive = false;
			Thread clientThread = new Thread(new SvClusterClient());
			clientThread.start();
			Thread notifierThread = new Thread(new SvClusterNotifierClient());
			notifierThread.start();
			Thread.sleep(200);

			String lockKey = "TEST_LOCK_AFTER_JOIN1";
			// validate a random token and see if the validation fails
			int lockHash = SvLock.getDistributedLockImpl(lockKey, false, SvClusterClient.nodeId);
			if (lockHash == 0)
				fail("Error acquiring lock");

			SvClusterClient.shutdown(false);
			SvClusterClient.initClient(ipAddressList);
			if (!SvClusterClient.localDistributedLocks.containsKey(lockKey))
				fail("Lock isn't there after join");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			SvClusterNotifierClient.shutdown();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

	@Test
	public void ClusterMaintenanceTest() {
		SvReader svr = null;
		try {
			SvCore.initSvCore();
			SvCluster.autoStartClient = false;
			SvCluster.initCluster();
			String ipAddressList = (String) SvCluster.getCoordinatorNode().getVal("local_ip");
			SvClusterClient.initClient(ipAddressList);
			Thread clientThread = new Thread(new SvClusterClient());
			// start the heart beat thread and sleep for 3 intervals
			clientThread.start();
			Thread.sleep(3 * SvConf.getHeartBeatInterval());

			svr = new SvReader();
			DbDataObject node = svr.getObjectById(SvClusterClient.nodeId, svCONST.OBJECT_TYPE_CLUSTER, new DateTime());
			SvClusterClient.shutdown();
			// invoke maintenance
			// SvCluster.maintenanceThread.interrupt();
			node = svr.getObjectById(SvClusterClient.nodeId, svCONST.OBJECT_TYPE_CLUSTER, new DateTime());

			if (node != null && !((DateTime) node.getVal("part_time")).isAfterNow())
				fail("Client has shutdown, but maintenance did not mark it as parted"
						+ ((DateTime) node.getVal("part_time")).toString());

		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			fail("Test raised exception");
		} finally {
			if (svr != null)
				svr.release();
			SvClusterClient.shutdown();
			SvCluster.shutdown();
			SvLock.clearLocks();
		}
	}

}
