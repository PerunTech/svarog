/*******************************************************************************
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Apache License
 *   Version 2.0 or the Svarog License Agreement (the "License");
 *   You may not use this file except in compliance with the License. 
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See LICENSE file in the project root for the specific language governing 
 *   permissions and limitations under the License.
 *  
 *******************************************************************************/
package com.prtech.svarog;

import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

/**
 * Class which performs connection cleaning in a separate thread
 * 
 * @author PR01
 *
 */
public class SvMaintenance implements Runnable {
	
	/**
	 * Timestamp of the next planned maintenance of the cluster table in the DB
	 */
	static DateTime nextMaintenance = new DateTime();

	/**
	 * Boolean object to be used as semaphone for thread sync in the cluster
	 */
	static final Object maintenanceSemaphore = new Object();

	/**
	 * Reference the maintenance thread object
	 */
	static Thread maintenanceThread = null;

	/**
	 * Atomic boolean flag to signify a maintenance thread is in progress
	 */
	static final AtomicBoolean maintenanceRunning = new AtomicBoolean();

	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvMaintenance.class);
	private static final AtomicBoolean isRunning = new AtomicBoolean(false);;

	/**
	 * Flag if the maintenance is in progress
	 */
	private static final AtomicBoolean maintenanceInProgress = new AtomicBoolean(false);

	/**
	 * shutdown procedure. Set the flag and notify the thread
	 */
	public static void shutdown() {
		if (isRunning.compareAndSet(true, false)) {
			if (SvMaintenance.maintenanceSemaphore != null)
				synchronized (SvMaintenance.maintenanceRunning) {
					SvMaintenance.maintenanceRunning.notifyAll();
				}
		}
	}

	public static void initMaintenance() {
		if (isRunning.compareAndSet(false, true)) {
			Thread maintenanceThrd = new Thread(new SvMaintenance());
			maintenanceThrd.setName(svCONST.maintenanceThreadId);
			// do we actually want to know when the cleaner finished?
			// Can we have multiple active cleaners? right?
			maintenanceThrd.start();
			setMaintenanceThread(maintenanceThrd);
		}
	}

	/**
	 * The default run method to perform cleaning of the tracked connections as well
	 * as cluster management
	 */
	@Override
	public void run() {
		while (isRunning.get()) {
			try {
				long timeout = performMaintenance();
				// if we aren't in a cluster, then try to join
				synchronized (SvMaintenance.maintenanceRunning) {
					SvMaintenance.maintenanceRunning.wait(timeout);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

		}
	}

	public static long performMaintenance() {
		long timeout = SvConf.getCoreIdleTimeout();

		if (SvCore.isValid.get() && maintenanceInProgress.compareAndSet(false, true)) {
			//
			
			// if the cluster is not disabled and not active, do activate
			if (SvConf.isClusterEnabled()) {
				if (!SvCluster.getIsActive().get())
					SvCluster.initCluster();
				else
					SvCluster.checkCluster();
			}
			// do the maintenance
			trackedConnCleanup();
			if (SvConf.isClusterEnabled())
				clusterListMaintenance();
			// establish waiting period
			timeout = SvCluster.getIsActive().get() ? (SvConf.getClusterMaintenanceInterval() - 2) * 1000
					: SvConf.getHeartBeatTimeOut();
			//finally check if any plugings are pending for registration
			SvPerunManager.registerPendingPlugins();
			
			maintenanceInProgress.compareAndSet(true, false);
		}

		return timeout;
	}

	/**
	 * Method which performs cleanup of of any non released svarog cores
	 */
	public static void trackedConnCleanup() {
		if (maintenanceRunning.compareAndSet(false, true)) {
			if (log4j.isDebugEnabled())
				log4j.trace("Performing SvCore cleanup");
			// perform the general cleanup of the tracked cores.
			SvConnTracker.cleanup();
			// now check for any enqueued references by the GC
			Reference<? extends SvCore> softRef = null;
			SvCore instanceToRelease = null;
			softRef = SvCore.svcQueue.poll();
			while (softRef != null) {
				// perform the real cleanup
				instanceToRelease = softRef.get();
				if (instanceToRelease != null)
					instanceToRelease.dbReleaseConn(false);
				softRef = SvCore.svcQueue.poll();
			}
		}
		// flag the core that maintenance is not running
		maintenanceRunning.compareAndSet(true, false);
	}

	/**
	 * Method to update all information related to a node in the cluster list. If
	 * the node is subject of historical date or should be removed from the list the
	 * method will return EMPTY string. If the node should not be removed the method
	 * will return the versioning id of the node.
	 * 
	 * @param node Reference to a DbDataObject containing the node info
	 * @return EMPTY string if the node should be delete, otherwise PKID
	 */
	static private DbDataArray clusterListUpdateNode(DbDataArray nodeList) {
		DbDataArray updatedList = new DbDataArray();
		for (DbDataObject node : nodeList.getItems()) {
			boolean nodeRemoved = false;
			node.setVal(SvCluster.LAST_MAINTENANCE, DateTime.now());
			node.setVal(SvCluster.NEXT_MAINTENANCE, DateTime.now().plusSeconds(SvConf.getClusterMaintenanceInterval()));

			if (!node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID)
					&& !SvClusterServer.nodeHeartBeats.containsKey(node.getObjectId())) {
				nodeRemoved = true;
			}

			if (!SvCluster.isRunning().get() && node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID))
				node.setVal(SvCluster.PART_TIME, DateTime.now());
			// if the node is not removed or it is the coordinator
			// record make sure we keep it
			if (!nodeRemoved || node.getObjectId().equals(svCONST.CLUSTER_COORDINATOR_ID)) {
				// add the node
				updatedList.addDataItem(node);

			}
		}
		return updatedList;

	}

	/**
	 * Method to delete all historical information from the cluster table. For nodes
	 * membership we don't really want to grow a big big table so its easier to
	 * delete everything which isn't in the list of dontDeleteIDs
	 * 
	 * @param conn       The JDBC connection to used for executing the query
	 * @param validNodes The list of objects which we want to keep (don't delete) in
	 *                   string format, comma separated
	 * @throws SQLException
	 * @throws SvException
	 * @throws Exception    Throw any underlying exception
	 */
	private static void clusterListDeleteHistory(Connection conn, DbDataArray validNodes)
			throws SQLException, SvException {
		PreparedStatement ps = null;
		// if there's no valid list, do nothing
		if (validNodes == null || validNodes.size() < 1)
			return;

		try {
			StringBuilder sbr = new StringBuilder(100);

			sbr = new StringBuilder(100);
			sbr.append("DELETE FROM ");
			sbr.append(SvConf.getDefaultSchema() + ".");
			sbr.append((String) SvCore.getDbt(svCONST.OBJECT_TYPE_CLUSTER).getVal("REPO_NAME") + " WHERE ");
			sbr.append(" OBJECT_TYPE=" + Long.toString(svCONST.OBJECT_TYPE_CLUSTER));

			// append the ids which we should not delete
			// if the list is empty then simply make sure we don't fail
			if (validNodes.size() > 0) {
				sbr.append(" AND  PKID NOT IN (");
				for (DbDataObject dbo : validNodes.getItems())
					sbr.append(dbo.getPkid().toString() + ",");

				sbr.setLength(sbr.length() - 1);
				sbr.append(")");
			}
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sbr.toString());
			ps.execute();
		} finally {
			try {
				SvCore.closeResource(ps, svCONST.systemUser);
			} catch (SvException e) {
				log4j.error("Svarog cluster maintenance failed!", e);
			}
		}

		try {
			StringBuilder sbr = new StringBuilder(100);

			sbr = new StringBuilder(100);
			sbr.append("DELETE FROM ");
			sbr.append(SvConf.getDefaultSchema() + ".");
			sbr.append((String) SvCore.getDbt(svCONST.OBJECT_TYPE_CLUSTER).getVal("TABLE_NAME"));
			// append the ids which we should not delete

			if (validNodes.size() > 0) {
				sbr.append(" WHERE PKID NOT IN (");
				for (DbDataObject dbo : validNodes.getItems())
					sbr.append(dbo.getPkid().toString() + ",");
				sbr.setLength(sbr.length() - 1);
				sbr.append(")");
			}
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sbr.toString());
			ps.execute();

		} finally {
			try {
				SvCore.closeResource(ps, svCONST.systemUser);
			} catch (SvException e) {
				log4j.error("Svarog cluster maintenance failed!", e);
			}
		}

	}

	/**
	 * Method to maintain the table of cluster nodes. It will first delete any old
	 * redundant records, then update the current cluster state based on the nodes
	 * which have been reporting via heartbeat in the last interval
	 */
	private static void clusterListMaintenance() {
		// we are the coordinator and the Heart Beat server is running
		if (SvCluster.isCoordinator() && SvClusterServer.isRunning.get()) {
			synchronized (SvCluster.isRunning()) {
				
				
				try (SvReader  svr = new SvReader(); SvWriter svw = new SvWriter(svr);){
					
					Connection conn = svr.dbGetConn();

					DbDataArray nodeList = svr.getObjects(null, svCONST.OBJECT_TYPE_CLUSTER, null, 0, 0);

					DbDataArray updatedList = new DbDataArray();
					updatedList = clusterListUpdateNode(nodeList);
					clusterListDeleteHistory(conn, updatedList);
					svw.isInternal = true;
					
					svw.saveObject(updatedList, false);
					svw.dbCommit();
					nextMaintenance = new DateTime().plusSeconds(SvConf.getClusterMaintenanceInterval());

				} catch (Exception e) {
					log4j.error("Svarog cluster maintenance failed! Restarting cluster", e);
					if (e instanceof SvException) {
						SvCluster.shutdown(false);
						SvCluster.initCluster();
					}
				} 
			}

		}
	}

	public static AtomicBoolean getMaintenanceInProgress() {
		return maintenanceInProgress;
	}

	/**
	 * Getter for the thread which is executing the maintenance
	 * 
	 * @return maintenance thread
	 */
	static Thread getMaintenanceThread() {
		return maintenanceThread;
	}

	/**
	 * Setter for the thread which is executing the maintenance
	 */
	static void setMaintenanceThread(Thread thrd) {
		maintenanceThread = thrd;
	}

}
