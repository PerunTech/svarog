/*******************************************************************************
 *   Copyright (c) 2013, 2019 Perun Technologii DOOEL Skopje.
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

import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

/**
 * Svarog Connection Tracker. Class to track the number of SvCore based objects
 * that use a single JDBC connection
 * 
 * @author PR01
 *
 */
class SvConnTracker {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvConnTracker.class);

	/**
	 * Map holding soft references to each created SvCore instance
	 */
	static private ConcurrentHashMap<SoftReference<SvCore>, SvConnTrace> tracker = new ConcurrentHashMap<SoftReference<SvCore>, SvConnTrace>();

	/**
	 * Method to check if the connection tracker has open DB connections
	 * 
	 * @return
	 */
	static Boolean hasTrackedConnections() {
		return hasTrackedConnections(false);
	}

	static Boolean hasTrackedConnections(boolean printStack) {
		return hasTrackedConnections(printStack, true);
	}

	static Boolean hasTrackedConnections(boolean printStack, boolean countInternal) {
		int connCount = 0;
		if (printStack || !countInternal) {
			for (SoftReference<SvCore> svSoft : tracker.keySet()) {
				SvCore svc = svSoft.get();
				if (SvCore.isDebugEnabled && printStack) {
					log4j.info("The rogue SvCore instance was created at: " + svc.getCoreTraceInfo());
				}
				if (countInternal || (!svc.isInternal && !countInternal))
					connCount++;
			}
		} else
			connCount = tracker.size();
		return connCount > 0;
	}

	static Boolean hasTrackedConnection(SoftReference<SvCore> toCore) {
		return tracker.get(toCore) != null;
	}

	/**
	 * A method to fetch a tracked connection from the connection pool
	 * 
	 * @param toCore
	 *            SoftReference to the SvCore instance which is requesting the
	 *            connection
	 * @param fromCore
	 *            SoftReference of the parent SvCore instance
	 * @return A JDBC connection instance
	 * @throws SvException
	 */
	static Connection getTrackedConnection(SoftReference<SvCore> toCore, SoftReference<SvCore> fromCore)
			throws SvException {
		if (toCore == null || toCore.get() == null)
			throw (new SvException("system.error.no_core2track_err", svCONST.systemUser));

		Connection conn = null;
		SvConnTrace connTrace = null;
		connTrace = tracker.get(toCore);
		if (connTrace == null)
			synchronized (tracker) {
				ArrayDeque<SoftReference<SvCore>> tmpTracker = new ArrayDeque<SoftReference<SvCore>>(4);
				SoftReference<SvCore> currentCore = toCore;
				// we need to pass all the parent cores in the chain up to find
				// the one with tracker
				while (currentCore != null && connTrace == null) {
					connTrace = tracker.get(currentCore);
					if (connTrace == null) {
						// if the current core doesn't have a tracker put it in
						// the tmpTracker map
						if (log4j.isDebugEnabled())
							log4j.trace("The core with ref:" + currentCore
									+ " doesn't have a tracer, we go up the chain to find a tracer");
						tmpTracker.add(currentCore);
						SvCore core = currentCore.get();
						if (core != null)
							currentCore = core.weakSrcCore;
						else
							currentCore = null;
					}
				}
				// if we iterated all chained cores and we still have no tracker
				// available
				// create a new one
				if (connTrace == null) {
					if (log4j.isDebugEnabled())
						log4j.trace(
								"The core chain " + tmpTracker.toString() + "doesn't have a tracer, create a new one");
					connTrace = new SvConnTrace();
				}
				// assign the new tracer to all chained cores.
				for (SoftReference<SvCore> sCore : tmpTracker) {
					tracker.put(sCore, connTrace);
					connTrace.usageCount++;
				}
			}
		conn = connTrace.acquire();
		return conn;
	}

	/**
	 * Method to release a tracked connection for a specific SvCore instance.
	 * 
	 * @param svCore
	 *            Soft reference to the instance for which we want to release a
	 *            tracked connection
	 * @param isManual
	 *            Boolean flag to signify if the release is done manually or by
	 *            the GC
	 */
	static void releaseTrackedConnection(SoftReference<SvCore> svCore, Boolean isManual) {
		releaseTrackedConnection(svCore, isManual, false);
	}

	/**
	 * Method which cleans up any rogue cores
	 */
	static int cleanup() {
		int coreCount = 0;
		Iterator<SoftReference<SvCore>> iter = tracker.keySet().iterator();
		long currentTime = DateTime.now().getMillis();
		while (iter.hasNext()) {
			SvCore currCore = iter.next().get();
			if (currCore != null) {
				if (!currCore.getIsLongRunning()) {
					long idleTime = currentTime - currCore.getCoreLastActivity();
					if (idleTime > SvConf.getCoreIdleTimeout()) {
						log4j.warn("Potential connection leak. Rogue core of type "
								+ currCore.getClass().getCanonicalName() + " detected with idle time of " + idleTime
								+ " miliseconds");
						if (SvCore.isDebugEnabled) {
							log4j.warn("The rogue SvCore instance was created at: " + currCore.getCoreTraceInfo());
						} else
							log4j.warn(
									"SvCore debug is not enabled. Can't find more info about the rogue instance. Enable debug via sys.core.is_debug = true in svarog.parameters");

						currCore.release();
						coreCount++;
					}
				}
				// to do cleanup
			}
		}
		return coreCount;
	}

	/**
	 * Method to release a tracked connection for a specific SvCore instance.
	 * 
	 * @param svCore
	 *            Soft reference to the instance for which we want to release a
	 *            tracked connection
	 * @param isManual
	 *            Boolean flag to signify if the release is done manually or by
	 *            the GC
	 * @param hardRelease
	 *            Boolean flag to signify hard release. A hard release will
	 *            release all chained SvCore instances
	 */
	static void releaseTrackedConnection(SoftReference<SvCore> svCore, Boolean isManual, Boolean hardRelease) {
		int coreUsageCount = 0;
		SvConnTrace connTrace = null;
		synchronized (tracker) {
			connTrace = tracker.get(svCore);
			if (connTrace != null) {
				coreUsageCount++;
				tracker.remove(svCore);
				if (hardRelease) {
					if (log4j.isDebugEnabled())
						log4j.trace("Hard release requested");
					Iterator<Map.Entry<SoftReference<SvCore>, SvConnTrace>> iter = tracker.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry<SoftReference<SvCore>, SvConnTrace> entry = iter.next();
						if (entry.getValue() == connTrace) {
							if (log4j.isDebugEnabled())
								log4j.trace("Removing chained svCore:" + entry.getKey());
							coreUsageCount++;
							iter.remove();
						}
					}
				}

			} else
				log4j.warn("No connection to be release for:" + svCore);
		}
		if (connTrace != null && coreUsageCount > 0)
			for (int i = 0; i < coreUsageCount; i++) {
				if (log4j.isDebugEnabled())
					log4j.trace("Performing the real release of tracer:" + connTrace);
				connTrace.release(isManual);
			}
	}

}
