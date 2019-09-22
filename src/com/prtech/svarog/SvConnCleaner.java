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

import java.lang.ref.Reference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class which performs connection cleaning in a separate thread
 * 
 * @author PR01
 *
 */
public class SvConnCleaner implements Runnable {
	/**
	 * The default run method to perform cleaning of the tracked connections
	 */
	@Override
	public void run() {
		if (SvCore.log4j.isDebugEnabled())
			SvCore.log4j.trace("Performing SvCore cleanup");
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
		// flag the core that maintenance is not running
		SvCore.maintenanceRunning.compareAndSet(true, false);
	}

}
