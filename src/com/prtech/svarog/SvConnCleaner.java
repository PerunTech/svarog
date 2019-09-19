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
