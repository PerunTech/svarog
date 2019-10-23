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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/***
 * Class for managing locks over long running svarog operations. Redis
 * integration for handling distributed locking should be handled by this class
 * 
 * @author PR01
 *
 */
public class SvLock {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = LogManager.getLogger(SvLock.class.getName());

	/**
	 * Map holding locks over long running ops over objects.
	 */
	private static ConcurrentHashMap<String, ReentrantLock> sysLocks = new ConcurrentHashMap<String, ReentrantLock>();

	/**
	 * Method to get a lock for a specific key
	 * 
	 * @param key
	 *            The key to identify the lock with
	 * @param isBlocking
	 *            Should the thread block on locking or just fail if it isn't
	 *            able to lock it
	 * @return true if the lock has been acquired
	 */
	@Deprecated
	public static boolean getLock(String key, Boolean isBlocking) {
		long timeout = 0;
		if (isBlocking) {
			timeout = SvConf.getMaxLockTimeout();
			isBlocking = false;
		}
		return getLock(key, isBlocking, timeout) != null;
	}

	/**
	 * Method to get a lock for a specific key
	 * 
	 * @param key
	 *            The key to identify the lock with
	 * @return true if the lock has been acquired
	 */
	@Deprecated
	public static boolean getLock(String key) {
		return getLock(key, true);
	}

	/**
	 * Method to get a lock for a specific key
	 * 
	 * @param key
	 *            The key to identify the lock with
	 * @param isBlocking
	 *            Should the thread block on locking or just fail if it isn't
	 *            able to lock it
	 * @param timeout
	 *            Timeout in mili-seconds to acquire the lock
	 * @return Instance of ReentrantLock, mapped to the System Locks using the
	 *         requested key
	 */
	public static ReentrantLock getLock(String key, Boolean isBlocking, long timeout) {
		if (log4j.isDebugEnabled())
			log4j.trace("Lock acquiring requested for key:" + key + ", blocking:" + isBlocking.toString() + ", timeout:"
					+ timeout);

		Boolean lockAcquired = false;
		ReentrantLock retLock = new ReentrantLock();
		// if this is non-blocking lock immediately try to lock it
		// and avoid long waiting. We want to exit the synchronized block
		// before we start the long wait
		retLock = sysLocks.putIfAbsent(key, retLock);
		if (!isBlocking)
			lockAcquired = retLock.tryLock();

		if (!lockAcquired) {
			if (isBlocking) {
				retLock.lock();
				lockAcquired = true;
			} else {
				try {
					lockAcquired = retLock.tryLock(timeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					log4j.info("Trying to lock " + key + ", was interrupted", e);
					lockAcquired = false;
				}
			}
		}
		if (lockAcquired) {
			// verify that the releasing thread didn't remove the lock from the
			// map. We need this to ensure that the lock wasn't lost
			retLock = sysLocks.putIfAbsent(key, retLock);
		} else
			retLock = null;

		if (log4j.isDebugEnabled())
			log4j.debug((isBlocking ? "" : "Non-") + "Blocking lock acquired:" + lockAcquired.toString());

		return retLock;
	}

	/**
	 * Method to release the lock with Key used as ID and a lock reference
	 * 
	 * @param key
	 *            The key to unlock
	 * @param lock
	 *            The lock reference acquired from
	 *            {@link #getLock(String, Boolean, long)}
	 * @param alwaysUnlock
	 *            If this parameter is true, the method will unlock the key even
	 *            if the lock is not matched in the System Locks hashmap
	 */
	public static void releaseLock(String key, ReentrantLock lock, boolean alwaysUnlock) {
		if (log4j.isDebugEnabled())
			log4j.debug("Lock release requested for key:" + key);

		if (lock != null || alwaysUnlock) {
			ReentrantLock mapLock = sysLocks.get(key);
			if (mapLock == null) {
				log4j.error("No lock to release under key:" + key);
			} else {

				if (mapLock.equals(lock) || alwaysUnlock) {
					// if we are the last one to hold the lock then remove it also.
					if (mapLock.getHoldCount() <= 1) {
						if (sysLocks.remove(key, mapLock))
						{
							if (log4j.isDebugEnabled())
								log4j.debug("Lock key:" + key + ", removed from lock list");
						}else 
								log4j.error("The lock for key:" + key + " was replaced!");
					}	
					mapLock.unlock();
					if (log4j.isDebugEnabled())
						log4j.trace("Lock decremented for key:" + key + ", hold count:" + mapLock.getHoldCount());
				} else
					log4j.error("Release requested for unlocked key:" + key + ", but lock reference is not equal");


			}
		} else
			log4j.error("Release requested for unlocked key:" + key + ", but lock reference was null");

	}

	/**
	 * Method to release the lock with Key used as ID and a lock reference. This
	 * method will not unlock the lock if the Lock reference isn't matched to
	 * the one in the System Locks map
	 * 
	 * @param key
	 *            The key to unlock
	 * @param lock
	 *            The lock reference acquired from
	 *            {@link #getLock(String, Boolean, long)}
	 */
	public static void releaseLock(String key, ReentrantLock lock) {
		releaseLock(key, lock, false);
	}

	/**
	 * Method to release the lock with Key used as ID. This method will ALWAYS
	 * unlock the key.
	 * 
	 * WARNING: This method allows you to create a bug, in which one thread
	 * acquires the lock and another one tries to unlock it, thus raising
	 * exception. If you are using this method, refactor your code to use
	 * {@link #releaseLock(String, ReentrantLock)}
	 * 
	 * @param key
	 *            The key to unlock
	 */
	@Deprecated
	public static void releaseLock(String key) {
		releaseLock(key, null, false);
	}

}
