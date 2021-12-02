/*******************************************************************************
 * Copyright (c) 2013, 2017 Perun Technologii DOOEL Skopje.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License
 * Version 2.0 or the Svarog License Agreement (the "License");
 * You may not use this file except in compliance with the License. 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See LICENSE file in the project root for the specific language governing 
 * permissions and limitations under the License.
 *
 *******************************************************************************/
package com.prtech.svarog;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.prtech.svarog.SvCluster.DistributedLock;

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
	private static final Logger log4j = LogManager.getLogger(SvLock.class.getName());

	/**
	 * Map holding locks over long running ops over objects.
	 */
	private static final LoadingCache<String, ReentrantLock> sysLocks = initSyslockCache(
			SvClusterServer.distributedLocks, SvClusterServer.nodeLocks);

	/**
	 * Map holding locks over long running ops over objects.
	 */
	static final LoadingCache<String, ReentrantLock> localSysLocks = initSyslockCache(
			SvClusterClient.localDistributedLocks, SvClusterClient.localNodeLocks);

	/**
	 * Private method to initialise the SvExecutors cache.
	 * 
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static LoadingCache<String, ReentrantLock> initSyslockCache(
			final ConcurrentHashMap<String, SvCluster.DistributedLock> distributedLocks,
			final ConcurrentHashMap<Long, CopyOnWriteArrayList<DistributedLock>> nodeLocks) {
		CacheBuilder builder = CacheBuilder.newBuilder();
		builder = builder.maximumSize(SvConf.getMaxLockCount());
		builder = builder.expireAfterAccess(SvConf.getMaxLockTimeout(), TimeUnit.MILLISECONDS);
		builder = builder.removalListener(new RemovalListener<String, ReentrantLock>() {
			@Override
			public void onRemoval(RemovalNotification<String, ReentrantLock> removal) {
				if (log4j.isDebugEnabled())
					log4j.trace("Removing key:" + removal.getKey() + ", lock:" + removal.getValue().toString());
				// if we are the cluster coordinator, then remove the lock from
				// the map of distributed locks
				if (distributedLocks.containsKey(removal.getKey())) {
					DistributedLock dlock = distributedLocks.remove(removal.getKey());
					if (dlock != null) {
						CopyOnWriteArrayList<DistributedLock> locks = nodeLocks.get(dlock.getNodeId());
						if (locks != null)
							locks.remove(dlock);
					}
				}

			}
		});
		return (LoadingCache<String, ReentrantLock>) builder
				.<String, ReentrantLock>build(new CacheLoader<String, ReentrantLock>() {
					@Override
					public ReentrantLock load(String queryKey) throws Exception {
						return new ReentrantLock();
					}
				});
	}

	/**
	 * Method to get a lock for a specific key
	 * 
	 * @param key        The key to identify the lock with
	 * @param isBlocking Should the thread block on locking or just fail if it isn't
	 *                   able to lock it
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
	 * @param key The key to identify the lock with
	 * @return true if the lock has been acquired
	 */
	@Deprecated
	public static boolean getLock(String key) {
		return getLock(key, true);
	}

	/**
	 * Method to get a lock for a specific key
	 * 
	 * @param key        The key to identify the lock with
	 * @param isBlocking Should the thread block on locking or just fail if it isn't
	 *                   able to lock it
	 * @param timeout    Timeout in mili-seconds to acquire the lock
	 * @return Instance of ReentrantLock, mapped to the System Locks using the
	 *         requested key
	 * 
	 */
	public static ReentrantLock getLock(String key, Boolean isBlocking, long timeout) {
		return getLock(key, isBlocking, timeout, sysLocks);
	}

	public static ReentrantLock getLock(String key, Boolean isBlocking, long timeout,
			LoadingCache<String, ReentrantLock> locks) {
		if (log4j.isDebugEnabled())
			log4j.trace("Lock acquiring requested for key:" + key + ", blocking:" + isBlocking.toString() + ", timeout:"
					+ timeout);

		Boolean lockAcquired = false;
		ReentrantLock retLock = null;

		try {
			retLock = locks == null ? sysLocks.get(key) : locks.get(key);
			if (log4j.isDebugEnabled())
				log4j.trace("New lock key:" + key + ", generated");

			if (!lockAcquired) {
				if (isBlocking) {
					retLock.lock();
					lockAcquired = true;
				} else {
					lockAcquired = retLock.tryLock(timeout, TimeUnit.MILLISECONDS);
				}
			}
		} catch (Exception e) {
			log4j.info("Trying to lock " + key + ", failed", e);
			lockAcquired = false;
		}

		if (!lockAcquired)
			retLock = null;

		if (log4j.isDebugEnabled())
			log4j.debug((isBlocking ? "" : "Non-") + "Blocking lock acquired:" + lockAcquired.toString());

		return retLock;
	}

	/**
	 * Method to release the lock with Key used as ID and a lock reference
	 * 
	 * @param key          The key to unlock
	 * @param lock         The lock reference acquired from
	 *                     {@link #getLock(String, Boolean, long)}
	 * @param alwaysUnlock If this parameter is true, the method will unlock the key
	 *                     even if the lock is not matched in the System Locks
	 *                     hashmap
	 */
	public static boolean releaseLock(String key, ReentrantLock lock, boolean alwaysUnlock) {
		return releaseLock(key, lock, alwaysUnlock, sysLocks);
	}

	public static boolean releaseLock(String key, ReentrantLock lock, boolean alwaysUnlock,
			LoadingCache<String, ReentrantLock> locks) {
		if (log4j.isDebugEnabled())
			log4j.debug("Lock release requested for key:" + key);
		boolean lockReleased = false;
		if (lock != null || alwaysUnlock) {
			ReentrantLock mapLock = locks == null ? sysLocks.getIfPresent(key) : locks.getIfPresent(key);
			if (mapLock == null) {
				log4j.error("No lock to release under key:" + key);
			} else {
				if (mapLock.equals(lock) || alwaysUnlock) {
					mapLock.unlock();
					lockReleased = true;
					if (log4j.isDebugEnabled())
						log4j.trace("Lock decremented for key:" + key + ", hold count:" + mapLock.getHoldCount());
				} else
					log4j.error("Release requested for unlocked key:" + key + ", but lock reference is not equal");

			}
		} else
			log4j.error("Release requested for unlocked key:" + key + ", but lock reference was null");
		if (alwaysUnlock && !lockReleased) {
			if (lock != null)
				lock.unlock();
			log4j.warn((lock == null ? "NO lock" : "Forced release") + " under key:" + key);

		}

		return lockReleased;
	}

	/**
	 * Method to release the lock with Key used as ID and a lock reference. This
	 * method will not unlock the lock if the Lock reference isn't matched to the
	 * one in the System Locks map
	 * 
	 * @param key  The key to unlock
	 * @param lock The lock reference acquired from
	 *             {@link #getLock(String, Boolean, long)}
	 */
	public static boolean releaseLock(String key, ReentrantLock lock) {
		return releaseLock(key, lock, false);
	}

	/**
	 * Method to release the lock with Key used as ID. This method will ALWAYS
	 * unlock the key.
	 * 
	 * WARNING: This method allows you to create a bug, in which one thread acquires
	 * the lock and another one tries to unlock it, thus raising exception. If you
	 * are using this method, refactor your code to use
	 * {@link #releaseLock(String, ReentrantLock)}
	 * 
	 * @param key The key to unlock
	 */
	@Deprecated
	public static void releaseLock(String key) {
		releaseLock(key, null, false);
	}

	/**
	 * Method to get a cluster wide, distributed lock for a specific key in the
	 * Svarog cluster. The distributed locks are by default non-blocking.
	 * 
	 * @param key The key to identify the lock with
	 * @return hash code of the acquired lock. If the value is 0, the lock wasn't
	 *         acquired
	 * @throws SvException 
	 * 
	 */
	public static int getDistributedLock(String lockKey) throws SvException {
		return getDistributedLockImpl(lockKey, SvCluster.isCoordinator(),
				SvCluster.isCoordinator() ? SvCluster.getCoordinatorNode().getObjectId() : SvClusterClient.nodeId);

	}

	/**
	 * Implementation method to get a cluster wide, distributed lock for a specific
	 * key in the Svarog cluster. The distributed locks are by default non-blocking.
	 * 
	 * @param key           The key to identify the lock with
	 * @param isCoordinator Flag if the lock is to be acquired by a worker or
	 *                      coordinator node
	 * @param nodeId        The node under which this lock shall be registered
	 * @return hash code of the acquired lock. If the value is 0, the lock wasn't
	 *         acquired
	 * @throws SvException
	 * 
	 */
	static int getDistributedLockImpl(String lockKey, boolean isCoordinator, Long nodeId) throws SvException {
		if (!SvCluster.getIsActive().get()) {
			log4j.error("Can't acquire a distributed lock when cluster is not active");
			return 0;
		}

		int lockHash = 0;

		if (isCoordinator) {
			// the coordinator gets the lock directly from the cluster server
			lockHash = SvClusterServer.getDistributedLock(lockKey, nodeId);
		} else {
			ReentrantLock lck = null;
			try {
				// first acquire local
				lck = SvClusterClient.acquireDistributedLock(lockKey, SvClusterClient.nodeId);
				// if acquired, then acquire cluster wide
				if (lck != null)
					lockHash = SvClusterClient.getLock(lockKey);
				// update the local lock with the unique hash from the
				// coordinator

			} finally {
				// if we've got it locally, but didn't acquire cluster wide,
				// then release
				if (lck != null && lockHash == 0)
					SvClusterClient.releaseDistributedLock(lck.hashCode());
			}

		}
		return lockHash;

	}

	/**
	 * Internal method to help unit testing to clear all locks after test
	 */
	static void clearLocks() {
		sysLocks.invalidateAll();
		localSysLocks.invalidateAll();
	}

	/**
	 * Method to get a cluster wide, distributed lock for a specific key in the
	 * Svarog cluster. The distributed locks are by default non-blocking.
	 * 
	 * @param key The key to identify the lock with
	 * @return False if the lock wasn't acquired
	 * @throws SvException 
	 * 
	 */
	public static boolean releaseDistributedLock(int lockHash) throws SvException {
		return releaseDistributedLockImpl(lockHash, SvCluster.isCoordinator(),
				SvCluster.isCoordinator() ? SvCluster.getCoordinatorNode().getObjectId() : SvClusterClient.nodeId);
	}

	/**
	 * Implementation method to get a cluster wide, distributed lock for a specific
	 * key in the Svarog cluster. The distributed locks are by default non-blocking.
	 * 
	 * @param key           The key to identify the lock with
	 * @param isCoordinator Flag if the lock is to be acquired by a worker or
	 *                      coordinator node
	 * @return False if the lock wasn't acquired
	 * @throws SvException 
	 * 
	 */
	static boolean releaseDistributedLockImpl(int lockHash, boolean isCoordinator, Long nodeId) throws SvException {
		if (!SvCluster.getIsActive().get()) {
			log4j.error("Can't release a distributed lock when cluster is not active");
			return false;
		}

		boolean lockReleased = false;
		if (isCoordinator) {
			lockReleased = SvClusterServer.releaseDistributedLock(lockHash, nodeId) != null;
		} else {
			// release the lock from the distributed list
			SvClusterClient.releaseDistributedLock(lockHash);

			lockReleased = SvClusterClient.releaseLock(lockHash);

		}
		return lockReleased;

	}
}