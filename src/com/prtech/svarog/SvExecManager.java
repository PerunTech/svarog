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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.ISvOnSave;
import com.prtech.svarog_interfaces.IPerunPlugin;
import com.prtech.svarog_interfaces.ISvExecutor;
import com.prtech.svarog_interfaces.ISvExecutorGroup;

/**
 * Svarog Execution Manager class is inherited from the SvCore and extends it
 * with interfacing the OSGI framework to find a list of services which
 * implement the ISvExecutor interface
 * 
 * @author ristepejov
 *
 */
public class SvExecManager extends SvCore {
	/**
	 * Internal SvExecInstance class to wrap the executor in order to allow changing
	 * of the default start and end dates coming from the executor itself.The real
	 * start and end dates are provided by configuration
	 */
	public class SvExecInstance {

		String status;
		DateTime startDate;
		DateTime endDate;
		ISvExecutor executor;

		SvExecInstance(ISvExecutor executor, DateTime startDate, DateTime endDate) {
			this.startDate = startDate != null ? startDate : executor.getStartDate();
			this.endDate = endDate != null ? endDate : executor.getEndDate();
			this.executor = executor;
			if (this.executor == null)
				throw new NullPointerException("Executor can't be null");
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			if (this.executor == null) {
				return false;
			}
			final SvExecInstance other = (SvExecInstance) obj;
			if (other.executor == null) {
				return false;
			}

			if (!getKey(this.executor).equals(getKey(other.executor))) {
				return false;
			}
			if (this.executor.versionUID() != other.executor.versionUID()) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 3;
			if (this.executor != null) {
				hash = 53 * hash + (getKey(this.executor).hashCode());
				hash = (int) (53 * hash + this.executor.versionUID());
			}
			return hash;
		}

		public DateTime getStartDate() {
			return startDate;
		}

		public void setStartDate(DateTime startDate) {
			this.startDate = startDate;
		}

		public DateTime getEndDate() {
			return endDate;
		}

		public void setEndDate(DateTime endDate) {
			this.endDate = endDate;
		}

		public ISvExecutor getExecutor() {
			return executor;
		}

		public void setExecutor(ISvExecutor executor) {
			this.executor = executor;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}
	}

	/**
	 * Global member which we can use for unit testing also! In JUnit just set the
	 * value of the services you want to test.
	 */
	static Object[] osgiServices = null;

	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvCore.class.getName());

	/**
	 * Cache with all executors responding to a specific command string. The Key is
	 * represents a CATEGORY.NAME
	 */
	static final Cache<String, CopyOnWriteArrayList<SvExecInstance>> executorMap = initExecutorCache();

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id String UID of the user session under which the SvCore
	 *                   instance will run
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvExecManager(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id   String UID of the user session under which the SvCore
	 *                     instance will run
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvExecManager(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvExecManager(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	SvExecManager() throws SvException {
		super();
	}

	/**
	 * Private method to initialise the SvExecutors cache.
	 * 
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static Cache<String, CopyOnWriteArrayList<SvExecInstance>> initExecutorCache() {
		/**
		 * Callback class to cleanup the executor cache according to changes in Svarog
		 * 
		 * @author XPS13
		 *
		 */
		class SvExecutorCacheCallback implements ISvOnSave {

			@Override
			public boolean beforeSave(SvCore parentCore, DbDataObject dbo) {
				// TODO Auto-generated method stub
				return true;
			}

			@Override
			public void afterSave(SvCore parentCore, DbDataObject dbo) {
				executorMap.invalidate(getKey((String) dbo.getVal("CATEGORY"), (String) dbo.getVal("NAME")));
			}
		}

		CacheBuilder builder = CacheBuilder.newBuilder();
		builder = builder.maximumSize(5000);
		builder = builder.expireAfterAccess(10, TimeUnit.MINUTES);
		ISvOnSave callback = new SvExecutorCacheCallback();
		SvCore.registerOnSaveCallback(callback, svCONST.OBJECT_TYPE_EXECUTORS);
		return (Cache<String, CopyOnWriteArrayList<SvExecInstance>>) builder
				.<String, CopyOnWriteArrayList<SvExecInstance>>build();
	}

	/**
	 * Method to invalidate executor in the cache by category/name
	 * 
	 * @param category The category of executor
	 * @param name     The name of the executor
	 */
	static void invalidateExecutor(String category, String name) {
		executorMap.invalidate(getKey(category, name));
	}

	/**
	 * Method to invalidate executor by executor key
	 * 
	 * @param key The executor key to be invalidated
	 */
	static void invalidateExecutor(String key) {
		executorMap.invalidate(key);
	}

	/**
	 * Method to create a consistent string key of an executor category.name
	 * 
	 * @param exec The executor instance
	 * @return The string key separated by coma.
	 */
	String getKey(ISvExecutor exec) {
		return getKey(exec.getCategory(), exec.getName());
	}

	/**
	 * Method to create a consistent string key from two strings separated by coma
	 * 
	 * @param category The first part of the key
	 * @param name     The second part of the key
	 * @return The string key separated by coma.
	 */
	static String getKey(String category, String name) {
		return (category != null ? category : "") + "." + (name != null ? name : "");
	}

	/**
	 * Method to create a consistent string name from key
	 * 
	 * @param category The first part of the key
	 * @param name     The second part of the key
	 * @return The string key separated by coma.
	 */
	static String getName(String category, String key) {
		return key.replace((category != null ? category : "") + ".", "");
	}

	/**
	 * Method to find an ISvExecutor in Svarog. First it will try to get the
	 * executor from the Cache, then if it doesn't exist, it will scan the OSGI
	 * container to find one.
	 * 
	 * @param category      Category of the executor
	 * @param name          Name of the Executor
	 * @param referenceDate Reference Date on which the executor must be valid.
	 * @return Instance of Svarog Executor
	 * @throws SvException Throws exception if executor isn't valid
	 */
	ISvExecutor getExecutor(String category, String name, DateTime referenceDate) throws SvException {
		String key = getKey(category, name);
		return getExecutor(key, referenceDate);
	}

	/**
	 * Method to find an ISvExecutor in Svarog. First it will try to get the
	 * executor from the Cache, then if it doesn't exist, it will scan the OSGI
	 * container to find one.
	 * 
	 * @param key           Key of the executor category and name, concatenated with
	 *                      a dot
	 * @param referenceDate Reference Date on which the executor must be valid.
	 * @return Object instance implementing the ISvExecutor interface
	 * @throws SvException Throws exception if executor isn't valid
	 */
	ISvExecutor getExecutor(String key, DateTime referenceDate) throws SvException {

		ISvExecutor executor = null;
		if (referenceDate == null)
			referenceDate = new DateTime();
		// try to get the executor from Cache
		executor = getExecutorFromCache(key, referenceDate);

		if (executor == null) {
			// if not, then look in the OSGI container and initialise
			String lockKey = key + "-LOADING";
			ReentrantLock lock = null;
			try {
				lock = SvLock.getLock(lockKey, true, SvConf.getMaxLockTimeout());
				if (lock != null) {
					executor = loadExecutor(key, referenceDate);
				}
			} finally {
				if (lock != null)
					SvLock.releaseLock(lockKey, lock);
			}
		}

		return executor;
	}

	/**
	 * Method to find an ISvExecutor in the local executor cache
	 * 
	 * @param key           Key of the executor category and name, concatenated with
	 *                      a dot
	 * @param referenceDate Reference Date on which the executor must be valid.
	 * @return Object instance implementing the ISvExecutor interface
	 * @throws SvException Throws exception if executor isn't valid
	 */
	ISvExecutor getExecutorFromCache(String key, DateTime referenceDate) throws SvException {
		ISvExecutor executor = null;
		CopyOnWriteArrayList<SvExecInstance> execs = executorMap.getIfPresent(key);
		if (execs != null) {
			for (SvExecInstance sve : execs) {
				if (referenceDate.isAfter(sve.getStartDate()) && referenceDate.isBefore(sve.getEndDate())) {
					if (!svCONST.STATUS_VALID.equals(sve.getStatus()))
						throw (new SvException("system.error.executor_not_valid", this.getInstanceUser(), null, key));
					executor = sve.getExecutor();
					break;
				}
			}
		}
		return executor;
	}

	DbDataObject getExecutorDbo(String name, String category, Long version) throws SvException {
		DbDataObject execDbo = null;
		try (SvReader svr = new SvReader()) {
			// switch to service user in order to be able to manage permissions
			DbSearchExpression search = new DbSearchExpression();
			search.addDbSearchItem(new DbSearchCriterion("NAME", DbCompareOperand.EQUAL, name));
			search.addDbSearchItem(new DbSearchCriterion("CATEGORY", DbCompareOperand.EQUAL, category));
			if (version != null)
				search.addDbSearchItem(new DbSearchCriterion(Sv.VERSION, DbCompareOperand.EQUAL, version));

			DbQueryObject dqo = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_EXECUTORS), search, null, null);
			DbDataArray objects = svr.getObjects(dqo, 0, 0);
			if (objects.size() > 0) {
				// make sure we reverse the sort to get the highest version
				ArrayList<DbDataObject> sort = objects.getSortedItems(Sv.VERSION);
				Collections.reverse(sort);
				execDbo = objects.get(0);
			}
		}
		return execDbo;
	}

	/**
	 * Method which initialises the executor metadata from the database. This part
	 * is important to support the changing of the start and end date by the admin
	 * user in the database in order override the dates coming from the executor
	 * itself.
	 * 
	 * @param executor Object implementing ISvExecutor interface.
	 * @return SvExecInstance object to be stored in the execution cache
	 */
	SvExecInstance initExecInstance(SvExecInstance exeInstance) {
		ISvExecutor executor = exeInstance.getExecutor();

		try (SvWriter svw = new SvWriter();) {

			DbDataObject dbo = getExecutorDbo(executor.getName(), executor.getCategory(), executor.versionUID());
			svw.switchUser(svCONST.serviceUser);
			svw.setAutoCommit(false);
			try (SvSecurity svs = new SvSecurity(svw);) {
				if (dbo != null) {
					exeInstance.setStartDate((DateTime) dbo.getVal("START_DATE"));
					exeInstance.setEndDate((DateTime) dbo.getVal("END_DATE"));
					exeInstance.setStatus(dbo.getStatus());
				} else {
					dbo = new DbDataObject(svCONST.OBJECT_TYPE_EXECUTORS);
					dbo.setVal("CATEGORY", executor.getCategory());
					dbo.setVal("NAME", executor.getName());
					dbo.setVal("JAVA_TYPE", executor.getReturningType().getClass().getCanonicalName());
					dbo.setVal("DESCRIPTION", executor.getDescription());
					dbo.setVal("START_DATE", executor.getStartDate());
					dbo.setVal("END_DATE", executor.getEndDate());
					dbo.setVal(Sv.VERSION, executor.versionUID());
					svw.saveObject(dbo);
					String executorKey = getKey(executor);
					DbDataArray perms = svs.getPermissions(executorKey);
					if (perms.size() < 1)
						svs.addPermission(SvCore.getDbt(svCONST.OBJECT_TYPE_EXECUTORS), executorKey, executorKey,
								SvAccess.EXECUTE);
					svw.dbCommit();
				}
			}
		} catch (SvException e) {
			log4j.error("Error loading executor data from db. Executor:'" + getKey(executor) + "', version:"
					+ executor.versionUID(), e);
		}
		return exeInstance;
	}

	public void initOSGIExecutors() throws SvException {
		if (this.isAdmin() || this.isSystem())
			loadOSGIExecutors(null, null);
	}

	/**
	 * Method to load executor services from the OSGI tracker
	 * 
	 * @param key   The key of the executor to load
	 * @param execs List of executors to add the executor to
	 */
	void loadOSGIExecutors(String key, CopyOnWriteArrayList<SvExecInstance> execs) {
		Object[] services = getOSGIServices();
		for (int i = 0; (services != null) && (i < services.length); i++) {
			SvExecInstance svx = null;
			if (services[i] instanceof ISvExecutor && services[i] != null
					&& (getKey((ISvExecutor) services[i]).equals(key) || (key == null && execs == null))) {
				ISvExecutor sve = (ISvExecutor) services[i];
				// get start and end date from database and
				// update the SVE
				svx = new SvExecInstance(sve, sve.getStartDate(), sve.getEndDate());
				svx.setStatus(svCONST.STATUS_VALID);
				SvExecInstance s = initExecInstance(svx);
				if (execs != null)
					execs.addIfAbsent(s);
			}
			if (services[i] instanceof ISvExecutorGroup && services[i] != null
					&& (getKeys((ISvExecutorGroup) services[i]).contains(key) || (key == null && execs == null))) {
				ISvExecutorGroup svg = (ISvExecutorGroup) services[i];
				List<String> keys = null;
				if (key == null)
					keys = getKeys(svg);
				else {
					keys = new ArrayList<String>();
					keys.add(key);
				}
				for (String singleKey : keys) {
					ISvExecutor svge = new SvExecutorWrapper(svg, getName(svg.getCategory(), singleKey));
					svx = new SvExecInstance(svge, svge.getStartDate(), svge.getEndDate());
					svx.setStatus(svCONST.STATUS_VALID);
					SvExecInstance s = initExecInstance(svx);
					if (execs != null)
						execs.addIfAbsent(s);
				}
			}

		}
	}

	/**
	 * Method which searches the tracked services from the osgi container, trying to
	 * find the appropriate executor service. If the service is found then we
	 * initialise it with the data stored in the svarog database if any along with
	 * setting up appropriate ACLs
	 * 
	 * @param key           Key of the executor category and name, concatenated with
	 *                      a dot
	 * @param referenceDate Reference Date on which the executor must be valid.
	 * @return Object instance implementing the ISvExecutor interface
	 * @throws SvException Throws exception if executor isn't valid
	 */
	ISvExecutor loadExecutor(String key, DateTime referenceDate) throws SvException {
		ISvExecutor executor = null;
		CopyOnWriteArrayList<SvExecInstance> execs = executorMap.getIfPresent(key);
		if (execs == null)
			execs = new CopyOnWriteArrayList<SvExecInstance>();
		else { // just in case the executor was loaded after the long wait, lets
				// give it one more try
			for (SvExecInstance sve : execs) {
				if (referenceDate.isAfter(sve.getStartDate()) && referenceDate.isBefore(sve.getEndDate())) {
					if (!svCONST.STATUS_VALID.equals(sve.getStatus()))
						throw (new SvException("system.error.executor_not_valid", this.getInstanceUser(), null, key));

					executor = sve.getExecutor();
					break;
				}
			}
		}
		if (executor == null) {
			// if we did reach this point, we haven't found the executor
			// in the cache so we must search for it in the OSGI world.
			// See if any of the currently tracked ISvExecutor services
			// match the specified Category/Name
			loadOSGIExecutors(key, execs);

			// if there's more than 1 executor then sort by start date.
			if (execs.size() > 1)
				Collections.sort(execs, new Comparator<SvExecInstance>() {
					@Override
					public int compare(SvExecInstance o1, SvExecInstance o2) {
						return o1.getStartDate().compareTo(o2.getStartDate());
					}
				});

			executorMap.put(key, execs);
			// once again try to find the executor
			for (SvExecInstance sve : execs) {
				if (referenceDate.isAfter(sve.getStartDate()) && referenceDate.isBefore(sve.getEndDate())) {
					if (!svCONST.STATUS_VALID.equals(sve.getStatus()))
						throw (new SvException("system.error.executor_not_valid", this.getInstanceUser(), null, key));
					executor = sve.getExecutor();
					break;
				}
			}
		}
		return executor;
	}

	/**
	 * Method to return a list of executor keys available in a ExecutorGroup object
	 * 
	 * @param svg Reference to the SvExecutorGroup object
	 * @return List of executor keys
	 */
	static List<String> getKeys(ISvExecutorGroup svg) {
		List<String> keys = new ArrayList<String>();
		if (svg.getNames() != null)
			for (String name : svg.getNames())
				keys.add(getKey(svg.getCategory(), name));
		return keys;
	}

	/**
	 * Method to get the available OSGI services if the OSGI container is running
	 * 
	 * @return
	 */
	private Object[] getOSGIServices() {
		if (SvCore.svDaemonRunning.get() && SvarogDaemon.svcTracker != null)
			osgiServices = SvarogDaemon.svcTracker.getServices();
		// if the daemon is not running we assume JUnit test has set the
		// services
		return osgiServices;
	}

	/**
	 * This method shall be used only for unit testing purposes. When Svarog runs in
	 * Daemon mode the OSGI services come from the OSGI platform and this setting
	 * will be ignored
	 * 
	 * @param services The list of services which shall be available to the platform
	 *                 for testing
	 */
	public static void setOSGIServices(Object[] services) {
		osgiServices = services;
	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name, limited
	 * to a validity date specifed by start and end dates. If the reference date is
	 * null, Svarog will execute using the current system time. If Svarog has two
	 * executors with the same category/name valid in the system on the same date,
	 * it will always invoke execute on the older executor (The one with older
	 * startDate).
	 * 
	 * @param category      Category of the Executor
	 * @param name          The name of the Executor
	 * @param params        Parameters which will be passed to the execution
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @param returnType    The return type of the result
	 * @return Object instance of class type described by return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>, if the return
	 *                     type is wrong it will raise
	 *                     <code>system.err.wrong_return_type</code>. If there's
	 *                     underlying Svarog Exception from the executor, Svarog
	 *                     will leave it as it is. If there's other type of system
	 *                     exception, Svarog will mark the executor as dirty and
	 *                     raise the exception as
	 *                     <code>system.err.exec_failure</code>
	 */
	public Object execute(String category, String name, Map<String, Object> params, DateTime referenceDate,
			Class<?> returnType) throws SvException {
		String executorKey = getKey(category, name);
		return execute(executorKey, params, referenceDate, returnType);
	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name, limited
	 * to a validity date specifed by start and end dates. If the reference date is
	 * null, Svarog will execute using the current system time. If Svarog has two
	 * executors with the same category/name valid in the system on the same date,
	 * it will always invoke execute on the older executor (The one with older
	 * startDate).
	 * 
	 * @param executorKey   Key of the executor, category and name, concatenated
	 *                      with a dot
	 * @param params        Parameters which will be passed to the execution
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @param returnType    The return type of the result
	 * @return Object instance of class type described by return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>, if the return
	 *                     type is wrong it will raise
	 *                     <code>system.err.wrong_return_type</code>. If there's
	 *                     underlying Svarog Exception from the executor, Svarog
	 *                     will leave it as it is. If there's other type of system
	 *                     exception, Svarog will mark the executor as dirty and
	 *                     raise the exception as
	 *                     <code>system.err.exec_failure</code>
	 */
	public Object execute(String executorKey, Map<String, Object> params, DateTime referenceDate, Class<?> returnType)
			throws SvException {

		if (!this.hasPermission(executorKey))
			throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, this.getInstanceUser(), null, executorKey));

		Object result = null;
		try {
			// find the executor valid for the specific reference date
			ISvExecutor exec = getExecutor(executorKey, referenceDate);
			if (exec != null) {
				if (returnType != null && !exec.getReturningType().equals(returnType)) {
					String error = getKey(exec) + ", return type:" + returnType.toString();
					throw (new SvException("system.err.wrong_return_type", this.getInstanceUser(), null, error));
				}
				result = exec.execute(params, this);
			} else {
				String error = executorKey + ", reference date:"
						+ (referenceDate != null ? referenceDate.toString() : new DateTime().toString());
				throw (new SvException("system.err.exec_not_found", this.getInstanceUser(), null, error));
			}
		} catch (Exception e) {
			// if the exception is SvExection, then we consider it Svarog
			// Internal and just raise it
			if (e instanceof SvException)
				throw (e);
			else {
				// otherwise, we consider the exception to be system related so
				// we will remove the faulty service from the cache and the wrap
				// the exception in a SvException
				executorMap.invalidate(executorKey);
				String error = executorKey + ", return type:"
						+ (returnType != null ? returnType.toString() : "no type");
				throw (new SvException("system.err.exec_failure", this.getInstanceUser(), null, error, e));
			}
		}
		return result;

	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name, limited
	 * to a validity date specifed by start and end dates. If the reference date is
	 * null, Svarog will execute using the current system time. If Svarog has two
	 * executors with the same category/name valid in the system on the same date,
	 * it will always invoke execute on the older executor (The one with older
	 * startDate).
	 * 
	 * This version will not raise <code>system.err.wrong_return_type</code>.
	 * 
	 * @param category      Category of the Executor
	 * @param name          The name of the Executor
	 * @param params        Parameters which will be passed to the execution
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @return Object instance of class type described by return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>. If there's
	 *                     underlying Svarog Exception from the executor, Svarog
	 *                     will leave it as it is. If there's other type of system
	 *                     exception, Svarog will mark the executor as dirty and
	 *                     raise the exception as
	 *                     <code>system.err.exec_failure</code>
	 */
	public Object execute(String category, String name, Map<String, Object> params, DateTime referenceDate)
			throws SvException {
		Object result = null;
		result = execute(category, name, params, referenceDate, null);
		return result;

	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name, limited
	 * to a validity date specifed by start and end dates. If the reference date is
	 * null, Svarog will execute using the current system time. If Svarog has two
	 * executors with the same category/name valid in the system on the same date,
	 * it will always invoke execute on the older executor (The one with older
	 * startDate).
	 * 
	 * This version will not raise <code>system.err.wrong_return_type</code>.
	 * 
	 * @param executorKey   Key of the executor, category and name, concatenated
	 *                      with a dot
	 * @param params        Parameters which will be passed to the execution
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @return Object instance of class type described by return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>. If there's
	 *                     underlying Svarog Exception from the executor, Svarog
	 *                     will leave it as it is. If there's other type of system
	 *                     exception, Svarog will mark the executor as dirty and
	 *                     raise the exception as
	 *                     <code>system.err.exec_failure</code>
	 */
	public Object execute(String executorKey, Map<String, Object> params, DateTime referenceDate) throws SvException {
		Object result = null;
		result = execute(executorKey, params, referenceDate, null);
		return result;

	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name, limited
	 * to a validity date specifed by start and end dates. If the reference date is
	 * null, Svarog will execute using the current system time. If Svarog has two
	 * executors with the same category/name valid in the system on the same date,
	 * it will always invoke execute on the older executor (The one with older
	 * startDate).
	 * 
	 * This version will not raise <code>system.err.wrong_return_type</code>.
	 * 
	 * @param executorKey   Key of the executor, category and name, concatenated
	 *                      with a dot
	 * @param params        Parameters which will be passed to the execution
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @return Object instance of class type described by return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>. If there's
	 *                     underlying Svarog Exception from the executor, Svarog
	 *                     will leave it as it is. If there's other type of system
	 *                     exception, Svarog will mark the executor as dirty and
	 *                     raise the exception as
	 *                     <code>system.err.exec_failure</code>
	 */
	public Object executePack(String executorPackCode, String executorPackItem, Map<String, Object> params,
			DateTime referenceDate) throws SvException {
		Object result = null;
		try (SvReader svr = new SvReader(this)) {
			DbDataObject pack = svr.getObjectByUnqConfId(executorPackCode,
					SvCore.getDbt(svCONST.OBJECT_TYPE_EXECUTOR_PACK));

			Map<String, ISvExecutor> packItems = this.getExecutorPackItems(pack, referenceDate);
			ISvExecutor sve = packItems.get(executorPackItem);
			result = execute(this.getKey(sve), params, referenceDate, null);
		}
		return result;

	}

	/**
	 * Method to provide information about specific executor's return type
	 * 
	 * @param category      Category of the Executor
	 * @param name          The name of the Executor
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @return Class object describing the return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>
	 */
	public Class<?> getReturnType(String category, String name, DateTime referenceDate) throws SvException {
		Class<?> result = null;
		// find the executor valid for the specific reference date
		ISvExecutor exec = getExecutor(category, name, referenceDate);
		if (exec != null) {
			result = exec.getReturningType();
		} else {
			String error = getKey(category, name) + ", reference date:"
					+ (referenceDate != null ? referenceDate.toString() : new DateTime());
			throw (new SvException("system.err.exec_not_found", this.getInstanceUser(), null, error));
		}
		return result;

	}

	/**
	 * Method to provide information about specific executor's return type
	 * 
	 * @param executorKey   Key of the executor, category and name, concatenated
	 *                      with a dot
	 * @param referenceDate The reference date on which the Executor must be valid.
	 * @return Class object describing the return type
	 * @throws SvException If the executor is not found Svarog will raise
	 *                     <code>system.err.exec_not_found</code>
	 */
	public Class<?> getReturnType(String executorKey, DateTime referenceDate) throws SvException {
		Class<?> result = null;
		// find the executor valid for the specific reference date
		ISvExecutor exec = getExecutor(executorKey, referenceDate);
		if (exec != null) {
			result = exec.getReturningType();
		} else {
			String error = getKey(exec) + ", reference date:" + referenceDate.toString();
			throw (new SvException("system.err.exec_not_found", this.getInstanceUser(), null, error));
		}
		return result;

	}

	/**
	 * Method to create a ROOT package of executors identified via the labelCode.
	 * Root packs are usually inherited by child packs.
	 * 
	 * @param labelCode unique identifier of the executor package
	 * @param notes     Free text comment
	 * @return DbDataObject containing the executor pack descriptor
	 * @throws SvException Standard SvException is raised is the pack can not be
	 *                     saved to the database or the label code already exists.
	 */
	public DbDataObject createExecutorPack(String labelCode, String notes) throws SvException {
		return createExecutorPack(labelCode, notes, 0L);
	}

	/**
	 * Method to create a package of executors identified via the labelCode. If the
	 * parent Id is null or 0, the pack is considered to be the root pack. Root
	 * packs are usually inherited by child packs.
	 * 
	 * @param labelCode    unique identifier of the executor package
	 * @param notes        Free text comment
	 * @param parentPackId The object id of the parent pack from which this pack
	 *                     should inherit available executors
	 * @return DbDataObject containing the executor pack descriptor
	 * @throws SvException Standard SvException is raised is the pack can not be
	 *                     saved to the database or the label code already exists.
	 */
	public DbDataObject createExecutorPack(String labelCode, String notes, Long parentPackId) throws SvException {
		DbDataObject dbo = new DbDataObject(svCONST.OBJECT_TYPE_EXECUTOR_PACK);
		dbo.setVal(Sv.LABEL_CODE, labelCode);
		dbo.setVal("NOTES", labelCode);
		dbo.setParentId(parentPackId);
		try (SvWriter svw = new SvWriter(this)) {
			svw.saveObject(dbo);
		}
		return dbo;
	}

	/**
	 * Method to create an executor pack item
	 * 
	 * @param execPack  The parent executor pack under which the item will be
	 *                  created
	 * @param labelCode The unique identifier of the item
	 * @param executor  The reference executor to be associated with the label code
	 * 
	 * @return DbDataObject containing the item descriptor
	 * @throws SvException Standard SvException is raised is the pack can not be
	 *                     saved to the database or the label code already exists.
	 */
	public DbDataObject createExecutorPackItem(DbDataObject execPack, String labelCode, ISvExecutor executor)
			throws SvException {
		return createExecutorPackItem(execPack, labelCode, executor.getCategory(), executor.getName());
	}

	/**
	 * Method to create an executor pack item
	 * 
	 * @param execPack    The parent executor pack under which the item will be
	 *                    created
	 * @param labelCode   The unique identifier of the item
	 * @param exeCategory The executor category
	 * @param exeName     The executor name
	 * @return DbDataObject containing the item descriptor
	 * @throws SvException Standard SvException is raised is the pack can not be
	 *                     saved to the database or the label code already exists.
	 */
	public DbDataObject createExecutorPackItem(DbDataObject execPack, String labelCode, String exeCategory,
			String exeName) throws SvException {
		if (execPack.getObjectId() > 0L) {
			DbDataObject dbo = new DbDataObject(svCONST.OBJECT_TYPE_EXECPACK_ITEM);
			dbo.setVal(Sv.EXECUTOR_KEY, SvExecManager.getKey(exeCategory, exeName));
			dbo.setVal(Sv.LABEL_CODE, labelCode);
			dbo.setParentId(execPack.getObjectId());
			try (SvWriter svw = new SvWriter(this)) {
				svw.saveObject(dbo);
			}
			return dbo;
		}
		return execPack;
	}

	/**
	 * Method to fetch all executor items configured under the specified executor
	 * pack. Method shall traverse recursively the parent tree of the pack in order
	 * get all inherited items up to the root pack
	 * 
	 * @param execPack        The executor pack items to return
	 * @param executorRefDate The executor reference date
	 * @return A map containing the pack item label code and the corresponding
	 *         ISvExecutor instance
	 * @throws SvException Standard SvException is raised if the SvReader raises
	 *                     any.
	 */
	public Map<String, ISvExecutor> getExecutorPackItems(DbDataObject execPack, DateTime executorRefDate)
			throws SvException {
		Map<String, ISvExecutor> items = null;
		if (execPack.getObjectId() > 0L) {
			try (SvReader svr = new SvReader(this)) {

				DbDataObject parentPack = null;
				if (!execPack.getParentId().equals(0L)) {
					parentPack = svr.getObjectById(execPack.getParentId(), execPack.getObjectType(), null);
				}
				if (parentPack != null)
					items = getExecutorPackItems(parentPack, executorRefDate);
				else {
					items = new HashMap<String, ISvExecutor>();
				}
				DbDataArray dba = svr.getObjectsByParentId(execPack.getObjectId(), svCONST.OBJECT_TYPE_EXECPACK_ITEM,
						null);
				for (DbDataObject dbo : dba.getItems()) {
					ISvExecutor sve = this.getExecutor((String) dbo.getVal(Sv.EXECUTOR_KEY), executorRefDate);
					items.put((String) dbo.getVal(Sv.LABEL_CODE), sve);
				}

			}

		}
		return items;
	}

}
