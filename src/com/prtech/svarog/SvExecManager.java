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

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog_interfaces.ISvExecutor;

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
	 * Log4j instance used for logging
	 */
	static final Logger log4j = LogManager.getLogger(SvCore.class.getName());

	/**
	 * Cache with all executors responding to a specific command string. The Key
	 * is represents a CATEGORY.NAME
	 */
	private static final Cache<String, CopyOnWriteArrayList<ISvExecutor>> executorMap = initExecutorCache();

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public SvExecManager(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public SvExecManager(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public SvExecManager(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
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
	static Cache<String, CopyOnWriteArrayList<ISvExecutor>> initExecutorCache() {
		CacheBuilder builder = CacheBuilder.newBuilder();
		builder = builder.maximumSize(5000);
		builder = builder.expireAfterAccess(10, TimeUnit.MINUTES);
		return (Cache<String, CopyOnWriteArrayList<ISvExecutor>>) builder
				.<String, CopyOnWriteArrayList<ISvExecutor>>build();
	}

	/**
	 * Method to create a consistent string key of an executor category.name
	 * 
	 * @param exec
	 *            The executor instance
	 * @return The string key separated by coma.
	 */
	String getKey(ISvExecutor exec) {
		return getKey(exec.getCategory(), exec.getName());
	}

	/**
	 * Method to create a consistent string key from two strings separated by
	 * coma
	 * 
	 * @param category
	 *            The first part of the key
	 * @param name
	 *            The second part of the key
	 * @return The string key separated by coma.
	 */
	String getKey(String category, String name) {
		return (category != null ? category : "") + "." + (name != null ? name : "");
	}

	/**
	 * Method to find an ISvExecutor in Svarog. First it will try to get the
	 * executor from the Cache, then if it doesn't exist, it will scan the OSGI
	 * container to find one.
	 * 
	 * @param category
	 *            Category of the executor
	 * @param name
	 *            Name of the Executor
	 * @param referenceDate
	 *            Reference Date on which the executor must be valid.
	 * @return
	 */
	ISvExecutor getExecutor(String category, String name, DateTime referenceDate) {
		String key = getKey(category, name);

		ISvExecutor executor = null;
		if (referenceDate == null)
			referenceDate = new DateTime();

		CopyOnWriteArrayList<ISvExecutor> execs = executorMap.getIfPresent(key);
		if (execs != null) {

			for (ISvExecutor sve : execs) {
				if (referenceDate.isAfter(sve.getStartDate()) && referenceDate.isBefore(sve.getEndDate())) {
					executor = sve;
					break;
				}
			}
		}
		if (executor == null) {
			if (execs == null)
				execs = new CopyOnWriteArrayList<ISvExecutor>();
			// if we did reach this point, we haven't found the executor in
			// the
			// cache so we must search for it in the OSGI world.

			// See if any of the currently tracked ISvExecutor services
			// match the specified Category/Name
			Object[] services = SvarogDaemon.svcTracker.getServices();
			for (int i = 0; (services != null) && (i < services.length); i++) {
				ISvExecutor sve = (ISvExecutor) services[i];

				if (getKey(sve).equals(key))
					execs.addIfAbsent(sve);
			}
			// if there's more than 1 executor then sort by start date.
			if (execs.size() > 1)
				Collections.sort(execs, new Comparator<ISvExecutor>() {
					@Override
					public int compare(ISvExecutor o1, ISvExecutor o2) {
						return o1.getStartDate().compareTo(o2.getStartDate());
					}
				});

			executorMap.put(key, execs);

			for (ISvExecutor sve : execs) {
				if (referenceDate.isAfter(sve.getStartDate()) && referenceDate.isBefore(sve.getEndDate())) {
					executor = sve;
					break;
				}
			}
		}
		return executor;
	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name,
	 * limited to a validity date specifed by start and end dates. If the
	 * reference date is null, Svarog will execute using the current system
	 * time. If Svarog has two executors with the same category/name valid in
	 * the system on the same date, it will always invoke execute on the older
	 * executor (The one with older startDate).
	 * 
	 * @param category
	 *            Category of the Executor
	 * @param name
	 *            The name of the Executor
	 * @param params
	 *            Parameters which will be passed to the execution
	 * @param referenceDate
	 *            The reference date on which the Executor must be valid.
	 * @param returnType
	 *            The return type of the result
	 * @return Object instance of class type described by return type
	 * @throws SvException
	 *             If the executor is not found Svarog will raise
	 *             <code>system.err.exec_not_found</code>, if the return type is
	 *             wrong it will raise
	 *             <code>system.err.wrong_return_type</code>. If there's
	 *             underlying Svarog Exception from the executor, Svarog will
	 *             leave it as it is. If there's other type of system exception,
	 *             Svarog will mark the executor as dirty and raise the
	 *             exception as <code>system.err.exec_failure</code>
	 */
	public Object execute(String category, String name, Map<String, Object> params, DateTime referenceDate,
			Class<?> returnType) throws SvException {
		
		if(!this.hasPermission(getKey(category, name)))
			throw (new SvException("system.error.not_authorised", this.getInstanceUser(), null, getKey(category, name)));

		Object result = null;
		try {
			// find the executor valid for the specific reference date
			ISvExecutor exec = getExecutor(category, name, referenceDate);
			if (exec != null) {
				if (exec.getReturningType().equals(returnType))
					result = exec.execute(params, this);
				else {
					String error = getKey(exec) + ", return type:" + returnType.toString();
					throw (new SvException("system.err.wrong_return_type", this.getInstanceUser(), null, error));
				}
			} else {
				String error = getKey(exec) + ", reference date:" + referenceDate.toString();
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
				executorMap.invalidate(getKey(category, name));
				String error = getKey(category, name) + ", return type:" + returnType.toString();
				throw (new SvException("system.err.exec_failure", this.getInstanceUser(), null, error, e));
			}
		}
		return result;

	}

	/**
	 * Method to provide global inter module execution inside Svarog. The Svarog
	 * Executors are identified by unique combination of Category and Name,
	 * limited to a validity date specifed by start and end dates. If the
	 * reference date is null, Svarog will execute using the current system
	 * time. If Svarog has two executors with the same category/name valid in
	 * the system on the same date, it will always invoke execute on the older
	 * executor (The one with older startDate).
	 * 
	 * This version will not raise <code>system.err.wrong_return_type</code>.
	 * 
	 * @param category
	 *            Category of the Executor
	 * @param name
	 *            The name of the Executor
	 * @param params
	 *            Parameters which will be passed to the execution
	 * @param referenceDate
	 *            The reference date on which the Executor must be valid.
	 * @param returnType
	 *            The return type of the result
	 * @return Object instance of class type described by return type
	 * @throws SvException
	 *             If the executor is not found Svarog will raise
	 *             <code>system.err.exec_not_found</code>. If there's underlying
	 *             Svarog Exception from the executor, Svarog will leave it as
	 *             it is. If there's other type of system exception, Svarog will
	 *             mark the executor as dirty and raise the exception as
	 *             <code>system.err.exec_failure</code>
	 */
	public Object execute(String category, String name, Map<String, Object> params, DateTime referenceDate)
			throws SvException {
		Object result = null;
		Class<?> returnType = getReturnType(category, name, referenceDate);
		result = execute(category, name, params, referenceDate, returnType);
		return result;

	}

	/**
	 * Method to provide information about specific executor's return type
	 * 
	 * @param category
	 *            Category of the Executor
	 * @param name
	 *            The name of the Executor
	 * @param referenceDate
	 *            The reference date on which the Executor must be valid.
	 * @return Class object describing the return type
	 * @throws SvException
	 *             If the executor is not found Svarog will raise
	 *             <code>system.err.exec_not_found</code>
	 */
	public Class<?> getReturnType(String category, String name, DateTime referenceDate) throws SvException {
		Class<?> result = null;
		// find the executor valid for the specific reference date
		ISvExecutor exec = getExecutor(category, name, referenceDate);
		if (exec != null) {
			result = exec.getReturningType();
		} else {
			String error = getKey(exec) + ", reference date:" + referenceDate.toString();
			throw (new SvException("system.err.exec_not_found", this.getInstanceUser(), null, error));
		}
		return result;

	}
}
