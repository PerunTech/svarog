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

import java.lang.reflect.Method;
import java.util.Map;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

/**
 * Class to execute an action configured in the Rule Engine, which is in the
 * form of a Java JAR file
 * 
 * @author PR01
 *
 */
public class ActionJAR extends SvCore {

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @param sharedSvCore
	 *            SvCore instance to be used as base for JDBC connection sharing
	 * @throws SvException
	 *             Pass-thru exception from the super class
	 */
	ActionJAR(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Svarog specific class loader
	 */
	static SvClassLoader svcl = getSvClassLoader();

	/**
	 * Method to get the SvClassLoader instance
	 * 
	 * @return SvClassLoader instance
	 */
	public static SvClassLoader getSvClassLoader() {
		SvClassLoader svclcurr = SvClassLoader.getInstance();
		return svclcurr;
	}

	/**
	 * Method for executing a JAR action of the rule engine
	 * 
	 * @param dataObject
	 *            The object upon which the action is executed
	 * @param resultsObject
	 *            The object containing the result of the action
	 * @param params
	 *            A map containing additional parameters to be sent to the
	 *            executed method
	 * @param className
	 *            The name of the class owner of the method
	 * @param methodName
	 *            The name of the method to be executed
	 * @param actionId
	 *            The id of the action which is executed
	 * @return The resulting object from the action execution
	 * @throws SvException
	 *             A rule engine raised exception
	 */
	@SuppressWarnings("resource")
	public Object execute(DbDataObject dataObject, DbDataArray resultsObject, Map<Object, Object> params,
			String className, String methodName, Long actionId) throws SvException {
		Object result = null;
		try {
			params.put("SV_CORE", this);
			Object o = null;
			SvClassLoader svl = SvClassLoader.getInstance();
			Class<?> actClass = svl.loadClass(className);
			if (actClass != null) {
				Class<?>[] cArg = new Class[3];
				cArg[0] = DbDataObject.class;
				cArg[1] = DbDataArray.class;
				cArg[2] = Map.class;
				Method m = actClass.getMethod(methodName, cArg);

				o = actClass.newInstance();
				Object myargs[] = { dataObject, resultsObject, params };
				result = m.invoke(o, myargs);
			}
		} catch (Exception e) {
			throw (new SvException("system.error.re_java_err", instanceUser, dataObject, actionId, e));
		}
		return result;
	}

}
