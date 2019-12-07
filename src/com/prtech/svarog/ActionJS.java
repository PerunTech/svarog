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

import javax.script.*;

/**
 * Class to execute an action configured in the Rule Engine, which is in the
 * form of a JavaScript file
 * 
 * @author PR01
 *
 */
public class ActionJS {

	/**
	 * Prepare statement using SQL query input string with DbDataObject
	 * properties as bind parameters. Execute query and return true if
	 * everything OK.
	 * 
	 * @param script
	 *            JavaScript code to be executed by engine
	 * @param action_id
	 *            The object id of the executed action
	 * @param jsonData
	 *            Data Object/Array in JSON format
	 * @param jsonResults
	 *            Execution results in JSON format
	 * @return jsonResults New execution results in JSON format, updated with
	 *         current result and errors
	 * @throws ScriptException
	 *             An exception raised by the JavaScript engine
	 */
	public String execute(String script, Long action_id, String jsonData, String jsonResults) throws ScriptException {

		// initialize script engine
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine engine = factory.getEngineByName("JavaScript");

		// set script input parameters
		engine.put("action_id", action_id);
		engine.put("data", jsonData);
		engine.put("results", jsonResults);

		// execute script
		engine.eval(script);

		// get results from script and return
		jsonResults = engine.get("results").toString();
		return jsonResults;
	}

}
