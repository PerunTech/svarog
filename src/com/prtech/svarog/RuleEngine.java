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

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.ISvRuleEngineSave;
import com.prtech.svarog.svCONST;

public class RuleEngine extends SvCore {

	private static final Logger log4j = SvConf.getLogger(RuleEngine.class);

	/**
	 * Private member holding reference to object implementing the save methods
	 * for the rule engine
	 */
	ISvRuleEngineSave ruleEngineSave = null;

	/**
	 * Constructor to create a SvCore inherited object according to a user
	 * session. This is the default constructor available to the public, in
	 * order to enforce the svarog security mechanisms based on the logged on
	 * user.
	 * 
	 * 
	 * @param session_id
	 *            String UID of the user session under which the SvCore instance
	 *            will run
	 * @throws SvException
	 *             Pass through exception from the super class constructor
	 */
	public RuleEngine(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public RuleEngine(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * 
	 * 
	 * @param sharedSvCore
	 *            The SvCore instance which will be used for JDBC connection
	 *            sharing (i.e. parent SvCore)
	 * @throws SvException
	 *             Pass through exception from the super class constructor
	 */
	public RuleEngine(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 *             Pass through exception from the super class constructor
	 */
	RuleEngine() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Main Rule engine entry point. Will commit on success or rollback on
	 * exception.
	 * 
	 * @param rule_id
	 *            The id of the rule subject of execution
	 * @param actionResults
	 *            The results from the execution
	 * @param obj
	 *            The object over which the rule was executed
	 * @param params
	 *            Additional parameters for the rule engine
	 * @return True if the execution was successful
	 * @throws SvException
	 */
	public Boolean execute(Long rule_id, DbDataArray actionResults, DbDataObject obj, Map<Object, Object> params)
			throws SvException {
		return execute(rule_id, actionResults, obj, params, this.autoCommit);
	}

	/**
	 * Main Rule engine entry point. It will execute a certain rule identified
	 * by rule id and return results in the actionResults
	 * 
	 * @param rule_id
	 *            The id of the rule subject of execution
	 * @param actionResults
	 *            The results from the execution
	 * @param obj
	 *            The object over which the rule was executed
	 * @param params
	 *            Additional parameters for the rule engine
	 * @param autoCommit
	 *            If the rule engine should commit the transaction or rollback
	 *            by default
	 * @return True if the execution was successful
	 * @throws SvException
	 */
	public Boolean execute(Long rule_id, DbDataArray actionResults, DbDataObject obj, Map<Object, Object> params,
			Boolean autoCommit) throws SvException {
		Boolean success = false;
		try {
			this.dbSetAutoCommit(false);
			success = executeImpl(rule_id, actionResults, obj, params);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);
		}
		return success;

	}

	/**
	 * Method to create and execution ActionSQL which based on executing custom
	 * SQL code
	 * 
	 * @param currentAction
	 *            The current executing action descriptor
	 * @param obj
	 *            The object over which the action is executed
	 * @param execObj
	 *            The execution descriptor
	 * @param actionResults
	 *            The array holding the action results
	 * @param params
	 *            Additional parameters passed to the action
	 * @param fileData
	 *            The fileData containing the SQL source code
	 * @throws SvException
	 */
	private void execSQL(DbDataObject currentAction, DbDataObject obj, DbDataObject execObj, DbDataArray actionResults,
			Map<Object, Object> params, byte[] fileData) throws SvException {
		ActionSQL as = null;
		try {
			as = new ActionSQL(this);
			String calltype = currentAction.getVal("CODE_SUBTYPE").toString();
			String actionReturn = currentAction.getVal("RETURN_TYPE").toString().toUpperCase();
			Long rettype = new Long(Class.forName("java.sql.Types").getDeclaredField(actionReturn).getInt(null));
			String className = currentAction.getVal("CLASS_NAME").toString();
			String sql = new String(fileData, "UTF-8");
			Object result = as.execute(sql, obj, execObj.getObject_id(), calltype, rettype, className, params, false);
			addResults(result, actionResults, currentAction.getObject_id());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw (new SvException("system.error.re_sql_err", instanceUser, obj, currentAction, e));
		} finally {
			if (as != null)
				as.release();
		}
	}

	/**
	 * Method to create and execute an ActionJar instance.
	 * 
	 * @param currentAction
	 *            The current executing action descriptor
	 * @param obj
	 *            The object over which the action is executed
	 * @param execObj
	 *            The execution descriptor
	 * @param actionResults
	 *            The array holding the action results
	 * @param params
	 *            Additional parameters passed to the action
	 * @param fileData
	 *            The fileData containing the jar byte code
	 * @throws SvException
	 */
	private void execJAR(DbDataObject currentAction, DbDataObject obj, DbDataObject execObj, DbDataArray actionResults,
			Map<Object, Object> params, byte[] fileData) throws SvException {
		ActionJAR aj = null;
		try {
			aj = new ActionJAR(this);
			String className = currentAction.getVal("CLASS_NAME").toString();
			String methodName = currentAction.getVal("METHOD_NAME").toString();

			Object result = aj.execute(obj, actionResults, params, className, methodName, currentAction.getObject_id());
			addResults(result, actionResults, currentAction.getObject_id());
		} catch (Exception e) {
			if (e instanceof SvException)
				throw (e);
			else
				throw (new SvException("system.error.java_exec_err", instanceUser, obj, currentAction, e));
		} finally {
			if (aj != null)
				aj.release();
		}
	}

	/**
	 * Method to create and execution ActionSQL which based on executing custom
	 * SQL code
	 * 
	 * @param currentAction
	 *            The current executing action descriptor
	 * @param obj
	 *            The object over which the action is executed
	 * @param actionResults
	 *            The array holding the action results
	 * @param fileData
	 *            The fileData containing the java script code
	 * @throws SvException
	 */
	private void execJS(DbDataObject currentAction, DbDataObject obj, DbDataArray actionResults, byte[] jsFileData)
			throws SvException {

		ActionJS ajs = new ActionJS();

		String jsonObj = obj.toJson().toString();
		String jsonResultsBefore = actionResults.toJson().toString();

		String js;
		try {
			js = new String(jsFileData, "UTF-8");
			String jsonResultsAfter = ajs.execute(js, currentAction.getObject_id(), jsonObj, jsonResultsBefore);

			// reload action results from JSON
			Gson gson = new GsonBuilder().create();
			JsonObject jobj = gson.fromJson(jsonResultsAfter, JsonObject.class);
			actionResults.fromJson(jobj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw (new SvException("system.error.re_javascript_err", instanceUser, obj, currentAction, e));
		}

	}
	
	/**
	 * Method to create and execute an Executor.
	 * 
	 * @param currentAction
	 *            The current executing action descriptor
	 * @param obj
	 *            The object over which the action is executed
	 * @param actionResults
	 *            The array holding the action results
	 * @param params
	 *            Additional parameters passed to the action
	 * @param fileData
	 *            The fileData containing the jar byte code
	 * @throws SvException
	 */
	private void execExecutor(DbDataObject currentAction, DbDataObject obj,
			DbDataArray actionResults, Map<Object, Object> params) throws SvException {
		SvExecManager sve = null;
		try {
			HashMap<String, Object> exeParams = new HashMap<>();
			sve = new SvExecManager(this);
			
			String executorKey = currentAction.getVal("METHOD_NAME").toString();
			for (Entry<Object, Object> entry : params.entrySet()) {
				exeParams.put(entry.getKey().toString(), entry.getValue());
			}
			exeParams.put("OBJECT", obj);
			Object result = sve.execute(executorKey, exeParams, null, Object.class);
			addResults(result, actionResults, currentAction.getObjectId());
		} catch (Exception e) {
			if (e instanceof SvException)
				throw (e);
			else
				throw (new SvException("system.error.java_exec_err", instanceUser, obj, currentAction, e));
		} finally {
			if (sve != null)
				sve.release();
		}
	}

	/**
	 * Method to fetch the File data containing the source code or byte code for
	 * the associated action
	 * 
	 * @param currentAction
	 *            The action for which we need the file
	 * @return The byte[] containing the binary file data
	 * @throws SvException
	 */
	private byte[] getActionFile(DbDataObject currentAction) throws SvException {
		SvFileStore fs = null;
		byte[] fileData = null;
		Boolean needFileLoad = true;
		try {
			// if the action is Java then the file is a JAR.
			// Check if the jar is already loaded
			fs = new SvFileStore(this);
			if (log4j.isDebugEnabled())
				log4j.trace("Executing action= " + currentAction.toJson().toString());
			DbDataArray actionFiles = fs.getFiles(currentAction, null, null);
			if (actionFiles.getItems().size() < 1) {
				log4j.warn("Action doesn't have any files attached. Skipping action execution. Action data:"
						+ currentAction.toJson());
				fileData = null;
			}
			DbDataObject fileObj = actionFiles.getItems().get(0);
			needFileLoad = !SvClassLoader.isJarLoaded(fileObj.getObject_id().toString());

			if (needFileLoad) {
				// read file to be executed from database

				fileData = fs.getFileAsByte(fileObj);

				if (fileData == null || fileData.length < 1) {
					log4j.warn("Action has attached an empty file!. Skipping action execution. Action data:"
							+ currentAction.toJson());
					fileData = null;
				}
				// lets load the JAR into the class loader if its a JAR
				if (((String) currentAction.getVal("code_type")).equals("Java")) {
					if (!SvClassLoader.isJarLoaded(fileObj.getObject_id().toString(), fileData)) {
						SvClassLoader svl = SvClassLoader.getInstance();
						svl.loadJar(fileData, fileObj.getObject_id().toString());
					}
				}
			} else {
				fileData = SvClassLoader.getJarData(fileObj.getObject_id().toString());
			}

		} finally {
			if (fs != null)
				fs.release();
		}
		return fileData;
	}

	/**
	 * The main rule execution method.
	 * 
	 * @param rule_id
	 *            The id of the rule to be executed
	 * @param actionResults
	 *            The array holding the results of all executed actions
	 * @param obj
	 *            The object over which the action is executed
	 * @param params
	 *            Additional parameters to be passed to the rule execution
	 * @return
	 * @throws SvException
	 */
	protected Boolean executeImpl(Long rule_id, DbDataArray actionResults, DbDataObject obj, Map<Object, Object> params)
			throws SvException {

		if (log4j.isDebugEnabled())
			log4j.trace("Start executing rule id = " + rule_id);
		SvReader svr = null;

		Boolean success = false;
		final String CODE_TYPE = "code_type";
		try {
			svr = new SvReader(this);
			DbDataObject ruleObj = svr.getObjectById(rule_id, svCONST.OBJECT_TYPE_RULE, null);
			if (ruleObj != null) {
				if (log4j.isDebugEnabled())
					log4j.trace("Executing rule object= " + ruleObj.toJson().toString());
				// get rule actions objects from database
				DbDataArray dbActions = svr.getObjectsByParentId(ruleObj.getObject_id(), svCONST.OBJECT_TYPE_ACTION,
						null, svCONST.MAX_ACTIONS_PER_RULE, 0);
				// if actions fetched ok, start executing
				if (dbActions != null && dbActions.getItems().size() > 0) {
					success = true;

					// initiate new rule execution object
					DbDataObject execObj = new DbDataObject();
					execObj.setObject_type(svCONST.OBJECT_TYPE_EXECUTION);
					execObj.setParent_id(rule_id);
					execObj.setVal("OBJ_EXEC_ON", obj.getObject_id());

					// Boolean loop = true;
					String actionType = null;

					for (DbDataObject currentAction : dbActions.getSortedItems("SORT_ORDER")) {

						byte[] fileData = null;
						if (!currentAction.getVal(CODE_TYPE).toString().equals("Executor")) {

							fileData = getActionFile(currentAction);
							if (fileData == null) {
								continue;
							}
						}
						try {
							// execute JavaScript action
							switch ((String) currentAction.getVal(CODE_TYPE)) {
							case "JavaScript":
								execJS(currentAction, obj, actionResults, fileData);
								break;
							case "SQL":
								execSQL(currentAction, obj, execObj, actionResults, params, fileData);
								break;
							case "Java":
								execJAR(currentAction, obj, execObj, actionResults, params, fileData);
								break;
							case "Executor":
								execExecutor(currentAction, obj, actionResults, params);
								break;
							default:
								throw (new SvException("system.error.re_unknown_action_type", instanceUser, obj,
										currentAction));
							}
						} catch (Exception e) {
							success = false;
							actionResults.addDataItem(
									makeNewResult(currentAction.getObject_id(), "", e.getMessage(), false));
							// save the current state before throwing exception
							saveCurrentState(svr, currentAction, execObj, actionResults, success);
							if (e instanceof SvException)
								throw (e);
							else
								throw (new SvException("system.error.re_exception_action", instanceUser, obj,
										currentAction, e));

						}
						saveCurrentState(svr, currentAction, execObj, actionResults, success);

						if (log4j.isDebugEnabled())
							log4j.trace("End executing action  " + currentAction.getVal("ACTION_NAME") + " of type "
									+ actionType);

						// check if action result is pass/fail in order to stop
						// further execution
						// if ((Boolean) ruleObj.getVal("IS_STOPPABLE")) {
						// loop = false;
						// log4j.info("Stop executing rule id = " + rule_id);

					} // loop rule actions
				} else {
					throw (new SvException("system.error.re_empty_rule", instanceUser, obj, rule_id));
				}
			} else {

				throw (new SvException("system.error.re_unknown_rule", instanceUser, obj, rule_id));
			}

		} finally {
			saveFinalState(svr, obj, actionResults, success);
			if (svr != null)
				svr.release();
		}

		log4j.info("End executing rule id = " + rule_id);

		return success;
	}

	/**
	 * Method that saves the final state of the rule execution
	 * 
	 * @param parentCore
	 *            the SvCore instance used for rule execution
	 * @param targetObject
	 *            The object upon which the rule was executed
	 * @param actionResults
	 *            The array with results from the action execution
	 * @param success
	 *            False if any of the actions failed
	 * @throws SvException
	 *             Any thrown exception should be forwarded up to the caller
	 */
	private void saveFinalState(SvCore parentCore, DbDataObject targetObject, DbDataArray actionResults,
			Boolean success) throws SvException {
		if (ruleEngineSave != null)
			ruleEngineSave.saveFinalState(parentCore, targetObject, actionResults, success);

	}

	private void saveCurrentState(SvCore parentCore, DbDataObject currentAction, DbDataObject execObj,
			DbDataArray actionResults, Boolean success) throws SvException {
		if (ruleEngineSave != null)
			ruleEngineSave.saveCurrentState(parentCore, currentAction, execObj, actionResults, success);
		else
			saveCurrentState(execObj, actionResults, success);
	}

	/**
	 * Method to parse back the results from the actions and add them to the
	 * results array
	 * 
	 * @param result
	 * @param actionResults
	 * @param action_id
	 * @throws SvException
	 */
	private void addResults(Object result, DbDataArray actionResults, Long action_id) throws SvException {
		// Class currClass = result.getClass();
		if (result instanceof java.sql.Array) {
			java.sql.Array arr = (java.sql.Array) result;
			try {
				String[] values = (String[]) arr.getArray();
				int length = Array.getLength(values);
				for (int j = 0; j < length; j++) {
					Object singleResult = Array.get(values, j);
					actionResults.addDataItem(makeNewResult(action_id, singleResult, "", true));
				}
			} catch (SQLException e) {
				throw (new SvException("system.error.re_exception_action", instanceUser, actionResults, result, e));
			}

		} else if (result instanceof List) {
			@SuppressWarnings("unchecked")
			List<Object> values = (List<Object>) result;
			for (Object singleResult : values) {

				actionResults.addDataItem(makeNewResult(action_id, singleResult, "", true));
			}
		} else {
			actionResults.addDataItem(makeNewResult(action_id, result, "", true));
		}
	}

	/**
	 * Method to create a new instance of action result
	 * 
	 * @param action_id
	 *            The action id
	 * @param result
	 *            The resulting object
	 * @param errors
	 *            Errors if any
	 * @param success
	 *            The success status
	 * @return
	 */
	private DbDataObject makeNewResult(Long action_id, Object result, String errors, Boolean success) {
		// save current action result
		DbDataObject resultObj = new DbDataObject(svCONST.OBJECT_TYPE_RESULT);
		resultObj.setVal("action_id", action_id);
		resultObj.setVal("errors", errors);
		resultObj.setVal("is_successful", success);

		if (errors != null && errors.length() > 0) {
			resultObj.setVal("result", null);
			resultObj.setVal("exec_state", "ERROR");

		} else {
			SvActionResult svResult = null;
			if (result.getClass().equals(String.class)) {
				Gson gson = new Gson();
				JsonObject jobj = gson.fromJson((String) result, JsonObject.class);
				svResult = new SvActionResult();
				svResult.fromJson(jobj);
			} else if (result.getClass().equals(SvActionResult.class)) {
				svResult = (SvActionResult) result;
			} else if (ruleEngineSave == null) {
				return null;
			}

			if (svResult != null) {
				resultObj.setVal("result", svResult.toJson().toString());
				resultObj.setVal("exec_state", svResult.getResult());
			} else
				resultObj.setVal("result", result);

		}
		return resultObj;
	}

	/**
	 * Method for saving the current state of the rule execution
	 * 
	 * @param execObj
	 *            The execution descriptor
	 * @param actionResults
	 *            The result of executed acctions
	 * @param success
	 *            The state of success
	 * @throws SvException
	 */
	private void saveCurrentState(DbDataObject execObj, DbDataArray actionResults, Boolean success) throws SvException {

		SvWriter svw = null;

		try {
			svw = new SvWriter(this);
			String currExecutionStatus = execObj.getVal("exec_state") != null ? execObj.getVal("exec_state").toString()
					: "PASS";
			for (DbDataObject actionResult : actionResults.getItems()) {
				String currResult = "";
				String exec_state = actionResult.getVal("exec_state").toString();
				if (exec_state.equals("PASS") || exec_state.equals("WARNING"))
					currResult = "PASS";
				else
					currResult = "FAIL";

				if (currExecutionStatus.equals("PASS") && currResult.equals("FAIL"))
					execObj.setVal("exec_state", "FAIL");

			}

			if (execObj.getIs_dirty() || !execObj.getVal("IS_SUCCESSFUL").equals(success)) {
				execObj.setVal("IS_SUCCESSFUL", success);
				svw.saveObject(execObj); // TO DO TRANSACTION
			}

			for (DbDataObject actionResult : actionResults.getItems()) {
				if (actionResult.getIs_dirty()) {
					actionResult.setParent_id(execObj.getObject_id());
					svw.saveObject(actionResult);
				}
			}
		} finally {
			if (svw != null)
				svw.release();
		}

	}

	public ISvRuleEngineSave getRuleEngineSave() {
		return ruleEngineSave;
	}

	public void setRuleEngineSave(ISvRuleEngineSave ruleEngineSave) {
		this.ruleEngineSave = ruleEngineSave;
	}

}
