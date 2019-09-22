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
package com.prtech.svarog_common;

import com.prtech.svarog.SvCore;
import com.prtech.svarog.SvException;

/**
 * Interface for implementing custom data saving methods for the RuleEngine
 * @author XPS13
 *
 */
public interface ISvRuleEngineSave {

	/**
	 * The beforeSave callback is aimed at providing external validation to objects.
	 * If the invoked method returns false, svarog will abort the objectSave method
	 * @param parentCore The SvCore under which the save operations are pending
	 * @param targetObject The object upon which the rule was executed
	 * @param actionResults The array with results from the action execution
	 * @param success False if any of the actions failed
	 * @throws SvException Any thrown exception should be forwarded up to the caller
	 */
	public void saveFinalState(SvCore parentCore, DbDataObject targetObject,  DbDataArray actionResults, Boolean success) throws SvException;

	/**
	 * Method to be called after each action is executed by the rule engine. It should be used to save the current state of execution
	 * @param parentCore The current SvCore used for rule execution
	 * @param currentAction The current action which is being executed
	 * @param execObj The object upon which is the action executed
	 * @param actionResults The results of all previous action executions
	 * @param success The current success status of the rule execution
	 * @throws SvException Any thrown exception should be forwarded up to the caller 
	 */
	public void saveCurrentState(SvCore parentCore, DbDataObject currentAction, DbDataObject execObj, DbDataArray actionResults, Boolean success) throws SvException;

}
