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

import java.sql.SQLException;
import java.util.HashMap;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.ISvRuleEngineSave;
import com.prtech.svarog_common.Jsonable;

/**
 * The implementation of workflows, such as movement of an object through
 * different statuses is implemented by this class.
 * 
 * @author ristepejov
 *
 */
public class SvWorkflow extends SvCore {

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvWorkflow(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvWorkflow(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public SvWorkflow(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvWorkflow() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Method to change the status of a DbDataObject and move it to a new state.
	 * This overloaded version performs commit on success and rollback on exception
	 * 
	 * @param dbo       The DbDataObject which is subject of change
	 * @param newStatus The status to which the object will be moved
	 * @throws SvException
	 */
	public void moveObject(DbDataObject dbo, String newStatus) throws SvException {
		moveObject(dbo, newStatus, this.autoCommit);
	}

	/**
	 * Method that moves an object from one status to another status, with option to
	 * auto commit or rollback if exception occured.
	 * 
	 * @param dbo        The object to be moved to another status
	 * @param newStatus  The status to which the object should be moved
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException Pass through of underlying exceptions
	 */
	public void moveObject(DbDataObject dbo, String newStatus, Boolean autoCommit) throws SvException {
		try {
			this.dbSetAutoCommit(false);
			moveObjectImpl(dbo, newStatus);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * Implementation method that moves an object from one status to another status
	 * 
	 * @param dbo       The object to be moved to another status
	 * @param newStatus The status to which the object should be moved
	 * @throws Exception
	 */
	void moveObjectImpl(DbDataObject dbo, String newStatus) throws SvException {

		if (checkTransitionValidity(dbo, newStatus)) {
			DbDataObject dbt = getDbt(dbo);
			dbo.setStatus(newStatus);
			DbDataArray dba = new DbDataArray();
			dba.addDataItem(dbo);

			try (SvWriter svw = new SvWriter(this)) {
				svw.isInternal = true;
				svw.saveRepoData(dbt, dba, false, false);
				SvWriter.cacheCleanup(dbo);
				svw.executeAfterSaveCallbacks(dbo);
			} catch (SQLException e) {
				throw (new SvException(Sv.Exceptions.JDBC_CANT_RELEASE, instanceUser, dba, dbt, e));
			}
		} else {
			throw (new SvException("workflow_engine.error.movementNotAllowed", instanceUser,
					new ExceptionString("movementNotAllowed in: " + newStatus), dbo));
		}
		// TODO add movement specific business rules and other things
	}

	Boolean checkTransitionValidity(DbDataObject dbo, String newStatus) throws SvException {
		if (dbo == null)
			throw (new SvException("system.error.object_is_null", instanceUser));

		Boolean result = true;
		SvReader svr = null;
		// TODO change as param in the future
		String codeListName = "OBJ_STATUS";
		try {
			svr = new SvReader(this);
			DbDataArray objectValidTransitions = svr.getObjectsByParentId(dbo.getObjectType(),
					svCONST.OBJECT_TYPE_WORKFLOW, null);
			// do we have workflow?
			boolean hasWflow = !(objectValidTransitions == null || objectValidTransitions.isEmpty());

			// if we do, do we have permission?
			if (hasWflow && !checkIfTransitionPermitable(objectValidTransitions.get(0), svr))
				throw (new SvException("workflow_engine.error.movementNotAllowed.userDoesNotHaveThePermissionRequired",
						instanceUser, new ExceptionString("userDoesNotHaveThePermissionRequired"), dbo));

			// if we do, is the destination valid?
			if (hasWflow && !checkIfStatusExists(newStatus, codeListName))
				throw (new SvException("workflow_engine.error.movementNotAllowed.DestinationStatusNotInCodeList",
						instanceUser, new ExceptionString("DestinationStatusNotInstalledInCodeList- status:" + newStatus
								+ ";codeList:" + codeListName),
						dbo));
			// all fine and dandy, lets find transition rules
			DbDataObject requestedTransition = findRequestedTransition(dbo, newStatus, svr);

			// if we have workflow and there's transition rule, lets execute it
			if (hasWflow && requestedTransition != null && requestedTransition.getVal("CHECKIN_RULE") != null)
				result = executeRuleEnginePerTransition(dbo, requestedTransition, newStatus);

		} finally

		{
			if (svr != null)
				svr.release();
		}
		return result;
	}

	DbDataObject findRequestedTransition(DbDataObject dbo, String newStatus, SvReader svr) throws SvException {
		DbDataObject requestedTransition = null;
		DbSearchCriterion cr1 = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, dbo.getObjectType());
		DbSearchCriterion cr2 = new DbSearchCriterion("ORIGINATING_STATUS", DbCompareOperand.EQUAL, dbo.getStatus());
		DbSearchCriterion cr3 = new DbSearchCriterion("DESTINATION_STATUS", DbCompareOperand.EQUAL, newStatus);

		DbSearchExpression dbs = new DbSearchExpression().addDbSearchItem(cr1).addDbSearchItem(cr2)
				.addDbSearchItem(cr3);
		DbDataArray results = svr.getObjects(dbs, svCONST.OBJECT_TYPE_WORKFLOW, null, 0, 0);
		if (results != null && !results.isEmpty()) {
			requestedTransition = results.get(0);
		}
		return requestedTransition;
	}

	Boolean executeRuleEnginePerTransition(DbDataObject dbo, DbDataObject requestedTransition, String newStatus)
			throws SvException {
		Boolean result = false;
		RuleEngine ruleEngine = new RuleEngine(this);
		try {
			ruleEngine.setRuleEngineSave(new ISvRuleEngineSave() {

				@Override
				public void saveFinalState(SvCore parentCore, DbDataObject targetObject, DbDataArray actionResults,
						Boolean success) throws SvException {
					// TODO Auto-generated method stub

				}

				@Override
				public void saveCurrentState(SvCore parentCore, DbDataObject currentAction, DbDataObject execObj,
						DbDataArray actionResults, Boolean success) throws SvException {
					// TODO Auto-generated method stub

				}
			});
			DbDataArray actionsRsult = new DbDataArray();
			HashMap<Object, Object> params = new HashMap<>();
			params.put("ORIGINATING_STATUS", dbo.getStatus());
			params.put("DESTINATION_STATUS", newStatus);
			result = ruleEngine.executeImpl((Long) requestedTransition.getVal("CHECKIN_RULE"), actionsRsult, dbo,
					params);
		} finally {
			if (ruleEngine != null) {
				ruleEngine.release();
			}
		}
		return result;
	}

	Boolean checkIfStatusExists(String statuscode, String codeListName) throws SvException {
		Boolean result = false;
		SvReader svr = new SvReader(this);
		try {
			DbSearchCriterion cr1 = new DbSearchCriterion("PARENT_CODE_VALUE", DbCompareOperand.EQUAL, codeListName);
			DbSearchCriterion cr2 = new DbSearchCriterion("CODE_VALUE", DbCompareOperand.EQUAL, statuscode);
			DbSearchExpression dbs = new DbSearchExpression().addDbSearchItem(cr1).addDbSearchItem(cr2);
			DbDataArray requestedStatusCode = svr.getObjects(dbs, svCONST.OBJECT_TYPE_CODE, null, 0, 0);
			if (requestedStatusCode != null && !requestedStatusCode.isEmpty()) {
				result = true;
			}
		} finally {
			if (svr != null)
				svr.release();
		}
		return result;
	}

	Boolean checkIfTransitionPermitable(DbDataObject dboTransition, SvReader svr) throws SvException {
		Boolean result = true;
		final String PERMISSION_CODE = "PERMISSION_CODE";
		if (dboTransition.getVal(PERMISSION_CODE) != null
				&& !dboTransition.getVal(PERMISSION_CODE).toString().trim().equals("")) {
			String permissionCode = dboTransition.getVal(PERMISSION_CODE).toString();
			if (!svr.hasPermission(permissionCode)) {
				result = false;
			}
		}
		return result;
	}

	public class ExceptionString extends Jsonable {
		String s;

		public String getS() {
			return s;
		}

		public void setS(String s) {
			this.s = s;
		}

		public ExceptionString(String s) {
			super();
			this.s = s;
		}

	}
}
