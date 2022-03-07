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

import java.util.LinkedHashMap;

import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.SvCharId;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

import com.prtech.svarog_common.DbSearchExpression;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SvConversation extends DbDataObject {

	private static final Logger log4j = LogManager.getLogger(SvConversation.class.getName());

	public SvConversation() {
	}

	public SvConversation(Long objectType, LinkedHashMap<SvCharId, Object> keyMap) {
		super(objectType, keyMap);
	}

	public SvConversation(Long objectType) {
		super(objectType);
	}

	public SvConversation(String objectTypeName) {
		super(SvCore.getDbtByName(objectTypeName).getObject_id());
	}

	/**
	 * method to return DbDataArray of all conversations that are created by dbUser
	 * , if user is not specified it will return all conversations created by the
	 * user that is logged in
	 * 
	 * @param svr    SvReader connected to database
	 * @param dbUser DbDataObject of the user from SVAROG_USERS
	 * @return DbDataArray
	 * @throws SvException
	 */
	public DbDataArray getCreatedConversations(SvReader svr, DbDataObject dbUser) throws SvException {
		if (dbUser == null)
			dbUser = svr.getInstanceUser();
		DbSearchCriterion critM = new DbSearchCriterion("CREATED_BY", DbCompareOperand.EQUAL, dbUser.getObject_id());
		DbDataArray myCreatedConv = svr.getObjects(critM, svCONST.OBJECT_TYPE_CONVERSATION, null, 0, 0);
		return myCreatedConv;
	}

	/**
	 * method to return all conversation that are assigned to specified user, if
	 * user is not specified it will return the conversations assigned to user that
	 * is logged in, it will also check the groups that the user belongs to and
	 * check for conversations assigned for all his groups
	 * 
	 * @param svr      SvReader connected to database
	 * @param isUnread Boolean , if true it will return only unread messages
	 * @param dbUser   DbDataObject of the user from SVAROG_USERS
	 * @return DbDataArray
	 * @throws SvException
	 */
	public DbDataArray getAssignedConversations(SvReader svr, Boolean isUnread, DbDataObject dbUSer)
			throws SvException {
		if (dbUSer == null)
			dbUSer = svr.getInstanceUser();
		DbSearchExpression expr = new DbSearchExpression();
		DbSearchCriterion crit1 = new DbSearchCriterion("ASSIGNED_TO", DbCompareOperand.EQUAL, dbUSer.getObject_id());
		expr.addDbSearchItem(crit1);
		if (isUnread) {
			DbSearchCriterion crit2 = new DbSearchCriterion("IS_READ", DbCompareOperand.EQUAL, !isUnread);
			expr.addDbSearchItem(crit2);
		}
		DbDataArray myAssignedConv = svr.getObjects(expr, svCONST.OBJECT_TYPE_CONVERSATION, null, 0, 0);
		// also get messages that are assigned to the group user belongs to
		DbDataArray userGroups = SvCore.getUserGroups(dbUSer, false);
		if (userGroups != null && !userGroups.getItems().isEmpty())
			for (DbDataObject groupObject : userGroups.getItems()) {
				expr = new DbSearchExpression();
				crit1 = new DbSearchCriterion("ASSIGNED_TO", DbCompareOperand.EQUAL, groupObject.getObject_id());
				expr.addDbSearchItem(crit1);
				if (isUnread) {
					DbSearchCriterion crit2 = new DbSearchCriterion("IS_READ", DbCompareOperand.EQUAL, !isUnread);
					expr.addDbSearchItem(crit2);
				}
				DbDataArray groupAssignedConv = svr.getObjects(expr, svCONST.OBJECT_TYPE_CONVERSATION, null, 0, 0);

				if (groupAssignedConv != null && !groupAssignedConv.getItems().isEmpty())
					for (DbDataObject conversationObject : groupAssignedConv.getItems())
						myAssignedConv.addDataItem(conversationObject);
			}
		return myAssignedConv;
	}

	/**
	 * method to generate list of all conversations in which the user typed a
	 * message for it, if the user is not specified, it will check for the user that
	 * is logged in
	 * 
	 * @param svr    SvReader connected to database
	 * @param dbUser DbDataObject of the user from SVAROG_USERS
	 * @return DbDataArray
	 * @throws SvException
	 */
	public DbDataArray getConversationsWithMyMessage(SvReader svr, DbDataObject dbUSer) throws SvException {
		if (dbUSer == null)
			dbUSer = svr.getInstanceUser();
		DbDataArray myCreatedMess = new DbDataArray();
		DbSearchCriterion search = new DbSearchCriterion("CREATED_BY", DbCompareOperand.EQUAL, dbUSer.getObject_id());
		DbDataObject databaseTypeConversation = svr.getObjectById(svCONST.OBJECT_TYPE_CONVERSATION,
				svCONST.OBJECT_TYPE_TABLE, null);
		DbDataObject databaseTypeMessage = svr.getObjectById(svCONST.OBJECT_TYPE_MESSAGE, svCONST.OBJECT_TYPE_TABLE,
				null);
		DbQueryObject dbqConversation = new DbQueryObject(databaseTypeConversation, null, DbJoinType.INNER, null,
				LinkType.CHILD, null, null);
		DbQueryObject dbqMessage = new DbQueryObject(databaseTypeMessage, search, null, null, null, null, null);
		DbQueryExpression q = new DbQueryExpression();
		q.addItem(dbqConversation);
		q.addItem(dbqMessage);
		JsonObject processed = new JsonObject();
		DbDataArray ret = svr.getObjects(q, null, null);
		if (ret != null && !ret.getItems().isEmpty())
			for (DbDataObject conMess : ret.getItems())
				if (!processed.has(conMess.getVal("TBL0_OBJECT_ID").toString())) {
					myCreatedMess.addDataItem(conMess);
					processed.addProperty((String) conMess.getVal("TBL0_OBJECT_ID").toString(), true);
				}
		return myCreatedMess;
	}

	public boolean deleteConversation(DbDataObject conversationObj) throws SvException {
		return true;
	}

	/**
	 * method to create new conversation object or update existing one, it will fill
	 * the field CREATED_BY only on new objects and it will never be changed,
	 * conversation can be assigned to valid users or user groups, on updating
	 * object it will check the PKID so we don't do WriteAfterWrite
	 * 
	 * @param svr      SvReader connected to database
	 * @param jsonData JsonObject with pairs of fieldName : fieldValue
	 * @return DbDataObject newly created or edited SvConversation Object
	 * @throws SvException
	 */
	public DbDataObject newConversation(SvReader svr, JsonObject jsonData) throws SvException {
		// check for data
		if (svr == null)
			throw (new SvException(Sv.INVALID_SESSION, null));
		if (jsonData == null)
			throw (new SvException("system.error.not_conversation_object", svr.getInstanceUser()));
		// only on new objects set CREATED_BY
		DbDataObject newConv = new DbDataObject();
		DbDataObject userObject = svr.getInstanceUser();
		if (userObject != null) {
			newConv.setVal("CREATED_BY_USERNAME", userObject.getVal("USER_NAME").toString());
			newConv.setVal("CREATED_BY", userObject.getObject_id());
			newConv.setObject_type(svCONST.OBJECT_TYPE_CONVERSATION);
		}
		// if its change for conversation try to load old object and make the
		// changes
		if (jsonData.has("OBJECT_ID") && jsonData.get("OBJECT_ID") != null && jsonData.has("PKID")
				&& jsonData.get("PKID") != null) {
			newConv = svr.getObjectById(jsonData.get("OBJECT_ID").getAsLong(), svCONST.OBJECT_TYPE_CONVERSATION, null);
			if (newConv == null)
				throw (new SvException("system.error.conversation_object_not_found", svr.getInstanceUser()));
			else
				newConv.setPkid(jsonData.get("PKID").getAsLong());
		}
		// add sequence only on new conversations that dont have object_id and ID

		if (newConv.getObject_id() == 0L && newConv.getVal("ID") == null) {

			newConv.setVal("ID", SvSequence.getSeqNextVal(newConv.getObject_type().toString(), svr));
		}
		// double check for user_name that we have to assign the conversation
		if (jsonData.has("ASSIGNED_TO_USERNAME") && jsonData.get("ASSIGNED_TO_USERNAME") != null) {
			jsonData.addProperty("ASSIGNED_TO_USERNAME",
					jsonData.get("ASSIGNED_TO_USERNAME").getAsString().toUpperCase());
			DbSearchCriterion crit1 = new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL,
					jsonData.get("ASSIGNED_TO_USERNAME").getAsString());
			DbDataArray vData = svr.getObjects(crit1, svCONST.OBJECT_TYPE_USER, null, 0, 0);
			if (vData != null) {
				if (vData.getItems().size() == 1)
					jsonData.addProperty("ASSIGNED_TO", (Long) vData.getItems().get(0).getObject_id());
				// TODO add in unread messages TABLE
				else {
					// try for groups
					crit1 = new DbSearchCriterion("GROUP_NAME", DbCompareOperand.EQUAL,
							jsonData.get("ASSIGNED_TO_USERNAME").getAsString());
					vData = svr.getObjects(crit1, svCONST.OBJECT_TYPE_GROUP, null, 0, 0);
					if (vData.getItems().size() == 1) {
						jsonData.addProperty("ASSIGNED_TO", (Long) vData.getItems().get(0).getObject_id());
						// TODO add in unread messages TABLE
					} else
						throw (new SvException("system.error.user_not_found", svr.getInstanceUser()));
				}
			} else
				throw (new SvException("system.error.user_not_found", svr.getInstanceUser()));
		} else
			throw (new SvException("system.error.user_not_found", svr.getInstanceUser()));
		// change or update all values that can be changed
		DbDataArray dbarr = svr.getObjectsByParentId(svCONST.OBJECT_TYPE_CONVERSATION, svCONST.OBJECT_TYPE_FIELD, null,
				0, 0);
		for (DbDataObject fieldObj : dbarr.getItems()) {
			String fieldName = (String) fieldObj.getVal("FIELD_NAME");
			if (!"PKID".equals(fieldName) && !"CREATED_BY".equals(fieldName) && !"CREATED_BY_USERNAME".equals(fieldName)
					&& !"ID".equals(fieldName) && (Boolean) fieldObj.getVal("IS_UPDATEABLE")) {
				switch ((String) fieldObj.getVal("FIELD_TYPE")) {
				case "NVARCHAR":
					if (jsonData.has(fieldName) && jsonData.get(fieldName) != null)
						newConv.setVal(fieldName, jsonData.get(fieldName).getAsString());
					else
						newConv.setVal(fieldName, null);
					break;
				case "NUMERIC":
					if (jsonData.has(fieldName) && jsonData.get(fieldName) != null)
						newConv.setVal(fieldName, jsonData.get(fieldName).getAsLong());
					else
						newConv.setVal(fieldName, null);
					break;
				default:
				}
			}
		}
		// make conversation unread
		newConv.setVal("IS_READ", false);
		return newConv;
	}

	/**
	 * method to attach objects to existing conversation
	 * 
	 * @param svr             SvReader connected to database
	 * @param conversationObj DbDataObject from SVAROG_CONVERSATION table that the
	 *                        other objects will be attached to
	 * @param attachments     JsonArray of JsonObject with keys : OBJECT_ID,
	 *                        OBJECT_TYPE, and object_id or key for the link type
	 * @throws SvException
	 */
	public void attachObjects(SvReader svr, DbDataObject conversationObj, JsonArray attachments) throws SvException {
		try (SvLink svl = new SvLink(svr)) {
			DbSearchExpression expr = new DbSearchExpression();
			// find the link types
			DbSearchCriterion critLT = new DbSearchCriterion("LINK_TYPE", DbCompareOperand.EQUAL,
					"LINK_CONVERSATION_ATTACHMENT");
			critLT.setNextCritOperand(DbLogicOperand.AND.toString());
			DbSearchCriterion critO1 = new DbSearchCriterion("LINK_OBJ_TYPE_1", DbCompareOperand.EQUAL,
					conversationObj.getObject_type());
			critO1.setNextCritOperand(DbLogicOperand.AND.toString());
			expr.addDbSearchItem(critLT);
			expr.addDbSearchItem(critO1);
			DbDataArray linkTypes = svr.getObjects(expr, svCONST.OBJECT_TYPE_LINK_TYPE, null, 0, 0);
			if ((linkTypes == null || linkTypes.getItems().isEmpty()) && attachments.size() > 0)
				throw new SvException("attachemts.not.found", svr.getInstanceUser());
			for (int j = 0; j < attachments.size(); j++) {
				JsonObject attachObject = (JsonObject) attachments.get(j);
				if (attachObject.has("OBJECT_ID") && attachObject.has("OBJECT_TYPE") && linkTypes != null) {
					Long objectId = attachObject.get("OBJECT_ID").getAsLong();

					DbDataObject linkedObject = svr.getObjectById(objectId, attachObject.get("OBJECT_TYPE").getAsLong(),
							null);
					for (DbDataObject linkObj : linkTypes.getItems())
						if (linkedObject.getObject_type().equals((Long) linkObj.getVal("LINK_OBJ_TYPE_2"))) {
							// link type found so check if link exist, if not create
							// one
							DbDataArray linkedObject1 = svr.getObjectsByLinkedId(conversationObj.getObject_id(),
									linkObj, null, 0, 0);
							Boolean linkexist = false;
							if (linkedObject1 != null && !linkedObject1.getItems().isEmpty())
								for (DbDataObject linkedItem : linkedObject1.getItems())
									if (((Long) linkedItem.getObject_id()).compareTo(objectId) == 0)
										linkexist = true;
							if (!linkexist)
								svl.linkObjects(conversationObj.getObject_id(), objectId, linkObj.getObject_id(),
										"attachment");
						}
				}
			}
		}
	}

	public DbDataObject responseConversation(DbDataObject conversationObj) throws SvException {
		return new DbDataObject();
	}

	public void markAsUnread(SvWriter svw, DbDataObject conversationObj) throws SvException {
		if (conversationObj.getObject_type().equals(svCONST.OBJECT_TYPE_CONVERSATION)) {
			conversationObj.setVal("IS_READ", false);
			svw.saveObject(conversationObj);
		} else
			throw (new SvException("system.error.not_conversation_object", svw.getInstanceUser()));
	}

	public void printConversation() throws SvException {

	}

	public void addWatchers(DbDataObject conversationObj, DbDataArray userArray) throws SvException {

	}

	public void removeWatchers(DbDataObject conversationObj, DbDataArray userArray) throws SvException {

	}

}
