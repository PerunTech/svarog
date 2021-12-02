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
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.SvCharId;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

public class SvMessage extends DbDataObject {

	private static final Logger log4j = LogManager.getLogger(SvMessage.class.getName());

	public SvMessage() {
	}

	public SvMessage(Long objectType, LinkedHashMap<SvCharId, Object> keyMap) {
		super(objectType, keyMap);
	}

	public SvMessage(Long objectType) {
		super(objectType);
	}

	public SvMessage(String objectTypeName) {
		super(SvCore.getDbtByName(objectTypeName).getObject_id());
	}

	/**
	 * method to get all sent message for the specific user
	 * 
	 * @param svr         SvReader connected to database
	 * @param dbUser      DbDataObject for the user that we are searching messages
	 *                    for
	 * @param allMessages Boolean if set to TRUE it will return all messages ever
	 *                    created, even replies to existing messages, if set to
	 *                    FALSE it will return only starter messages
	 * @return DbDataArray with messages
	 * @throws SvException
	 */
	public DbDataArray getSentMessages(SvReader svr, DbDataObject dbUser, Boolean allMessages) throws SvException {
		DbDataArray ret = null;
		if (svr != null && dbUser != null && dbUser.getObject_type().compareTo(svCONST.OBJECT_TYPE_USER) == 0) {
			DbSearchCriterion crit1 = new DbSearchCriterion("ASSIGNED_TO", DbCompareOperand.EQUAL,
					dbUser.getObject_id());
			crit1.setNextCritOperand(DbLogicOperand.AND.toString());
			DbSearchCriterion crit2 = new DbSearchCriterion("REPLY_TO", DbCompareOperand.ISNULL);
			DbSearchExpression expr = new DbSearchExpression();
			expr.addDbSearchItem(crit1);
			if (!allMessages)
				expr.addDbSearchItem(crit2);
			ret = svr.getObjects(expr, svCONST.OBJECT_TYPE_MESSAGE, null, 0, 0);
		} else {
			throw (new SvException("message.user.not.found", (svr != null ? svr.instanceUser : svCONST.systemUser)));

		}
		return ret;
	}

	/**
	 * method to get all received message for the specific user
	 * 
	 * @param svr         SvReader connected to database
	 * @param dbUser      DbDataObject for the user that we are searching messages
	 *                    for
	 * @param allMessages Boolean if set to TRUE it will return all messages ever
	 *                    received, even replies to existing messages, if set to
	 *                    FALSE it will return only starter messages
	 * @return DbDataArray with messages
	 * @throws SvException
	 */
	public DbDataArray getReceivedMessages(SvReader svr, DbDataObject dbUser, Boolean allMessages) throws SvException {
		DbDataArray ret = null;
		if (svr != null && dbUser != null && dbUser.getObject_type().compareTo(svCONST.OBJECT_TYPE_USER) == 0) {
			DbSearchCriterion crit1 = new DbSearchCriterion("ASSIGNED_TO", DbCompareOperand.EQUAL,
					dbUser.getObject_id());
			DbSearchExpression expr = new DbSearchExpression();
			expr.addDbSearchItem(crit1);
			if (!allMessages) {
				crit1.setNextCritOperand(DbLogicOperand.AND.toString());
				DbSearchCriterion crit2 = new DbSearchCriterion("REPLY_TO", DbCompareOperand.ISNULL);
				expr.addDbSearchItem(crit2);
			}
			ret = svr.getObjects(expr, svCONST.OBJECT_TYPE_MESSAGE, null, 0, 0);
		}
		return ret;
	}

	/**
	 * method to get all received message for the user that is logged in
	 * 
	 * @param svr SvReader connected to database
	 * @return DbDataArray with messages
	 * @throws SvException
	 */
	public DbDataArray getMyRecivedMessages(SvReader svr) throws SvException {
		DbDataObject dbUser = svr.getInstanceUser();
		return getReceivedMessages(svr, dbUser, false);
	}

	/**
	 * method to get all sebt message for the user that is logged in
	 * 
	 * @param svr SvReader connected to database
	 * @return DbDataArray with messages
	 * @throws SvException
	 */
	public DbDataArray getMySentMessages(SvReader svr) throws SvException {
		DbDataObject dbUser = svr.getInstanceUser();
		return getSentMessages(svr, dbUser, false);
	}

	private DbDataObject loadMessageObjects(SvReader svr, JsonObject formVals) throws SvException {
		DbDataObject newMessage = null;
		DbDataObject oldMessage = null;
		if (formVals != null && formVals.has("OBJECT_ID")) {
			oldMessage = svr.getObjectById(formVals.get("OBJECT_ID").getAsLong(), svCONST.OBJECT_TYPE_MESSAGE, null);
		}

		if (oldMessage != null && oldMessage.getObjectType().compareTo(svCONST.OBJECT_TYPE_MESSAGE) == 0) {
			newMessage = oldMessage;
		} else {
			newMessage = new DbDataObject();
			newMessage.setObjectType(svCONST.OBJECT_TYPE_MESSAGE);
			newMessage.setVal("CREATED_BY", svr.getInstanceUser().getObjectId());

			DbSearchCriterion critM = new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL, "ADMIN");
			DbDataArray adminUserArray = svr.getObjects(critM, svCONST.OBJECT_TYPE_USER, null, null, null);
			newMessage.setVal("ASSIGNED_TO", 0L);
			if (adminUserArray != null && !adminUserArray.getItems().isEmpty()
					&& adminUserArray.getItems().size() == 1) {
				DbDataObject userObject = adminUserArray.getItems().get(0);
				newMessage.setVal("ASSIGNED_TO", userObject.getObjectId());
			}

		}
		return newMessage;

	}

	/**
	 * 
	 * @param svr        SvReader connected to database
	 * @param formVals
	 * @param oldMessage DbDataObject of the old message if we are doing changes ,
	 *                   null if it is a new message
	 * @return DbDataObject of the new message
	 * @throws SvException
	 */
	public DbDataObject saveMessage(SvReader svr, DbDataObject conversationObj, DbDataObject messageObj,
			JsonObject formVals) throws SvException {
		DbDataObject newMessage = loadMessageObjects(svr, formVals);

		if (conversationObj != null) {
			newMessage.setParentId(conversationObj.getObjectId());
		}
		if (messageObj != null) {
			newMessage.setVal("REPLY_TO", messageObj.getObjectId());
		}

		if (formVals != null) {
			if (formVals.has("PKID") && newMessage.getPkid() != null && newMessage.getPkid() > 0) {
				newMessage.setPkid(formVals.get("PKID").getAsLong());
			}

			if (formVals.has("MESSAGE_TEXT") && formVals.get("MESSAGE_TEXT") != null) {
				newMessage.setVal("MESSAGE_TEXT", formVals.get("MESSAGE_TEXT").getAsString());
			}

		} else {
			DateTime dtNow = new DateTime();
			String dtString = dtNow.toString();
			newMessage.setVal("MESSAGE_TEXT", "message text on date: " + dtString);

		}

		return newMessage;
	}

	/**
	 * method to delete the message as we get it it from the GUI POST data
	 * 
	 * @param svr      SvReader connected to database
	 * @param formVals
	 * @throws SvException
	 */
	public void deleteMessage(SvReader svr, JsonObject formVals) throws SvException {
		// TODO, should we also delete all messages that are replies to this message??
		DbDataObject oldMessage = null;
		if (formVals != null && formVals.has("OBJECT_ID")) {
			oldMessage = svr.getObjectById(formVals.get("OBJECT_ID").getAsLong(), svCONST.OBJECT_TYPE_MESSAGE, null);
		} else {
			throw (new SvException("message.not.found", svr.instanceUser));
		}
		if (oldMessage != null && formVals.has("PKID")
				&& ((Long) formVals.get("PKID").getAsLong()).compareTo(oldMessage.getPkid()) == 0) {
			SvWriter svw = new SvWriter(svr);
			svw.deleteObject(oldMessage);
			svw.dbCommit();
			svw.release();
		} else {
			throw (new SvException("message.error.deleting", svr.instanceUser));
		}
	}

	public void changeMessageStatus(SvReader svr, String newStatus) throws SvException {

	}

	public void addAttachment() throws SvException {

	}

}
