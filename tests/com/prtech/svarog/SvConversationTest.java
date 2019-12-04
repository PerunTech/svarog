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

import static org.junit.Assert.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

public class SvConversationTest {

	static final Logger log4j = SvConf.getLogger(SvConversationTest.class);

	public void releaseAll(SvCore svc) {
		if (svc != null)
			svc.release();
	}

	@Test
	public void createConversation() {
		SvReader svr = null;
		SvWriter svw = null;
		SvSecurity svsec = null;

		try {
			svsec = new SvSecurity();
			//String token = svsec.logon("ADMIN", SvUtil.getMD5("welcome"));
			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			SvConversation convSv = new SvConversation();
			Gson gson = new Gson();
			String jsonString = "{\"OBJECT_TYPE\":88,\"MODULE_NAME\":\"EDINSTVENO_2018\",\"CATEGORY\":\"ALL\",\"TITLE\":\"TEST_ALERT_AFTERSAVE\",\"PRIORITY\":\"NORMAL\",\"ASSIGNED_TO_USERNAME\":\"ADMIN\"}";
			JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
			DbDataObject conv = convSv.newConversation(svr, jsonObject);
			svw.saveObject(conv);
			svw.dbCommit();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			releaseAll(svsec);
			releaseAll(svw);
			releaseAll(svr);
		}

	}

	@Test
	public void createConversationAndMessage() {
		SvReader svr = null;
		SvWriter svw = null;
		SvSecurity svSec = null;
		try {
			svSec = new SvSecurity();
			//String token = svSec.logon("ADMIN", SvUtil.getMD5("welcome"));
			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			SvConversation convSv = new SvConversation();
			SvMessage messSv = new SvMessage();

			Gson gson = new Gson();
			String jsonString = "{\"OBJECT_TYPE\":88,\"MODULE_NAME\":\"EDINSTVENO_2018\",\"CATEGORY\":\"ALL\",\"TITLE\":\"TEST_ALERT_AFTERSAVE\",\"PRIORITY\":\"NORMAL\",\"ASSIGNED_TO_USERNAME\":\"ADMIN\"}";
			JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
			DbDataObject conv = convSv.newConversation(svr, jsonObject);
			svw.saveObject(conv);
			DbDataObject message = messSv.saveMessage(svr, conv, null, null);
			svw.saveObject(message);
			// svw.dbRollback();
			svw.dbCommit();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			releaseAll(svr);
			releaseAll(svw);
			releaseAll(svSec);
		}
	}

	@Test
	public void addMessage() {
		SvReader svr = null;
		SvWriter svw = null;
		SvSecurity svSec = null;
		try {
			svSec = new SvSecurity();
			//String token = svSec.logon("ADMIN", SvUtil.getMD5("welcome"));
			svr = new SvReader();
			svr.switchUser("ADMIN");
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			SvConversation convSv = new SvConversation();
			SvMessage messSv = new SvMessage();
			DbSearchCriterion critM = new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL, "ADMIN");
			DbDataArray adminUserArray = svr.getObjects(critM, svCONST.OBJECT_TYPE_USER, null, null, null);
			DbDataObject userObject = null;
			if (adminUserArray != null && !adminUserArray.getItems().isEmpty()
					&& adminUserArray.getItems().size() == 1) {
				userObject = adminUserArray.getItems().get(0);
				System.out.println(userObject.getVal("USER_NAME"));
			} else
				fail("admin user not found or more instances of that user");
			DbDataArray convArr = convSv.getAssignedConversations(svr, false, null);

			if (convArr.getItems().size() < 1) {
				createConversation();
				convArr = convSv.getAssignedConversations(svr, false, null);
			}
			DbDataObject conv = convArr.getItems().get(0);
			DbDataObject message = messSv.saveMessage(svr, conv, null, null);
			svw.saveObject(message);
			svw.dbCommit();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			releaseAll(svr);
			releaseAll(svw);
			releaseAll(svSec);
		}
	}

	@Test
	public void getMessages() throws SvException {
		SvSecurity svSec = null;
		SvReader svr = null;
		try {
			svSec = new SvSecurity();
			//String token = svSec.logon("ADMIN", SvUtil.getMD5("welcome"));
			svr = new SvReader();
			DbDataObject admctrolUser = svSec.getUser("ADMIN");
			SvConversation convSv = new SvConversation();
			SvMessage messSv = new SvMessage();
			DbDataObject userObject = svr.getInstanceUser();
			DbDataArray myMessages = messSv.getMyRecivedMessages(svr);
			Long noMessages = 0L;
			if (myMessages != null && !myMessages.getItems().isEmpty()) {
				noMessages = (long) myMessages.getItems().size();
			}
			log4j.info("user: " + userObject.getVal("USER_NAME") + " has " + noMessages + " messages");

			DbDataArray myAsConversations = convSv.getAssignedConversations(svr, true, admctrolUser);
			Long noConv = 0L;
			if (myAsConversations != null && !myAsConversations.getItems().isEmpty()) {
				noConv = (long) myAsConversations.getItems().size();
			}
			log4j.info("user: " + userObject.getVal("USER_NAME") + " has " + noConv + " assigned unread conversations");

			myAsConversations = convSv.getAssignedConversations(svr, false, admctrolUser);
			noConv = 0L;
			if (myAsConversations != null && !myAsConversations.getItems().isEmpty()) {
				noConv = (long) myAsConversations.getItems().size();
			}
			log4j.info("user: " + userObject.getVal("USER_NAME") + " has " + noConv + " assigned total conversations");

			DbDataArray myConversations = convSv.getCreatedConversations(svr, admctrolUser);
			noConv = 0L;
			if (myConversations != null && !myConversations.getItems().isEmpty()) {
				noConv = (long) myConversations.getItems().size();
			}
			log4j.info("user: " + userObject.getVal("USER_NAME") + " has " + noConv + " created conversations");

			DbDataArray myMentionConversations = convSv.getConversationsWithMyMessage(svr, admctrolUser);
			noConv = 0L;
			if (myMentionConversations != null && !myMentionConversations.getItems().isEmpty()) {
				noConv = (long) myMentionConversations.getItems().size();
			}
			log4j.info("user: " + userObject.getVal("USER_NAME") + " has " + noConv
					+ " conversations that he wrote some message into");

		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		} finally {
			releaseAll(svr);
			releaseAll(svSec);
		}

	}

}
