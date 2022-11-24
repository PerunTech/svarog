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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;

public class SvNotification extends SvCore {

	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvCore.class.getName());

	/**
	 * Constructor to create a SvNotification object according to a user session.
	 * This is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id String UID of the user session under which the SvCore
	 *                   instance will run
	 * @throws SvException Pass through exception from the super class constructor
	 * 
	 */
	public SvNotification(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * SvCore chained constructor. This constructor will re-use the JDBC connection
	 * from the chained SvCore
	 * 
	 * 
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvNotification(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	SvNotification() throws SvException {
		super(svCONST.systemUser, null);
	}

	public DbDataObject createNotificationObj(String type, String title, String message, String sender, Long eventId) {
		DbDataObject notificationObj = new DbDataObject();
		notificationObj.setObjectType(svCONST.OBJECT_TYPE_NOTIFICATION);
		notificationObj.setStatus(svCONST.STATUS_VALID);
		notificationObj.setVal("TYPE", type);
		notificationObj.setVal("TITLE", title);
		notificationObj.setVal("MESSAGE", message);
		notificationObj.setVal("SENDER", sender);
		notificationObj.setVal("EVENT_ID", eventId);
		return notificationObj;
	}

	/**
	 * Default method for creating Notification object
	 * 
	 * @param type
	 * @param title
	 * @param message
	 * @param sender
	 * @param eventId
	 * @param validFrom
	 * @param validTo
	 * @return
	 */
	public DbDataObject createNotificationObj(String type, String title, String message, String sender, Long eventId,
			String validFrom, String validTo) {
		DbDataObject notificationObj = new DbDataObject();
		notificationObj.setObjectType(svCONST.OBJECT_TYPE_NOTIFICATION);
		notificationObj.setStatus(svCONST.STATUS_VALID);
		notificationObj.setVal("TYPE", type);
		notificationObj.setVal("TITLE", title);
		notificationObj.setVal("MESSAGE", message);
		notificationObj.setVal("SENDER", sender);
		notificationObj.setVal("EVENT_ID", eventId);
		notificationObj.setVal("VALID_FROM", validFrom);
		notificationObj.setVal("VALID_TO", validTo);
		return notificationObj;
	}

	public void createNotificationPerUser(String type, String title, String message, String sender, Long eventId,
			DbDataObject userObj, Boolean autoCommit) {
		try (SvReader svr = new SvReader(this); SvWriter svw = new SvWriter(svr); SvLink svl = new SvLink(svw);) {
			// link new notification per user
			DbDataObject notificationObj = createNotificationObj(type, title, message, sender, eventId);
			svw.saveObject(notificationObj, autoCommit);
			DbDataObject linkNotificationUser = SvLink.getLinkType("LINK_NOTIFICATION_USER",
					svCONST.OBJECT_TYPE_NOTIFICATION, svCONST.OBJECT_TYPE_USER);
			svl.linkObjects(notificationObj.getObjectId(), userObj.getObjectId(), linkNotificationUser.getObjectId(),
					"", false, autoCommit);
		} catch (SvException sv) {
			log4j.error("Error in createNotificationPerUser" + sv.getFormattedMessage(), sv.fillInStackTrace());
		}
	}

	/**
	 * Method for linking Notification object with User or Usr_Group. This can be
	 * used for linking same Notification to multiple Users or User_Groups.
	 * 
	 * @param dboNotification
	 * @param userOrUserGroup
	 */
	public void createLinkBetweenNotificationAndUserOrUserGroup(DbDataObject dboNotification,
			DbDataObject userOrUserGroup, Boolean autoCommit) {
		try (SvLink svl = new SvLink(this);) {
			String linkName = "LINK_NOTIFICATION_GROUP";
			Long objectType = svCONST.OBJECT_TYPE_GROUP;
			if (!userOrUserGroup.getObjectType().equals(objectType)) {
				linkName = "LINK_NOTIFICATION_USER";
				objectType = svCONST.OBJECT_TYPE_USER;
			}
			DbDataObject linkNotificationUser = SvLink.getLinkType(linkName, svCONST.OBJECT_TYPE_NOTIFICATION,
					objectType);
			svl.linkObjects(dboNotification.getObjectId(), userOrUserGroup.getObjectId(),
					linkNotificationUser.getObjectId(), "", false, autoCommit);
		} catch (SvException sv) {
			log4j.error("Error in linkNotificationToUserOrGroup" + sv.getFormattedMessage(), sv.fillInStackTrace());
		}
	}

	public DbDataArray getNotificationPerUser(DbDataObject userObj) throws SvException {
		DbDataArray allNotificationsFound = new DbDataArray();
		try (SvReader svr = new SvReader(this); SvLink svl = new SvLink(svr)) {
			DbDataArray notificationsFoundPerUser = new DbDataArray();
			DbDataObject linkNotificationUser = SvLink.getLinkType("LINK_NOTIFICATION_USER",
					svCONST.OBJECT_TYPE_NOTIFICATION, svCONST.OBJECT_TYPE_USER);
			// get all notifications per user
			notificationsFoundPerUser = svr.getObjectsByLinkedId(userObj.getObjectId(), svCONST.OBJECT_TYPE_USER,
					linkNotificationUser, svCONST.OBJECT_TYPE_NOTIFICATION, true, null, 0, 0);
			if (notificationsFoundPerUser.size() > 0)
				allNotificationsFound = notificationsFoundPerUser;
			// find the userGroup for the user
			DbDataArray userGroups = svr.getUserGroups();
			// get all notifications per userGroup on which the user belongs
			DbDataArray notificationsFoundPerUserGroup = new DbDataArray();
			if (userGroups.size() > 0) {
				DbDataObject linkNotificationUserGroup = SvLink.getLinkType("LINK_NOTIFICATION_GROUP",
						svCONST.OBJECT_TYPE_NOTIFICATION, svCONST.OBJECT_TYPE_GROUP);
				DbDataArray tempNotificationsFound = new DbDataArray();
				// check if notifications exist, per each userGroup
				for (DbDataObject tempUserGroup : userGroups.getItems()) {
					tempNotificationsFound = svr.getObjectsByLinkedId(tempUserGroup.getObjectId(),
							svCONST.OBJECT_TYPE_GROUP, linkNotificationUserGroup, svCONST.OBJECT_TYPE_NOTIFICATION,
							true, null, 0, 0);
					if (tempNotificationsFound.size() > 0) {
						for (DbDataObject tempNotification : tempNotificationsFound.getItems())
							notificationsFoundPerUserGroup.addDataItem(tempNotification);
					}
					tempNotificationsFound = new DbDataArray();
				}
				if (notificationsFoundPerUserGroup.size() > 0)
					for (DbDataObject tempNotification : notificationsFoundPerUserGroup.getItems())
						allNotificationsFound.addDataItem(tempNotification);
			}

			return allNotificationsFound;

		}

	}

	/**
	 * Method to return an array of notification for specific notification type
	 * 
	 * @param type The type of the notifications
	 * 
	 * @return The resulting array of notification in the same type
	 * @throws SvException Underlying exception
	 */
	public static DbDataArray getNotificationPerType(String type) throws SvException {
		DbDataArray notifications = null;
		try (SvReader svr = new SvReader()) {
			DateTime now = new DateTime();
			DbSearchCriterion dscType = new DbSearchCriterion("TYPE", DbCompareOperand.EQUAL, type);
			DbSearchCriterion dscValidFrom = new DbSearchCriterion("VALID_FROM", DbCompareOperand.LESS_EQUAL, now);
			DbSearchCriterion dscValidTo = new DbSearchCriterion("VALID_TO", DbCompareOperand.GREATER_EQUAL, now);
			DbSearchExpression dse = new DbSearchExpression();
			dse.addDbSearchItem(dscType).addDbSearchItem(dscValidFrom).addDbSearchItem(dscValidTo);

			notifications = svr.getObjects(dse, svCONST.OBJECT_TYPE_NOTIFICATION, null, 0, 0);

		}
		return notifications;
	}
}