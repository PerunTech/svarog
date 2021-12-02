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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.DboFactory;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;

/**
 * The main class for handling security issues like users, groups, etc.
 * SvSecurity is inherited from SvCore but unlike the SvReader and alike, it
 * does provide a constructor to enable using it even without user session.
 * 
 * The SvSecurity's main role which should be used without a user session is to
 * authenticate a user against the Svarog framework
 * 
 * @author PR01
 *
 */
public class SvSecurity extends SvCore {

	/**
	 * Cache to hold details about all public ip adresses trying to use system
	 * resources without having proper user access, we shall throttle the requests.
	 */
	static LoadingCache<String, DbDataObject> ipThrottle = CacheBuilder.newBuilder()
			.expireAfterAccess(10, TimeUnit.MINUTES)
			.<String, DbDataObject>build(new CacheLoader<String, DbDataObject>() {
				@Override
				public DbDataObject load(String key) {
					DbDataObject dbo = new DbDataObject();
					dbo.setVal(Sv.Security.FIRST_REQUEST_TIME, DateTime.now());
					dbo.setVal(Sv.Security.REQUEST_COUNT, 0);
					return dbo;
				}
			});
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvSecurity.class.getName());

	/**
	 * Constructor which allows SvCore chaining
	 * 
	 * @param sharedSvCore the parent SvCore instance to be used for connection
	 *                     sharing
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public SvSecurity(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * DEPRECATED!!!! Replace with <b>SvSecurity(String publicUserID)</b>
	 * 
	 * @throws SvException Re-throw any underlying Svarog exception
	 * @deprecated This constructor is replaced with SvSecurity(String publicUserID)
	 */
	@Deprecated
	public SvSecurity() throws SvException {
		super(svCONST.systemUser, null);
	}

	private void blockPublicUser(String publicUserID) throws SvException, ExecutionException {
		DbDataObject info = ipThrottle.get(publicUserID);
		Integer blockCount = (Integer) info.getVal(Sv.Security.BLOCKED_COUNT);
		if (blockCount == null)
			blockCount = 1;
		else
			blockCount++;
		info.setVal(Sv.Security.BLOCKED_UNTIL, DateTime.now().plusMinutes(blockCount));
		info.setVal(Sv.Security.BLOCKED_COUNT, blockCount);
		log4j.info("Public user session throttled. Key:" + publicUserID + ", reason:" + info.toSimpleJson().toString());
		throw (new SvException(Sv.Exceptions.IP_BLOCKED, svCONST.systemUser, null, publicUserID));
	}

	/**
	 * Method to unblock a public IP found on the list of blacklisted peers
	 * 
	 * @param publicUserID The IP of the peer or any other uniquely identifying
	 *                     toked
	 * @throws ExecutionException
	 */
	static void unblockPublicUser(String publicUserID) throws ExecutionException {
		DbDataObject info = ipThrottle.get(publicUserID);
		info.setVal(Sv.Security.REQUEST_COUNT, 0);
		info.setVal(Sv.Security.FIRST_REQUEST_TIME, DateTime.now());
	}

	/**
	 * The public anonymous constructor. This is the ONLY SvCore inherited classes
	 * which allows anonymous constructors without valid user session. The only
	 * input parameter is used as a way to identify a user or group of users. Most
	 * commonly this will be the IP Address of the connection as provided by the
	 * public web services. The sole purpose of this ID is to prevent DDoS and to
	 * enable Svarog to throttle the connections of sessions being established at
	 * high rate from single IP address in attempt to perform brute force attack of
	 * any kind of DoS.
	 * 
	 * @param publicUserID A string identification of a user or a user group. In a
	 *                     classic web application the IP address of peer is a
	 *                     perfect candidate for public identification of potential
	 *                     system users
	 * 
	 * @throws SvException Re-throw any underlying Svarog exception
	 * 
	 */
	public SvSecurity(String publicUserID) throws SvException {
		super(svCONST.systemUser, null);

		try {
			DbDataObject info = ipThrottle.get(publicUserID);
			DateTime blockedUntil = (DateTime) info.getVal(Sv.Security.BLOCKED_UNTIL);

			synchronized (info) {
				if (blockedUntil != null && blockedUntil.isAfterNow())
					blockPublicUser(publicUserID);
				int count = (int) info.getVal(Sv.Security.REQUEST_COUNT);
				DateTime firstRequest = (DateTime) info.getVal(Sv.Security.FIRST_REQUEST_TIME);
				DateTime window = firstRequest.plusMinutes(1);
				// if window is in the future then increase the count
				if (window.isAfterNow()) {
					count++;
					info.setVal(Sv.Security.REQUEST_COUNT, count);
				} else
					unblockPublicUser(publicUserID);

				// if the count of sessions is more than the max allowed
				if (count > SvConf.getMaxRequestsPerMinute())
					blockPublicUser(publicUserID);

			}
		} catch (ExecutionException e) {
			throw (new SvException(Sv.Exceptions.IP_BLOCKED, svCONST.systemUser, null, publicUserID, e));
		}

	}

	@Override
	/**
	 * Overriden method to switch the current user. It will check if the caller
	 * class is a registered services class in svarog.properties and if not will
	 * raise an exception since anonymous users should not be allowed to switch
	 * without having a valid session
	 * 
	 * @param userName The name of user which should be switched
	 * @throws SvException If the caller class is not registered service class it
	 *                     will throw "system.error.cant_switch_system_user"
	 */
	public void switchUser(String userName) throws SvException {
		DbDataObject user = this.getUser(userName);
		this.switchUser(user);
	}

	@Override
	/**
	 * Overriden method to switch the current user. It will check if the caller
	 * class is a registered services class in svarog.properties and if not will
	 * raise an exception since anonymous users should not be allowed to switch
	 * without having a valid session
	 * 
	 * @param user DbDataObject which describes the user to be switched
	 * @throws SvException If the caller class is not registered service class it
	 *                     will throw "system.error.cant_switch_system_user"
	 */
	public void switchUser(DbDataObject user) throws SvException {
		if (!this.isService() && !SvConf.isServiceClass(SvUtil.getCallerClassName(this.getClass())))
			throw (new SvException("system.error.cant_switch_system_user", instanceUser));

		super.switchUser(user);
	}

	/**
	 * Method that returns value of a public string parameter. A public string
	 * parameter is considered any parameter which has parent of type
	 * svCONST.OBJECT_TYPE_SECURITY_LOG (currently long value 5)
	 * 
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public String getPublicParam(String label) throws SvException {

		String result = null;
		try (SvParameter svp = new SvParameter()) {
			DbDataObject securityDbt = SvCore.getDbt(svCONST.OBJECT_TYPE_SECURITY_LOG);
			result = svp.getParamString(securityDbt, label);
		}
		return result;
	}

	/**
	 * Method returning list of objects over which the specific user has been
	 * empowered with Power of Attorney over.
	 * 
	 * @param userObjectId      The Object ID of the user
	 * @param poaObjectTypeName The name of object types over which the user is
	 *                          empowered
	 * @return DbDataArray containing all object over which the user is empowered
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataArray getPOAObjects(Long userObjectId, String poaObjectTypeName) throws SvException {
		DbDataArray poaObjects = null;

		try (SvReader svr = new SvReader()) {
			DbDataObject dbl = getLinkType("POA", svCONST.OBJECT_TYPE_USER, getTypeIdByName(poaObjectTypeName));
			poaObjects = svr.getObjectsByLinkedId(userObjectId, svCONST.OBJECT_TYPE_USER, dbl,
					getTypeIdByName(poaObjectTypeName), false, null, 0, 0);
			return poaObjects;
		}

	}

	/**
	 * Method that loads a user object according to username from the database.
	 * 
	 * @param userId The user name
	 * @return The user object associated with the specific username
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject getUser(String userId) throws SvException {
		return getSid(userId, svCONST.OBJECT_TYPE_USER);
	}

	/**
	 * Method that loads a SID object according to sid name from the database.
	 * 
	 * @param sidName The SID name
	 * @param sidType The type of Security Identifier (OBJECT_TYPE_GROUP or
	 *                OBJECT_TYPE_USER)
	 * @return The sid descriptor
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject getSid(String sidName, Long sidType) throws SvException {
		if (sidType.equals(svCONST.OBJECT_TYPE_USER) || sidType.equals(svCONST.OBJECT_TYPE_GROUP)) {
			DbDataObject sid = null;
			sid = DbCache.getObject(sidName, sidType);
			if (sid == null)
				sid = getSidImpl(sidName, sidType);

			return sid;
		} else
			throw (new SvException("system.error.sid_type_not_valid", instanceUser));
	}

	/**
	 * Method that loads a SID object according to sid object id from the database.
	 * 
	 * @param sidId   The object id of the requested sid
	 * @param sidType The type of Security Identifier (OBJECT_TYPE_GROUP or
	 *                OBJECT_TYPE_USER)
	 * @return The sid descriptor
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject getSid(Long sidId, Long sidType) throws SvException {

		DbDataObject sid = null;
		sid = DbCache.getObject(sidId, sidType);
		if (sid == null)
			sid = getSidImpl(sidId, sidType);
		return sid;
	}

	/**
	 * Method to load a sid from the database according to either object id or sid
	 * name
	 * 
	 * @param sidId   The identifier of this sid (either string/name or
	 *                long/object_id)
	 * @param sidType The type of sid which should be loaded
	 * @return The sid descriptor loaded from the database
	 * @throws SvException Any underlying exception is re-thrown s
	 */
	DbDataObject getSidImpl(Object sidId, Long sidType) throws SvException {
		DbDataObject sid = null;
		DbSearchExpression expr = null;
		String sidTypeName = sidType.equals(svCONST.OBJECT_TYPE_GROUP) ? "group_name" : "user_name";

		expr = new DbSearchExpression();
		DbSearchCriterion critU = null;
		// if sidID is string, we search by sid name, otherwise we assume its
		// Long and search by object id.
		if (sidId instanceof String)
			critU = new DbSearchCriterion(sidTypeName, DbCompareOperand.EQUAL, sidId, DbLogicOperand.AND);
		else
			critU = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, sidId);

		expr.addDbSearchItem(critU);

		try (SvReader svr = new SvReader()) {
			DbDataArray ret = (svr).getObjects(expr, sidType, null, 1, 0);

			if (ret != null && ret.getItems().size() > 0) {
				sid = ret.getItems().get(0);
				// if user, remove the hash
				if (sidType.equals(svCONST.OBJECT_TYPE_USER))
					sid.setVal("PASSWORD_HASH", null);
				// if not user, nor group, return null
				else if (!sidType.equals(svCONST.OBJECT_TYPE_GROUP))
					sid = null;

			}
			if (sid != null)
				DbCache.addObject(sid, (String) sid.getVal(sidTypeName));
		}

		if (sid == null)
			throw (new SvException("system.error.no_user_found", instanceUser, null, expr));

		return sid;

	}

	/**
	 * Method to add system level permissions which are not linked to specific
	 * object types
	 * 
	 * @param permissionKey The permission key
	 * @param permission    The access level of the permission
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public void addSysPermission(String permissionKey, SvAccess permission) throws SvException {
		addPermission(SvCore.repoDbt, permissionKey, permissionKey, permission);
	}

	/**
	 * Method to add security permission for different object types (table
	 * descriptors)
	 * 
	 * @param secureObject  The object type/descriptor for which this permission
	 *                      will be valid
	 * @param unqConfigKey  The unique configuration key of the object type (if any)
	 * @param permissionKey The permission key which should be added to the list of
	 *                      permissions
	 * @param permission    The access level for this permission
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public void addPermission(DbDataObject secureObject, String unqConfigKey, String permissionKey, SvAccess permission)
			throws SvException {
		if (isSystem())
			throw (new SvException("system.error.sysuser_cant_manage_security", instanceUser));
		if (secureObject == null)
			throw (new SvException("system.error.no_dbt_found", instanceUser));

		try (SvWriter svw = new SvWriter(); SvReader svr = new SvReader(svw)) {

			DbQueryObject dqoAcl = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_ACL), null, null,
					DbJoinType.INNER);
			DbSearchExpression dbx = new DbSearchExpression();
			// DbSearchCriterion dbsKey = new DbSearchCriterion("LABEL_CODE",
			// DbCompareOperand.EQUAL, permissionKey);
			DbSearchCriterion dbsUnq = null;
			if (unqConfigKey != null)
				dbsUnq = new DbSearchCriterion("acl_config_unq", DbCompareOperand.EQUAL, unqConfigKey);
			else
				dbsUnq = new DbSearchCriterion("acl_config_unq", DbCompareOperand.ISNULL);

			DbSearchCriterion dbsOid = new DbSearchCriterion("acl_object_id", DbCompareOperand.EQUAL,
					secureObject.getObjectId());
			DbSearchCriterion dbsTypeId = new DbSearchCriterion("acl_object_type", DbCompareOperand.EQUAL,
					secureObject.getObjectType());

			dbx.addDbSearchItem(dbsTypeId).addDbSearchItem(dbsOid).addDbSearchItem(dbsUnq);

			dqoAcl.setSearch(dbx);
			DbDataArray arr = svr.getObjects(dqoAcl, 0, 0);

			if (arr != null && arr.size() > 0)
				throw (new SvException("system.error.acl_tuple_exists", instanceUser, dqoAcl, permissionKey));
			else {
				DbDataObject aclDbo = new DbDataObject(svCONST.OBJECT_TYPE_ACL);
				aclDbo.setVal("access_type", permission.toString());
				aclDbo.setVal("label_code", permissionKey);
				aclDbo.setVal("acl_config_unq", unqConfigKey);
				aclDbo.setVal("acl_object_id", secureObject.getObject_id());
				aclDbo.setVal("acl_object_type", secureObject.getObject_type());
				svw.saveObject(aclDbo);
			}
		}

	}

	/**
	 * Method to grant permission to a specific SID
	 * 
	 * @param sid           The SID object (user/group) to which the specific
	 *                      permission should be granted
	 * @param permissionKey The key of the permissions to be granted
	 * @throws SvException Throws exception if the SvSecurity is running as system
	 *                     without logon or if the SID was not found in the
	 *                     database. It also raises exception if the permission key
	 *                     was not found in the list of ACLs
	 */
	public void grantPermission(DbDataObject sid, String permissionKey) throws SvException {
		if (isSystem())
			throw (new SvException("system.error.sysuser_cant_manage_security", instanceUser));
		if (sid == null)
			throw (new SvException("system.error.no_sid_found", instanceUser));

		DbDataObject acl = null;

		try (SvWriter svw = new SvWriter(); SvReader svr = new SvReader(svw);) {

			DbDataArray permissions = svw.getPermissions(sid, svr);
			permissions.rebuildIndex("LABEL_CODE", true);
			if (permissions.getItemByIdx(permissionKey) == null) {
				DbQueryObject dqoAcl = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_ACL), null, null,
						DbJoinType.INNER);
				DbSearchCriterion dbs = new DbSearchCriterion("LABEL_CODE", DbCompareOperand.EQUAL, permissionKey);
				dqoAcl.setSearch(dbs);
				DbDataArray arr = svr.getObjects(dqoAcl, 0, 0);
				arr.rebuildIndex("LABEL_CODE", true);
				acl = arr.getItemByIdx(permissionKey);
				if (acl == null)
					throw (new SvException("system.error.no_acl_found", instanceUser, dqoAcl, permissionKey));

				DbDataObject aclSid = new DbDataObject(svCONST.OBJECT_TYPE_SID_ACL);

				aclSid.setVal("SID_OBJECT_ID", sid.getObjectId());
				aclSid.setVal("SID_TYPE_ID", sid.getObjectType());
				aclSid.setVal("ACL_OBJECT_ID", acl.getObjectId());
				svw.saveObject(aclSid);
			}
		}

	}

	/**
	 * Method to revode a permission from a specific SID
	 * 
	 * @param sid           The SID object for which the permission should be
	 *                      revoked
	 * @param permissionKey The key under which the permission is registered in the
	 *                      ACLs table
	 * @throws SvException Throws exception if the SvSecurity is running as system
	 *                     without logon or if the SID was not found in the
	 *                     database. It also raises exception if the permission key
	 *                     was not found in the list of ACLs
	 */
	public void revokePermission(DbDataObject sid, String permissionKey) throws SvException {
		if (isSystem())
			throw (new SvException("system.error.sysuser_cant_manage_security", instanceUser));
		if (sid == null)
			throw (new SvException("system.error.no_sid_found", instanceUser));

		DbDataObject acl = null;

		try (SvWriter svw = new SvWriter(); SvReader svr = new SvReader(svw);) {

			DbDataArray permissions = svw.getPermissions(sid, svr);
			permissions.rebuildIndex("LABEL_CODE", true);
			acl = permissions.getItemByIdx(permissionKey);
			if (acl != null) {
				DbQueryObject dqoSidAcl = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_SID_ACL), null, null,
						DbJoinType.INNER);
				DbSearchExpression dbx = new DbSearchExpression()
						.addDbSearchItem(
								new DbSearchCriterion("SID_OBJECT_ID", DbCompareOperand.EQUAL, sid.getObjectId()))
						.addDbSearchItem(
								new DbSearchCriterion("SID_TYPE_ID", DbCompareOperand.EQUAL, sid.getObjectType()))
						.addDbSearchItem(
								new DbSearchCriterion("ACL_OBJECT_ID", DbCompareOperand.EQUAL, acl.getObjectId()));
				dqoSidAcl.setSearch(dbx);
				DbDataArray sidAcls = svr.getObjects(dqoSidAcl, 0, 0);

				if (sidAcls.size() > 0) {
					DbDataObject aclSid = sidAcls.get(0);
					svw.deleteObject(aclSid);
				}
			}
		}

	}

	/**
	 * Method to return list of permissions according to specific permission mask
	 * 
	 * @param permissionMask The permission mask to be used.
	 * @return The list of permissions in DbDataArray variable
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public DbDataArray getPermissions(String permissionMask) throws SvException {
		if (isSystem())
			throw (new SvException("system.error.sysuser_cant_manage_security", instanceUser));

		DbDataArray acls = null;

		try (SvWriter svw = new SvWriter(); SvReader svr = new SvReader(svw);) {

			DbSearchExpression dbx = new DbSearchExpression()
					.addDbSearchItem(new DbSearchCriterion("LABEL_CODE", DbCompareOperand.LIKE, permissionMask));

			acls = svr.getObjects(dbx, svCONST.OBJECT_TYPE_ACL, null, 0, 0);

		}
		return acls;

	}

	/**
	 * Method for authenticating a user against the svarog database
	 * 
	 * @param user The username to be authenticated
	 * @param pass The password to be used for authentication
	 * @param svw  SvWriter instance to be used for the writing to database
	 * @return Svarog Security token which can be further used to get a DbUtil
	 *         instance
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	String logonImpl(String user, String pass, SvWriter svw) throws SvException {
		String sessionToken = null;

		try (SvReader svr = new SvReader(svw);) {
			DbSearchExpression expr = new DbSearchExpression();
			DbSearchCriterion critU = new DbSearchCriterion("user_name", DbCompareOperand.EQUAL, user,
					DbLogicOperand.AND);
			DbSearchCriterion critP = new DbSearchCriterion("password_hash", DbCompareOperand.EQUAL,
					SvUtil.getMD5(pass), DbLogicOperand.AND);

			expr.addDbSearchItem(critU);
			expr.addDbSearchItem(critP);

			DbDataObject userData = null;
			DbDataArray ret = svr.getObjects(expr, svCONST.OBJECT_TYPE_USER, null, 1, 0);

			if (ret.getItems().size() > 0) {
				userData = ret.getItems().get(0);
				if (userData.getStatus().equals(SvWriter.getDefaultStatus(getDbt(userData.getObjectType())))) {

					sessionToken = SvUtil.getUUID();

					DbDataObject audit = new DbDataObject(svCONST.OBJECT_TYPE_SECURITY_LOG);
					audit.setVal("session_id", sessionToken);
					audit.setVal("user_object_id", userData.getObjectId());
					audit.setVal("ACTIVITY_TYPE", "login");
					svw.isInternal = true;
					svw.saveObject(audit);
					audit.setVal("last_refresh", DateTime.now());

					userData.setIsDirty(false);
					audit.setIsDirty(false);
					DboFactory.makeDboReadOnly(userData);
					DboFactory.makeDboReadOnly(audit);
					DbCache.addObject(userData);
					DbCache.addObject(audit, sessionToken);
					// distribute the token to the cluster
					if (SvCluster.getIsActive().get() && !SvCluster.isCoordinator())
						SvClusterClient.putToken(audit);
				} else
					throw (new SvException("system.error.user_status_invalid", instanceUser, userData, expr));
			} else {
				throw (new SvException("system.error.no_user_found", instanceUser, ret, expr));
			}
			return sessionToken;
		}

	}

	/**
	 * Method to perform svarog logon, with option to use your own writer, in case
	 * you need to control the database transaction explicitly
	 * 
	 * @param user The user name of the user
	 * @param pass The password of the user
	 * @param svw  SvWriter instance to control your own transaction if needed
	 * @return A string containing a session token
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public String logon(String user, String pass, SvWriter svw) throws SvException {
		return logonImpl(user, pass, svw);
	}

	/**
	 * Method to perform svarog logon, with commit on success
	 * 
	 * @param user The user name of the user
	 * @param pass The password of the user
	 * @return A string containing a session token
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public String logon(String user, String pass) throws SvException {
		String sessionToken = null;
		try (SvWriter svw = new SvWriter()) {
			svw.dbSetAutoCommit(false);
			sessionToken = logonImpl(user, pass, svw);
			svw.dbCommit();
		} catch (SvException e) {
			throw (e);

		}
		return sessionToken;

	}

	/**
	 * Method to remove all cached data from the security log
	 * 
	 * @param sessionId The user session which should be removed from the security
	 *                  log
	 * @throws SvException
	 */
	public void logoff(String sessionId) throws SvException {
		DbDataObject svToken = DbCache.getObject(sessionId, svCONST.OBJECT_TYPE_SECURITY_LOG);
		if (svToken != null) {
			DbCache.removeObject(svToken.getObjectId(), sessionId, svCONST.OBJECT_TYPE_SECURITY_LOG);
			DbCache.removeByParentId(svCONST.OBJECT_TYPE_ACL, (Long) svToken.getVal("user_object_id"));
			// distribute the token to the cluster
			if (SvCluster.getIsActive().get() && !SvCluster.isCoordinator())
				SvClusterNotifierClient.publishLogoff(sessionId);

		}
	}

	/**
	 * Method which activates a pending external user registration
	 * 
	 * @param uuid UID of the user which should be activated
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public void activateExternalUser(String uuid) throws SvException {
		DbSearchExpression dse = new DbSearchExpression();
		DbSearchCriterion searchByUID = new DbSearchCriterion("USER_UID", DbCompareOperand.EQUAL, uuid);
		dse.addDbSearchItem(searchByUID);

		try (SvReader svr = new SvReader(); SvWorkflow svw = new SvWorkflow(svr);) {
			Boolean userFound = false;
			DbDataArray dba = svr.getObjects(dse, svCONST.OBJECT_TYPE_USER, null, 0, 0);
			if (dba != null) {
				DbDataObject user = dba.getItems().size() > 0 ? dba.getItems().get(0) : null;
				if (user != null && user.getStatus().equals("PENDING")) {
					(svw).moveObject(user, svCONST.STATUS_VALID);
					userFound = true;
				} else if (user != null && user.getStatus().equals("VALID")) {
					userFound = true;
				}
			}

			if (!userFound)
				throw (new SvException("system.error.activate_no_user", instanceUser, null, dse));
		}
	}

	/**
	 * Implementation method to create a new user in the svarog system. If the
	 * SvSecurity instance runs as System User then all created users will be
	 * userType=EXTERNAL with status=PENDING. If the instance runs as admin user, it
	 * creates the user according to all parameter values
	 * 
	 * @param userName  The username
	 * @param password  Password of the user
	 * @param firstName First name of the user
	 * @param lastName  Last name of the user
	 * @param e_mail    E-mail of the user
	 * @param pin       Personal ID number
	 * @param tax_id    Secondary ID of the user. Tax ID if legal entity.
	 * @param userType  The type of the user (EXTERNAL is specific type for public
	 *                  users)
	 * @return The descriptor of the newly created user
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	DbDataObject createUserImpl(String userName, String password, String firstName, String lastName, String e_mail,
			String pin, String tax_id, String userType, String status, Boolean overWrite, SvWriter svw)
			throws SvException {

		// If the SvSecurity instance was started without valid user session,
		// enforce EXTERNAL user creation
		if (this.instanceUser.equals(svCONST.systemUser))
			userType = "EXTERNAL";

		try (SvReader svr = new SvReader(svw)) {
			DbSearchExpression expr = new DbSearchExpression()
					.addDbSearchItem(new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL, userName));

			DbDataArray uExisting = svr.getObjects(expr, svCONST.OBJECT_TYPE_USER, null, 0, 0);

			DbDataObject dboUser = null;
			if (uExisting.getItems().size() > 0) {
				if (!overWrite)
					throw (new SvException("system.error.user_exists", instanceUser, uExisting, expr));
				else
					dboUser = uExisting.getItems().get(0);
			} else {
				dboUser = new DbDataObject(svCONST.OBJECT_TYPE_USER);
				dboUser.setVal("USER_NAME", userName);
				dboUser.setVal("USER_TYPE", userType);
				dboUser.setVal("USER_UID", SvUtil.getUUID());
				if (userType.equals("EXTERNAL"))
					status = "PENDING";
			}

			if (firstName != null)
				dboUser.setVal("FIRST_NAME", firstName);
			if (lastName != null)
				dboUser.setVal("LAST_NAME", lastName);
			if (e_mail != null)
				dboUser.setVal("E_MAIL", e_mail);
			if (pin != null)
				dboUser.setVal("PIN", pin);
			if (tax_id != null)
				dboUser.setVal("TAX_ID", tax_id);
			if (password != null)
				dboUser.setVal("PASSWORD_HASH", SvUtil.getMD5(password));
			if (status != null)
				dboUser.setStatus(status);

			svw.saveObject(dboUser, false);

			// for external users link to the USERS SID as default group
			if (userType.equals("EXTERNAL") && !overWrite) {
				linkUserToGroup(dboUser, svCONST.usersGroup, true, svr);
			}
			return dboUser;
		}

	}

	/**
	 * Implementation method to recovery a user password in a svarog system.
	 * 
	 * @param userName The username
	 * @param pin      Personal ID number
	 * @param newPass  New password of the user
	 * @return
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject recoverPassword(String userName, String pin, String newPass) throws SvException {
		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {

			svw.dbSetAutoCommit(false);
			DbSearchExpression expr = new DbSearchExpression()
					.addDbSearchItem(new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL, userName));

			DbDataArray uExisting = svr.getObjects(expr, svCONST.OBJECT_TYPE_USER, null, 0, 0);

			DbDataObject dboUser = null;
			if (uExisting.getItems().size() > 0) {
				dboUser = uExisting.getItems().get(0);
			}
			if (dboUser == null)
				throw (new SvException("updatePassword.error.userNotFound", instanceUser, dboUser, null));
			if (!dboUser.getVal("PIN").toString().equals(pin)) {
				throw (new SvException("changePassword.error.incorrectPin", instanceUser, dboUser, null));
			}
			dboUser.setVal("PASSWORD_HASH", SvUtil.getMD5(newPass.toUpperCase()));
			svw.saveObject(dboUser, false);
			svw.dbCommit();
			DbDataObject refreshCacheUserObj = svr.getObjectById(dboUser.getObjectId(), svCONST.OBJECT_TYPE_USER, null);
			return refreshCacheUserObj;
		}
	}

	/**
	 * Implementation method to change a user password in a svarog system.
	 * 
	 * @param userName The username
	 * @param oldPass  Old (Current) password of the user
	 * @param newPass  New password of the user
	 * @return
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject updatePassword(String userName, String oldPass, String newPass) throws SvException {

		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {

			svw.dbSetAutoCommit(false);
			DbSearchExpression expr = new DbSearchExpression()
					.addDbSearchItem(new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL, userName));

			DbDataArray uExisting = svr.getObjects(expr, svCONST.OBJECT_TYPE_USER, null, 0, 0);

			DbDataObject dboUser = null;
			if (uExisting.getItems().size() > 0) {
				dboUser = uExisting.getItems().get(0);
			}
			if (dboUser == null)
				throw (new SvException("updatePassword.error.userNotFound", instanceUser, dboUser, null));
			if (!dboUser.getVal("PASSWORD_HASH").toString()
					.equals(SvUtil.getMD5(oldPass.toUpperCase()).toUpperCase())) {
				throw (new SvException("changePassword.error.incorrectOldPass", instanceUser, dboUser, null));
			}
			dboUser.setVal("PASSWORD_HASH", SvUtil.getMD5(newPass.toUpperCase()));
			svw.saveObject(dboUser, false);
			svw.dbCommit();
			DbDataObject refreshCacheUserObj = svr.getObjectById(dboUser.getObject_id(), svCONST.OBJECT_TYPE_USER,
					null);
			return refreshCacheUserObj;
		}

	}

	/**
	 * Method to create a new user in the svarog system, with option to auto commit.
	 * If the SvSecurity instance runs as System User then all created users will be
	 * userType=EXTERNAL with status=PENDING. If the instance runs as admin user, it
	 * creates the user according to all parameter values. The SvWriter parameter
	 * allows you to control your own db transaction
	 * 
	 * @param userName  The username
	 * @param password  Password of the user
	 * @param firstName First name of the user
	 * @param lastName  Last name of the user
	 * @param e_mail    E-mail of the user
	 * @param pin       Personal ID number
	 * @param tax_id    Secondary ID of the user. Tax ID if legal entity.
	 * @param userType  The type of the user (EXTERNAL is specific type for public
	 *                  users)
	 * @return
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject createUser(String userName, String password, String firstName, String lastName, String e_mail,
			String pin, String tax_id, String userType, String status, Boolean overWrite, SvWriter svw)
			throws SvException {

		return createUserImpl(userName, password, firstName, lastName, e_mail, pin, tax_id, userType, status, overWrite,
				svw);
	}

	/**
	 * Method that checks if a user already exists according the userName in
	 * registration forms. If exists return true.
	 * 
	 * @param userName the userName entered in the user registration form
	 */
	public Boolean checkIfUserExistsByUserName(String userName) throws SvException {
		return checkIfUserExistsByUserName(userName, null);
	}

	/**
	 * Method that checks if a user already exists according the userName in
	 * registration forms. If exists return true.
	 * 
	 * @param userName the userName entered in the user registration form
	 * @param status   The status for which the user should be checked. Status check
	 *                 is void if null.
	 */
	public Boolean checkIfUserExistsByUserName(String userName, String status) throws SvException {
		Boolean result = false;
		try (SvReader svr = new SvReader();) {

			DbSearchExpression getUserByUName = new DbSearchExpression();
			DbSearchCriterion filterByUserName = new DbSearchCriterion("USER_NAME", DbCompareOperand.EQUAL, userName);
			getUserByUName.addDbSearchItem(filterByUserName);
			if (status != null)
				getUserByUName.addDbSearchItem(new DbSearchCriterion("STATUS", DbCompareOperand.EQUAL, status));

			DbDataArray searchResult = svr.getObjects(filterByUserName, svCONST.OBJECT_TYPE_USER, null, 0, 0);

			if (searchResult.getItems().size() > 0) {
				result = true;
			}
		}
		return result;
	}

	/**
	 * Method that checks if a user already exists according the entered pin number
	 * in registration forms. If exists return true.
	 * 
	 * @param pinNo the pin number entered in the user registration form
	 * @return True if the user identified by the pin exists.
	 * @throws SvException Passthrough of underlying SvException
	 */
	public Boolean checkIfUserExistsByPin(String pinNo) throws SvException {
		return checkIfUserExistsByPin(pinNo, null);
	}

	/**
	 * Method that checks if a user already exists according the entered pin number
	 * in registration forms along with status of the user. If exists return true.
	 * 
	 * @param pinNo  the pin number entered in the user registration form
	 * @param status The status for which the user should be checked. Status check
	 *               is void if null.
	 */
	public Boolean checkIfUserExistsByPin(String pinNo, String status) throws SvException {
		Boolean result = false;
		try (SvReader svr = new SvReader()) {

			DbSearchExpression getUserByPin = new DbSearchExpression();
			DbSearchCriterion filterByPin = new DbSearchCriterion("PIN", DbCompareOperand.EQUAL, pinNo);
			getUserByPin.addDbSearchItem(filterByPin);
			if (status != null)
				getUserByPin.addDbSearchItem(new DbSearchCriterion("STATUS", DbCompareOperand.EQUAL, status));

			DbDataArray searchResult = svr.getObjects(getUserByPin, svCONST.OBJECT_TYPE_USER, null, 0, 0);

			if (searchResult.getItems().size() > 0) {
				result = true;
			}
		}
		return result;
	}

	/**
	 * Method to create a new user in the svarog system, with commit on success
	 * (rollback on exception). If the SvSecurity instance runs as System User then
	 * all created users will be userType=EXTERNAL with status=PENDING. If the
	 * instance runs as admin user, it creates the user according to all parameter
	 * values
	 * 
	 * @param userName  The username
	 * @param password  Password of the user
	 * @param firstName First name of the user
	 * @param lastName  Last name of the user
	 * @param e_mail    E-mail of the user
	 * @param pin       Personal ID number
	 * @param tax_id    Secondary ID of the user. Tax ID if legal entity.
	 * @param userType  The type of the user (EXTERNAL is specific type for public
	 *                  users)
	 * @return
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject createUser(String userName, String password, String firstName, String lastName, String e_mail,
			String pin, String tax_id, String userType, String status) throws SvException {

		DbDataObject user = null;
		try (SvWriter svw = new SvWriter()) {

			svw.dbSetAutoCommit(false);
			user = createUserImpl(userName, password, firstName, lastName, e_mail, pin, tax_id, userType, status, false,
					svw);

			svw.dbCommit();
		}
		return user;
	}

	/**
	 * Method to create a new user in the svarog system, with commit on success
	 * (rollback on exception). If the SvSecurity instance runs as System User then
	 * all created users will be userType=EXTERNAL with status=PENDING. If the
	 * instance runs as admin user, it creates the user according to all parameter
	 * values
	 * 
	 * @param userName  The username
	 * @param password  Password of the user
	 * @param firstName First name of the user
	 * @param lastName  Last name of the user
	 * @param e_mail    E-mail of the user
	 * @param pin       Personal ID number
	 * @param tax_id    Secondary ID of the user. Tax ID if legal entity.
	 * @param userType  The type of the user (EXTERNAL is specific type for public
	 *                  users)
	 * @return
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public DbDataObject createUser(String userName, String password, String firstName, String lastName, String e_mail,
			String pin, String tax_id, String userType, String status, Boolean overwrite) throws SvException {

		DbDataObject user = null;

		try (SvWriter svw = new SvWriter()) {
			svw.dbSetAutoCommit(false);
			user = createUserImpl(userName, password, firstName, lastName, e_mail, pin, tax_id, userType, status,
					overwrite, svw);
			svw.dbCommit();
		} catch (SvException e) {
			throw (e);
		}
		return user;
	}

	/**
	 * Method to empower a user over a unique object identified by search criteria
	 * 
	 * @param userObj        The object describing the user
	 * @param objectTypeName
	 * @param searchCriteria
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public void empowerUser(DbDataObject userObj, String objectTypeName, DbSearch searchCriteria) throws SvException {

		// find the object to which we should link
		try (SvLink svl = new SvLink()) {
			empowerUser(userObj, objectTypeName, searchCriteria, svl);
			svl.dbCommit();
		}

	}

	/**
	 * Method to empower a user over a unique object identified by search criteria
	 * 
	 * @param userObj        The object describing the user
	 * @param objectTypeName
	 * @param searchCriteria
	 * @param svl            SvLink to be used for the empowerment to enable
	 *                       transaction control
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public void empowerUser(DbDataObject userObj, String objectTypeName, DbSearch searchCriteria, SvLink svl)
			throws SvException {

		Long objectTypeId = getTypeIdByName(objectTypeName);

		// find the object to which we should link
		SvReader svr = null;
		try {
			svr = new SvReader(svl);
			DbDataArray dbArray = svr.getObjects(searchCriteria, objectTypeId, null, 0, 0);

			if (dbArray.getItems().size() != 1)
				throw (new SvException("system.error.poa_is_not_unique", instanceUser, userObj,
						dbArray.getItems().get(0)));
			else
				empowerUser(userObj, dbArray.getItems().get(0), svl);
		} finally {
			if (svr != null)
				svr.release();
		}

	}

	/**
	 * Method to empower a user over a svarog object type with auto commit.
	 * 
	 * @param userObj           The user object which should be empowered
	 * @param empowerOverObject The object over which the userObj shall be empowered
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public void empowerUser(DbDataObject userObj, DbDataObject empowerOverObject) throws SvException {
		try (SvLink svl = new SvLink()) {
			svl.dbSetAutoCommit(false);
			empowerUser(userObj, empowerOverObject, svl);
			svl.dbCommit();
		}

	}

	/**
	 * Method to empower a user over a svarog object type by using your own SvLink
	 * instance and have full transaction control.
	 * 
	 * @param userObj           The user object which should be empowered
	 * @param empowerOverObject The object over which the userObj shall be empowered
	 * @param svl               SvLink instance to be used for linking the objects
	 *                          and allow transaction control
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public void empowerUser(DbDataObject userObj, DbDataObject empowerOverObject, SvLink svl) throws SvException {
		DbDataObject dboLType = null;
		// TODO better handling of relating email data of farmer and user in
		// future
		// if empowerOverObject is FARMER, and it has not MAIL address,
		// insert the mail from the user
		if ((empowerOverObject.getObject_type().equals(SvReader.getTypeIdByName("FARMER")))) {
			SvWriter svw = null;
			try {
				svw = new SvWriter(svl);
				// do not change the e-mail for the farmer since it is imported
				// data and it also triggers another automatic import since
				// there are changes in data between local and the DB that we
				// are importing from
				/*
				 * if (empowerOverObject.getVal("MAIL") == null ||
				 * empowerOverObject.getVal("MAIL").toString().trim().equals("") ) {
				 * empowerOverObject.setVal("MAIL", userObj.getVal("E_MAIL"));
				 * svw.saveObject(empowerOverObject, false);
				 * 
				 * }
				 */
				String fullName = (String) empowerOverObject.getVal("full_name");
				String lastName = (String) empowerOverObject.getVal("surname");
				String firstName = (String) empowerOverObject.getVal("fname");
				if (firstName != null && firstName.trim().length() > 0)
					userObj.setVal("first_name", firstName);
				else
					userObj.setVal("first_name", fullName);

				if (lastName != null && lastName.trim().length() > 0)
					userObj.setVal("last_name", lastName);
				else
					userObj.setVal("last_name", " ");

				svw.dbCommit();
			} finally {
				if (svw != null) {
					svw.release();
				}
			}
		}
		dboLType = getLinkType("POA", svCONST.OBJECT_TYPE_USER, empowerOverObject.getObject_type());
		if (dboLType != null) {
			svl.linkObjects(userObj.getObject_id(), empowerOverObject.getObject_id(), dboLType.getObject_id(), "");
		} else
			throw (new SvException("system.error.no_poa_link", instanceUser, userObj, empowerOverObject));
	}

	/**
	 * Method to link a user to user group. Will throw exception if the user already
	 * is member of the requested user group, or if the user already has another
	 * default group.
	 * 
	 * @param user           The user to become a member of the user group
	 * @param userGroup      The user group object
	 * @param isDefaultGroup Is this user group the default one?
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	void linkUserToGroup(DbDataObject user, DbDataObject userGroup, Boolean isDefaultGroup, SvReader svr)
			throws SvException {

		SvLink svl = new SvLink(svr);
		// create default group
		try {
			DbDataObject dblt = getLinkType("USER_" + (isDefaultGroup ? "DEFAULT_" : "") + "GROUP",
					svCONST.OBJECT_TYPE_USER, svCONST.OBJECT_TYPE_GROUP);
			DbDataArray uGroups = (svr).getObjectsByLinkedId(user.getObject_id(), dblt, null, 0, 0);

			if (isDefaultGroup && uGroups.getItems().size() > 0)
				throw (new SvException("system.error.user_has_default_group", instanceUser, user, userGroup));

			for (DbDataObject dbg : uGroups.getItems()) {
				if (dbg.getObject_id().equals(userGroup.getObject_id()))
					throw (new SvException("system.error.user_is_group_member", instanceUser, user, userGroup));
			}

			svl.linkObjects(user.getObject_id(), userGroup.getObject_id(), dblt.getObject_id(), "");
		} finally {
			svl.release();
		}
	}

	/**
	 * Method for adding a user to a specific user group
	 * 
	 * @param user           The user object descriptor
	 * @param userGroup      The user group object descriptor
	 * @param isDefaultGroup Flag if the user group should be the default
	 * @throws SvException In case the user is already a member or it has a
	 *                     different default group an exception will be thrown. In
	 *                     case the SvSecurity has been instantiated as SYSTEM
	 *                     exception will be thrown to prevent non-authenticated
	 *                     use.
	 */
	public void addUserToGroup(DbDataObject user, DbDataObject userGroup, Boolean isDefaultGroup) throws SvException {
		if (isSystem())
			throw (new SvException("system.error.sysuser_cant_manage_security", instanceUser));

		SvReader svr = null;
		try {
			svr = new SvReader();
			linkUserToGroup(user, userGroup, isDefaultGroup, svr);
		} finally {
			if (svr != null)
				svr.release();
		}
	}

	/**
	 * Method to remove the requested user from the user group
	 * 
	 * @param user      The user object descriptor
	 * @param userGroup The user group object descriptor
	 * @throws SvException In case the SvSecurity has been instantiated as SYSTEM
	 *                     exception will be thrown to prevent non-authenticated
	 *                     use.
	 */
	public void removeUserFromGroup(DbDataObject user, DbDataObject userGroup) throws SvException {
		if (isSystem())
			throw (new SvException("system.error.sysuser_cant_manage_security", instanceUser));

		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {

			DbDataObject dblt = getLinkType("USER_GROUP", svCONST.OBJECT_TYPE_USER, svCONST.OBJECT_TYPE_GROUP);
			DbDataObject dbltDefault = getLinkType("USER_DEFAULT_GROUP", svCONST.OBJECT_TYPE_USER,
					svCONST.OBJECT_TYPE_GROUP);

			DbSearchExpression dbxType = new DbSearchExpression().addDbSearchItem(new DbSearchCriterion("LINK_TYPE_ID",
					DbCompareOperand.EQUAL, dbltDefault.getObject_id(), DbLogicOperand.OR));

			if (dblt != null)
				dbxType.addDbSearchItem(
						new DbSearchCriterion("LINK_TYPE_ID", DbCompareOperand.EQUAL, dblt.getObject_id()));

			DbSearchExpression dbx = new DbSearchExpression().addDbSearchItem(dbxType)
					.addDbSearchItem(
							new DbSearchCriterion("link_obj_id_1", DbCompareOperand.EQUAL, user.getObject_id()))
					.addDbSearchItem(
							new DbSearchCriterion("link_obj_id_2", DbCompareOperand.EQUAL, userGroup.getObject_id()));

			DbDataArray links = svr.getObjects(dbx, getDbt(svCONST.OBJECT_TYPE_LINK), null, 0, 0);

			if (links.size() > 0) {
				DbDataObject lnk = links.get(0);
				svw.deleteObject(lnk);
			} else
				throw (new SvException("system.error.user_not_member_of_group", instanceUser, user, userGroup));

		}
	}

	/**
	 * Method to check if a user already exists in the database
	 * 
	 * @param objectTypeName the name of the table where to search
	 * @param searchCriteria
	 * @throws SvException Re-throw any underlying Svarog exception
	 * @return Yes if the user exists
	 */
	public Boolean checkIfExists(String objectTypeName, DbSearch searchCriteria, SvReader svReader) throws SvException {

		Boolean result = false;
		Long objectTypeId = getTypeIdByName(objectTypeName);
		if (objectTypeId == null) {
			return result;
		}
		DbDataArray dbArray = svReader.getObjects(searchCriteria, objectTypeId, null, 0, 0);
		if (dbArray.getItems().size() > 0) {
			result = true;
		}

		return result;

	}

	/**
	 * Method to check if a user already exists in the database
	 * 
	 * @param objectTypeName the name of the table where to search
	 * @param searchCriteria
	 * @throws SvException Re-throw any underlying Svarog exception
	 */
	public Boolean checkIfExists(String objectTypeName, DbSearch searchCriteria) throws SvException {
		SvReader svReader = new SvReader();
		Boolean result = false;
		try {
			result = checkIfExists(objectTypeName, searchCriteria, svReader);
		} finally {
			svReader.release();
		}

		return result;

	}

	/**
	 * Method to check if there exist matching between userName (FIC) and pin/vat of
	 * farmer
	 * 
	 * @param objectTypeName the name of the table where to search
	 * @param searchCriteria
	 * @throws SvException Re-throw any underlying Svarog exception
	 * @return Yes if the user exists
	 */
	public Boolean checkIfExistsConditional(String objectTypeName, DbSearch searchCriteria, String columnToCompare,
			String compareValue) throws SvException {

		Boolean result = false;
		try (SvReader svReader = new SvReader()) {
			Long objectTypeId = getTypeIdByName(objectTypeName);
			if (objectTypeId == null) {
				return result;
			}
			DbDataArray dbArray = svReader.getObjects(searchCriteria, objectTypeId, null, 0, 0);
			if (dbArray.getItems().size() > 0) {
				if (dbArray.getItems().get(0).getVal(columnToCompare).equals(compareValue))
					result = true;
			}
		}
		return result;

	}

}
