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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.prtech.svarog.SvCore.SvAccess;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;

public class SvarogRolesTest {
	@BeforeClass
	public static void init() {
		SvConf.setClusterEnabled(false);
		SvMaintenance.shutdown();
		SvClusterClient.shutdown();
		SvClusterNotifierClient.shutdown();
		SvClusterServer.shutdown();
		SvClusterNotifierProxy.shutdown();
		SvCluster.shutdown();
		SvLock.clearLocks();

	}

	@AfterClass
	public static void after() {
		SvConf.setClusterEnabled(true);
	}

	static final String testUserName = "SVAROG_TEST_USER";
	static final String testPassword = "SVAROG_TEST_PASS";
	static final String testPin = "1213";

	static final String testAdminName = "SVAROG_ADMIN_USER";
	static final String testAdminPassword = "SVAROG_ADMIN_PASS";
	static final String testAdminPin = "6783";

	static DbDataObject createTestUser(String testUserName, String testPassword, String testPin, boolean isAdmin)
			throws SvException {
		DbDataObject user = null;
		SvSecurity svs = null;
		SvSecurity svsAdmin = null;

		SvWorkflow svw = null;
		SvWriter svwr = null;
		SvReader svr = null;
		try {

			svs = new SvSecurity();
			svw = new SvWorkflow();
			svw.setAutoCommit(false);
			svwr = new SvWriter(svw);
			svr = new SvReader(svwr);

			if (svs.checkIfUserExistsByUserName(testUserName, svCONST.STATUS_VALID)) {
				user = svs.getUser(testUserName);
			} else {
				user = svs.createUser(testUserName, testPassword, testUserName, testUserName, "contact@prtech.mk",
						testPin, "", "", "VALID");
			}
			if (!user.getStatus().equals("VALID")) {
				svw.moveObject(user, "VALID", true);
			}
			if (isAdmin) {
				// switch our writer to use the admin user.
				svwr.switchUser("ADMIN");
				// instantiate SvSecurity under the admin user
				svsAdmin = new SvSecurity(svwr);
				// switch the Reader under the test user name
				svr.switchUser(testUserName);
				if (!svr.isAdmin())// check if the test username is Admin?
				{
					DbDataObject ug = svr.getDefaultUserGroup();
					svsAdmin.removeUserFromGroup(user, ug);
					svsAdmin.addUserToGroup(user, svCONST.adminsGroup, true);
				}
			}
			svw.dbCommit();
		} finally {
			if (svs != null)
				svs.release();
			if (svw != null)
				svw.hardRelease();
		}

		return user;

	}

	@Test
	public void anonymousSwitchUser() {
		SvSecurity svs = null;
		SvReader svr = null;
		try {
			svs = new SvSecurity();
			DbDataObject user = null;
			if (svs.checkIfUserExistsByUserName(testUserName, svCONST.STATUS_VALID)) {
				user = svs.getUser(testUserName);
			} else {
				user = svs.createUser(testUserName, testPassword, testUserName, testUserName, "contact@prtech.mk",
						testPin, "", "", "VALID");
			}

			try {

				((SvCore) svs).switchUser(svCONST.serviceUser);
				fail("switchUser(DbDataObject user) was success without priveleges!");
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals("system.error.cant_switch_system_user")) {
					ex.printStackTrace();
					fail("The test raised an exception!");
				}
			}

			((SvCore) svs).switchUser(testUserName);
			fail("switchUser(String userName) was success without priveleges!");
		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.cant_switch_system_user")) {
				e.printStackTrace();
				fail("The test raised an exception!");
			}
		} finally {
			if (svs != null)
				svs.release();
		}

		SvConf.serviceClasses.add(this.getClass().getName());
		try {
			svs = new SvSecurity();
			((SvCore) svs).switchUser(testUserName);
			svs.resetUser();
			((SvCore) svs).switchUser(svCONST.serviceUser);
			svr = new SvReader(svs);

		} catch (SvException e) {
			e.printStackTrace();
			fail("The test raised an exception!");
		} finally {
			if (svs != null)
				svs.release();
		}

	}

	@Test
	public void testGetSid() {
		SvSecurity svs = null;
		SvReader svr = null;
		try {
			svs = new SvSecurity();
			DbDataObject dboUser = svs.getUser("ADMIN");
			dboUser.setVal("no_cache", 1);
			dboUser.setIs_dirty(false);
			DbDataObject dboUserCached = svs.getUser("ADMIN");
			if (dboUserCached.getVal("no_cache") == null)
				fail("user not cached");

		} catch (SvException e) {
			System.out.println(e.getFormattedMessage());
			e.printStackTrace();
			fail("Failed with exception");
		} finally {
			if (svs != null)
				svs.release();
			if (svr != null)
				svr.release();
		}

	}

	@Test
	public void testChangesUserPass() {
		SvSecurity svs = null;
		SvReader svr = null;
		try {
			svs = new SvSecurity();
			String token = getUserToken(false);
			svr = new SvReader(token);
			DbDataObject dboUser = svr.getUserBySession(token);
			svs.updatePassword(dboUser.getVal("USER_NAME").toString(), testPassword, testPassword);
		} catch (SvException e) {
			System.out.println(e.getFormattedMessage());
			e.printStackTrace();
			fail("Failed with exception");
		} finally {
			if (svs != null)
				svs.release();
			if (svr != null)
				svr.release();
		}
	}

	@Test
	public void testUserLocale() {
		try {
			SvWriter svw = new SvWriter();
			SvSecurity svc = new SvSecurity();

			DbDataObject admin = svc.getUser("ADMIN");
			DbDataObject locale = svw.getUserLocale(admin);
			svw.setUserLocale("ADMIN", "en_US");
			locale = svw.getUserLocale(admin);
			locale = svw.getUserLocale(admin);

			locale = svw.getUserLocale("ADMIN");

			if (locale == null)
				fail("no locale found");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getFormattedMessage());
			fail("Exception was raised");
		}

	}

	@Test
	public void switchServiceUser() {
		SvReader svs = null;
		try {
			svs = new SvReader();
			System.out.println(svs.instanceUser.toSimpleJson());

			// this is the way to switch to serviceUser
			((SvCore) svs).switchUser(svCONST.serviceUser);
			// end switch

			System.out.println(svs.instanceUser.toSimpleJson());
			((SvCore) svs).switchUser("ADMIN");
			System.out.println(svs.instanceUser.toSimpleJson());

		} catch (SvException e) {
			e.printStackTrace();
			fail("The test raised an exception!");
		} finally {
			if (svs != null)
				svs.release();
		}

	}

	@Test
	public void testResetUser() {
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			svs = new SvSecurity();
			svr = new SvReader();
			DbDataObject user = null;
			if (svs.checkIfUserExistsByUserName(testUserName, svCONST.STATUS_VALID)) {
				user = svs.getUser(testUserName);
			} else {
				user = svs.createUser(testUserName, testPassword, testUserName, testUserName, "contact@prtech.mk",
						testPin, "", "", "VALID");
			}
			svr.switchUser(testUserName);
			svr.resetUser();
			// one more reset to check if exception is thrown
			try {
				svr.resetUser();
			} catch (SvException e) {
				if (!e.getLabelCode().equals("system.error.illegal_reset_state")) {
					throw (e);
				}
			}

		} catch (SvException e) {
			e.printStackTrace();
			fail("The test raised an exception! " + e.getFormattedMessage());
		} finally {
			if (svr != null)
				svr.release();
			if (svs != null)
				svs.release();
		}

	}

	@Test
	public void getAcls() {
		SvReader svr = null;
		try {

			svr = new SvReader();
			if (svr.getPermissions() != null)
				fail("System user permissions should be null");

			svr.switchUser(testUserName);

			if (svr.getPermissions() == null)
				fail("Test user permissions should NOT be null");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("The test raised an exception!");
		} finally {
			if (svr != null)
				svr.release();
		}

	}

	public void sudoTest(boolean isAdmin) {
		SvReader svr = null;
		try {
			String token = getUserToken(isAdmin);
			svr = new SvReader(token);
			try {
				svr.switchUser("ADMIN");
			} catch (SvException e) {
				if (!isAdmin && !"system.error.not_authorised".equals(e.getLabelCode()))
					throw (e);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised an exception");
		} finally {
			if (svr != null)
				svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	public void getForms(boolean isAdmin, boolean withAutoInstance) throws SvException {
		SvReader svr = null;
		try {
			String token = getUserToken(isAdmin);
			svr = new SvReader(token);

			String formLabel = withAutoInstance ? "form_type.test_multi_auto" + DateTime.now().toString()
					: "form_type.test_multi";
			DbDataObject dboFormType = SvarogInstall.getFormType(formLabel, "1", !withAutoInstance, withAutoInstance,
					true, true);
			DbDataArray formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType.getObject_id(), null,
					null);
			if (withAutoInstance && formVals.size() < 1)
				fail("The form was not auto instantiated");

		} finally {
			if (svr != null)
				svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(true)) {
			fail("You have a connection leak, you dirty animal!");
		}
	}

	public boolean setSidPermission(String sidName, Long sidType, ArrayList<String> permissionKeys, String operation) {
		SvSecurity svs = null;
		SvReader svr = null;
		DbDataObject sid = null;
		try {
			svr = new SvReader();
			svr.switchUser("ADMIN");
			svs = new SvSecurity(svr);
			svs.setAutoCommit(false);
			sid = svs.getSid(sidName, sidType);
			for (String permissionKey : permissionKeys)
				if (operation.equals("GRANT"))
					svs.grantPermission(sid, permissionKey);
				else if (operation.equals("REVOKE"))
					svs.revokePermission(sid, permissionKey);
				else
					System.out.println("Operation must be either GRANT or REVOKE, Wrong operation:" + operation);
			svs.dbCommit();
		} catch (SvException e) {
			return false;
		} finally {
			if (svs != null)
				svs.release();
			if (svr != null)
				svr.release();
		}
		return true;

	}

	@Test
	public void userFormsAuthTest() {
		ArrayList<String> permissions = new ArrayList<String>(
				Arrays.asList("SVAROG_FORM.READ", "SVAROG_FORM_FIELD.READ"));
		try {
			SvCore.initSvCore();

			if (setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "GRANT")) {

				SvCore.initSvCore(true);
				// just read the forms without autoinstance
				getForms(false, false);

				try {
					// now read with autoinstance (this should require a WRITE
					// privilege!
					// and throw not_authorised exception
					getForms(false, true);
					fail("User with read privileges should be prevented to auto instance forms!");
				} catch (SvException ex) {
					if (!ex.getLabelCode().equals(Sv.Exceptions.NOT_AUTHORISED))
						throw (ex);
				}

			} else
				fail("Can't grant permission");
		} catch (Exception e) {
			if (e instanceof SvException)
				System.out.println(((SvException) e).getFormattedMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised an exception");
		} finally {
			if (!setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "REVOKE"))
				fail("Can't revoke permission");

		}
	}

	public static void verifyPermissionExists(DbDataObject secureObject, String unqConfigKey, String permissionKey,
			SvAccess permission) throws SvException {
		SvSecurity svs = null;
		SvWriter svw = null;
		try {
			svw = new SvWriter();
			svw.switchUser("ADMIN");
			svs = new SvSecurity(svw);
			try {
				svs.addPermission(secureObject, unqConfigKey, permissionKey, permission);
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals("system.error.acl_tuple_exists"))
					throw (ex);
			}
		} finally {
			if (svs != null)
				svs.release();
			if (svw != null)
				svw.release();
		}

	}

	@Test
	public void userFormTypes() {
		String formsUnqType = "form_type.test_multi";
		ArrayList<String> permissions = new ArrayList<String>(
				Arrays.asList("SVAROG_FORM.READ", "SVAROG_FORM_FIELD.READ", formsUnqType));
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			DbDataObject secureObject = SvCore.getDbt(svCONST.OBJECT_TYPE_FORM_TYPE);
			verifyPermissionExists(secureObject, formsUnqType, formsUnqType, SvAccess.READ);
			verifyPermissionExists(secureObject, formsUnqType, formsUnqType, SvAccess.READ);
			String token = null;
			DbQueryObject dqo = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_FORM_TYPE), null, null, null);
			try {
				token = getUserToken(false);
				svr = new SvReader(token);
				DbDataArray result = svr.getObjects(dqo, 0, 0);
				fail("no exception was raised");
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals(Sv.Exceptions.NOT_AUTHORISED))
					throw (ex);
			}
			// we must log off, to force refresh of the permissions
			svs = new SvSecurity();
			svs.logoff(token);

			if (setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "GRANT")) {
				token = getUserToken(false);
				svr = new SvReader(token);
				DbDataArray result = svr.getObjects(dqo, 0, 0);
				System.out.println(result.toSimpleJson());
			} else
				fail("Can't grant permission");

		} catch (Exception e) {
			if (e instanceof SvException)
				System.out.println(((SvException) e).getFormattedMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised an exception");
		} finally {
			if (!setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "REVOKE"))
				fail("Can't revoke permission");

			if (svs != null)
				svs.release();
			if (svr != null)
				svr.release();

		}

	}

	@Test
	public void userFormInstanceByType() {
		String formsUnqType = "form_type.test_multi";
		String aclCode = "form.test_multi";
		ArrayList<String> permissions = new ArrayList<String>(Arrays.asList(aclCode));
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			SvCore.initSvCore();
			DbDataObject secureObject = SvCore.getDbt(svCONST.OBJECT_TYPE_FORM);
			// revode the general permission if any
			setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER,
					new ArrayList<String>(Arrays.asList("SVAROG_FORM.READ")), "REVOKE");
			setSidPermission("USERS", svCONST.OBJECT_TYPE_GROUP,
					new ArrayList<String>(Arrays.asList("SVAROG_FORM.READ")), "REVOKE");
			SvCore.initSvCore(true);
			verifyPermissionExists(secureObject, formsUnqType, aclCode, SvAccess.READ);
			String token = null;
			DbQueryObject dqo = new DbQueryObject(secureObject, null, null, null);
			try {
				token = getUserToken(false);
				svr = new SvReader(token);
				DbDataArray result = svr.getObjects(dqo, 0, 0);
				fail("no exception was raised");
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals(Sv.Exceptions.NOT_AUTHORISED))
					throw (ex);
			}
			// we must log off, to force refresh of the permissions
			svs = new SvSecurity();
			svs.logoff(token);

			if (setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "GRANT")) {
				token = getUserToken(false);
				svr = new SvReader(token);
				DbDataArray result = svr.getObjects(dqo, 1, 0);

				System.out.println(result.toSimpleJson());
			} else
				fail("Can't grant permission");

		} catch (Exception e) {
			if (e instanceof SvException)
				System.out.println(((SvException) e).getFormattedMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised an exception:" + e.getMessage());
		} finally {
			if (!setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "REVOKE"))
				fail("Can't revoke permission");
			if (!setSidPermission("USERS", svCONST.OBJECT_TYPE_GROUP, permissions, "REVOKE"))
				fail("Can't revoke permission");

			if (svs != null)
				svs.release();
			if (svr != null)
				svr.release();

		}

	}

	@Test
	public void userFormsAutoInstanceAuthTest() {
		ArrayList<String> permissions = new ArrayList<String>(
				Arrays.asList("SVAROG_FORM.WRITE", "SVAROG_FORM_FIELD.WRITE"));
		try {
			if (setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "GRANT"))
				getForms(false, true);
			else
				fail("Can't grant permission");

		} catch (Exception e) {
			if (e instanceof SvException)
				System.out.println(((SvException) e).getFormattedMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised an exception");
		} finally {
			if (!setSidPermission(testUserName, svCONST.OBJECT_TYPE_USER, permissions, "REVOKE"))
				fail("Can't revoke permission");
		}
	}

	@Test
	public void adminFormsAuthTest() {
		try {
			getForms(true, false);
		} catch (Exception e) {
			if (e instanceof SvException)
				System.out.println(((SvException) e).getFormattedMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised an exception");
		}
	}

	@Test
	public void userSudoTest() {
		sudoTest(false);
	}

	@Test
	public void adminSudoTest() {
		sudoTest(true);
	}

	public static String getUserToken(boolean isAdmin) throws SvException {
		String token = null;
		SvReader db = null;
		SvSecurity svs = null;

		String userName = isAdmin ? testAdminName : testUserName;
		String password = isAdmin ? testAdminPassword : testPassword;
		String pin = isAdmin ? testAdminPin : testPin;

		try {
			DbDataObject user = createTestUser(userName, password, pin, isAdmin);
			if (user == null)
				fail("Can't create test user");

			svs = new SvSecurity();
			token = svs.logon(userName, password);
			if (token == null)
				fail("Can't logon!");
			db = new SvReader(token);
			if (db.isSystem())
				fail("The logon created a SvCore with System privileges!");

			boolean isCoreAdmin = db.isAdmin();
			if (isCoreAdmin != isAdmin)
				fail("The logon created a SvCore with wrong Admin privileges!");

		} finally {
			if (svs != null)
				svs.release();
			if (db != null)
				db.release();
		}
		return token;

	}

	@Test
	public void testSvarogLogon() {

		try {
			String token = getUserToken(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Can't use session to instantiate SvReader");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testSvarogAdminLogon() {
		try {
			String token = getUserToken(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Can't use session to instantiate SvReader");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testACL() {
		System.out.println("before test edbar install");

		try (SvSecurity svSec = new SvSecurity()) {
			svSec.setInstanceUser(svCONST.serviceUser);
			DbDataObject pesevsk = svSec.getUser("Z.PESEVSKI");


			try (SvReader svr = new SvReader()) {
				svr.switchUser(pesevsk);

				DbDataArray obb = svr.getObjects(null, svCONST.OBJECT_TYPE_BATCH_JOB_TYPE, null, 0, 0);

				System.out.println(obb.toSimpleJson().toString());
			} catch (Exception e) {
				e.printStackTrace();
				fail("exception was raised");
			}

		} catch (SvException e) {
			System.out.println(e.getFormattedMessage());
			if (!e.getLabelCode().equals("system.error.no_user_found"))
				fail("exception was raised");

		}

		System.out.println("test edbar install finished");
	}

}
