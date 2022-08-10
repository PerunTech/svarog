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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.IDbFilter;
import com.prtech.svarog_common.ISvOnSave;
import com.prtech.svarog_common.SvCharId;

/**
 * @author PR01
 * 
 */
public class SvarogTest {
	public class SvTestCallback implements ISvOnSave {
		@Override
		public boolean beforeSave(SvCore parentCore, DbDataObject dbo) throws SvException {
			if (dbo.getVal("FAIL_VAL") != null) {
				DbDataObject dbt = new DbDataObject();
				dbt.setVal("CUSTOM_ERROR_MSG", "ova e nekoj error");
				throw (new SvException("zokan.custom.exception", svCONST.systemUser, dbt, "sss"));
			} else
				return true;
		}

		@Override
		public void afterSave(SvCore parentCore, DbDataObject dbo) {
			System.out.println("After save:" + dbo.toSimpleJson().toString());
		}

	}

	/**
	 * Implementation of DbDAtaArray filter to select only the tables whose name
	 * begins with the repo name
	 * 
	 * @author XPS13
	 *
	 */
	public class FilterByName implements IDbFilter {

		@Override
		public boolean filterObject(DbDataObject dbo) {
			if (dbo.getVal("TABLE_NAME") != null
					&& ((String) dbo.getVal("TABLE_NAME")).startsWith(SvConf.getMasterRepo() + "_FORM"))
				return true;
			else
				return false;
		}

	}

	@Test
	public void testGetUnqCfgCaseInsensitive() {
		SvReader svr = null;
		try {
			svr = new SvReader();
			DbDataObject tblObj = SvCore.getDbt(svCONST.OBJECT_TYPE_GROUP);
			DbDataObject cfgObj = svr.getObjectByUnqConfId("ADMINISTRATORS", tblObj, true);
			if (!(cfgObj != null && cfgObj.getObject_id() != 0))
				fail("Object not found");

			cfgObj = svr.getObjectByUnqConfId("ADMINISTRATors", tblObj, true);
			if (cfgObj != null && cfgObj.getObject_id() != 0)
				fail("Object found case insensitive but flag was set to case sensitive");

			cfgObj = svr.getObjectByUnqConfId("ADMINISTRATors", tblObj, false);
			if (!(cfgObj != null && cfgObj.getObject_id() != 0))
				fail("Object not found, while searching case insensitive");

		} catch (

		Exception ex) {
			ex.printStackTrace();
			fail("Test failed with exception");
		} finally {
			svr.release();
		}
	}

	@Test
	public void testLink() {
		try (SvReader svr = new SvReader()) {
			if (SvReader.getTypeIdByName("APPLICATION") == 0L)
				return;

			
			DbDataArray arrLinkedApp = svr.getObjectsByLinkedId(20516159L, SvReader.getTypeIdByName("APPLICATION"),
					SvLink.getLinkType("LINK NEW APPLICATION WITH OLD ONE", SvReader.getTypeIdByName("APPLICATION"),
							SvReader.getTypeIdByName("APPLICATION")),
					SvReader.getTypeIdByName("APPLICATION"), false, null, 0, 0);

			if (arrLinkedApp.size() < 1)
				fail("Ammended app not found");

			DbDataArray arrLinkedApp2 = svr.getObjectsByLinkedId(20516159L, SvReader.getTypeIdByName("APPLICATION"),
					SvLink.getLinkType("LINK NEW APPLICATION WITH OLD ONE", SvReader.getTypeIdByName("APPLICATION"),
							SvReader.getTypeIdByName("APPLICATION")),
					SvReader.getTypeIdByName("APPLICATION"), true, null, 0, 0);

			if (arrLinkedApp.size() > 0)
				fail("Ammending app found!!!");

		} catch (SvException ex) {
			if (!ex.getLabelCode().equals("system.error.no_dbt_found")) {
				ex.printStackTrace();
				fail("Test failed with exception");
			}
		}
	}

	@Test
	public void testNotification() {
		SvReader svr = null;
		SvNotification svnf = null;
		SvSecurity svs = null;
		try {
			svr = new SvReader();
			svnf = new SvNotification(svr);
			svs = new SvSecurity();
			svnf.dbSetAutoCommit(false);
			DbDataObject user = svs.getUser("ADMIN");
			svnf.createNotificationPerUser("SYSTEM", "NOTIFICATION", "This is system notification", "ADMINISTRATOR", 2L,
					user, true);
			DbDataArray notificationsFound = svnf.getNotificationPerUser(user);
			if (notificationsFound.size() == 0) {
				fail("Notification not found");
			}
		} catch (SvException e) {
			System.out.println(e.getFormattedMessage());
			e.printStackTrace();
			fail("Failed with exception");
		} finally {
			if (svr != null)
				svr.release();
			if (svnf != null)
				svnf.release();
			if (svs != null)
				svs.release();
		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testLabels() {
		DbDataObject labelText;
		try {
			DbDataArray labels = I18n.getLabels("en_US", "codes");
			if (labels == null || labels.size() < 1)
				fail("Failed getting Labels list");

			labelText = I18n.getLabel("en_US", "codes.master");
			if (labelText == null)
				fail("Failed getting label text");

		} catch (SvException e) {
			e.printStackTrace();
			fail("Failed with exception");

		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testRefDate() {

		SvReader svr = null;
		try {
			Long appType = SvCore.getTypeIdByName("APPPLICATION");
			if (appType == 0)
				return;

			Long farmTypeId = SvCore.getTypeIdByName("FARMER");
			if (farmTypeId == 0)
				return;

			svr = new SvReader();
			DbDataArray asd = svr.getObjectsByParentId(15461L, appType, null, 0, 0);
			if (asd.get(0).getVal("REFERENCE_DATE") != null)
				fail("ref date should be null");
			System.out.println(asd.toJson());
		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				e.printStackTrace();
				fail("Test failed with exception");
			}
		} finally {
			if (svr != null)
				svr.release();
		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testLandClaim_2018() {

		String uniqueCacheId = "TEST-LAND";
		SvWriter svw = null;
		SvReader svr = null;
		try {
			Long appTypeId = SvReader.getTypeIdByName("APPLICATION");
			Long cadTypeId = SvReader.getTypeIdByName("CAD_PARCEL");

			if (appTypeId.equals(0L) || cadTypeId.equals(0L))
				return;

			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);

			DbDataObject app = svr.getObjectById(2088142L, appTypeId, null);
			DateTime submitDate = (DateTime) app.getVal("SUBMIT_DATE");
			DbSearchCriterion search = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, app.getParent_id());
			// DbSearchCriterion search2 = new DbSearchCriterion("CROP_CODE",
			// DbCompareOperand.EQUAL, "LUC.LAND_USE_CODE",
			// true);
			DbSearchCriterion search1 = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, app.getObject_id());
			search1.setNextCritOperand("OR");
			DbSearchCriterion search2 = new DbSearchCriterion("PARENT_ID", DbCompareOperand.ISNULL);
			DbSearchExpression expr1 = new DbSearchExpression().addDbSearchItem(search1).addDbSearchItem(search2);

			// SvRelationCache src = new
			// SvRelationCache(SvCore.getDbtByName("CAD_PARCEL"), search,
			// "CP",null,null,null,submitDate);
			// link between cad_parcel and land_use_plan
			DbDataObject dblCadParcelLUP = SvCore.getLinkType("LINK_LANDUSE_CAD",
					SvReader.getTypeIdByName("LAND_USE_PLAN"), cadTypeId);

			// link between land_use_plan and supp_claim
			DbDataObject dblLUPClaim = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_LANDUSE",
					SvReader.getTypeIdByName("SUPPORT_CLAIM"), SvReader.getTypeIdByName("LAND_USE_PLAN"));

			// link between supp_claim and supp_type
			// DbDataObject dblSuppClaimType =
			// SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_SUPPORT_TYPE",
			// SvReader.getTypeIdByName("SUPPORT_CLAIM"),
			// SvReader.getTypeIdByName("SUPPORT_TYPE"));

			// link between land_use_code and supp_type
			// DbDataObject dblLandUseCodeSuppType =
			// SvCore.getLinkType("LINK_SUPPORT_TYPE_WITH_LAND_USE_CODE",
			// SvReader.getTypeIdByName("SUPPORT_TYPE"),
			// SvReader.getTypeIdByName("LAND_USE_CODE"));

			// SvRelationCache lnkd2 = new
			// SvRelationCache(SvCore.getDbtByName("SUPPORT_TYPE"), null, "ST",
			// LinkType.DBLINK, null, dblSuppClaimType, null);

			SvRelationCache src = new SvRelationCache(SvCore.getDbtByName("CAD_PARCEL"), search,
					"CP");/*
							 * , LinkType.DBLINK_REVERSE, null, dblCadParcelLUP, submitDate);
							 */

			SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("LAND_USE_PLAN"), null, "LUP",
					LinkType.DBLINK_REVERSE, null, dblCadParcelLUP, submitDate);

			SvRelationCache lnkd2 = new SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), search1, "SC",
					LinkType.DBLINK_REVERSE, null, dblLUPClaim, submitDate);

			// SvRelationCache lnkd4 = new
			// SvRelationCache(SvCore.getDbtByName("LAND_USE_CODE"), null,
			// "LUC",
			// LinkType.DBLINK, null, dblLandUseCodeSuppType, null);

			lnkd1.setJoinToParent(DbJoinType.LEFT);
			// lnkd2.setJoinToParent(DbJoinType.LEFT);
			// lnkd3.setJoinToParent(DbJoinType.INNER);
			// lnkd4.setJoinToParent(DbJoinType.INNER);

			src.addCache(lnkd1);
			// src.addCache(lnkd2);
			lnkd1.addCache(lnkd2);
			// lnkd2.addCache(lnkd4);

			SvComplexCache.addRelationCache(uniqueCacheId, src, false);

			DbDataArray values = SvComplexCache.getData(uniqueCacheId, svr);
			if (values.size() != 23)
				fail("Inner join will return other than 23, while proper left should be 23. Array size:"
						+ values.size());
			// Double dcl = values.sum("SPC_DCL");
			values.get(0).setVal("CACHE_TEST_FLAG", 1);
			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (!values.get(0).getVal("CACHE_TEST_FLAG").equals(1))
				fail("no data was cached!!");

			if (values.size() < 1)
				fail("no data returned");

			// System.out.println(values.toSimpleJson());

		} catch (SvException e) {
			// TODO Auto-generated catch block
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {

				e.printStackTrace();
				fail("Exception occured");
			}
		} finally {
			if (svw != null)
				svw.release();
			if (svr != null)
				svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void getAnimalClaimsForAllMeasures() {
		Long appid = 10150020L;
		SvReader svr = null;
		try {
			svr = new SvReader();
			Long appTypeId = SvReader.getTypeIdByName("APPLICATION");
			if (appTypeId.equals(0L))
				return;
			DbDataObject dboApp = svr.getObjectById(appid, SvReader.getTypeIdByName("APPLICATION"), null);
			DbDataArray result = null;

			boolean found = false;
			result = this.getAnimalsClaimsForAllMeasures2017(dboApp, new DateTime(), svr);
			DbDataObject dbo = result.get(0);
			for (SvCharId id : dbo.getMapKeys()) {
				if (id.toString().startsWith("LNK")) {
					found = true;
					break;
				}
			}
			System.out.println(dbo.toJson());

			if (!found)
				fail("link data not found");
			System.out.println(dbo.toJson());
		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fail("Test failed with exception: " + e.getLabelCode());
			}
		} finally {
			if (svr != null)
				svr.release();
		}
		// return SvComplexCache.getData(uniqueCacheId, svr);

	}

	public DbDataArray getAnimalsClaimsForAllMeasures2017(DbDataObject dboApp, DateTime ref_Date, SvReader svr)
			throws SvException {

		Long farmerTypeId = SvReader.getTypeIdByName("FARMER");
		Long animalTypeId = SvReader.getTypeIdByName("ANIMAL");
		Long claimTypeId = SvReader.getTypeIdByName("SUPPORT_CLAIM");
		Long supportTypeId = SvReader.getTypeIdByName("SUPPORT_TYPE");
		Long vyear = 2016L;
		SvParameter svp = null;
		DateTime refAnimalDate = null;

		String linkSuppClaimWithAnimal = "LINK_SUPPORT_CLAIM_WITH_ANIMAL";
		String linkTypeAnimalFarmer = "LINK_ANIMAL_FARMER";
		String linkSupClaimSupType = "LINK_SUPPORT_CLAIM_WITH_SUPPORT_TYPE";

		try {
			DbDataObject dbAppTypeObj = svr.getObjectById(Long.valueOf(dboApp.getVal("APP_TYPE_ID").toString()),
					SvReader.getTypeIdByName("APPLICATION_TYPE"), null);
			if (dbAppTypeObj.getVal("APP_TYPE_ID") != null)
				vyear = Long.valueOf(dbAppTypeObj.getVal("APP_TYPE_ID").toString());
			if (vyear > 2016)
				linkTypeAnimalFarmer = linkTypeAnimalFarmer + "_" + vyear.toString();
			if (dbAppTypeObj.getVal("YEAR") != null && !dbAppTypeObj.getVal("YEAR").toString().equals("2016")) {
				linkTypeAnimalFarmer += "_" + dbAppTypeObj.getVal("YEAR").toString();
			}
		} finally {
			;
		}

		try {
			svp = new SvParameter(svr);
			refAnimalDate = svp.getParamDateTime(dboApp, "param.admControl.2017.animalsAdmImport.ref_date");
		} catch (SvException e) {
			refAnimalDate = new DateTime();
		} finally {
			if (svp != null) {
				svp.release();
			}
		}
		String uniqueCacheId = "CLAIMS-ANIMAL-" + dboApp.getObject_id();

		DbDataArray result = SvComplexCache.getData(uniqueCacheId, svr);
		if (result != null) {
			return result;
		}
		DbSearchCriterion searchClaims = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL,
				dboApp.getObject_id());
		SvRelationCache src = new SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), searchClaims, "SC");

		DbDataObject vlinkClaimAnimal = SvCore.getLinkType(linkSuppClaimWithAnimal, claimTypeId, animalTypeId);
		DbDataObject vlinkClaimSupType = SvCore.getLinkType(linkSupClaimSupType, claimTypeId, supportTypeId);
		DbDataObject vlinkAnimalFarmer = SvCore.getLinkType(linkTypeAnimalFarmer, animalTypeId, farmerTypeId);

		SvRelationCache lnkAnimal = new SvRelationCache(SvCore.getDbtByName("ANIMAL"), null, "ANI", LinkType.DBLINK,
				null, vlinkClaimAnimal, refAnimalDate);

		SvRelationCache lnkSupType = new SvRelationCache(SvCore.getDbtByName("SUPPORT_TYPE"), null, "SUPT",
				LinkType.DBLINK, null, vlinkClaimSupType, null);

		SvRelationCache lnkAnimalFarmer = new SvRelationCache(SvCore.getDbtByName("FARMER"), null, "FRM",
				LinkType.DBLINK, null, vlinkAnimalFarmer, null);

		/*
		 * ArrayList<String> linkStatusList = new ArrayList<>();
		 * linkStatusList.add("VALID"); lnkd2.setLinkStatusList(linkStatusList);
		 */

		lnkAnimal.setJoinToParent(DbJoinType.INNER);
		lnkSupType.setJoinToParent(DbJoinType.INNER);
		lnkAnimalFarmer.setJoinToParent(DbJoinType.INNER);
		lnkAnimalFarmer.setReturnLinkObjects(true);
		lnkAnimal.setReturnLinkObjects(true);
		src.addCache(lnkAnimal);
		src.addCache(lnkSupType);
		lnkAnimal.addCache(lnkAnimalFarmer);

		SvComplexCache.addRelationCache(uniqueCacheId, src, false);

		return SvComplexCache.getData(uniqueCacheId, svr);
	}

	@Test
	public void testDbFilter() {
		SvReader svr = null;
		try {
			svr = new SvReader();
			DbDataArray allTbls = svr.getObjects(null, svCONST.OBJECT_TYPE_TABLE, null, 0, 0);
			IDbFilter filter = new FilterByName();

			DbDataArray sysTbls = allTbls.applyFilter(filter);

			if (sysTbls.size() == 1)
				fail("Filter returned more than one table");

		} catch (Exception e) {
			e.printStackTrace();
			fail("The test failed with exception");
		} finally {
			if (svr != null)
				svr.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void OnSaveCallBackTest() {
		ISvOnSave onSave = new SvTestCallback();
		SvWriter svw = null;

		try {
			svw = new SvWriter();
			svw.setAutoCommit(false);

			SvCore.registerOnSaveCallback(onSave);
			DbDataObject dboFormType = SvarogInstall.getFormType("form_type.test_multi", "1", true, false, true, true);
			// now try to get form field types linked to the form type
			DbDataObject dboFieldType = new DbDataObject();
			dboFieldType.setObject_type(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
			dboFieldType.setVal("FIELD_TYPE", "NVARCHAR");
			dboFieldType.setVal("LABEL_CODE", "form_type.test");
			dboFieldType.setVal("FIELD_SIZE", 10);
			dboFieldType.setVal("IS_NULL", false);
			SvarogInstall.registerFormFieldType(dboFieldType, dboFormType, svw);

			DbDataObject dboFieldType2 = new DbDataObject();
			dboFieldType2.setObject_type(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
			dboFieldType2.setVal("FIELD_TYPE", "NUMERIC");
			dboFieldType2.setVal("LABEL_CODE", "form_type.test_numeric");
			dboFieldType2.setVal("IS_NULL", false);
			SvarogInstall.registerFormFieldType(dboFieldType2, dboFormType, svw);

			DbDataObject dbFormInstance = new DbDataObject(svCONST.OBJECT_TYPE_FORM);
			dbFormInstance.setVal("form_type_id", dboFormType.getObject_id());
			dbFormInstance.setParent_id(svCONST.OBJECT_TYPE_REPO);
			dbFormInstance.setVal("form_type.test", "DUMMY");
			dbFormInstance.setVal("form_type.test_numeric", 100);
			dbFormInstance.setVal("FORM_VALIDATION", true);
			dbFormInstance.setVal("FAIL_VAL", true);
			try {
				svw.saveObject(dbFormInstance);
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals("zokan.custom.exception"))
					throw (ex);
				else
					System.out.println("Proper handling of zokan.custom.exception");
			}
			dbFormInstance.setVal("FAIL_VAL", null);
			svw.saveObject(dbFormInstance);

		} catch (Exception e) {
			if (e instanceof SvException)
				System.out.println(((SvException) e).getFormattedMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception");
		} finally {
			SvCore.unregisterOnSaveCallback(onSave);
			if (svw != null)
				svw.release();
		}
	}

	@Test
	public void testLandClaim() {

		String uniqueCacheId = "TEST-LAND";
		SvWriter svw = null;
		SvReader svr = null;
		try {
			Long appTypeId = SvReader.getTypeIdByName("APPLICATION");
			Long cadTypeId = SvReader.getTypeIdByName("CAD_PARCEL");

			if (appTypeId.equals(0L) || cadTypeId.equals(0L))
				return;

			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);

			DbDataObject app = svr.getObjectById(2088142L, appTypeId, null);
			DateTime submitDate = (DateTime) app.getVal("SUBMIT_DATE");
			DbSearchCriterion search = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, app.getObject_id());
			DbSearchCriterion search2 = new DbSearchCriterion("CROP_CODE", DbCompareOperand.EQUAL, "LUC.LAND_USE_CODE",
					true);
			DbSearchCriterion search1 = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, app.getParent_id());

			SvRelationCache src = new SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), search, "SC");
			// SvRelationCache src = new
			// SvRelationCache(SvCore.getDbtByName("CAD_PARCEL"), search,
			// "CP",null,null,null,submitDate);
			// link between cad_parcel and land_use_plan
			DbDataObject dblCadParcelLUP = SvCore.getLinkType("LINK_LANDUSE_CAD",
					SvReader.getTypeIdByName("LAND_USE_PLAN"), cadTypeId);

			// link between land_use_plan and supp_claim
			DbDataObject dblLUPClaim = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_LANDUSE",
					SvReader.getTypeIdByName("SUPPORT_CLAIM"), SvReader.getTypeIdByName("LAND_USE_PLAN"));

			// link between supp_claim and supp_type
			DbDataObject dblSuppClaimType = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_SUPPORT_TYPE",
					SvReader.getTypeIdByName("SUPPORT_CLAIM"), SvReader.getTypeIdByName("SUPPORT_TYPE"));

			// link between land_use_code and supp_type
			DbDataObject dblLandUseCodeSuppType = SvCore.getLinkType("LINK_SUPPORT_TYPE_WITH_LAND_USE_CODE",
					SvReader.getTypeIdByName("SUPPORT_TYPE"), SvReader.getTypeIdByName("LAND_USE_CODE"));

			SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("LAND_USE_PLAN"), search2, "LUP",
					LinkType.DBLINK, null, dblLUPClaim, submitDate);

			SvRelationCache lnkd2 = new SvRelationCache(SvCore.getDbtByName("SUPPORT_TYPE"), null, "ST",
					LinkType.DBLINK, null, dblSuppClaimType, null);

			SvRelationCache lnkd3 = new SvRelationCache(SvCore.getDbtByName("CAD_PARCEL"), search1, "CP",
					LinkType.DBLINK, null, dblCadParcelLUP, submitDate);

			SvRelationCache lnkd4 = new SvRelationCache(SvCore.getDbtByName("LAND_USE_CODE"), null, "LUC",
					LinkType.DBLINK, null, dblLandUseCodeSuppType, null);

			lnkd1.setJoinToParent(DbJoinType.INNER);
			lnkd2.setJoinToParent(DbJoinType.INNER);
			lnkd3.setJoinToParent(DbJoinType.INNER);
			lnkd4.setJoinToParent(DbJoinType.INNER);

			src.addCache(lnkd1);
			src.addCache(lnkd2);
			lnkd1.addCache(lnkd3);
			lnkd2.addCache(lnkd4);

			SvComplexCache.addRelationCache(uniqueCacheId, src, false);

			DbDataArray values = SvComplexCache.getData(uniqueCacheId, svr);
			// Double dcl = values.sum("SPC_DCL");
			values.get(0).setVal("CACHE_TEST_FLAG", 1);
			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (!values.get(0).getVal("CACHE_TEST_FLAG").equals(1))
				fail("no data was cached!!");

			if (values.size() < 1)
				fail("no data returned");

			// System.out.println(values.toSimpleJson());

		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fail("Exception occured");
			}
		} finally {
			if (svw != null)
				svw.release();
			if (svr != null)
				svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void animalPerf2() {
		SvSecurity svSec = null;

		Long appOID = 20295107L;
		try {
			svSec = new SvSecurity();

			String token = svSec.logon("ADMIN", SvUtil.getMD5("welcome"));
			SvReader svr = new SvReader(token);

			long time1, time2;

			// time1 = System.nanoTime();
			// DbDataArray dbr = conf.getAnimalMeasureDataCached2(appOID,
			// 20200098L, svr);

			// time2 = System.nanoTime();
			// System.out.println("Get animal measures (" + dbr.size() + "):\t"
			// + (double) (time2 - time1) / 1000000000.0);

			time1 = System.nanoTime();
			DbDataArray dbr = this.getAnimalMeasureDataCached(appOID, 20200098L, svr);

			time2 = System.nanoTime();
			if (dbr != null)
				System.out.println(
						"Get animal measures (" + dbr.size() + "):\t" + (double) (time2 - time1) / 1000000000.0);

		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fail("Test raised exception");
			}
		}

	}

	public DbDataArray getAnimalMeasureDataCached(Long appid, Long measureId, SvReader svr) throws SvException {
		DbDataArray ret = null;
		Long appTypeId = SvReader.getTypeIdByName("APPLICATION");
		Long farmerTypeId = SvReader.getTypeIdByName("FARMER");
		Long animalTypeId = SvReader.getTypeIdByName("ANIMAL");
		Long supType = SvReader.getTypeIdByName("SUPPORT_TYPE");

		if (appTypeId.equals(0L) || farmerTypeId.equals(0L) || animalTypeId.equals(0L))
			return null;

		String uniqueCacheId = "ANIMAL-" + appid + "-" + measureId;
		DbDataArray values = null;

		DbDataObject app = svr.getObjectById(appid, appTypeId, null);
		DbDataObject measure = svr.getObjectById(measureId, supType, null);
		values = SvComplexCache.getData(uniqueCacheId, svr);
		if (values == null) {
			DbSearchCriterion search = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, app.getParent_id());
			DbSearchCriterion search1 = new DbSearchCriterion("SUPP_TYPE_ID", DbCompareOperand.EQUAL,
					measure.getObject_id());
			// link between farmer and animal
			// TODO change the link based on year of allication
			String linkType = "LINK_ANIMAL_FARMER";
			ArrayList<String> linkStatus = new ArrayList<String>();
			linkStatus.add("VALID");

			DbDataObject dblAnimalFarmer = SvCore.getLinkType(linkType, SvCore.getTypeIdByName("ANIMAL", null),
					SvCore.getTypeIdByName("FARMER", null));

			// link between land_use_plan and supp_claim
			DbDataObject dblTypeClaimAnimal = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_ANIMAL",
					SvCore.getTypeIdByName("SUPPORT_CLAIM", null), SvCore.getTypeIdByName("ANIMAL", null));

			SvRelationCache src = new SvRelationCache(SvCore.getDbtByName("FARMER"), search, "tbl0");

			SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("ANIMAL"), null, "tbl1",
					LinkType.DBLINK_REVERSE, null, dblAnimalFarmer, null);
			lnkd1.setLinkStatusList(linkStatus);

			SvRelationCache lnkd2 = new SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), search1, "tbl2",
					LinkType.DBLINK_REVERSE, null, dblTypeClaimAnimal, null);
			ArrayList<String> statusList = new ArrayList<String>();
			lnkd2.setJoinToParent(DbJoinType.LEFT);
			statusList.add("VALID");
			lnkd1.setLinkStatusList(statusList);
			src.addCache(lnkd1);
			lnkd1.addCache(lnkd2);

			SvComplexCache.addRelationCache(uniqueCacheId, src, false);
			values = SvComplexCache.getData(uniqueCacheId, svr);
		}

		return values;
	}

	@Test
	public void testAnimalClaim() {
		String uniqueCacheId = "TEST-ANIM";
		SvWriter svr = null;
		try {
			svr = new SvWriter();
			svr.setAutoCommit(false);

			// link between animal and farmer
			DbDataObject dblAnimalFarmer = SvCore.getLinkType("LINK_ANIMAL_FARMER", SvReader.getTypeIdByName("ANIMAL"),
					SvReader.getTypeIdByName("FARMER"));

			// link between animal and supp_claim
			DbDataObject dblAnimalClaim = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_ANIMAL",
					SvReader.getTypeIdByName("SUPPORT_CLAIM"), SvReader.getTypeIdByName("ANIMAL"));

			// link between supp_claim and supp_type
			DbDataObject dblSuppClaimType = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_SUPPORT_TYPE",
					SvReader.getTypeIdByName("SUPPORT_CLAIM"), SvReader.getTypeIdByName("SUPPORT_TYPE"));

			if (dblAnimalFarmer == null || dblAnimalClaim == null || dblSuppClaimType == null)
				return;

			DbSearchCriterion search1 = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, 207808L);
			// SvRelationCache src1 = new
			// SvRelationCache(SvCore.getDbtByName("FARMER"), search1, "FRMT");

			DbSearchCriterion dbs = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.DBLINK, 207808L,
					dblAnimalFarmer.getObject_id());

			SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("ANIMAL"), dbs, "ANIM",
					LinkType.DBLINK_REVERSE, null, dblAnimalFarmer, null);

			SvRelationCache lnkd2 = new SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), null, "SPC",
					LinkType.DBLINK_REVERSE, null, dblAnimalClaim, null);

			SvRelationCache lnkd3 = new SvRelationCache(SvCore.getDbtByName("SUPPORT_TYPE"), null, "SPT",
					LinkType.DBLINK, null, dblSuppClaimType, null);
			// lnkd1.setJoinToParent(DbJoinType.LEFT);
			lnkd2.setJoinToParent(DbJoinType.LEFT);
			lnkd3.setJoinToParent(DbJoinType.LEFT);
			lnkd1.addCache(lnkd2);
			lnkd2.addCache(lnkd3);

			SvComplexCache.addRelationCache(uniqueCacheId, lnkd1, false);

			DbDataArray values = SvComplexCache.getData(uniqueCacheId, svr);
			values.get(0).setVal("CACHE_TEST_FLAG", 1);
			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (!values.get(0).getVal("CACHE_TEST_FLAG").equals(1))
				fail("no data was cached!!");

			if (values.size() < 1)
				fail("no data returned");

		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fail("Exception occured");
			}
		} finally {
			// svw.release();
			svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testDenormalizedFieldJoin() {
		String uniqueCacheId = "TEST-FORM";
		SvWriter svr = null;
		try {
			svr = new SvWriter();
			svr.setAutoCommit(false);

			DbSearchCriterion crit = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, 50L);
			SvRelationCache lnkd2 = new SvRelationCache(SvCore.getDbtByName("FORM"), crit, "SVF", LinkType.CHILD,
					"FORM_TYPE_ID", null, null);

			SvRelationCache lnkd3 = new SvRelationCache(SvCore.getDbtByName("FORM_TYPE"), null, "SVFT",
					LinkType.DENORMALIZED_FULL, "OBJECT_ID", null, null);

			lnkd2.setJoinToParent(DbJoinType.LEFT);
			lnkd3.setJoinToParent(DbJoinType.LEFT);
			lnkd2.addCache(lnkd3);

			SvComplexCache.addRelationCache(uniqueCacheId, lnkd2, false);

			DbDataArray values = SvComplexCache.getData(uniqueCacheId, svr);
			values.get(0).setVal("CACHE_TEST_FLAG", 1);
			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (!values.get(0).getVal("CACHE_TEST_FLAG").equals(1))
				fail("no data was cached!!");

			DbDataObject dboFormType = new DbDataObject();
			dboFormType.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType.setVal("FORM_CATEGORY", "1");
			dboFormType.setVal("MULTI_ENTRY", true);
			dboFormType.setVal("AUTOINSTANCE_SINGLE", false);
			dboFormType.setVal("MANDATORY_BASE_VALUE", true);
			dboFormType.setVal("LABEL_CODE", "form_type.test_multi2");
			svr.saveObject(dboFormType);

			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (values.get(0).getVal("CACHE_TEST_FLAG") == null || !values.get(0).getVal("CACHE_TEST_FLAG").equals(1))
				fail("no data was cached!!");

			if (values.size() < 1)
				fail("no data returned");
			// System.out.println(values.toSimpleJson());

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Exception occured");
		} finally {
			// svw.release();
			svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetLocale() {
		DbDataObject test = I18n.getLocaleId("en_US");

		if (!(test != null && test.getVal("LOCALE_ID").equals("en_US")))
			fail("Didn't load locale!");
	}

	@Test
	public void testNotes() {
		SvNote svn = null;
		try {
			svn = new SvNote();
			svn.setNote(1L, "test Note",
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
			String note = svn.getNote(1L, "Test note");
			if (!note.equals(
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."))
				fail("Note text not equal");
			svn.setNote(1L, "Test note", "Small update");
			note = svn.getNote(1L, "Test note");
			if (!note.equals("Small update"))
				fail("Note updating doesn't work");

		} catch (SvException e) {
			fail("Test failed");
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (svn != null)
				svn.release();
		}

	}

	@Test
	public void testSaveAsUser() {
		SvWriter svn = null;
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			svn = new SvWriter();
			svn.setAutoCommit(false);
			svr = new SvReader(svn);
			svs = new SvSecurity();
			DbDataObject user = svs.getUser("ADMIN");
			svn.setSaveAsUser(user);
			DbDataObject testLabel = new DbDataObject(SvCore.getDbtByName("LABELS").getObject_id());
			testLabel.setVal("LABEL_CODE", "autoSaveTest");
			testLabel.setVal("label_text", "autoSaveTest");
			testLabel.setVal("locale_id", SvConf.getDefaultLocale());
			svn.saveObject(testLabel);

			if (!testLabel.getUser_id().equals(user.getObject_id()))
				fail("Label saved as system instead of Admin");
		} catch (SvException e) {
			e.printStackTrace();
			fail("Test failed");
		} finally {
			if (svn != null)
				svn.release();
			svr.release();
			svs.release();
		}

	}

	@Test
	public void testSaveUserContactData() {
		SvWriter svw = null;
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			svw = new SvWriter();
			svw.setAutoCommit(false);
			svr = new SvReader(svw);
			svs = new SvSecurity();
			DbDataObject user = svs.getUser("ADMIN");
			svw.setSaveAsUser(user);
			DbDataObject testContact = new DbDataObject();
			testContact.setObject_type(svCONST.OBJECT_TYPE_CONTACT_DATA);
			testContact.setParent_id(user.getObject_id());
			testContact.setVal("STREET_TYPE", "Str.");
			testContact.setVal("STREET_NAME", "Kosta N.");
			testContact.setVal("HOUSE_NUMBER", "23");
			testContact.setVal("POSTAL_CODE", "1000");
			testContact.setVal("CITY", "Skopje");
			testContact.setVal("STATE", "R.Macedonia");
			testContact.setVal("PHONE_NUMBER", "");
			testContact.setVal("MOBILE_NUMBER", "+38974566121");
			testContact.setVal("FAX", "+389756451125");
			testContact.setVal("EMAIL", "contact_me@test.com");
			svw.saveObject(testContact);

			if (testContact.getObject_id() == null || testContact.getObject_id() == 0L
					|| !testContact.getParent_id().equals(user.getObject_id()))
				fail("Contact data not properly saved.");
		} catch (SvException e) {
			e.printStackTrace();
			fail("Test failed");
		} finally {
			if (svw != null)
				svw.release();
			if (svr != null)
				svr.release();
			if (svs != null)
				svs.release();
		}

	}

	@Test
	public void TestLeftJoin() {
		SvSecurity svSec = null;
		SvReader svr = null;
		Long appOID = 20295107L;
		try {
			svSec = new SvSecurity();

			String token = svSec.logon("ADMIN", SvUtil.getMD5("welcome"));
			svr = new SvReader(token);
			long time1, time2;

			time1 = System.nanoTime();
			DbDataArray dbr = this.getAnimalMeasureDataCached2(appOID, 20200098L, svr);

			time2 = System.nanoTime();
			if (dbr != null)
				System.out.println(
						"Get animal measures (" + dbr.size() + "):\t" + (double) (time2 - time1) / 1000000000.0);

		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				e.printStackTrace();
				fail("Test raised exception");
			}
		} finally {
			svSec.release();
			svr.release();
		}

	}

	public DbDataArray getAnimalMeasureDataCached2(Long appid, Long measureId, SvReader svr) throws SvException {
		DbDataArray ret = null;
		Long appTypeId = SvReader.getTypeIdByName("APPLICATION");
		Long farmerTypeId = SvReader.getTypeIdByName("FARMER");
		Long animalTypeId = SvReader.getTypeIdByName("ANIMAL");
		Long supType = SvReader.getTypeIdByName("SUPPORT_TYPE");

		if (appTypeId.equals(0L) || farmerTypeId.equals(0L) || animalTypeId.equals(0L))
			return null;

		String uniqueCacheId = "ANIMAL2-" + appid + "-" + measureId;
		DbDataArray values = null;

		DbDataObject app = svr.getObjectById(appid, appTypeId, null);
		DbDataObject measure = svr.getObjectById(measureId, supType, null);
		values = SvComplexCache.getData(uniqueCacheId, svr);
		if (values == null) {
			DbSearchCriterion search = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, app.getParent_id());
			DbSearchCriterion search1 = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, app.getObject_id());
			// link between farmer and animal
			// TODO change the link based on year of allication
			String linkType = "LINK_ANIMAL_FARMER";
			ArrayList<String> linkStatus = new ArrayList<String>();
			linkStatus.add("VALID");

			DbDataObject dblAnimalFarmer = SvCore.getLinkType(linkType, SvCore.getTypeIdByName("ANIMAL", null),
					SvCore.getTypeIdByName("FARMER", null));

			// link between land_use_plan and supp_claim
			DbDataObject dblTypeClaimAnimal = SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_ANIMAL",
					SvCore.getTypeIdByName("SUPPORT_CLAIM", null), SvCore.getTypeIdByName("ANIMAL", null));

			// SvRelationCache src = new
			// SvRelationCache(SvCore.getDbtByName("FARMER"), search, "tbl0");

			DbSearchCriterion crit = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.DBLINK, app.getParent_id(),
					dblAnimalFarmer.getObject_id());

			SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("ANIMAL"), crit, "tbl1", null, null,
					dblAnimalFarmer, null);
			lnkd1.setLinkStatusList(linkStatus);

			SvRelationCache lnkd2 = new SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), search1, "tbl2",
					LinkType.DBLINK_REVERSE, null, dblTypeClaimAnimal, null);
			ArrayList<String> statusList = new ArrayList<String>();
			lnkd1.setJoinToParent(DbJoinType.LEFT);
			lnkd2.setJoinToParent(DbJoinType.LEFT);
			statusList.add("VALID");
			lnkd1.setLinkStatusList(statusList);
			lnkd1.addCache(lnkd2);

			SvComplexCache.addRelationCache(uniqueCacheId, lnkd1, false);
			values = SvComplexCache.getData(uniqueCacheId, svr);
		}
		return values;
	}

	@Test
	public void testDbLinkWhere() {
		// configure the core to clean up every 5 seconds
		SvReader svr = null;
		try {
			svr = new SvReader();

			DbSearchCriterion crit = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.DBLINK_REVERSE, 10780L,
					10762L);
			DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
					SvCore.getDbt(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE),
					SvCore.getFields(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE), crit, null, null);
			DbDataArray objects = svr.getObjects(query, null, null);

			System.out.println(objects.toSimpleJson());
		} catch (SvException e) {
			fail("Test failed");
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (svr != null)
				svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("There are still active connections");

	}

	@Test
	public void testReturnType() throws SvException {

		SvReader svr = null;
		try {

			svr = new SvReader();
			String uniqueCacheId = "YN-DOCS-TEST";
			DbDataArray result = SvComplexCache.getData(uniqueCacheId, svr);
			DbSearchCriterion search = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL,
					svCONST.OBJECT_TYPE_TABLE);
			DbSearchCriterion search1 = new DbSearchCriterion("FORM_CATEGORY", DbCompareOperand.EQUAL, "1");
			SvRelationCache src = new SvRelationCache(SvCore.getDbtByName("SVAROG_FORM"), search, "SF", null,
					"FORM_TYPE_ID", null, null);
			SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("SVAROG_FORM_TYPE"), search1, "SFT",
					LinkType.DENORMALIZED_FULL, "OBJECT_ID", null, null);
			lnkd1.setJoinToParent(DbJoinType.INNER);
			src.addCache(lnkd1);
			SvComplexCache.addRelationCache(uniqueCacheId, src, false);

			DbDataArray asd = SvComplexCache.getData(uniqueCacheId, svr);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Test failed with exception");
		} finally {
			svr.release();
		}

	}

	@Test
	public void testCleanup() {
		// configure the core to clean up every 5 seconds
		SvConf.setCoreIdleTimeoutMilis(1000);
		SvCore.isDebugEnabled = true;
		SvNote svn = null;
		SvReader svr = null;
		try {
			// clean up before the test is executed
			SvConnTracker.cleanup();
			svn = new SvNote();
			svr = new SvReader(svn);

			svn.setNote(1L, "test Note",
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
			String note = svn.getNote(1L, "Test note");
			if (!note.equals(
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."))
				fail("Note text not equal");
			svn.setNote(1L, "Test note", "Small update");
			note = svn.getNote(1L, "Test note");
			DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
					SvCore.getDbt(svCONST.OBJECT_TYPE_TABLE), SvCore.getFields(svCONST.OBJECT_TYPE_TABLE), null, null,
					null);
			DbDataArray objects = svr.getObjects(query, null, null);

			if (!note.equals("Small update"))
				fail("Note updating doesn't work");
			try {
				System.out.println("The test will sleep for 1 seconds!");
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				fail("Thread sleep failed");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			SvConnTracker.cleanup();
			svn = new SvNote();
			svn.setIsLongRunning(true);
			note = svn.getNote(13L, "Test note");
			if (SvConnTracker.cleanup() > 0)
				fail("The tracker killed a long running Core!");

			svn.setIsLongRunning(false);
			try {
				System.out.println("The test will sleep for 1 seconds!");
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				fail("Thread sleep failed");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			note = svn.getNote(15L, "Test note");
		} catch (SvException e) {
			fail("Test failed");
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (svn != null)
				svn.release();
			if (svr != null)
				svr.hardRelease();
		}
		if (SvConnTracker.hasTrackedConnections(true))
			fail("There are still active connections");

	}

	@Test
	public void testEnableHistory() {
		SvReader dbu = null;
		SvWriter svw = null;
		SvWorkflow swf = null;

		try {
			dbu = new SvReader();
			svw = new SvWriter(dbu);
			swf = new SvWorkflow(svw);
			DbDataObject dboFormType = new DbDataObject();

			dboFormType.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType.setVal("FORM_CATEGORY", "1");
			dboFormType.setVal("MULTI_ENTRY", false);
			dboFormType.setVal("MANDATORY_BASE_VALUE", false);
			dboFormType.setVal("AUTOINSTANCE_SINGLE", true);
			dboFormType.setVal("LABEL_CODE", "form_type.test_auto" + new DateTime().toString());

			svw.saveObject(dboFormType, false);
			// sleep for few millis because it will save the object with the
			// same
			// dt insert and will fail
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			swf.moveObject(dboFormType, "SUBMITTED", false);
			// sleep for few millis because it will save the object with the
			// same
			// dt insert and will fail
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			svw.saveObject(dboFormType, false);
			/*
			 * DbSearchCriterion search = new
			 * DbSearchCriterion("OBJECT_ID",DbCompareOperand.EQUAL,166); DbDataArray dbo =
			 * dbu.getObjectsHistory(search,svCONST.OBJECT_TYPE_TABLE, 0, 0);
			 */
			DbSearchCriterion search = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL,
					dboFormType.getObject_id());
			DbDataArray dba = dbu.getObjectsHistory(search, svCONST.OBJECT_TYPE_FORM_TYPE, 0, 0);

			if (dba.getItems().size() < 2)
				fail("Get object history should return at least two object versions");
		} catch (SvException ex) {
			ex.printStackTrace();
			System.out.print(ex.getCause());
			fail(ex.getFormattedMessage());
		} finally {
			dbu.release();
			svw.release();
			swf.release();

			if (SvConnTracker.hasTrackedConnections(false, false))
				fail("You have a connection leak, you dirty animal!");

		}

	}

	@Test
	public void testStatusChange() {
		SvReader svr = null;
		SvWriter svw = null;
		String label_code = "ut.label2test_status_change";

		try {
			svw = new SvWriter();
			svr = new SvReader(svw);
			DbSearchCriterion dbs = new DbSearchCriterion("LABEL_CODE", DbCompareOperand.EQUAL, label_code);
			DbDataArray dba = svr.getObjects(dbs, svCONST.OBJECT_TYPE_LABEL, null, 0, 0);
			DbDataObject dbo = null;
			if (dba.size() < 1) {
				dbo = new DbDataObject(svCONST.OBJECT_TYPE_LABEL);
				dbo.setVal("LABEL_CODE", label_code);
				dbo.setVal("label_text", "test label");
				dbo.setVal("locale_id", SvConf.getDefaultLocale());
				dbo.setParent_id(SvCore.getDefaultLocale().getObject_id());
				svw.saveObject(dbo);
			} else
				dbo = dba.get(0);

			dbo.setStatus("INVALID");
			svw.saveObject(dbo);

			dba = svr.getObjects(dbs, svCONST.OBJECT_TYPE_LABEL, null, 0, 0);
			dbo = dba.get(0);
			if (dbo.getStatus().equals("INVALID")) {
				dbo.setStatus("VALID");
				svw.saveObject(dbo);

			} else
				fail("Can't change status via saveObject");
		} catch (SvException ex) {
			ex.printStackTrace();
			System.out.println(ex.getFormattedMessage());
			fail(ex.getFormattedMessage());
		} finally {
			svw.release();
			svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetByLinkedWithRefDate() {
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			svr = new SvReader();

			DbDataObject dblt = SvCore.getLinkType("USER_DEFAULT_GROUP", svCONST.OBJECT_TYPE_USER,
					svCONST.OBJECT_TYPE_GROUP);
			// svCONST.SID_ADMINISTRATORS);

			svs = new SvSecurity();
			DbDataObject userSv = svs.getUser("ADMIN");
			DbDataArray linkedArr = svr.getObjectsByLinkedId(userSv.getObject_id(), dblt, new DateTime(), 1, 0);

			/*
			 * (svCONST.OBJECT_TYPE_FORM, svCONST.OBJECT_TYPE_TABLE, "LINK_FILE",
			 * svCONST.OBJECT_TYPE_TABLE, true, null, 0, 0, "VALID");
			 * 
			 */
			if (linkedArr.getItems().size() < 1)
				fail("Couldn't find link between admin and administrators group");

		} catch (SvException e) {
			System.out.println("Test failed:" + e.getFormattedMessage());
			e.printStackTrace();
			fail("Couldn't get by linked id with ref date");
		} finally {
			svr.release();
			svs.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testLinkBetweenSameTypeObjects() {
		SvReader svr = null;
		SvWriter svw = null;
		SvLink svl = null;
		DbDataObject dblink = null;
		try {

			svw = new SvWriter();
			svr = new SvReader(svw);
			svl = new SvLink(svr);
			dblink = SvCore.getLinkType("LINK_FILE", svCONST.OBJECT_TYPE_TABLE, svCONST.OBJECT_TYPE_TABLE);
			if (dblink == null) {
				dblink = new DbDataObject();
				dblink.setObject_type(svCONST.OBJECT_TYPE_LINK_TYPE);
				dblink.setVal("link_type", "LINK_FILE");
				dblink.setVal("link_obj_type_1", svCONST.OBJECT_TYPE_TABLE);
				dblink.setVal("link_obj_type_2", svCONST.OBJECT_TYPE_TABLE);
				dblink.setVal("LINK_TYPE_DESCRIPTION", "same type link for test");
				svw.saveObject(dblink, true);
				SvCore.initSvCore(true);
			}

			DbDataArray linkedArr = svr.getObjectsByLinkedId(svCONST.OBJECT_TYPE_FORM, svCONST.OBJECT_TYPE_TABLE,
					"LINK_FILE", svCONST.OBJECT_TYPE_TABLE, true, null, 0, 0, "VALID");

			if (linkedArr != null && linkedArr.getItems().size() > 0) {
				if (!linkedArr.getItems().get(0).getObject_id().equals(svCONST.OBJECT_TYPE_FORM_TYPE)) {
					svw.deleteObject(dblink, false);
					fail("wrong object returned");
				}
				// all ook
			} else {
				svl.linkObjects(svCONST.OBJECT_TYPE_FORM_TYPE, svCONST.OBJECT_TYPE_FORM, dblink.getObject_id(),
						"test forward link between form type and form", true, false);

				linkedArr = svr.getObjectsByLinkedId(svCONST.OBJECT_TYPE_FORM, svCONST.OBJECT_TYPE_TABLE, "LINK_FILE",
						svCONST.OBJECT_TYPE_TABLE, true, null, 0, 0);
				if (linkedArr != null && linkedArr.getItems().size() > 0) {
					if (!linkedArr.getItems().get(0).getObject_id().equals(svCONST.OBJECT_TYPE_FORM_TYPE)) {
						svw.deleteObject(dblink, false);
						fail("Reverse link test failed: wrong object returned");
					} else {
						// now test the forward link
						linkedArr = svr.getObjectsByLinkedId(svCONST.OBJECT_TYPE_FORM_TYPE, svCONST.OBJECT_TYPE_TABLE,
								"LINK_FILE", svCONST.OBJECT_TYPE_TABLE, false, null, 0, 0);
						if (!linkedArr.getItems().get(0).getObject_id().equals(svCONST.OBJECT_TYPE_FORM)) {
							svw.deleteObject(dblink, false);
							fail("Forward link test failed: wrong object returned");
						}

					}

				} else
					fail("Linked objects not returned");
			}
			svw.dbCommit();
		} catch (Exception ex) {
			ex.printStackTrace();
			if (ex instanceof SvException)
				fail(((SvException) ex).getFormattedMessage());
			else
				fail("General exception");
		} finally {
			if (dblink != null && svw != null)
				try {
					svw.deleteObject(dblink, true);
				} catch (SvException e) {
					e.printStackTrace();
					fail("Test cleanup failed. The test left data in the DB");
				}
			svw.release();
			svr.release();
			svl.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testLinkMove() {
		SvReader svr = null;
		SvWorkflow swf = null;
		try {
			svr = new SvReader();
			swf = new SvWorkflow(svr);

			DbDataObject dblt = SvCore.getLinkType("USER_DEFAULT_GROUP", svCONST.OBJECT_TYPE_USER,
					svCONST.OBJECT_TYPE_GROUP);
			DbSearchCriterion dbs1 = new DbSearchCriterion("link_type_id", DbCompareOperand.EQUAL, dblt.getObject_id());
			DbSearchCriterion dbs2 = new DbSearchCriterion("link_obj_id_2", DbCompareOperand.EQUAL,
					svCONST.SID_ADMINISTRATORS);

			DbSearchExpression dbx = new DbSearchExpression().addDbSearchItem(dbs1).addDbSearchItem(dbs2);

			DbDataArray dba = svr.getObjects(dbx, svCONST.OBJECT_TYPE_LINK, null, 0, 0);
			if (dba.getItems().size() < 1)
				fail("Couldn't find link between admin and administrators group");

			DbDataObject dbl = dba.getItems().get(0);

			String oldStatus = dbl.getStatus();
			swf.moveObject(dbl, "INVALID", false);
			swf.moveObject(dbl, oldStatus, false);
		} catch (SvException e) {
			System.out.println("Test failed:" + e.getFormattedMessage());
			e.printStackTrace();
			fail("Couldn't move the link to a different status");
		} finally {
			svr.release();
			swf.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testConnHardRelease() {
		SvReader svr = null;
		SvReader svr2 = null;
		SvWriter svw = null;
		SvLink svl = null;
		try {

			svw = new SvWriter();
			svr = new SvReader(svw);
			svl = new SvLink(svr);
			svr2 = new SvReader(svl);

			Long typeId = SvCore.getTypeIdByName("TABLES");
			DbDataArray array = svr2.getObjects(null, typeId, null, null, null);

		} catch (Exception ex) {
			ex.printStackTrace();
			if (ex instanceof SvException)
				fail(((SvException) ex).getFormattedMessage());
			else
				fail("General exception");
		} finally {
			svl.hardRelease();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testRelationCache() {
		String uniqueCacheId = "TEST-1";
		SvWriter svr = null;
		try {
			svr = new SvWriter();
			svr.setAutoCommit(false);
			DbSearchCriterion search = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, 0L);
			SvRelationCache src = new SvRelationCache(SvCore.getDbtByName("FORM_TYPE"), search, "FRMT");

			DbDataObject dbl = SvCore.getLinkType("FORM_FIELD_LINK", svCONST.OBJECT_TYPE_FORM_TYPE,
					svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);

			SvRelationCache lnkd = new SvRelationCache(SvCore.getDbtByName("FORM_FIELD_TYPE"), null, "FRMFT",
					LinkType.DBLINK, null, dbl, null);
			lnkd.setJoinToParent(DbJoinType.LEFT);
			src.addCache(lnkd);

			SvComplexCache.addRelationCache(uniqueCacheId, src, false);

			DbDataArray values = SvComplexCache.getData(uniqueCacheId, svr);
			values.get(0).setVal("CACHE_TEST_FLAG", 1);
			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (!values.get(0).getVal("CACHE_TEST_FLAG").equals(1))
				fail("no data was cached!!");

			DbDataObject dboFormType = new DbDataObject();
			dboFormType.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType.setVal("FORM_CATEGORY", "1");
			dboFormType.setVal("MULTI_ENTRY", true);
			dboFormType.setVal("AUTOINSTANCE_SINGLE", false);
			dboFormType.setVal("MANDATORY_BASE_VALUE", true);
			dboFormType.setVal("LABEL_CODE", "form_type.test_multi2");
			svr.saveObject(dboFormType);

			values = SvComplexCache.getData(uniqueCacheId, svr);
			if (values.get(0).getVal("CACHE_TEST_FLAG") != null)
				fail("cache was not invalidated by save object");

			if (values.size() < 1)
				fail("no data returned");
			else
				System.out.println(values.toSimpleJson());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Exception occured");
		} finally {
			// svw.release();
			svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testSvActionGetVal() {
		Gson gs = new Gson();
		String strinJson = "{\"com.prtech.svarog.SvActionResult\":{\"result\":\"PASS\",\"resultMessage\":\"0 different parcels found\",\"values\":[{\"VALUE\":0},{\"POINTS\":0}]}}";
		JsonObject jobj = gs.fromJson(strinJson, JsonObject.class);
		SvActionResult svact = new SvActionResult();
		svact.fromJson(jobj);
		if (svact.getVal("POINTS") != null) {
			Long points = (Long) svact.getVal("POINTS");
			if (points != 0L) {
				fail("Points field is NOT ZERO");
			}
		} else
			fail("Points field is NULL");
	}

	/*
	 * @Test public void testAnimalClaim() { String uniqueCacheId = "TEST-ANIM";
	 * SvWriter svr = null; try { svr = new SvWriter(); svr.setAutoCommit(false);
	 * 
	 * // link between animal and farmer DbDataObject dblAnimalFarmer =
	 * SvCore.getLinkType("LINK_ANIMAL_FARMER", SvReader.getTypeIdByName("ANIMAL"),
	 * SvReader.getTypeIdByName("FARMER"));
	 * 
	 * // link between animal and supp_claim DbDataObject dblAnimalClaim =
	 * SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_ANIMAL",
	 * SvReader.getTypeIdByName("SUPPORT_CLAIM"),
	 * SvReader.getTypeIdByName("ANIMAL"));
	 * 
	 * // link between supp_claim and supp_type DbDataObject dblSuppClaimType =
	 * SvCore.getLinkType("LINK_SUPPORT_CLAIM_WITH_SUPPORT_TYPE",
	 * SvReader.getTypeIdByName("SUPPORT_CLAIM"),
	 * SvReader.getTypeIdByName("SUPPORT_TYPE"));
	 * 
	 * DbSearchCriterion search1 = new DbSearchCriterion("OBJECT_ID",
	 * DbCompareOperand.EQUAL, 207808L); // SvRelationCache src1 = new //
	 * SvRelationCache(SvCore.getDbtByName("FARMER"), search1, "FRMT");
	 * 
	 * DbSearchCriterion dbs = new DbSearchCriterion("OBJECT_ID",
	 * DbCompareOperand.DBLINK, 207808L, dblAnimalFarmer.getObject_id());
	 * 
	 * SvRelationCache lnkd1 = new SvRelationCache(SvCore.getDbtByName("ANIMAL"),
	 * dbs, "ANIM", LinkType.DBLINK_REVERSE, null, dblAnimalFarmer, null);
	 * 
	 * SvRelationCache lnkd2 = new
	 * SvRelationCache(SvCore.getDbtByName("SUPPORT_CLAIM"), null, "SPC",
	 * LinkType.DBLINK_REVERSE, null, dblAnimalClaim, null);
	 * 
	 * SvRelationCache lnkd3 = new
	 * SvRelationCache(SvCore.getDbtByName("SUPPORT_TYPE"), null, "SPT",
	 * LinkType.DBLINK, null, dblSuppClaimType, null); //
	 * lnkd1.setJoinToParent(DbJoinType.LEFT);
	 * lnkd2.setJoinToParent(DbJoinType.LEFT);
	 * lnkd3.setJoinToParent(DbJoinType.LEFT); lnkd1.addCache(lnkd2);
	 * lnkd2.addCache(lnkd3);
	 * 
	 * SvComplexCache.addRelationCache(uniqueCacheId, lnkd1, false);
	 * 
	 * DbDataArray values = SvComplexCache.getData(uniqueCacheId, svr);
	 * values.get(0).setVal("CACHE_TEST_FLAG", 1); values =
	 * SvComplexCache.getData(uniqueCacheId, svr); if
	 * (!values.get(0).getVal("CACHE_TEST_FLAG").equals(1)) fail(
	 * "no data was cached!!");
	 * 
	 * DbDataObject dboFormType = new DbDataObject();
	 * dboFormType.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
	 * dboFormType.setVal("FORM_CATEGORY", "1"); dboFormType.setVal("MULTI_ENTRY",
	 * true); dboFormType.setVal("AUTOINSTANCE_SINGLE", false);
	 * dboFormType.setVal("MANDATORY_BASE_VALUE", true);
	 * dboFormType.setVal("LABEL_CODE", "form_type.test_multi2");
	 * svr.saveObject(dboFormType);
	 * 
	 * values = SvComplexCache.getData(uniqueCacheId, svr); if
	 * (values.get(0).getVal("CACHE_TEST_FLAG") == null ||
	 * !values.get(0).getVal("CACHE_TEST_FLAG").equals(1)) fail(
	 * "no data was cached!!");
	 * 
	 * if (values.size() < 1) fail("no data returned"); else
	 * System.out.println(values.toSimpleJson());
	 * 
	 * } catch (SvException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); fail("Exception occured"); } finally { // svw.release();
	 * svr.release(); } if (SvConnTracker.hasTrackedConnections(false,false)) fail(
	 * "You have a connection leak, you dirty animal!"); }
	 */
	@Test
	public void testObjectUnique() {
		SvWriter svw = null;
		SvReader svr = null;
		try {
			svr = new SvReader();
			svw = new SvWriter(svr);
			DbDataObject dbo = svr.getObjectById(svCONST.OBJECT_TYPE_FORM, svCONST.OBJECT_TYPE_TABLE, null);

			DbDataObject duplicateObj = new DbDataObject();
			duplicateObj.setObject_type(dbo.getObject_type());
			Set<Map.Entry<String, Object>> vals = dbo.getValues();
			for (Map.Entry<String, Object> val : vals) {
				duplicateObj.setVal(val.getKey(), val.getValue());
			}
			svw.saveObject(duplicateObj);
			fail("Object was saved although its duplicate");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			if (!e.getLabelCode().equals("system.error.unq_constraint_violated")) {
				System.out.println(e.getFormattedMessage());
				e.printStackTrace();
				fail("Test failed, error is not: system.error.unq_constraint_violated");
			}

		} finally {
			svw.release();
			svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testObjectMoveSave() {
		SvWriter svw = null;
		SvWorkflow swf = null;
		try {
			swf = new SvWorkflow();
			svw = new SvWriter(swf);

			DbDataObject dboFormType = new DbDataObject();
			dboFormType.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType.setVal("FORM_CATEGORY", "1");
			dboFormType.setVal("MULTI_ENTRY", false);
			dboFormType.setVal("MANDATORY_BASE_VALUE", false);
			dboFormType.setVal("AUTOINSTANCE_SINGLE", true);
			dboFormType.setVal("LABEL_CODE", "form_type.test_auto" + new DateTime().toString());

			svw.saveObject(dboFormType, false);
			swf.moveObject(dboFormType, "SUBMITTED", false);
			svw.saveObject(dboFormType, false);
			svw.dbCommit();
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// fail("Test failed:"+e.getFormattedMessage());
		} finally {
			svw.release();
			swf.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	/**
	 * This test is not the best one, it uses cloning of the repo object which is
	 * dumb.
	 */
	/*
	 * @Test public void testCloneObject() {
	 * 
	 * SvReader svr = null; SvWriter svw = null; SvLink svl = null; SvSecurity svs =
	 * null; DbDataObject dblink = null; DbDataArray dboChildren = null;
	 * DbDataObject user = null; try {
	 * 
	 * svw = new SvWriter(); svw.setAutoCommit(false); svr = new SvReader(svw); svl
	 * = new SvLink(svr);
	 * 
	 * registerLinkType(String linkType, Long objType1, Long objType2, String
	 * linkDescription)
	 * 
	 * dboChildren = svr.getObjectsByParentId(svCONST.OBJECT_TYPE_REPO,
	 * svCONST.OBJECT_TYPE_FIELD, null, 0, 0);
	 * 
	 * svs = new SvSecurity(svr); user = svs.getUser("ADMIN"); // link the form_type
	 * table to the repo table try {
	 * svl.linkObjects(dboChildren.getItems().get(0).getObject_id(),
	 * user.getObject_id(), dblink.getObject_id(),
	 * "test forward link between form type and repo", true, true); } catch
	 * (SvException sv) { if
	 * (!sv.getMessage().equals("system.error.unq_constraint_violated")) throw (sv);
	 * 
	 * } } catch (SvException e) { System.out.println("Test failed:" +
	 * e.getFormattedMessage()); e.printStackTrace(); fail("Can't link object"); }
	 * finally { svw.release(); svr.release(); svl.release(); }
	 * 
	 * try { svw = new SvWriter(); svr = new SvReader(svw); svl = new SvLink(svr);
	 * DbDataArray res = svr.getObjects( new DbSearchCriterion("LABEL_CODE",
	 * DbCompareOperand.EQUAL, "form_type.test_multi"),
	 * svCONST.OBJECT_TYPE_FORM_TYPE, null, 0, 0);
	 * 
	 * 
	 * // we will clone the repo table DbDataArray formVals =
	 * svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, , null, null);
	 * if(formVals.size()<1) fail("No object to clone");
	 * 
	 * DbDataObject dbo = formVals.get(0);
	 * 
	 * // in order to test the links cloning we will create a new dummy // link type
	 * between the field and the forms
	 * 
	 * // the repo must have fields, so we aren't checking the result DbDataObject
	 * clonedRepo = svw.cloneObject(dbo, true, true, false);
	 * 
	 * svw.dbCommit(); DbDataArray dboClonedChildren =
	 * svr.getObjectsByParentId(clonedRepo.getObject_id(),
	 * svCONST.OBJECT_TYPE_FIELD, null, 0, 0); if
	 * (dboClonedChildren.getItems().size() != dboChildren.getItems().size())
	 * fail("Number of cloned children is wrong");
	 * 
	 * DbDataObject findTheChildWithLink = null; for (DbDataObject dboc :
	 * dboClonedChildren.getItems()) { if
	 * (dboc.getVal("FIELD_NAME").equals(dboChildren.getItems().get(0).getVal(
	 * "FIELD_NAME"))) findTheChildWithLink = dboc; } if (findTheChildWithLink ==
	 * null) fail("Children not properly cloned");
	 * 
	 * DbDataObject formClone = null; DbDataArray dboLinked =
	 * svr.getObjectsByLinkedId(findTheChildWithLink.getObject_id(), dblink, null,
	 * 0, 0); for (DbDataObject linkedDbo : dboLinked.getItems()) { if
	 * (linkedDbo.getObject_id().equals(user.getObject_id())) formClone = linkedDbo;
	 * } if (formClone == null) fail( "Children links not properly cloned");
	 * 
	 * } catch (Exception e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); fail("Test failed with exception"); } finally { //
	 * remove the new dummy link at the end of the test svw.release();
	 * svr.release(); svl.release(); svs.release();
	 * 
	 * } if (SvConnTracker.hasTrackedConnections(false,false)) fail(
	 * "You have a connection leak, you dirty animal!");
	 * 
	 * }
	 */
	@Test
	public void testSequence() {
		String seqName = "TEST_SEQ";
		SvSequence dbu = null;
		try {
			dbu = new SvSequence();

			Long seqId = dbu.getSeqNextVal(seqName);
			Long seqId2 = dbu.getSeqNextVal(seqName);
			if (seqId >= seqId2)
				fail("Sequence doesn't increment!!!");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			dbu.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testStaticSequence() {
		String seqName = "TEST_SEQ";

		SvReader svr = null;
		SvSecurity svs = null;
		SvConf.serviceClasses.add(this.getClass().getName());
		try {
			svs = new SvSecurity();
			Long seqVal = null;
			try {
				seqVal = SvSequence.getSeqNextVal("TEST_SEQ", svs);
			} catch (SvException se) {
				if (!se.getLabelCode().equals("system.error.core_isnot_service")) {
					se.printStackTrace();
					fail("The test raised an exception!");
				}
			}

			((SvCore) svs).switchUser(svCONST.serviceUser);
			svr = new SvReader(svs);
			svr.switchUser("ADMIN");
			seqVal = SvSequence.getSeqNextVal("TEST_SEQ", svr);
		} catch (SvException e) {
			e.printStackTrace();
			fail("The test raised an exception!");
		} finally {
			if (svs != null)
				svs.release();
			if (svr != null)
				svr.release();

		}

	}

	@Test
	public void testSvReaderInstanceWithoutToken() {
		SvReader dbu = null;
		try {
			dbu = new SvReader(UUID.randomUUID().toString());
			fail("SvReader didn't raise an exception");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		} finally {
			if (dbu != null)
				dbu.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void getObjectsByParentId() {

		SvReader svr = null;
		try {
			svr = new SvReader();

			DbDataArray object = svr.getObjectsByParentId(svCONST.OBJECT_TYPE_TABLE, svCONST.OBJECT_TYPE_FIELD, null,
					500, 0);

			if (object != null && object.getItems().size() > 0) {
				Gson gson = new Gson();

				JsonObject complexJS = object.toJson();
				JsonObject simpleJS = object.toSimpleJson();
				DbDataArray newTestObj = new DbDataArray();
				newTestObj.fromSimpleJson(simpleJS);
				object.fromJson(complexJS);
				System.out.println(complexJS.toString());
				System.out.println(object.toJson().toString());
				if (!complexJS.toString().equals(object.toJson().toString()))
					fail("Deserialization failed");

				// System.out.println("Testing simpple serialization:");
				// System.out.println(simpleJS.toString());
				// System.out.println(newTestObj.toSimpleJson().toString());
				if (!simpleJS.toString().equals(newTestObj.toSimpleJson().toString()))
					fail("Simple Deserialization failed");

			} else
				fail("The test didn't return any objects");
		} catch (Exception e) {
			fail("The test raised an exception");
			e.printStackTrace();
		} finally {
			if (svr != null)
				svr.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testCache() {

		SvReader svr = null;
		Long scalarGroupId = 273916L;
		try {
			Long typeId = SvReader.getTypeIdByName("APPLICATION");
			if (typeId.equals(0L)) {
				System.out.println("Environment does not support application objects");
				return;
			}

			svr = new SvReader();
			for (int i = 0; i < 10; i++) {

				DbDataArray arrScalarClass = svr.getObjectsByParentId(scalarGroupId,
						SvReader.getTypeIdByName("APPLICATION"), null, 0, 0);
				DbCache.addArrayByParentId(arrScalarClass, typeId, 273916L);

				arrScalarClass = DbCache.getObjectsByParentId(273916L, typeId);
				if (arrScalarClass == null)
					fail("could not load from cache");
				System.out.println(scalarGroupId.toString() + "-" + arrScalarClass.toSimpleJson());
			}
		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				e.printStackTrace();
				fail("Test raised an exception");
			}

		} finally {
			if (svr != null)
				svr.release();
		}

	}

	@Test
	public void testGetObjectByType() {

		SvReader svr = null;
		CodeList cl = null;
		try {

			svr = new SvReader();
			Long typeId = SvCore.getTypeIdByName("TABLES");
			DbDataArray array = svr.getObjects(null, typeId, null, null, null);

			DbDataArray arr = new DbDataArray();
			int i = 0;
			for (DbDataObject dbt : array.getItems()) {
				arr.addDataItem(dbt);
				i++;
				if (i > 10)
					break;
			}
			array = arr;

			if (array != null && array.getItems().size() > 0) {
				Gson gson = new Gson();

				JsonObject exParams = new JsonObject();
				exParams.addProperty("dt_dateformat", SvConf.getDefaultDateFormat());
				exParams.addProperty("dt_timeformat", SvConf.getDefaultTimeFormat());
				cl = new CodeList(svr);
				JsonObject jSobj2 = array.getMembersJson().getTabularJson("", array, svr.getRepoDbtFields(),
						svr.getFields(typeId), exParams, cl);

				String retvalString = gson.toJson(jSobj2);
				System.out.println(retvalString);

			} else
				fail("The test didn't return any objects");
		} catch (Exception e) {
			e.printStackTrace();

			fail("The test raised an exception");

		} finally {
			svr.release();
			cl.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testGetCode() {
		CodeList lst = null;
		SvSecurity svs = null;
		try {
			svs = new SvSecurity();
			String token = svs.logon("ADMIN", SvUtil.getMD5("welcome"));
			if (token == null)
				fail("Can't logon!");
			lst = new CodeList(token);
			DbDataArray object = lst.getCodeListBase(0L);

			if (object != null) {
				Gson gson = new GsonBuilder().setPrettyPrinting().create();

				JsonObject jSobj2 = object.toJson();
				String jsonToCompare = gson.toJson(jSobj2);
				System.out.println("Found " + object.size() + " codes");
			} else
				fail("Object not found!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			lst.release();
			svs.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testAttachFile() {
		SvFileStore svfs = null;
		SvReader svr = null;
		SvWriter svw = null;
		DbDataObject dbl = null;
		InputStream fis = null;
		// save it
		try {
			svfs = new SvFileStore();
			svr = new SvReader(svfs);
			svw = new SvWriter(svfs);

			Long linkedObjectTypeId = SvCore.getTypeIdByName("SVAROG_TABLES");
			dbl = SvCore.getLinkType("LINK_FILE", linkedObjectTypeId, svCONST.OBJECT_TYPE_FILE);
			if (dbl == null) {
				dbl = DbInit.createLinkType("LINK_FILE", "test file link", linkedObjectTypeId, svCONST.OBJECT_TYPE_FILE,
						true, svw);

			}

			fis = SvCore.class.getResourceAsStream(SvarogInstall.LOCALE_PATH);
			if (fis == null) {
				String path = "." + SvarogInstall.LOCALE_PATH;
				fis = new FileInputStream(path);
			}
			byte[] fileData = IOUtils.toByteArray(fis);
			DbDataObject fileDescriptor = new DbDataObject();
			fileDescriptor.setObject_type(svCONST.OBJECT_TYPE_FILE);
			fileDescriptor.setVal("file_notes", "TEST NOTE");
			fileDescriptor.setVal("file_size", 12);
			fileDescriptor.setVal("file_date", new DateTime());
			fileDescriptor.setVal("file_name", "TEST NAME" + new DateTime().toString());
			fileDescriptor.setVal("file_type", "ATTACHMENT");
			fileDescriptor.setVal("content_type", "application/json");

			DbDataObject linkedObject = svr.getObjectById(svCONST.OBJECT_TYPE_FIELD, linkedObjectTypeId, null);
			svfs.saveFile(fileDescriptor, linkedObject, fileData, false);

			DbDataArray dba = svfs.getFiles(svCONST.OBJECT_TYPE_FIELD, linkedObjectTypeId, "ATTACHMENT", null);
			if (dba.getItems().size() < 1)
				fail("File not saved to DB");
			DbDataObject fileDbo = dba.getItems().get(0);

			byte[] fdata = svfs.getFileAsByte(fileDbo);

			if (fdata == null)
				fail("File can not be read!!");

			if (!IOUtils.toString(fdata, "UTF-8").equals(IOUtils.toString(fileData, "UTF-8")))
				fail("File data is wrong!!");

		} catch (Exception e) {
			String retMsg = "system.error.err";
			if (e instanceof SvException) {
				retMsg = ((SvException) e).getFormattedMessage();
			}
			e.printStackTrace();
			fail("Exception was raised:" + retMsg);
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			if (svfs != null)
				svfs.release();
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetCodeCategories() {
		CodeList lst = null;
		SvSecurity svs = null;
		try {
			svs = new SvSecurity();
			String token = svs.logon("ADMIN", SvUtil.getMD5("welcome"));
			if (token == null)
				fail("Can't logon!");
			lst = new CodeList(token);
			HashMap<Long, String> list = lst.getCodeListId(0L);
			if (list.size() > 0) {
				System.out.println("Master categories:" + list.toString());
				for (Entry<Long, String> entry : list.entrySet()) {
					;
					System.out.println("Children for category \"" + entry.getValue() + "\" "
							+ lst.getCodeList((Long) entry.getKey(), true).toString());
					break;
				}
			} else
				fail("Object not found!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			lst.release();
			svs.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testAutoInstanceForms() {
		SvSecurity svs = null;
		SvReader svr = null;
		SvWriter svw = null;
		SvLink svl = null;
		ArrayList<Long> formTypes = new ArrayList<Long>();
		// DbDataObject dboFormType = null;
		try {
			svs = new SvSecurity();
			String token = svs.logon("ADMIN", SvUtil.getMD5("welcome"));
			if (token == null)
				fail("Can't logon!");
			svr = new SvReader(token);
			svw = new SvWriter(svr);
			svw.setAutoCommit(true);
			svl = new SvLink(svr);

			DbDataObject dboFormType1 = null;
			DbDataObject dboFormType2 = null;
			// try to load the form config
			// else create our own form type
			DbDataArray res = svr.getObjects(
					new DbSearchCriterion("LABEL_CODE", DbCompareOperand.LIKE, "form_type.test_inst_auto%"),
					svCONST.OBJECT_TYPE_FORM_TYPE, null, 0, 0);

			// if the config exists, load it
			for (DbDataObject dboFormType : res.getItems())
				if (dboFormType.getVal("FORM_CATEGORY").equals("1"))
					dboFormType1 = dboFormType;
				else
					dboFormType2 = dboFormType;

			if (dboFormType1 == null) {

				dboFormType1 = new DbDataObject();
				dboFormType1.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
				dboFormType1.setVal("FORM_CATEGORY", "1");
				dboFormType1.setVal("MULTI_ENTRY", false);
				dboFormType1.setVal("MANDATORY_BASE_VALUE", false);
				dboFormType1.setVal("AUTOINSTANCE_SINGLE", true);
				dboFormType1.setVal("LABEL_CODE", "form_type.test_inst_auto" + new DateTime().toString());
				svw.saveObject(dboFormType1);
				formTypes.add(dboFormType1.getObject_id());
			}
			if (dboFormType2 == null) {
				// else create our own form type
				dboFormType2 = new DbDataObject();
				dboFormType2.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
				dboFormType2.setVal("FORM_CATEGORY", "2");
				dboFormType2.setVal("MULTI_ENTRY", false);
				dboFormType2.setVal("MANDATORY_BASE_VALUE", false);
				dboFormType2.setVal("AUTOINSTANCE_SINGLE", true);
				dboFormType2.setVal("LABEL_CODE", "form_type.test_inst_auto2" + new DateTime().toString());
				svw.saveObject(dboFormType2);
				formTypes.add(dboFormType2.getObject_id());

			}
			SvCore.initSvCore(true);

			SvarogInstall.registerFormFieldType("NVARCHAR", "form_type.auto_inst_test", true, dboFormType1, svw);
			SvarogInstall.registerFormFieldType("NUMERIC", "form_type.auto_inst_test_numeric", true, dboFormType1, svw);

			// reload the config after the test has been set up
			SvCore.initSvCore(true);

			DbDataArray formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType1.getObject_id(), null,
					null);

			if (formVals.getItems().size() > 0) {
				DbDataObject firstFrm = formVals.getItems().get(0);
				if (firstFrm.getVal("LABEL_CODE") == null)
					fail("form is missing the label code");
			}
			formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType1.getObject_id(), null, null);

			if (formVals.getItems().size() > 0)

			{

				// System.out.print(formVals.toJson().toString());
				DbDataObject firstFrm = formVals.getItems().get(0);
				if (firstFrm.getVal("LABEL_CODE") == null)
					fail("form is missing the label code");

				firstFrm.setVal("form_type.auto_inst_test", "NAN");
				svw.saveObject(firstFrm);

				firstFrm.setVal("form_type.auto_inst_test", null);
				svw.saveObject(firstFrm);

				formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType1.getObject_id(), null, null);

				DbDataArray formCacheVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE,
						dboFormType1.getObject_id(), null, null);

				if (formCacheVals.getItems().size() > 0) {
					DbDataObject dbg1 = formCacheVals.getItems().get(0);
					if (dbg1.getVal("form_type.auto_inst_test") != null)
						fail("field was set to null but has value");
					// if (dbg1.getVal("NON_SERIALIZABLE") == null ||
					// !dbg1.getVal("NON_SERIALIZABLE").equals("NAN"))
					// fail("The object was fetched from the DB, since the in
					// memory mod was not found");
				}
			} else

				fail("Can't get auto instance forms");

		} catch (Exception e) {
			if (e instanceof SvException) {
				System.err.println(((SvException) e).getFormattedMessage());
			}
			e.printStackTrace();
			fail("Auto-instance test raised an exception");
		} finally {
			svl.release();
			svw.release();
			svr.release();
			svs.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))

			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testBaseForms() {

		DbDataArray formVals = null;
		SvSecurity svs = null;
		SvReader svr = null;
		SvWriter svw = null;
		SvLink svl = null;
		DbDataObject dboFormType = null;
		try {
			svs = new SvSecurity();
			String token = svs.logon("ADMIN", SvUtil.getMD5("welcome"));
			if (token == null)
				fail("Can't logon!");
			svr = new SvReader(token);
			svw = new SvWriter(svr);
			svl = new SvLink(svr);

			svr.setAutoCommit(false);

			dboFormType = SvarogInstall.getFormType("form_type.test_multi17", "1", true, false, true, true);

			// now try to get form field types linked to the form type
			DbDataObject dboFieldType = new DbDataObject();
			dboFieldType.setObject_type(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
			dboFieldType.setVal("FIELD_TYPE", "NVARCHAR");
			dboFieldType.setVal("LABEL_CODE", "form_type.test17");
			dboFieldType.setVal("FIELD_SIZE", 10);
			dboFieldType.setVal("IS_NULL", false);
			SvarogInstall.registerFormFieldType(dboFieldType, dboFormType, svw);

			DbDataObject dboFieldType2 = new DbDataObject();
			dboFieldType2.setObject_type(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
			dboFieldType2.setVal("FIELD_TYPE", "NUMERIC");
			dboFieldType2.setVal("LABEL_CODE", "form_type.test_numeric17");
			dboFieldType2.setVal("IS_NULL", false);
			SvarogInstall.registerFormFieldType(dboFieldType2, dboFormType, svw);

			// ok, our config is all right, so lets try to same a form
			DbDataObject dboFormTypeToSave = null;
			DbDataArray resultFormType = svr.getObjects(
					new DbSearchCriterion("LABEL_CODE", DbCompareOperand.EQUAL, "form_type.test_multi17"),
					svCONST.OBJECT_TYPE_FORM_TYPE, null, 0, 0);

			if (resultFormType.getItems().size() > 0)
				dboFormTypeToSave = resultFormType.getItems().get(0);
			else
				fail("Can't find form type");

			DbDataObject formInstance = new DbDataObject();
			formInstance.setObject_type(svCONST.OBJECT_TYPE_FORM);
			formInstance.setParent_id(svCONST.OBJECT_TYPE_TABLE);

			formInstance.setVal("FORM_TYPE_ID", dboFormTypeToSave.getObject_id());
			formInstance.setVal("FORM_VALIDATION", true);
			formInstance.setVal("VALUE", 12);
			// add more than 10 chars
			formInstance.setVal("form_type.test17", "TEST VALUE1234567890");
			formInstance.setVal("form_type.test_numeric17", 79);
			formInstance.setVal("form_type.test17_1st", "TEST VALUE");
			formInstance.setVal("form_type.test_numeric17_1st", 79);
			svw.setAutoCommit(true);

			try {
				svw.saveObject(formInstance);
				fail("No exception was raised");
			} catch (SvException sv) {
				if (!sv.getLabelCode().equals("system.error.field_value_too_long"))
					fail("Field validation failed, value exceeds maximum field size:" + sv.getMessage());

			}
			formInstance.setVal("form_type.test17", "TEST VALUE");
			formInstance.setVal("form_type.test_numeric17", null);

			try {
				svw.saveObject(formInstance);

			} catch (SvException sv) {
				if (!sv.getLabelCode().equals("system.error.field_must_have_value")) {
					sv.printStackTrace();
					fail("Form is saved although field test_numeric doesn't have any value");
				}

			}

			formInstance.setVal("form_type.test_numeric17", 79);
			// retry save after we added the mandatory value

			svw.saveObject(formInstance);

			// if the form is saved successfully, then try to load what is saved
			formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType.getObject_id(), null, null);

			DbDataObject dbo = formVals.getItems().get(0);
			Boolean nextIsAdm = false;
			Boolean nextIsOTS = false;
			for (Entry<String, Object> val : dbo.getValues()) {
				String key = val.getKey();
				if (nextIsOTS) {
					if (!key.equalsIgnoreCase("form_type.test17_2nd")) {
						fail("AFTER 1st CHECK, NEXT VALUE IS NOT 2nd CHECK");
					} else {
						nextIsOTS = false;
					}
				}

				if (nextIsAdm) {
					if (!key.equalsIgnoreCase("form_type.test17_1st")) {
						fail("NEXT VALUE IS NOT 1st CHECK");
					} else {
						nextIsAdm = false;
						nextIsOTS = true;
					}
				}
				if (key.equalsIgnoreCase("form_type.test17")) {
					nextIsAdm = true;
				}

			}

			if (formVals != null && formVals.getItems().size() == 1) {
				DbDataObject form1 = formVals.getItems().get(0);
				form1.setVal("form_type.test_numeric17", 999);
				svw.saveObject(form1);
				DbDataArray formVals2 = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType.getObject_id(),
						null, null);
				if (formVals2 != null && formVals2.getItems().size() > 0) {
				}

				System.out.print(formVals.toJson().toString());

				svw.deleteObject(form1);
			} else
				fail("Form count was not 1 after save");

			svw.dbCommit();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Form test failed");
		} finally {
			try {
				formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType.getObject_id(), null, null);

				if (formVals != null) {
					for (DbDataObject form : formVals.getItems())
						svw.deleteObject(form);
					svw.dbCommit();
				}
			} catch (SvException e) {
				// TODO Auto-generated catch block
				fail("Form delete failed: " + e.getFormattedMessage());

			}
			svl.release();
			svr.release();
			svw.release();
			svs.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testSingleEntryMaxCountForms() {

		SvSecurity svs = null;
		SvReader svr = null;
		SvWriter svw = null;
		SvLink svl = null;

		try {
			svs = new SvSecurity();
			String token = svs.logon("ADMIN", SvUtil.getMD5("welcome"));
			if (token == null)
				fail("Can't logon!");
			svr = new SvReader(token);
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			svl = new SvLink(svr);
			DbDataObject dboFormType = null;

			// try to load the form config

			dboFormType = SvarogInstall.getFormType("form_type.testSingle", "1", false, false, true, true);

			DbDataObject dboFieldType = new DbDataObject();
			dboFieldType.setObject_type(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
			dboFieldType.setVal("FIELD_TYPE", "NVARCHAR");
			dboFieldType.setVal("LABEL_CODE", "form_type.test");
			dboFieldType.setVal("IS_NULL", false);
			SvarogInstall.registerFormFieldType(dboFieldType, dboFormType, svw);
			SvCore.initSvCore(true);

			// ok, our config is all right, so lets try to save a form
			DbDataObject dboFormTypeToSave = null;

			DbDataArray resultFormType = svr.getObjects(
					new DbSearchCriterion("LABEL_CODE", DbCompareOperand.EQUAL, "form_type.testSingle"),
					svCONST.OBJECT_TYPE_FORM_TYPE, null, 0, 0);

			if (resultFormType.getItems().size() > 0)
				dboFormTypeToSave = resultFormType.getItems().get(0);
			else
				fail("Can't find form type");

			DbDataArray formExists = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType.getObject_id(), null,
					null);
			DbDataObject formInstance = null;

			if (formExists != null && formExists.getItems().size() < 1) {

				formInstance = new DbDataObject();
				formInstance.setObject_type(svCONST.OBJECT_TYPE_FORM);
				formInstance.setParent_id(svCONST.OBJECT_TYPE_TABLE);

				formInstance.setVal("FORM_TYPE_ID", dboFormTypeToSave.getObject_id());
				formInstance.setVal("FORM_VALIDATION", true);
				formInstance.setVal("VALUE", 12);
				formInstance.setVal("form_type.test", "TEST VALUE");
				formInstance.setVal("form_type.test_1st", "TEST VALUE");

				try {
					svw.saveObject(formInstance);
				} catch (SvException ex) {
					fail("Basic form wasn't saved");
				}
			} else
				formInstance = formExists.getItems().get(0);

			// if the form is saved successfully, then try to load what is saved
			DbDataArray formVals = svr.getFormsByParentId(svCONST.OBJECT_TYPE_TABLE, dboFormType.getObject_id(), null,
					null);
			System.out.print(formVals.toJson().toString());
			formInstance.setVal("form_type.test_1st", "TESTafteru");
			svw.saveObject(formInstance);

			formInstance.setPkid(0L);
			formInstance.setObject_id(0L);

			try {
				svw.saveObject(formInstance);
			} catch (SvException sv) {
				if (!(sv.getLabelCode().equals("system.error.form_type_is_single")
						|| sv.getLabelCode().equals("system.error.form_max_count_exceeded")))
					fail("Form type is single but svarog allowed saving a duplicate instance:"
							+ sv.getFormattedMessage());

			}

			dboFormType.setVal("MULTI_ENTRY", true);
			dboFormType.setVal("MAX_INSTANCES", 1L);
			svw.saveObject(dboFormType);
			svw.dbCommit();
			SvCore.initSvCore(true);

			try {
				svw.saveObject(formInstance);
			} catch (SvException sv) {
				if (!sv.getLabelCode().equals("system.error.form_max_count_exceeded"))
					fail("Form type has max count but svarog allowed saving more instances than allowed!");
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			if (e instanceof SvException)
				fail(((SvException) e).getFormattedMessage());
			fail("Test failed with exception");
		} finally {
			svl.release();
			svr.release();
			svw.release();
			svs.release();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testCustomFreeTextJoin() throws Exception {

		SvReader db = null;
		try {
			SvSecurity svSec = new SvSecurity();
			String token = svSec.logon("ADMIN", SvUtil.getMD5("welcome"));

			db = new SvReader(token);
			db.dbSetAutoCommit(false);

			DbDataObject repoDbt = SvReader.getRepoDbt();

			DbDataObject dbtParamType = db.getObjectById(SvReader.getTypeIdByName("PARAM_TYPE"),
					svCONST.OBJECT_TYPE_TABLE, null);

			DbDataObject dbtParam = db.getObjectById(SvReader.getTypeIdByName("PARAM"), svCONST.OBJECT_TYPE_TABLE,
					null);

			DbDataObject dbtParamValue = db.getObjectById(svCONST.OBJECT_TYPE_PARAM_VALUE, svCONST.OBJECT_TYPE_TABLE,
					null);

			// FIELDS CONFIG FOR EACH OBJECT

			DbDataArray paramTypeRepoFLD = new DbDataArray();
			DbDataArray paramTypeObjFLD = new DbDataArray();

			DbDataArray paramRepoFLD = new DbDataArray();
			DbDataArray paramObjFLD = new DbDataArray();

			DbDataArray paramValueRepoFLD = new DbDataArray();
			DbDataArray paramValueObjFLD = new DbDataArray();

			// FILLING FIELDS FOR EACH OBJECT
			paramTypeRepoFLD = SvReader.getFields(repoDbt.getObject_id());
			paramTypeObjFLD = SvReader.getFields(dbtParamType.getObject_id());

			paramRepoFLD = SvReader.getFields(repoDbt.getObject_id());
			paramObjFLD = SvReader.getFields(dbtParam.getObject_id());

			paramValueRepoFLD = SvReader.getFields(repoDbt.getObject_id());
			paramValueObjFLD = SvReader.getFields(dbtParamValue.getObject_id());

			DbQueryObject dqo_itJob = new DbQueryObject(repoDbt, paramTypeRepoFLD, dbtParamType, paramTypeObjFLD, null,
					DbJoinType.INNER, null, LinkType.CUSTOM_FREETEXT, null);
			dqo_itJob.setCustomFreeTextJoin("	on tbl0.object_id = tbl1.PARAM_TYPE_ID");

			DbQueryObject dqo_itJobTask = new DbQueryObject(repoDbt, paramRepoFLD, dbtParam, paramObjFLD, null,
					DbJoinType.INNER, null, LinkType.CHILD, null);

			DbQueryObject dqo_itTask = new DbQueryObject(repoDbt, paramValueRepoFLD, dbtParamValue, paramValueObjFLD,
					null, null, null, null, null, null);

			DbQueryExpression q = new DbQueryExpression();
			q.addItem(dqo_itJob);
			q.addItem(dqo_itJobTask);
			q.addItem(dqo_itTask);

			DbDataArray ar = db.getObjects(q, 0, 0);
			System.out.println(q.getSQLExpression());

			System.out.println(q.toJson().toString());
		} catch (SvException e) {
			if (e.getLabelCode().equals("system.error.sql_statement_err"))
				fail("Free text join failed");
			else {
				fail("Generic svarog error");
				e.printStackTrace();
			}
		} finally {
			if (db != null)
				db.release();
		}

	}

	@Test
	public void getClaimsTest() {
		SvReader svr = null;

		try {
			Long sclTypeId = SvReader.getTypeIdByName("SUPPORT_CLAIM");
			if (!sclTypeId.equals(0L)) {
				System.out.println("Can't run claim test since the environment doesn't contain support claim object");
				return;
			}
			svr = new SvReader();
			DbDataArray allClaims = svr.getObjectsByParentId(2091383L, sclTypeId, null, 0, 0);

		} catch (SvException e) {
			if (!e.getLabelCode().equals("system.error.no_dbt_found")) {
				e.printStackTrace();
				fail("Unhandled exception");
			}
		} finally {
			if (svr != null)
				svr.release();
		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testUpdateFile() {
		SvFileStore svfs = null;
		SvReader svr = null;
		SvWriter svw = null;
		DbDataObject dbl = null;
		InputStream fis = null;
		InputStream fileToUpdate = null;
		InputStream streamData = null;
		// save it
		try {
			svfs = new SvFileStore();
			svr = new SvReader(svfs);
			svw = new SvWriter(svfs);

			Long linkedObjectTypeId = SvCore.getTypeIdByName("SVAROG_TABLES");
			dbl = SvCore.getLinkType("LINK_FILE", linkedObjectTypeId, svCONST.OBJECT_TYPE_FILE);
			if (dbl == null) {
				dbl = DbInit.createLinkType("LINK_FILE", "test file link", linkedObjectTypeId, svCONST.OBJECT_TYPE_FILE,
						true, svw);

			}

			String codesPath = SvarogInstall.masterCodesPath + "codes.properties";

			fis = DbInit.class.getResourceAsStream("/" + codesPath);
			if (fis == null)
				fis = ClassLoader.getSystemClassLoader().getResourceAsStream(codesPath);

			byte[] fileData = IOUtils.toByteArray(fis);
			DbDataObject fileDescriptor = new DbDataObject();
			fileDescriptor.setObject_type(svCONST.OBJECT_TYPE_FILE);
			fileDescriptor.setVal("file_notes", "TEST NOTE");
			fileDescriptor.setVal("file_size", 12);
			fileDescriptor.setVal("file_date", new DateTime());
			fileDescriptor.setVal("file_name", "TEST NAME" + new DateTime().toString());
			fileDescriptor.setVal("file_type", "ATTACHMENT");
			fileDescriptor.setVal("content_type", "application/json");

			DbDataObject linkedObject = svr.getObjectById(svCONST.OBJECT_TYPE_FIELD, linkedObjectTypeId, null);
			svfs.saveFile(fileDescriptor, linkedObject, fileData, false);

			DbDataArray dba = svfs.getFiles(svCONST.OBJECT_TYPE_FIELD, linkedObjectTypeId, "ATTACHMENT", null);
			if (dba.getItems().size() < 1)
				fail("File not saved to DB");
			DbDataObject fileDbo = dba.getItems().get(0);

			// start update

			{
				fileToUpdate = SvCore.class.getResourceAsStream(SvarogInstall.LOCALE_PATH);
				if (fileToUpdate == null) {
					String path = "." + SvarogInstall.LOCALE_PATH;
					fileToUpdate = new FileInputStream(path);
				}
				byte[] fileDataToUpdate = IOUtils.toByteArray(fileToUpdate);
				svfs.saveFile(fileDbo, null, fileDataToUpdate, false);

				DbDataArray dbaCheckUpdate = svfs.getFiles(svCONST.OBJECT_TYPE_FIELD, linkedObjectTypeId, "ATTACHMENT",
						null);
				if (dba.getItems().size() < 1)
					fail("File not saved to DB");
				DbDataObject fileUpdatedDbo = dbaCheckUpdate.getItems().get(0);

				streamData = svfs.getFileAsStream(fileUpdatedDbo);

				byte[] fdata = svfs.getFileAsByte(fileUpdatedDbo);

				if (fdata == null)
					fail("File can not be read!!");

				if (!Arrays.equals(fdata, fileDataToUpdate))
					fail("File data is wrong!!");

			}

		} catch (Exception e) {
			String retMsg = "system.error.err";
			if (e instanceof SvException) {
				retMsg = ((SvException) e).getFormattedMessage();
			}
			e.printStackTrace();
			fail("Exception was raised:" + retMsg);
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			if (streamData != null)
				try {
					streamData.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			if (fileToUpdate != null)
				try {
					fileToUpdate.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			if (svfs != null)
				svfs.release();
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetObjectByUnqConfId() {
		SvReader svr = null;
		SvWriter svw = null;
		try {
			svr = new SvReader();
			svw = new SvWriter(svr);

			DbDataObject testObj = new DbDataObject();
			testObj.setObject_type(svCONST.OBJECT_TYPE_UI_STRUCTURE_SOURCE);
			testObj.setDt_insert(new DateTime());
			testObj.setDt_delete(SvConf.MAX_DATE);
			testObj.setVal("NAME", "123");
			testObj.setVal("IS_MASTER", true);
			testObj.setVal("NOTE", "test");

			svw.saveObject(testObj, false);

			DbDataObject result = svr.getObjectByUnqConfId("123", "SVAROG_UI_STRUCT_SOURCE");

			if (result == null || (result != null && !result.getVal("NAME").equals(testObj.getVal("NAME")))) {
				fail("getObjectByUnqConfId has not returned the testObject");
			}

		} catch (SvException e) {
			String retMsg = "system.error.err";
			if (e instanceof SvException) {
				retMsg = ((SvException) e).getFormattedMessage();
			}
			e.printStackTrace();
			fail("Exception was raised:" + retMsg);
		} finally {
			if (svr != null) {
				svr.release();
			}
			if (svw != null) {
				svw.release();
			}
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testAggregatedFunction() {

		SvReader svr = null;
		SvWriter svw = null;
		try {
			svr = new SvReader();
			svw = new SvWriter();

			DbDataArray arr = new DbDataArray();

			DbDataObject dboFormType = new DbDataObject();
			dboFormType.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType.setVal("FORM_CATEGORY", "2");
			dboFormType.setVal("MULTI_ENTRY", false);
			dboFormType.setVal("AUTOINSTANCE_SINGLE", false);
			dboFormType.setVal("MANDATORY_BASE_VALUE", true);
			dboFormType.setVal("LABEL_CODE", "form_type.test_1");
			svw.saveObject(dboFormType, false);

			DbDataObject formInstance_1 = new DbDataObject();
			formInstance_1.setObject_type(svCONST.OBJECT_TYPE_FORM);
			formInstance_1.setParent_id(svCONST.OBJECT_TYPE_TABLE);
			formInstance_1.setVal("FORM_TYPE_ID", dboFormType.getObject_id());
			formInstance_1.setVal("FORM_VALIDATION", true);
			formInstance_1.setVal("VALUE", 12);
			// svw.saveObject(formInstance_1, false);

			DbDataObject formInstance_2 = new DbDataObject();
			formInstance_2.setObject_type(svCONST.OBJECT_TYPE_FORM);
			formInstance_2.setParent_id(svCONST.OBJECT_TYPE_TABLE);
			formInstance_2.setVal("FORM_TYPE_ID", dboFormType.getObject_id());
			formInstance_2.setVal("FORM_VALIDATION", true);
			formInstance_2.setVal("VALUE", 100);
			formInstance_2.setVal("FIRST_CHECK", 50);
			// svw.saveObject(formInstance_2, false);

			DbDataObject dboFormType_2 = new DbDataObject();
			dboFormType_2.setObject_type(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType_2.setVal("FORM_CATEGORY", "2");
			dboFormType_2.setVal("MULTI_ENTRY", false);
			dboFormType_2.setVal("AUTOINSTANCE_SINGLE", false);
			dboFormType_2.setVal("MANDATORY_BASE_VALUE", true);
			dboFormType_2.setVal("LABEL_CODE", "form_type.test_2");
			svw.saveObject(dboFormType_2, false);

			DbDataObject formInstance_3 = new DbDataObject();
			formInstance_3.setObject_type(svCONST.OBJECT_TYPE_FORM);
			formInstance_3.setParent_id(svCONST.OBJECT_TYPE_TABLE);
			formInstance_3.setVal("FORM_TYPE_ID", dboFormType_2.getObject_id());
			formInstance_3.setVal("FORM_VALIDATION", false);
			formInstance_3.setVal("VALUE", 100);
			formInstance_3.setVal("FIRST_VALUE", 95);
			formInstance_3.setVal("SECOND_VALUE", 64);
			// svw.saveObject(formInstance_3, false);

			DbDataObject formInstance_4 = new DbDataObject();
			formInstance_4.setObject_type(svCONST.OBJECT_TYPE_FORM);
			formInstance_4.setParent_id(svCONST.OBJECT_TYPE_TABLE);
			formInstance_4.setVal("FORM_TYPE_ID", dboFormType_2.getObject_id());
			formInstance_4.setVal("FORM_VALIDATION", false);
			formInstance_4.setVal("VALUE", 15);
			formInstance_4.setVal("FIRST_VALUE", 10);
			formInstance_4.setVal("SECOND_VALUE", 15);

			arr.addDataItem(formInstance_1);
			arr.addDataItem(formInstance_2);
			arr.addDataItem(formInstance_3);
			arr.addDataItem(formInstance_4);

			System.out.println("Testing SUM");
			HashMap<String, Double> result = arr.sum(null, new String[] { "VALUE" },
					new String[] { "FORM_TYPE_ID", "FORM_VALIDATION" });
			for (Entry<String, Double> entry : result.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " value: " + entry.getValue());
			}
			System.out.println("Testing SUM with horizontal MIN operaton");
			HashMap<String, Double> result6 = arr.sum("MI", new String[] { "VALUE", "FIRST_VALUE", "SECOND_VALUE" },
					new String[] { "FORM_TYPE_ID", "FORM_VALIDATION" });
			for (Entry<String, Double> entry : result6.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " value: " + entry.getValue());
			}
			System.out.println("Testing COUNT");
			HashMap<String, Integer> result1 = arr.count(new String[] { "FORM_TYPE_ID", "FORM_VALIDATION" });
			for (Entry<String, Integer> entry : result1.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " count: " + entry.getValue());
			}

			System.out.println("Testing AVG");
			HashMap<String, Double> result5 = arr.avg(null, new String[] { "VALUE" },
					new String[] { "FORM_TYPE_ID", "FORM_VALIDATION" });
			for (Entry<String, Double> entry : result5.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " count: " + entry.getValue());
			}

			System.out.println("Testing COUNTIF");
			HashMap<String, Integer> result2 = arr.countIf(new String[] { "FORM_VALIDATION" }, new String[] { "true" },
					"AND", new String[] { "FORM_TYPE_ID" });
			for (Entry<String, Integer> entry : result2.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " count: " + entry.getValue());
			}

			System.out.println("Testing LEAST:DbDataArray");
			HashMap<String, Double> result3 = arr.least("VALUE", new String[] { "FORM_TYPE_ID" });
			for (Entry<String, Double> entry : result3.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " count: " + entry.getValue());
			}

			System.out.println("Testing GREATEST:DbDataArray");
			HashMap<String, Double> result4 = arr.greatest("VALUE", new String[] { "FORM_TYPE_ID" });
			for (Entry<String, Double> entry : result4.entrySet()) {
				System.out.println("Key: " + entry.getKey() + " count: " + entry.getValue());
			}

			System.out.println("Testing LEAST: DbDataObject");
			System.out.println(formInstance_2.least(new String[] { "VALUE", "FIRST_CHECK", "SECOND_CHECK" }));

			System.out.println("Testing LEASTwithNullSkip: DbDataObject");
			System.out
					.println(formInstance_2.leastWithNullSkip(new String[] { "VALUE", "FIRST_CHECK", "SECOND_CHECK" }));

			System.out.println("Testing LEASTwithNvl: DbDataObject");
			System.out.println(formInstance_2.leastWithNvl(new String[] { "VALUE", "FIRST_CHECK", "SECOND_CHECK" }));

			System.out.println("Testing GREATEST: DbDataObject");
			System.out.println(formInstance_2.greatest(new String[] { "VALUE", "FIRST_CHECK", "SECOND_CHECK" }));

			System.out.println("Testing GREATESTwithNullSkip: DbDataObject");
			System.out.println(
					formInstance_2.greatestWithNullSkip(new String[] { "VALUE", "FIRST_CHECK", "SECOND_CHECK" }));

			System.out.println("Testing GREATESTwithNvl: DbDataObject");
			System.out.println(formInstance_2.greatestWithNvl(new String[] { "VALUE", "FIRST_CHECK", "SECOND_CHECK" }));

		} catch (Exception e) {
			fail("The test raised an exception");
			e.printStackTrace();
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetDistinctValuesPerColumn() {
		SvReader svr = null;
		try {
			svr = new SvReader();
			DbDataArray fieldsTable = svr.getObjectsByTypeId(svCONST.OBJECT_TYPE_FIELD, null, 0, 0);
			List<String> columnsSpecified = new ArrayList<String>();
			columnsSpecified.add("FIELD_TYPE");
			columnsSpecified.add("IS_NULL");
			columnsSpecified.add("FIELD_SIZE");
			columnsSpecified.add("FIELD_SCALE");
			columnsSpecified.add("SEQUENCE_NAME");
			columnsSpecified.add("UNQ_CONSTRAINT_NAME");
			columnsSpecified.add("STATUS");
			DbDataArray fieldsPerObjectType = svr.getObjectsByParentId(fieldsTable.getItems().get(0).getObject_type(),
					svCONST.OBJECT_TYPE_FIELD, null, 0, 0);

			Map<String, List<Object>> sqlresults = fieldsTable.getDistinctValuesPerColumns(columnsSpecified,
					fieldsPerObjectType);
			Iterator<Entry<String, List<Object>>> it = sqlresults.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, List<Object>> pair = it.next();
				String currColumnName = pair.getKey();
				List<Object> distinctValuesForKeyFound = pair.getValue();
				System.out.println(currColumnName + " " + distinctValuesForKeyFound.toString());
			}
			if (sqlresults.size() == 0) {
				fail("Error in getDistinctValuesPerColumn.");
			}
			DbDataArray allCodes = svr.getObjectsByTypeId(svCONST.OBJECT_TYPE_CODE, null, 0, 0);
			columnsSpecified.add("CODE_TYPE");
			columnsSpecified.add("CODE_VALUE");
			columnsSpecified.add("PARENT_CODE_VALUE");
			// not distinct by pkid or other svarog fields, only by custom
			// fields
			columnsSpecified.add("STATUS");
			fieldsPerObjectType = svr.getObjectsByParentId(allCodes.getItems().get(0).getObject_type(),
					svCONST.OBJECT_TYPE_FIELD, null, 0, 0);
			sqlresults = allCodes.getDistinctValuesPerColumns(columnsSpecified, fieldsPerObjectType);
			it = sqlresults.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, List<Object>> pair = it.next();
				String currColumnName = pair.getKey();
				List<Object> distinctValuesForKeyFound = pair.getValue();
				System.out.println(currColumnName + " " + distinctValuesForKeyFound.toString());
			}
			if (sqlresults.size() == 0) {
				fail("Error in getDistinctValuesPerColumn.");
			}
		} catch (Exception e) {
			fail("The test raised an exception");
			e.printStackTrace();
		} finally {
			if (svr != null)
				svr.release();

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testLabelsCache() {
		DbDataObject labelText;
		SvWriter svw = null;
		try {
			svw = new SvWriter();

			DbDataArray labels = I18n.getLabels("en_US", "codes");
			if (labels == null || labels.size() < 1)
				fail("Failed getting Labels list");

			labels.rebuildIndex("LABEL_CODE", true);
			DbDataObject newLabelExists = labels.getItemByIdx("codes.test1");
			if (newLabelExists != null)
				svw.deleteObject(newLabelExists, true);

			labelText = I18n.getLabel("en_US", "codes.master");
			if (labelText == null)
				fail("Failed getting label text");

			DbDataObject newLabel = new DbDataObject(svCONST.OBJECT_TYPE_LABEL);
			newLabel.setVal("LABEL_CODE", "codes.test1");
			newLabel.setVal("LABEL_TEXT", "Test code master");
			newLabel.setVal("LOCALE_ID", "en_US");
			svw.saveObject(newLabel, true);

			labelText = I18n.getLabel("en_US", "codes.test1");
			if (labelText == null)
				fail("Failed getting label text");
			svw.deleteObject(newLabel, true);

		} catch (SvException e) {
			e.printStackTrace();
			fail("Failed with exception");
		} finally {
			if (svw != null) {
				svw.release();
			}
		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");

	}
}
