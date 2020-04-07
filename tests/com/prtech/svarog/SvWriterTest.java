package com.prtech.svarog;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

public class SvWriterTest {

	@Test
	public void testImport() {

		SvReader svr = null;
		SvWriter svw = null;
		try {
			String token = SvarogRolesTest.getUserToken(true);

			DbDataObject dboToken = DbCache.getObject(token, svCONST.OBJECT_TYPE_SECURITY_LOG);
			SvConf.setOverrideTimeStamps(false);
			svw = new SvWriter();
			svr = new SvReader(svw);
			svw.isInternal = true;
			DbDataObject dbo = new DbDataObject(dboToken.getObjectType());
			dbo.setObjectId(dboToken.getObjectId());
			dbo.setDtInsert(DateTime.now().minusDays(100));
			dbo.setDtDelete(DateTime.now().minusDays(50));
			dbo.setValuesMap(dboToken.getValuesMap());
			svw.saveObject(dbo, true);
			DbSearch dbs = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, dboToken.getObjectId());
			DbDataArray dba = svr.getObjectsHistory(dbs, dboToken.getObjectType(), 0, 0);
			boolean foundCustomDate = false;
			for (DbDataObject dboGet : dba.getItems()) {
				if (dboGet.getDtDelete().isBefore(DateTime.now().minusDays(49)))
					foundCustomDate = true;
			}
			if (!foundCustomDate || dba.size() < 2)
				fail("coulnd't save and load object with custom dates");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception" + e.getFormattedMessage());
		} finally {
			SvConf.setOverrideTimeStamps(true);

			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
		}

	}

	@Test
	public void testImportNonInternal() {

		SvReader svr = null;
		SvWriter svw = null;
		SvSecurity svs = null;
		try {
			String token = SvarogRolesTest.getUserToken(true);

			SvConf.setOverrideTimeStamps(false);
			svw = new SvWriter(token);
			svr = new SvReader(svw);
			svs = new SvSecurity(svw);

			DbDataObject dboToUpdate = svs.getUser(SvarogRolesTest.testUserName);
			// passsword hash is mandatory field so we have to set it in order
			// to run to test at all
			dboToUpdate.setVal("PASSWORD_HASH", SvUtil.getMD5(SvarogRolesTest.testPassword.toUpperCase()));

			DbDataObject dbo = new DbDataObject(dboToUpdate.getObjectType());
			dbo.setObjectId(dboToUpdate.getObjectId());
			dbo.setDtInsert(DateTime.now().minusDays(600));
			dbo.setDtDelete(DateTime.now().minusDays(550));
			dbo.setValuesMap(dboToUpdate.getValuesMap());
			try {
				svw.saveObject(dbo, true);
				fail("exception was not thrown!!!");
			} catch (SvException e) {
				if (!e.getLabelCode().equals("system.error.obj_not_updateable"))
					fail("test raised wrong exception!");
			}
			// now lets set this class as system class, then set internal and
			// save again
			SvConf.systemClasses.add(this.getClass().getName());
			svw.setIsInternal(true);

			svw.saveObject(dbo, true);
			DbSearch dbs = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, dboToUpdate.getObjectId());
			DbDataArray dba = svr.getObjectsHistory(dbs, dboToUpdate.getObjectType(), 0, 0);
			boolean foundCustomDate = false;
			for (DbDataObject dboGet : dba.getItems()) {
				if (dboGet.getDtDelete().isBefore(DateTime.now().minusDays(49)))
					foundCustomDate = true;
			}
			if (!foundCustomDate || dba.size() < 2)
				fail("coulnd't save and load object with custom dates");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception" + e.getFormattedMessage());
		} finally {
			SvConf.setOverrideTimeStamps(true);

			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
		}

	}
}
