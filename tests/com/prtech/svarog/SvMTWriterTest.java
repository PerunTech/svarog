package com.prtech.svarog;

import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvMTWriterTest {

	@BeforeClass
	public static void deleteRecs() throws SQLException {
		String sql = "DELETE FROM " + SvConf.getDefaultSchema() + "." + SvConf.getMasterRepo()
				+ "_SYS_PARAMS WHERE param_name like 'MTST%'";
		try (SvReader svr = new SvReader(); Statement st = svr.dbGetConn().createStatement()) {
			st.execute(sql);
			svr.dbCommit();
		} catch (SvException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void MTTestSingleThread() {
		SvCore.isDebugEnabled = true;

		SvMTWriter mt = null;
		try (SvWriter sv = new SvWriter();
				SvWriter sv1 = new SvWriter();
				SvWriter sv2 = new SvWriter();
				SvReader svr = new SvReader();) {
			DbDataArray ab = SvarogInstall.getLocaleList();
			DbDataArray a = new DbDataArray();
			for (DbDataObject dbo : ab.getItems()) {
				DbDataObject dbParamValue = new DbDataObject(svCONST.OBJECT_TYPE_SYS_PARAMS);
				dbParamValue.setVal(Sv.PARAM_NAME, "MTST" + dbo.getVal("LOCALE_ID").toString());
				dbParamValue.setVal(Sv.PARAM_VALUE, dbo.getVal("COUNTRY").toString());
				dbParamValue.setVal(Sv.PARAM_TYPE, String.class);
				dbParamValue.setParentId(50L);
				a.addDataItem(dbParamValue);
			}

			ArrayList<SvWriter> svs = new ArrayList<SvWriter>();
			svs.add(sv);
			svs.add(sv1);
			svs.add(sv2);
			mt = new SvMTWriter(svs);
			mt.start();
			mt.saveObject(a, true);
			mt.commit();

			// test if the result is good
			DbDataArray dba = svr.getObjectsByParentId(50L, svCONST.OBJECT_TYPE_SYS_PARAMS, null, null, null);
			if (dba.size() != 112 && !dba.get(0).getVal("LOCALE_ID").toString().startsWith("MTST")) {
				fail("Multithreaded save failed");
			}

		} catch (SvException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("test failed because of exception");
		} finally {
			try {
				mt.rollback();
			} catch (SvException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				mt.shutdown();
			} catch (SvException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (SvConnTracker.hasTrackedConnections(true, false))
			fail("You have a connection leak, you dirty animal!");
	}
}
