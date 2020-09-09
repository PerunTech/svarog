package com.prtech.svarog;

import static org.junit.Assert.*;

import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvarogInstallTest {

	@Test
	public void shouldUpgradeConfig() {
		try (SvReader svr = new SvReader()) {
			DbDataObject dbt = SvCore.getDbt(svCONST.OBJECT_TYPE_FIELD);
			DbDataObject dbo1 = new DbDataObject(dbt.getObjectId());
			DbDataObject dbo2 = new DbDataObject(dbt.getObjectId());
			DbDataArray dboFields = svr.getFields(dbt.getObjectId());
			// set the fields which should not trigger update and see if we need
			// upgrade
			dbo2.setVal(Sv.GUI_METADATA, "SOME");
			dbo2.setVal(Sv.EXTENDED_PARAMS, "SOME");
			if (SvarogInstall.shouldUpgradeConfig(dbo1, dbo2, dboFields))
				fail("shouldUpgrade returned true!");
			// no upgrade needed we are good, now set a field which must trigger
			// upgrade
			dbo2.setVal(dboFields.getItems().get(dboFields.size() - 2).getVal("FIELD_NAME").toString(), "SOME");
			if (!SvarogInstall.shouldUpgradeConfig(dbo1, dbo2, dboFields))
				fail("shouldUpgrade returned false!");

		} catch (Exception ex) {
			ex.printStackTrace();
			fail("Exception was raised");
		}

	}
}
