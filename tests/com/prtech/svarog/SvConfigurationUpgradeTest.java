package com.prtech.svarog;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvConfigurationMulti;

public class SvConfigurationUpgradeTest {

	static {
		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr)) {
			DbSearchCriterion sq = new DbSearchCriterion(Sv.CONFIGURATION_CLASS, DbCompareOperand.LIKE,
					SvConfigurationDryRun.class.getName() + "%");

			DbQueryObject q = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_CONFIGURATION_LOG), sq, null, null);
			DbDataArray upgrages = svr.getObjects(q, null, null);
			if (upgrages.size() > 0) {
				svw.deleteObjects(upgrages);
				svw.dbCommit();
			}

		} catch (SvException e) {
			e.printStackTrace();
			fail("Exception was raised");
		}
	}

	void testSingleExec(ISvConfiguration.UpdateType upType, int executionCount) throws Exception {
		SvConfigurationUpgrade.executeConfiguration(upType);

		for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
			SvConfigurationDryRun dr = (SvConfigurationDryRun) svc;
			if (!dr.typesExecuted().contains(upType) || dr.typesExecuted().size() != executionCount)
				fail(upType.toString() + " was not executed under dry run");
		}
	}

	@Test
	public void testSingleExecution() {
		try {
			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			ISvConfiguration svcDryRun = new SvConfigurationDryRun();
			SvConfigurationUpgrade.getiSvCfgs().add(svcDryRun);
			// SvConfigurationUpgrade.iSvCfgs.add(new SvConfigurationDryRunMulti());
			testSingleExec(ISvConfiguration.UpdateType.SCHEMA, 1);
			testSingleExec(ISvConfiguration.UpdateType.CODES, 2);
			testSingleExec(ISvConfiguration.UpdateType.LABELS, 3);
			testSingleExec(ISvConfiguration.UpdateType.TYPES, 4);
			testSingleExec(ISvConfiguration.UpdateType.LINKTYPES, 5);
			testSingleExec(ISvConfiguration.UpdateType.ACL, 6);
			testSingleExec(ISvConfiguration.UpdateType.SIDACL, 7);
			testSingleExec(ISvConfiguration.UpdateType.FINAL, 8);

			// ensure we trigger refresh of the history for the sake of testing
			SvConfigurationUpgrade.upgradeHistory = SvConfigurationUpgrade.getUpgradeHistory();

			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.SCHEMA))
				fail("The configuration was not marked as executed");
			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.CODES))
				fail("The configuration was not marked as executed");
			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.LABELS))
				fail("The configuration was not marked as executed");
			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.TYPES))
				fail("The configuration was not marked as executed");
			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.LINKTYPES))
				fail("The configuration was not marked as executed");
			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.ACL))
				fail("The configuration was not marked as executed");
			if (SvConfigurationUpgrade.shouldExecute(svcDryRun, ISvConfiguration.UpdateType.SIDACL))
				fail("The configuration was not marked as executed");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("testSingleExecution - exception was raised");
		}
	}

	@Test
	public void testExecutionSwitch() {
		try {
			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			// SvConfigurationUpgrade.iSvCfgs.add(new SvConfigurationDryRun());
			List<ISvConfiguration.UpdateType> up = Arrays.asList(ISvConfiguration.UpdateType.ACL);
			SvConfigurationUpgrade.getiSvCfgs().add(new SvConfigurationDryRunMulti(1, up));

			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.ACL);

			for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
				if (svc instanceof ISvConfigurationMulti) {
					SvConfigurationDryRunMulti dr = (SvConfigurationDryRunMulti) svc;
					if (!dr.typesExecuted().contains(ISvConfiguration.UpdateType.ACL))
						fail("Schema was not executed under dry run");
				}
			}
			SvConfigurationUpgrade.upgradeHistory = SvConfigurationUpgrade.getUpgradeHistory();

			DbDataObject executionLog = SvConfigurationUpgrade.upgradeHistory.getItemByIdx(
					SvConfigurationDryRunMulti.class.getName() + "-" + ISvConfiguration.UpdateType.ACL.toString());
			if (executionLog == null)
				fail("Schema registered in the log!");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("testSingleExecution - exception was raised");
		}
	}

	@Test
	public void testMultiTypesExecution() {
		try {
			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			// SvConfigurationUpgrade.iSvCfgs.add(new SvConfigurationDryRun());
			List<ISvConfiguration.UpdateType> up = Arrays.asList(ISvConfiguration.UpdateType.SCHEMA);
			SvConfigurationUpgrade.getiSvCfgs().add(new SvConfigurationDryRunMulti(1, up));

			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.SCHEMA);

			for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
				if (svc instanceof ISvConfigurationMulti) {
					SvConfigurationDryRunMulti dr = (SvConfigurationDryRunMulti) svc;
					if (!dr.typesExecuted().contains(ISvConfiguration.UpdateType.SCHEMA))
						fail("Schema was not executed under dry run");
				}
			}
			SvConfigurationUpgrade.upgradeHistory = SvConfigurationUpgrade.getUpgradeHistory();

			DbDataObject executionLog = SvConfigurationUpgrade.upgradeHistory.getItemByIdx(
					SvConfigurationDryRunMulti.class.getName() + "-" + ISvConfiguration.UpdateType.SCHEMA.toString());
			if (executionLog != null)
				fail("Schema registered in the log!");

			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			// SvConfigurationUpgrade.iSvCfgs.add(new SvConfigurationDryRun());
			up = Arrays.asList(ISvConfiguration.UpdateType.TYPES);
			SvConfigurationUpgrade.getiSvCfgs().add(new SvConfigurationDryRunMulti(1, up));

			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.TYPES);

			for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
				if (svc instanceof ISvConfigurationMulti) {
					SvConfigurationDryRunMulti dr = (SvConfigurationDryRunMulti) svc;
					if (!dr.typesExecuted().contains(ISvConfiguration.UpdateType.TYPES))
						fail("Schema was not executed under dry run");
				}
			}
			SvConfigurationUpgrade.upgradeHistory = SvConfigurationUpgrade.getUpgradeHistory();

			executionLog = SvConfigurationUpgrade.upgradeHistory.getItemByIdx(
					SvConfigurationDryRunMulti.class.getName() + "-" + ISvConfiguration.UpdateType.TYPES.toString());
			if (executionLog == null || (Long) executionLog.getVal(Sv.VERSION) != 1L)
				fail("Types was not executed properly");
			// also check if the schema from the first step is available now after flushing
			executionLog = SvConfigurationUpgrade.upgradeHistory.getItemByIdx(
					SvConfigurationDryRunMulti.class.getName() + "-" + ISvConfiguration.UpdateType.SCHEMA.toString());
			if (executionLog == null)
				fail("Schema didn't show in the log even after flush!");

			// ensure we trigger refresh of the history for the sake of testing

			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			SvConfigurationUpgrade.getiSvCfgs().add(new SvConfigurationDryRunMulti(2, up));
			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.TYPES);

			for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
				if (svc instanceof ISvConfigurationMulti) {
					SvConfigurationDryRunMulti dr = (SvConfigurationDryRunMulti) svc;
					if (!dr.typesExecuted().contains(ISvConfiguration.UpdateType.TYPES))
						fail("Schema was not executed under dry run");
				}
			}

			SvConfigurationUpgrade.upgradeHistory = SvConfigurationUpgrade.getUpgradeHistory();

			executionLog = SvConfigurationUpgrade.upgradeHistory.getItemByIdx(
					SvConfigurationDryRunMulti.class.getName() + "-" + ISvConfiguration.UpdateType.TYPES.toString());
			if (executionLog == null || (Long) executionLog.getVal(Sv.VERSION) != 2L)
				fail("Types was not executed properly");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testMultiWrongUpdateExecution() {
		try {
			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			// SvConfigurationUpgrade.iSvCfgs.add(new SvConfigurationDryRun());
			List<ISvConfiguration.UpdateType> up = Arrays.asList(ISvConfiguration.UpdateType.FINAL);
			SvConfigurationUpgrade.getiSvCfgs().add(new SvConfigurationDryRunMulti(1, up));

			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.SCHEMA);

			for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
				if (svc instanceof ISvConfigurationMulti) {
					SvConfigurationDryRunMulti dr = (SvConfigurationDryRunMulti) svc;
					if (dr.typesExecuted().contains(ISvConfiguration.UpdateType.SCHEMA))
						fail("Schema was wrongly executed under dry run");
				}
			}
			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.FINAL);

			for (ISvConfiguration svc : SvConfigurationUpgrade.getiSvCfgs()) {
				if (svc instanceof ISvConfigurationMulti) {
					SvConfigurationDryRunMulti dr = (SvConfigurationDryRunMulti) svc;
					if (!dr.typesExecuted().contains(ISvConfiguration.UpdateType.FINAL))
						fail("ACL was not executed under dry run");
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	
	@Test
	public void testErrorHandling() {
		try {
			SvConfigurationUpgrade.setiSvCfgs(new ArrayList<ISvConfiguration>());
			// SvConfigurationUpgrade.iSvCfgs.add(new SvConfigurationDryRun());
			List<ISvConfiguration.UpdateType> up = Arrays.asList(ISvConfiguration.UpdateType.CODES);
			SvConfigurationUpgrade.getiSvCfgs().add(new SvConfigurationDryRunMulti(1, up));

			SvConfigurationUpgrade.executeConfiguration(ISvConfiguration.UpdateType.CODES);

			SvConfigurationUpgrade.upgradeHistory = SvConfigurationUpgrade.getUpgradeHistory();

			DbDataObject executionLog = SvConfigurationUpgrade.upgradeHistory.getItemByIdx(
					SvConfigurationDryRunMulti.class.getName() + "-" + ISvConfiguration.UpdateType.CODES.toString());
			if (executionLog == null || (Boolean) executionLog.getVal(Sv.IS_SUCCESSFUL))
				fail("Types was not executed properly");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
