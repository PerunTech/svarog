package com.prtech.svarog;

import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.hamcrest.core.IsInstanceOf;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.prtech.svarog.SvExecManagerTest.TestExecutor;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvExecutor;

public class SvWorkflowTest {
	static final Logger log4j = SvConf.getLogger(SvExecManagerTest.class);
	private final String category = "TEST";
	private final String name = "TESTWORKFLOW";

	public void releaseAll(SvCore svc) {
		if (svc != null)
			svc.release();
	}

	DbDataObject constructWorkflowObject(Long objectTypeId, String labelCode, String originStatus,
			String destinationStatus, Long ruleId) throws SvException {
		DbDataObject wrfl = new DbDataObject();
		wrfl.setObjectType(svCONST.OBJECT_TYPE_WORKFLOW);
		wrfl.setParentId(objectTypeId);
		wrfl.setVal("WORKFLOW_UID", "TEST123");
		wrfl.setVal("WORKFLOW_TYPE", "INTERNAL");
		wrfl.setVal("WORKFLOW_LABEL_CODE", "label.code." + labelCode);
		wrfl.setVal("OBJECT_SUB_CODE", "null");
		wrfl.setVal("ORIGINATING_STATUS", originStatus);
		wrfl.setVal("DESTINATION_STATUS", destinationStatus);
		wrfl.setVal("CHECKIN_RULE", ruleId);
		return wrfl;
	}

	@Test
	public void testWorkflowModelPerUser() {
		SvReader svr = null;
		SvWriter svw = null;
		SvSecurity svsec = null;
		SvWorkflow sww = null;
		try {
			svsec = new SvSecurity();
			svr = new SvReader();
			svw = new SvWriter(svr);
			sww = new SvWorkflow(svw);
			svw.setAutoCommit(false);
			sww.setAutoCommit(false);
			DbDataObject wrfl = constructWorkflowObject(svCONST.OBJECT_TYPE_USER, "test", "VALID", "INVALID", null);
			svw.saveObject(wrfl, false);
			DbDataObject dboUser = SvReader.getUserBySession(SvarogRolesTest.getUserToken(true));
			sww.moveObject(dboUser, "INVALID");
			sww.moveObject(dboUser, "BLOCKED");
		} catch (SvException ex) {
			ex.printStackTrace();
			// fail(ex.getFormattedMessage());
		} finally {
			// releaseAll(svsec);
			releaseAll(svw);
			releaseAll(svr);
			releaseAll(sww);
			releaseAll(svsec);
		}
		if (SvConnTracker.hasTrackedConnections(true, false))
			fail("You have a connection leak, you dirty animal!");

	}

	@Test
	public void testWorkflowOnObject() {
		SvWriter svw = null;
		SvReader svr = null;
		SvWorkflow sww = null;
		SvSecurity svsec = null;
		try {
			svsec = new SvSecurity();
			svr = new SvReader();
			svr.setAutoCommit(false);
			svw = new SvWriter(svr);
			sww = new SvWorkflow(svr);

			// create rule
			DbDataObject rule = new DbDataObject();
			rule.setObjectType(svCONST.OBJECT_TYPE_RULE);
			rule.setVal("rule_name", "rule.test");
			rule.setVal("rule_label", "rule.test.workflow");
			rule.setVal("is_stoppable", true);
			rule.setVal("is_transactional", true);
			rule.setVal("is_rolling_back", true);
			svw.saveObject(rule, false);

			// create action
			DbDataObject action = new DbDataObject();
			action = new DbDataObject(svCONST.OBJECT_TYPE_ACTION);
			action.setParentId(rule.getObjectId());
			action.setVal("action_name", "test.workflow");
			action.setVal("action_label", "action.test.workflow");
			action.setVal("ACTION_TYPE", "TEST");
			action.setVal("SORT_ORDER", 1L);
			action.setVal("CODE_TYPE", "Executor");
			action.setVal("RETURN_TYPE", "Object");
			action.setVal("METHOD_NAME", "TEST.WORKFLOW");
			svw.saveObject(action, false);

			// create form_type
			DbDataObject dboFormType = new DbDataObject();
			dboFormType.setObjectType(svCONST.OBJECT_TYPE_FORM_TYPE);
			dboFormType.setVal("FORM_CATEGORY", "1");
			dboFormType.setVal("MULTI_ENTRY", false);
			dboFormType.setVal("AUTOINSTANCE_SINGLE", false);
			dboFormType.setVal("MANDATORY_BASE_VALUE", false);
			dboFormType.setVal("LABEL_CODE", "form_type.test.1");
			svw.saveObject(dboFormType, false);

			// create workflow
			DbDataObject wrfl = constructWorkflowObject(svCONST.OBJECT_TYPE_FORM_TYPE, "test", "INVALID", "DELETED",
					rule.getObjectId());
			svw.saveObject(wrfl, false);

			wrfl = constructWorkflowObject(svCONST.OBJECT_TYPE_FORM_TYPE, "test1", "VALID", "INVALID", null);
			svw.saveObject(wrfl, false);

			try {
				sww.moveObject(dboFormType, "PENDING", false);
			} catch (Exception e) {
				if (e instanceof SvException) {
					SvException e1 = (SvException) e;
					if (!e1.getLabelCode().equals("workflow_engine.error.movementNotAllowed in: PENDING")) {
						throw e;
					}
				}
			}
			sww.moveObject(dboFormType, "INVALID", false);

			if (!dboFormType.getStatus().equals("INVALID")) {
				fail("Not changed status");
			}

			sww.moveObject(dboFormType, "DELETED", false);

		} catch (Exception e) {
			if (e instanceof SvException) {
				SvException e1 = (SvException) e;
				if (!e1.getLabelCode().equals("system.err.exec_not_found")) {
					fail("Test raised execption" + e.toString());
				}
			} else {
				e.printStackTrace();
				fail("Test raised execption" + e.toString());
			}
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
			if (sww != null)
				sww.release();
		}
		if (SvConnTracker.hasTrackedConnections(true, false))
			fail("You have a connection leak, you dirty animal!");

	}

	public class TestExecutor implements ISvExecutor {

		private final String category = "TEST";
		private final String name = "WORKFLOW";
		private final String description = "Text executor";
		private final DateTime start = new DateTime("2012-12-31T00:00:00+00");
		private final DateTime end = new DateTime("9999-12-31T00:00:00+00");
		private final Class<?> type = DbDataObject.class;

		@Override
		public Class<?> getReturningType() {
			return type;
		}

		@Override
		public String getCategory() {
			return category;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getDescription() {
			return description;
		}

		@Override
		public DateTime getStartDate() {
			return start;
		}

		@Override
		public DateTime getEndDate() {
			return end;
		}

		@Override
		public long versionUID() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object execute(Map<String, Object> params, ISvCore svCore) throws SvException {
			// TODO Auto-generated method stub
			log4j.info("Executor called:" + (params != null ? params.toString() : "no params"));
			return null;
		}

	}
}
