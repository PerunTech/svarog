package com.prtech.svarog;

import static org.junit.Assert.fail;

import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataObject;

public class SvWorkflowTests {
	static final Logger log4j = SvConf.getLogger(SvExecManagerTests.class);
	private final String category = "TEST";
	private final String name = "TESTWORKFLOW";

	public void releaseAll(SvCore svc) {
		if (svc != null)
			svc.release();
	}

	DbDataObject constructWorkflowObject(Long objectTypeId, String labelCode, String originStatus,
			String destinationStatus) throws SvException {
		DbDataObject wrfl = new DbDataObject();
		wrfl.setObjectType(svCONST.OBJECT_TYPE_WORKFLOW);
		wrfl.setParentId(objectTypeId);
		wrfl.setVal("WORKFLOW_UID", "TEST123");
		wrfl.setVal("WORKFLOW_TYPE", "INTERNAL");
		wrfl.setVal("WORKFLOW_LABEL_CODE", "label.code." + labelCode);
		wrfl.setVal("OBJECT_SUB_CODE", "null");
		wrfl.setVal("ORIGINATING_STATUS", originStatus);
		wrfl.setVal("DESTINATION_STATUS", destinationStatus);
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
			String token = svsec.logon("ADMIN", SvUtil.getMD5("welcome"));
			svr = new SvReader();
			svw = new SvWriter(svr);
			sww = new SvWorkflow(svw);
			svw.setAutoCommit(false);
			sww.setAutoCommit(false);
			DbDataObject wrfl = constructWorkflowObject(svCONST.OBJECT_TYPE_USER, "test", "VALID", "INVALID");
			svw.saveObject(wrfl, false);
			DbDataObject dboUser = SvReader.getUserBySession(token);
			sww.moveObject(dboUser, "INVALID");
			sww.moveObject(dboUser, "BLOCKED");
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			// releaseAll(svsec);
			releaseAll(svw);
			releaseAll(svr);
		}

	}

}
