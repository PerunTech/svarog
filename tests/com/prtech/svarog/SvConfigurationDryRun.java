package com.prtech.svarog;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvCore;

public class SvConfigurationDryRun implements ISvConfiguration {
	private static final Logger log4j = SvConf.getLogger(SvConfigurationDryRun.class);
	private List<UpdateType> execs = new ArrayList<UpdateType>();

	public List<UpdateType> typesExecuted() {
		return execs;
	}

	@Override
	public int executionOrder(UpdateType updateType) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String beforeSchemaUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.SCHEMA);
		return "Dry-run SCHEMA update executed with " + (svc == null ? " IN valid" : " valid") + " SvCore";
		
	}

	@Override
	public String beforeLabelsUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.LABELS);
		return "Dry-run before LABELS update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeCodesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.CODES);
		return "Dry-run before CODES update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeTypesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.TYPES);
		return "Dry-run before DBT TYPES  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";

	}

	@Override
	public String beforeLinkTypesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.LINKTYPES);
		return "Dry-run before LINK TYPES  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeAclUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.ACL);
		return "Dry-run before ACL  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeSidAclUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.SIDACL);
		return "Dry-run before SID ACL  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String afterUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		execs.add(ISvConfiguration.UpdateType.FINAL);
		return "Dry-run AFTER update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}
}
