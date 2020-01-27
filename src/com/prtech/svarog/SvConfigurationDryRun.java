package com.prtech.svarog;

import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvCore;

public class SvConfigurationDryRun implements ISvConfiguration {
	private static final Logger log4j = SvConf.getLogger(SvConfigurationDryRun.class);

	@Override
	public int executionOrder(UpdateType updateType) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String beforeSchemaUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run SCHEMA update executed with " + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeLabelsUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run before LABELS update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeCodesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run before CODES update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeTypesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run before DBT TYPES  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";

	}

	@Override
	public String beforeLinkTypesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run before LINK TYPES  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeAclUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run before ACL  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String beforeSidAclUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run before SID ACL  update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

	@Override
	public String afterUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "Dry-run AFTER update executed" + (svc == null ? " IN valid" : " valid") + " SvCore";
	}

}
