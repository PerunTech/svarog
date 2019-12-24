package com.prtech.svarog;

import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_interfaces.ISvConfiguration;

public class SvConfigurationDryRun implements ISvConfiguration {
	private static final Logger log4j = SvConf.getLogger(SvConfigurationDryRun.class);

	@Override
	public int executionOrder(UpdateType updateType) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String beforeSchemaUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run SCHEMA update executed";
	}

	@Override
	public String beforeLabelsUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run before LABELS update executed";
	}

	@Override
	public String beforeCodesUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run before CODES update executed";
	}

	@Override
	public String beforeTypesUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run before DBT TYPES  update executed";

	}

	@Override
	public String beforeLinkTypesUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run before LINK TYPES  update executed";
	}

	@Override
	public String beforeAclUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run before ACL  update executed";
	}

	@Override
	public String beforeSidAclUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run before SID ACL  update executed";
	}

	@Override
	public String afterUpdate(Connection conn, String schema) throws Exception {
		return "Dry-run AFTER update executed";
	}

}
