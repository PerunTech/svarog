package com.prtech.svarog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvCore;

public class SvConfigurationImpl implements ISvConfiguration {
	private static final Logger log4j = SvConf.getLogger(SvConfigurationImpl.class);

	@Override
	public int executionOrder(UpdateType updateType) {
		// TODO Auto-generated method stub
		return 0;
	}

	String dropCoumnWorkflowUID(Connection conn, ISvCore svc, String schema) throws SvException {
		String columnName = "WORKFLOW_UID";
		String tableName = SvConf.getMasterRepo() + "_WORKFLOW";
		String errorMsg = "";
		String sqlDrop = "ALTER TABLE " + schema + "." + tableName + " DROP COLUMN " + columnName;
		// ALTER TABLE table_name DROP COLUMN column_name;
		if (SvarogInstall.tableColumnExists(columnName, tableName, conn, schema)) {
			PreparedStatement ps = null;
			try {
				ps = conn.prepareStatement(sqlDrop);
				ps.execute();
				errorMsg = "Successfully dropped column:" + columnName + " from table: " + tableName;
			} catch (SQLException e) {
				throw (new SvException("sys.err.drop_column", svc.getInstanceUser(), null, sqlDrop));
			} finally {
				SvCore.closeResource(ps, svc.getInstanceUser());
			}
		}

		return errorMsg;

	}

	@Override
	public String beforeSchemaUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return dropCoumnWorkflowUID(conn, svc, schema);

	}

	@Override
	public String beforeLabelsUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";
	}

	@Override
	public String beforeCodesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";
	}

	@Override
	public String beforeTypesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";

	}

	@Override
	public String beforeLinkTypesUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";
	}

	@Override
	public String beforeAclUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";
	}

	@Override
	public String beforeSidAclUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";
	}

	@Override
	public String afterUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		return "";
	}

}
