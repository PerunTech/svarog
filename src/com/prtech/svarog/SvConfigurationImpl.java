package com.prtech.svarog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvCore;

public class SvConfigurationImpl implements ISvConfiguration {
	private static final Logger log4j = SvConf.getLogger(SvConfigurationImpl.class);

	@Override
	public int executionOrder(UpdateType updateType) {
		// TODO Auto-generated method stub
		return 0;
	}

	String dropCoumn(Connection conn, ISvCore svc, String schema, String tableName, String columnName)
			throws SvException {
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

	void deleteRedundantDbt(ISvCore svc, Long objectId) {
		try (SvWriter svr = new SvWriter((SvCore) svc);) {
			DbDataObject dbo104 = SvCore.getDbt(objectId);
			svr.deleteObject(dbo104, true);
		} catch (SvException ex) {
			if (!ex.getLabelCode().equals("system.error.no_dbt_found")) {
				log4j.error("Error deleting reduntand dbt with id:" + objectId, ex);
			}
		}

	}

	@Override
	public String beforeSchemaUpdate(Connection conn, ISvCore svc, String schema) throws Exception {
		String msg = dropCoumn(conn, svc, schema, SvConf.getMasterRepo() + "_WORKFLOW", "WORKFLOW_UID");
		msg += "; ";
		msg += dropCoumn(conn, svc, schema, SvConf.getMasterRepo() + "_WORKFLOW", "OBJECT_SUB_CODE");
		deleteRedundantDbt(svc, svCONST.OBJECT_TYPE_RESERVED_DONTUSE1);
		deleteRedundantDbt(svc, svCONST.OBJECT_TYPE_RESERVED_DONTUSE2);
		return msg;
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
