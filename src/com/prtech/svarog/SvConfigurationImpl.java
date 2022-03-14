package com.prtech.svarog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog.SvConf.SvDbType;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbDataTable;
import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvConfigurationMulti;
import com.prtech.svarog_interfaces.ISvCore;

public class SvConfigurationImpl implements ISvConfigurationMulti {
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

		DbDataObject user = svc != null ? svc.getInstanceUser() : svCONST.systemUser;
		// ALTER TABLE table_name DROP COLUMN column_name;
		if (SvarogInstall.tableColumnExists(columnName, tableName, conn, schema)) {
			PreparedStatement ps = null;
			try {
				HashMap<String, String> params = new HashMap<String, String>();
				params.put("DB_TYPE", SvConf.getDbType().toString());
				params.put("DB_USER", SvConf.getUserName());
				params.put("VIEW_NAME", "V" + tableName);
				params.put("VIEW_SCHEMA", SvConf.getDefaultSchema());

				if (SvConf.getDbType().equals(SvDbType.POSTGRES)
						&& SvarogInstall.dbObjectExists("V" + tableName, conn)) {
					SvarogInstall.executeDbScript("drop_view.sql", params, conn);
				}

				ps = conn.prepareStatement(sqlDrop);
				ps.execute();
				errorMsg = "Successfully dropped column:" + columnName + " from table: " + tableName;
			} catch (SQLException e) {
				throw (new SvException("sys.err.drop_column", user, null, sqlDrop, e));
			} finally {
				SvCore.closeResource(ps, user);
			}
		}

		return errorMsg;

	}

	void deleteRedundantDbt(ISvCore svc, Long objectId) {
		if (svc != null)
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
		msg += "; ";
		msg += dropCoumn(conn, svc, schema, SvConf.getMasterRepo() + "_EXEC_PACK", "NAME");
		msg += dropCoumn(conn, svc, schema, SvConf.getMasterRepo() + "_EXEC_PACK", "PACK_LEVEL");
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
		String errorMsg = "";
		String sqlDrop = "delete from " + schema + "." + SvConf.getMasterRepo()
				+ "_sys_params where param_type ='java.lang.Integer' and (param_name  like 'SDI_MIN_POINT_DISTANCE' or param_name  like 'SDI_VERTEX_ALIGN_TOLERANCE') ";
		DbDataObject user = svc != null ? svc.getInstanceUser() : svCONST.systemUser;
		// ALTER TABLE table_name DROP COLUMN column_name;
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sqlDrop);
			ps.execute();
			errorMsg = "Successfully executed " + sqlDrop;
			conn.commit();
		} catch (SQLException e) {
			throw (new SvException("sys.err.bad_sql", user, null, sqlDrop, e));
		} finally {
			SvCore.closeResource(ps, user);
		}

		return errorMsg;

	}

	@Override
	public int getVersion(int currentVersion) {
		// version 1 of the switch to multi configuration
		return 3;
	}

	@Override
	public List<UpdateType> getUpdateTypes() {
		List<UpdateType> types = new ArrayList<UpdateType>();
		types.add(UpdateType.SCHEMA);
		types.add(UpdateType.FINAL);
		return types;
	}

}
