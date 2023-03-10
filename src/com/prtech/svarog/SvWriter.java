/*******************************************************************************
 *   Copyright (c) 2013, 2019 Perun Technologii DOOEL Skopje.
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Apache License
 *   Version 2.0 or the Svarog License Agreement (the "License");
 *   You may not use this file except in compliance with the License. 
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See LICENSE file in the project root for the specific language governing 
 *   permissions and limitations under the License.
 *  
 *******************************************************************************/
package com.prtech.svarog;

import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.prtech.svarog.SvConf.SvDbType;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_interfaces.ISvDatabaseIO;
import com.prtech.svarog_common.ISvOnSave;
import com.prtech.svarog_common.DbDataField.DbFieldType;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

/**
 * General Writer class of the Svarog framework. The SvWriter is responsible for
 * persisting DbDataObject and DbDataArray instances in the target database.
 * According to the information existing in the DbDataObject, like the object
 * type, the writer should decide in which underlying table the meta-data should
 * be saved. If proper configuration exists the SvWriter shall successfully
 * persist DbDataObjects which can be retrieved in identical maner by the
 * SvReader.
 * 
 * @author ristepejov
 *
 */
public class SvWriter extends SvCore {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvWriter.class);

	/**
	 * Constructor to create a SvWriter object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvWriter(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvWriter object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvWriter(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public SvWriter(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvWriter() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Internal writer instance to get connection for performing constraints checks
	 * out of the standard transaction
	 */
	SvReader internalReader = null;
	/**
	 * Variable to hold the old transaction level of the connection] so we can reset
	 * it before releasing it
	 */
	int oldTrxIsolation = 0;

	/**
	 * Map to store pre-built queries
	 */
	static HashMap<Long, String> queryCache = new HashMap<Long, String>();
	/**
	 * Map to store pre-built queries
	 */
	static HashMap<Long, String> querySDICache = new HashMap<Long, String>();

	/**
	 * Constant for the keyword update
	 */
	static final String UPDATE = "UPDATE";

	/**
	 * String array holding the svarog standard keys PKID and OBJECT_ID
	 */
	private static String[] genKeyIds = new String[] {
			SvConf.getDbType().equals(SvDbType.ORACLE) ? new String("PKID").toUpperCase()
					: new String("PKID").toLowerCase(),
			SvConf.getDbType().equals(SvDbType.ORACLE) ? new String("OBJECT_ID").toUpperCase()
					: new String("OBJECT_ID").toLowerCase() };

	/**
	 * Prebuilt static query for inserting and updating a repo record
	 */
	private static HashMap<String, String> repoSQL = new HashMap<String, String>();

	/**
	 * Method used to validate forms before saving.
	 * 
	 * @param dbo Data object containing the form
	 * @throws SvException
	 */
	void canSaveForm(DbDataObject dbo) throws SvException {
		DbDataObject formType = null;
		SvReader svr = new SvReader(this);
		try {
			Long formTypeId = (Long) dbo.getVal("FORM_TYPE_ID");
			if (formTypeId == null) {
				throw (new SvException("system.error.form_type_missing", instanceUser, dbo, null));
			} else {
				formType = (svr).getObjectById(formTypeId, getDbt(svCONST.OBJECT_TYPE_FORM_TYPE), null, false);
				if (formType == null) {
					throw (new SvException("system.error.form_type_config_err", instanceUser, dbo, null));
				}
			}
			// Multi entry checks
			// 1. If the form is not multi entry check for existing forms
			// 2. If the form is multi entry but has a maximum instance limit
			if (dbo.getObjectId() == 0 && (!(Boolean) formType.getVal("multi_entry")
					|| ((Boolean) formType.getVal("multi_entry") && formType.getVal("MAX_INSTANCES") != null))) {

				// TODO Check the form existence in the cache instead of firing
				// a get objects request
				DbSearchExpression dbse = new DbSearchExpression();
				dbse.addDbSearchItem(new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, dbo.getParentId()));
				dbse.addDbSearchItem(new DbSearchCriterion("FORM_TYPE_ID", DbCompareOperand.EQUAL, formTypeId));
				DbDataArray existingForms = (svr).getObjects(dbse, getDbt(svCONST.OBJECT_TYPE_FORM), null, 0, 0);
				if (existingForms != null) {
					if (existingForms.getItems().size() > 0 && !(Boolean) formType.getVal("multi_entry"))
						throw (new SvException("system.error.form_type_is_single", instanceUser, dbo, null));
					else if ((Boolean) formType.getVal("multi_entry") && formType.getVal("MAX_INSTANCES") != null)
						if (!(((Long) formType.getVal("MAX_INSTANCES")).intValue() > existingForms.getItems().size()))
							throw (new SvException("system.error.form_max_count_exceeded", instanceUser, dbo, null));
				}
			}
		} finally {
			svr.release();
		}
	}

	/**
	 * Method to fetch the PKID of all dependent objects. The flags allow to control
	 * if we want to get the child object or the linked objects. The linked objects
	 * shall be filtered according to the list of link types
	 * 
	 * @param dbt               The object type descriptor matching the object types
	 *                          in the DBA array.
	 * @param dba               The list of objects whose dependents we need to
	 *                          delete.
	 * @param includingChildren Shall the method include the child objects in the
	 *                          result
	 * @param includingLinks    Shall the method include link objects in the result.
	 * @param linkTypes         The link types which should be included in the
	 *                          result
	 * @return List of PKIDs of the resulting objects
	 * @throws SvException  Any underlying exception which may be raised.
	 * @throws SQLException Any SQL exception which was raised by the database while
	 *                      reading the objects
	 */
	private ArrayList<Long> getPKIDs(DbDataObject dbt, DbDataArray dba, boolean includingChildren,
			boolean includingLinks, ArrayList<Long> linkTypes) throws SvException, SQLException {
		// TODO Auto-generated method stub
		StringBuilder strParam = new StringBuilder();
		for (int i = 0; i < dba.getItems().size(); i++)
			strParam.append("?,");

		strParam.setLength(strParam.length() - 1);
		String sqlParam = strParam.toString();

		PreparedStatement ps = null;
		ResultSet rs = null;

		ArrayList<Long> supportingObjectPKIDs = new ArrayList<>();
		if (includingChildren)
			try {
				String strSQL = getChildIdSQLString(dbt, sqlParam).toString();
				ps = this.dbGetConn().prepareStatement(strSQL.toString());
				bindChildObjectId(ps, dba);
				if (log4j.isDebugEnabled())
					log4j.trace("Executing SQL:" + strSQL.toString());
				rs = ps.executeQuery();

				while (rs.next()) {
					supportingObjectPKIDs.add(rs.getLong(1));
				}
			} finally {
				SvCore.closeResource(rs, dbt);
				SvCore.closeResource(ps, dbt);
			}

		if (includingLinks)
			try {
				String strSQL = getLinksIdSQLString(dbt, linkTypes, sqlParam).toString();
				ps = this.dbGetConn().prepareStatement(strSQL.toString());
				bindLinkObjectId(ps, dba, linkTypes);
				if (log4j.isDebugEnabled())
					log4j.trace("Executing SQL:" + strSQL.toString());
				rs = ps.executeQuery();
				while (rs.next()) {
					supportingObjectPKIDs.add(rs.getLong(1));
				}
			} finally {
				SvCore.closeResource(rs, dbt);
				SvCore.closeResource(ps, dbt);
			}

		return supportingObjectPKIDs;
	}

	StringBuilder getChildIdSQLString(DbDataObject dbt, String sqlParam) {
		StringBuilder strSQL = new StringBuilder(50);
		strSQL.append("SELECT 	max(pkid) pkid ,object_id	FROM " + dbt.getVal("schema") + "."
				+ dbt.getVal("repo_name") + " WHERE ");
		strSQL.append(" PARENT_ID in (");
		strSQL.append(sqlParam);
		strSQL.append(") AND CURRENT_TIMESTAMP<DT_DELETE GROUP BY object_id");
		return strSQL;

	}

	void bindChildObjectId(PreparedStatement ps, DbDataArray dba) throws SQLException {
		int paramPos = 1;
		for (DbDataObject dbo : dba.getItems()) {
			ps.setLong(paramPos, dbo.getObjectId());
			paramPos++;
		}
	}

	StringBuilder getLinksIdSQLString(DbDataObject dbt, ArrayList<Long> linkTypes, String sqlParam) {
		StringBuilder strSQL = new StringBuilder(50);

		strSQL.append(
				"SELECT 	max(pkid) pkid ,object_id	FROM " + dbt.getVal("schema") + "." + "VSVAROG_LINK WHERE ");
		strSQL.append(" (LINK_OBJ_ID_1 in (");
		strSQL.append(sqlParam);
		strSQL.append(") OR ");
		strSQL.append(" LINK_OBJ_ID_2 in (");
		strSQL.append(sqlParam);
		strSQL.append(")) ");
		if (linkTypes != null && !linkTypes.isEmpty()) {
			strSQL.append(" AND LINK_TYPE_ID  in (");
			for (int i = 0; i < linkTypes.size(); i++)
				strSQL.append("?,");
			strSQL.setLength(strSQL.length() - 1);
			strSQL.append(") ");
		}
		strSQL.append(" AND CURRENT_TIMESTAMP<DT_DELETE GROUP BY object_id");

		return strSQL;

	}

	void bindLinkObjectId(PreparedStatement ps, DbDataArray dba, ArrayList<Long> linkTypes) throws SQLException {
		int paramPos = 1;
		for (DbDataObject dbo : dba.getItems()) {
			ps.setLong(paramPos, dbo.getObjectId());
			paramPos++;
		}
		for (DbDataObject dbo : dba.getItems()) {
			ps.setLong(paramPos, dbo.getObjectId());
			paramPos++;
		}
		if (linkTypes != null && !linkTypes.isEmpty()) {
			for (Long type : linkTypes) {
				ps.setLong(paramPos, type);
				paramPos++;
			}
		}
	}

	StringBuilder getRepoSQLString(DbDataObject dbt, DbDataArray dba, ArrayList<Long> supportingPKIDs) {
		StringBuilder strSQL = new StringBuilder();
		strSQL.append("SELECT pkid,object_id,parent_id, object_type, meta_pkid, dt_insert, dt_delete, status FROM "
				+ dbt.getVal("schema") + "." + dbt.getVal("repo_name") + " WHERE ((PKID in (");
		for (int i = 0; i < dba.getItems().size(); i++)
			strSQL.append("?,");
		strSQL.setLength(strSQL.length() - 1);

		strSQL.append(") ");
		if (supportingPKIDs != null && supportingPKIDs.size() > 0) {
			strSQL.append("OR PKID in (");
			for (int i = 0; i < supportingPKIDs.size(); i++)
				strSQL.append("?,");
			strSQL.setLength(strSQL.length() - 1);
			strSQL.append(")");
		}
		strSQL.append(") AND CURRENT_TIMESTAMP<DT_DELETE) ");

		return strSQL;

	}

	void bindObjectId(PreparedStatement ps, DbDataArray dba, ArrayList<Long> supportingPKIDs) throws SQLException {
		int paramPos = 1;
		for (DbDataObject dbo : dba.getItems()) {
			ps.setLong(paramPos, dbo.getPkid());
			paramPos++;
		}
		for (Long pkid : supportingPKIDs) {
			ps.setLong(paramPos, pkid);
			paramPos++;
		}
	}

	/**
	 * Method which checks if the object identified by object_id and pkid can be
	 * written to the DataTable defined by parameter dbt
	 * 
	 * @param dbt               The object type descriptor
	 * @param dbo               The list of data objects subject of update
	 * @param includingChildren Flag to let the method fetch repo data of the child
	 *                          objects
	 * @param includingLinks    Flag to let the method fetch repo data of the link
	 *                          objects (linked to the objects in the input array)
	 * @param linkTypes         List of IDs of the requested link types. If empty or
	 *                          null, it will return all types
	 * @param repoObjects
	 * @throws SvException
	 */
	HashMap<Long, Object[]> getRepoData(DbDataObject dbt, DbDataArray dba, boolean includingChildren,
			boolean includingLinks, ArrayList<Long> linkTypes) throws SvException {
		HashMap<Long, Object[]> oldRepoData = new LinkedHashMap<>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		Object[] repoObjects = null;

		try {
			ArrayList<Long> supportingObjectPKIDs = getPKIDs(dbt, dba, includingChildren, includingLinks, linkTypes);

			StringBuilder strSQL = getRepoSQLString(dbt, dba, supportingObjectPKIDs);
			// for mssql do not use FOR UPDATE
			if (!SvConf.getDbType().equals(SvDbType.MSSQL))
				strSQL.append(" FOR UPDATE");

			ps = this.dbGetConn().prepareStatement(strSQL.toString());
			bindObjectId(ps, dba, supportingObjectPKIDs);
			rs = ps.executeQuery();
			if (log4j.isDebugEnabled())
				log4j.trace("Executing SQL:" + strSQL.toString());

			while (rs.next()) {
				repoObjects = new Object[repoDbtFields.getItems().size()];
				for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
					if (i < 5)
						repoObjects[i] = rs.getLong(i + 1);
					else
						repoObjects[i] = rs.getObject(i + 1);
				}
				oldRepoData.put((Long) repoObjects[1], repoObjects);
				// TODO
				// currentStatus = rs.getString("status");
				// check if the user still has privileges over the object
			}
			if (!isInternal) {
				for (DbDataObject dbo : dba.getItems())
					if (!oldRepoData.containsKey(dbo.getObjectId()))
						throw (new SvException(Sv.Exceptions.OBJECT_NOT_UPDATEABLE, instanceUser, dbo, null));

			}
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.SQL_ERR, instanceUser, dba, null, e));
		} finally {
			closeResource((AutoCloseable) rs, instanceUser);
			closeResource((AutoCloseable) ps, instanceUser);
		}
		return oldRepoData;

	}

	String getRepoInsertSQL(Boolean isUpdate, Boolean withMetaUpdate, String schema, String repo_name) {
		// this is the repo insert. The sequence is hardcoded cause
		// it is expected that the repo must have it!
		if (!isUpdate) {
			if (repoSQL.get(repo_name + "1") == null) {
				repoSQL.put(repo_name + "1",
						"INSERT INTO " + schema + "." + repo_name + "(pkid,object_id,  dt_insert, dt_delete, "
								+ "parent_id, object_type, meta_pkid, status, user_id)  VALUES ("
								+ SvConf.getSqlkw().getString("SEQ_NEXTVAL").replace("{SEQUENCE_NAME}",
										(String) repoDbt.getVal("SCHEMA") + "." + (String) repoDbt.getVal("TABLE_NAME")
												+ "_pkid")
								+ ","
								+ SvConf.getSqlkw().getString("SEQ_NEXTVAL").replace("{SEQUENCE_NAME}",
										(String) repoDbt.getVal("SCHEMA")
												+ "." + (String) repoDbt.getVal("TABLE_NAME") + "_oid")
								+ ", ?, ?, ?, ?,"
								+ SvConf.getSqlkw().getString("SEQ_CURRVAL").replace("{SEQUENCE_NAME}",
										(String) repoDbt.getVal("SCHEMA") + "." + (String) repoDbt.getVal("TABLE_NAME")
												+ "_pkid")
								+ ", ?, ?)");
			}
			return repoSQL.get(repo_name + "1");
		} else {
			if (withMetaUpdate) {
				if (repoSQL.get(repo_name + "2") == null) {
					repoSQL.put(repo_name + "2",
							"INSERT INTO " + schema + "." + repo_name + "(pkid,object_id,  dt_insert, dt_delete, "
									+ "parent_id, object_type, meta_pkid, status, user_id)  VALUES ("
									+ SvConf.getSqlkw().getString("SEQ_NEXTVAL")
											.replace("{SEQUENCE_NAME}",
													(String) repoDbt.getVal("SCHEMA") + "."
															+ (String) repoDbt.getVal("TABLE_NAME") + "_pkid")
									+ ",?, ?, ?, ?, ?,"
									+ SvConf.getSqlkw().getString("SEQ_CURRVAL")
											.replace("{SEQUENCE_NAME}",
													(String) repoDbt.getVal("SCHEMA") + "."
															+ (String) repoDbt.getVal("TABLE_NAME") + "_pkid")
									+ ", ?, ?)");
				}
				return repoSQL.get(repo_name + "2");
			} else {
				if (repoSQL.get(repo_name + "3") == null) {
					repoSQL.put(repo_name + "3",
							"INSERT INTO " + schema + "." + repo_name + "(pkid,object_id,  dt_insert, dt_delete, "
									+ "parent_id, object_type, meta_pkid, status, user_id)  VALUES ("
									+ SvConf.getSqlkw().getString("SEQ_NEXTVAL").replace("{SEQUENCE_NAME}",
											(String) repoDbt.getVal("SCHEMA") + "."
													+ (String) repoDbt.getVal("TABLE_NAME") + "_pkid")
									+ ",?,?,?,?,?,?,?, ?)");
				}
				return repoSQL.get(repo_name + "3");
			}
		}
		// return sqlInsRepo;
	}

	/**
	 * Method to cache the prebuilt queries
	 * 
	 * @param dbt          The type of object for which we want to built a query
	 * @param objectFields The list of fields which we plan to insert
	 * @param oldPKID      The old PKID of the object in case of update
	 * @return
	 * @throws SvException
	 */
	protected String getQryInsertTableData(DbDataObject dbt, DbDataArray objectFields, Boolean isUpdate,
			Boolean hasPKID, boolean hasNullGeometry) throws SvException {
		String sql = null;
		HashMap<Long, String> qCache = null;
		if (SvCore.hasGeometries(dbt.getObjectId()) && !hasNullGeometry)
			qCache = querySDICache;
		else
			qCache = queryCache;

		if (!isUpdate)
			sql = qCache.get(dbt.getObjectId());
		else
			sql = qCache.get(-dbt.getObjectId());
		if (sql != null)
			return sql;

		sql = "INSERT INTO " + (String) dbt.getVal("schema") + "." + (String) dbt.getVal("table_name");
		StringBuilder fieldList = new StringBuilder(300);
		StringBuilder fieldVals = new StringBuilder();
		// create the field lists and check if all fields are writeable

		// fieldList.append("PKID");
		for (DbDataObject obj : objectFields.getItems()) {
			// if (!obj.getVal("field_name").equals("PKID"))
			fieldList.append(SvConf.getSqlkw().getString("OBJECT_QUALIFIER_LEFT") + obj.getVal("field_name")
					+ SvConf.getSqlkw().getString("OBJECT_QUALIFIER_RIGHT") + ",");

			// maybe add field level checks in 2020?
			// if (!isFieldWriteable(obj, "Replace with User", status)) {
			// fieldVals.append((String) obj.getVal("field_name"));

			if (((String) obj.getVal("field_type")).equals("GEOMETRY") && !hasNullGeometry) {
				ISvDatabaseIO gio = SvConf.getDbHandler();
				fieldVals.append(gio.getGeomWriteSQL() + ",");
			} else
				fieldVals.append("?,");
		}

		fieldList.setLength(fieldList.length() - 1);
		fieldVals.setLength(fieldVals.length() - 1);

		sql += "(" + fieldList + ") ";

		if (!isUpdate) {
			sql += "VALUES(" + fieldVals + ")";
			qCache.put(dbt.getObjectId(), sql);
		} else {
			sql += "SELECT " + fieldVals + " FROM " + (String) dbt.getVal("schema") + "."
					+ (String) dbt.getVal("table_name") + " WHERE pkid=?";
			qCache.put(-dbt.getObjectId(), sql);
		}
		if (log4j.isDebugEnabled())
			log4j.trace(sql);
		return sql;
	}

	/**
	 * Method that performs basic checks on object before its being saved in the
	 * repo
	 * 
	 * @param dbo               The object to be saved
	 * @param isFirstOld        If it is batch we need to ensure that all objects
	 *                          either new or updates.
	 * @param skipPreSaveChecks If pre-save checks should be skipped.
	 * @throws SvException
	 */
	void checkRepoData(DbDataObject dbo, Boolean isUpdate, Boolean skipPreSaveChecks) throws SvException {
		Boolean isOld = dbo.getObjectId() != 0L;
		if (isUpdate && !isOld)
			throw (new SvException("system.error.multi_type_batch_err", instanceUser, dbo, null));
		if (dbo.getObjectType().equals(svCONST.OBJECT_TYPE_FORM) && dbo.getParentId() == 0)
			throw (new SvException("system.error.form_must_have_parent", instanceUser, dbo, null));
		if (skipPreSaveChecks && (dbo.getPkid() != 0 || dbo.getObjectId() != 0
				|| dbo.getObjectType().equals(svCONST.OBJECT_TYPE_FORM)))
			throw (new SvException("system.error.skip_presave_err", instanceUser, dbo, null));
		// if this is installation process don't execute the ID checks
		if (!isInternal) {
			if (!((dbo.getPkid() != 0 && dbo.getObjectId() != 0) || (dbo.getPkid() == 0 && dbo.getObjectId() == 0)))
				throw (new SvException("system.error.no_obj_id", instanceUser, dbo, null));

			if (dbo.getObjectType() < svCONST.MIN_WRITEABLE_OBJID)
				throw (new SvException("system.error.type_not_writeable", instanceUser, dbo, null));
		}
	}

	/**
	 * Method specific for SQL server. Uses server data table to send a batch of
	 * repo insert requests.
	 * 
	 * @return
	 * @throws SvException
	 * @throws SQLServerException SQLServerDataTable prepRepoDataTable() throws
	 *                            SQLServerException { if
	 *                            (SvConf.getDbType().equals(SvDbType.MSSQL)) {
	 *                            SQLServerDataTable repoTypeDT = new
	 *                            SQLServerDataTable();
	 *                            repoTypeDT.addColumnMetadata("PKID",
	 *                            java.sql.Types.NUMERIC);
	 *                            repoTypeDT.addColumnMetadata("META_PKID",
	 *                            java.sql.Types.NUMERIC);
	 *                            repoTypeDT.addColumnMetadata("OBJECT_ID",
	 *                            java.sql.Types.NUMERIC);
	 *                            repoTypeDT.addColumnMetadata("DT_INSERT",
	 *                            java.sql.Types.TIMESTAMP);
	 *                            repoTypeDT.addColumnMetadata("DT_DELETE",
	 *                            java.sql.Types.TIMESTAMP);
	 *                            repoTypeDT.addColumnMetadata("PARENT_ID",
	 *                            java.sql.Types.NUMERIC);
	 *                            repoTypeDT.addColumnMetadata("OBJECT_TYPE",
	 *                            java.sql.Types.NUMERIC);
	 *                            repoTypeDT.addColumnMetadata("STATUS",
	 *                            java.sql.Types.NVARCHAR);
	 *                            repoTypeDT.addColumnMetadata("USER_ID",
	 *                            java.sql.Types.NUMERIC); return repoTypeDT; } else
	 *                            return null; }
	 */

	/**
	 * Method to execute on save call backs on the object subject of saving
	 * 
	 * @param dbo the reference to the object
	 * @throws SvException any exception that is thrown by the callbacks
	 */
	private void executeOnSaveCallbacks(DbDataObject dbo) throws SvException {
		CopyOnWriteArrayList<ISvOnSave> globalCallback = onSaveCallbacks.get(0L);
		CopyOnWriteArrayList<ISvOnSave> localCallbacks = null;
		if (globalCallback != null)
			for (ISvOnSave onSave : globalCallback) {
				onSave.beforeSave(this, dbo);
			}
		localCallbacks = onSaveCallbacks.get(dbo.getObjectType());
		if (localCallbacks != null)
			for (ISvOnSave onSave : localCallbacks) {
				onSave.beforeSave(this, dbo);
			}
	}

	/**
	 * Method to perform final preparations of the batch save.
	 * 
	 * @param dbt                The type descriptor
	 * @param dba                The list of objects to be saved to the DB
	 * @param oldRepoData        The existing repo data (if its update operation)
	 * @param withMetaUpdate     Flag, if we need to update the object metadata
	 * @param isUpdate           Flag is its update operation
	 * @param skipPreSaveChecks  Flag if pre-save checks shall be skipped
	 * @param psInvalidate       The prepared statement used for invalidation of
	 *                           existing records
	 * @param psInsert           The prepared statement used for insertion of new
	 *                           records
	 * @param extendedRepoStruct The extender sql structure supporting the insert
	 *                           (for some SQL engines)
	 * @throws SvException  "system.error.sdi.sdi_type_limit" is raised if we try to
	 *                      save SDI object but no geometries have been found
	 * @throws SQLException any other underlying sql exception
	 */
	void prepareRepoBatch(DbDataObject dbt, DbDataArray dba, HashMap<Long, Object[]> oldRepoData,
			Boolean withMetaUpdate, Boolean isUpdate, Boolean skipPreSaveChecks, PreparedStatement psInvalidate,
			PreparedStatement psInsert, Object extendedRepoStruct) throws SvException, SQLException {
		// sort the milis of the ending/starting time
		long milis = new DateTime().getMillis();
		Timestamp dtEndPrev = new Timestamp(milis - 1);
		Timestamp dtInsert = new Timestamp(milis);
		int rowIndex = 0;
		for (DbDataObject dbo : dba.getItems()) {
			// execute the call backs
			executeOnSaveCallbacks(dbo);

			// make sure we save SDI objects only when SvWriter is used
			// internally by SvGeometry
			if (!isInternal && hasGeometries(dbo.getObjectType()))
				throw (new SvException(Sv.Exceptions.SDI_TYPE_LIMIT, instanceUser, dbo, dbt));

			// perform the basic repo checks over the object before saving
			checkRepoData(dbo, isUpdate, skipPreSaveChecks);

			// if the object is a new object, make sure we assign the
			// default status to it.
			if ((dbo.getPkid() == 0 || dbo.getObjectId() == 0) && (dbo.getStatus() == null || dbo.getStatus() == ""))
				dbo.setStatus(getDefaultStatus(dbt));
			// get the old repo objects if any
			Object[] repoObjects = isUpdate ? oldRepoData.get(dbo.getObjectId()) : null;

			// ensure the object is batched for saving
			addRepoBatch(dbt, dbo, withMetaUpdate, repoObjects, psInvalidate, psInsert, dtInsert, dtEndPrev,
					extendedRepoStruct, rowIndex);
			rowIndex++;

		}
	}

	/**
	 * Method to execute the insertion of repo data into the svarog database and
	 * update the list of objects with the corresponding database keys
	 * 
	 * @param dba                The list of objects subject to database save
	 *                           operation. They primary keys will be loaded into
	 *                           this list
	 * @param psInsRepo          The prepared statement used for performing the SQL
	 *                           insert/call
	 * @param extendedRepoStruct The extended repo structure needed for certain SQL
	 *                           servers (like MSMSQL)
	 * @return Returns count of loaded primary keys
	 * @throws SQLException any underlyding SQL exception
	 * @throws SvException  "system.error.jdbc_bad_database_handler" if the database
	 *                      handler doesn't return the database primary keys
	 */
	int loadRepoKeys(DbDataArray dba, PreparedStatement psInsRepo, Object extendedRepoStruct)
			throws SQLException, SvException {
		int objectIndex = 0;
		if (!SvConf.getDbHandler().getOverrideInsertRepo()) {
			psInsRepo.executeBatch();
			try (ResultSet rsGenKeys = psInsRepo.getGeneratedKeys()) {
				while (rsGenKeys.next()) {
					setKeys(dba, rsGenKeys.getLong(1), rsGenKeys.getLong(2), objectIndex);
					objectIndex++;
				}
			}
		} else {
			Map<Long, Long> repoKeys = SvConf.getDbHandler().repoSaveGetKeys(psInsRepo, extendedRepoStruct);
			if (repoKeys == null)
				throw (new SvException(Sv.Exceptions.BAD_JDBC_HANDLER, instanceUser));
			for (Entry<Long, Long> entry : repoKeys.entrySet()) {
				setKeys(dba, entry.getKey(), entry.getValue(), objectIndex);
				objectIndex++;
			}
		}
		return objectIndex;
	}

	/**
	 * Method to save the base repo data for the object.
	 * 
	 * @param dbt               The object type descriptor
	 * @param dba               The array of objects to be saved
	 * @param withMetaUpdate    If there is pending meta data update, the meta
	 *                          sequence should be increased. If not no change to
	 *                          the meta_pkid will happen
	 * @param skipPreSaveChecks In certain situations, the pre-save checks need to
	 *                          be skipped.
	 * @param oldRepoObjs
	 * @throws SvException
	 * @throws SQLException
	 */
	HashMap<Long, Object[]> saveRepoData(DbDataObject dbt, DbDataArray dba, Boolean withMetaUpdate,
			Boolean skipPreSaveChecks) throws SvException, SQLException {

		PreparedStatement psInvalidate = null;
		PreparedStatement psInsert = null;
		Object extendedRepoStruct = null;
		HashMap<Long, Object[]> oldRepoData = null;
		int[] updatedRows = null;

		try {
			// get the system configured DB handler
			String schema = dbt.getVal("schema").toString();
			String repoName = dbt.getVal("repo_name").toString();
			Boolean isUpdate = dba.getItems().get(0).getObjectId() != 0L;
			String sqlInsRepo = getRepoInsertSQL(isUpdate, withMetaUpdate, schema, repoName);
			if (log4j.isDebugEnabled())
				log4j.trace(sqlInsRepo);

			Connection conn = this.dbGetConn();

			// prepare the repo insert statements
			if (!SvConf.getDbHandler().getOverrideInsertRepo())
				psInsert = conn.prepareStatement(sqlInsRepo, genKeyIds);
			else {// if the handler overrides the repo insert, pass the
					// generation of the statement to the handler
				psInsert = SvConf.getDbHandler().getInsertRepoStatement(conn, sqlInsRepo, schema, repoName);
				if (psInsert == null)
					throw (new SvException("system.error.jdbc_bad_database_handler", instanceUser));
				extendedRepoStruct = SvConf.getDbHandler().getInsertRepoStruct(conn, dba.size());
			}
			// execute pre-save checks if needed
			if (!skipPreSaveChecks || isUpdate)
				oldRepoData = preSaveChecks(dbt, dba, isUpdate, skipPreSaveChecks);

			// if we are updating, invalidate the old versions
			if (isUpdate)
				psInvalidate = conn.prepareStatement(getUpdateRepoSql(schema, repoName));

			// prepare the insert and invalidate and bind all params.
			prepareRepoBatch(dbt, dba, oldRepoData, withMetaUpdate, isUpdate, skipPreSaveChecks, psInvalidate, psInsert,
					extendedRepoStruct);

			// if we have valid invalidate statement execute it
			updatedRows = psInvalidate != null ? psInvalidate.executeBatch() : null;

			// finally execute the repo insert and load the keys back into the objects
			int objectIndex = loadRepoKeys(dba, psInsert, extendedRepoStruct);

			// basic sanity check
			if (dba.getItems().size() != objectIndex || (isUpdate && !isInternal
					&& objectIndex != (updatedRows != null ? updatedRows.length : objectIndex)))
				throw (new SvException(Sv.Exceptions.BATCH_SIZE_ERR, instanceUser, dba, dbt));

		} catch (SQLException ex) {
			throw (new SvException(Sv.Exceptions.REPOSAVE_ERR, instanceUser, null, dba, ex));
		} finally {
			if (psInvalidate != null)
				psInvalidate.close();
			if (psInsert != null)
				psInsert.close();
		}
		return oldRepoData;

	}

	/**
	 * Method to generate the sql statement for update of the delete repo objects
	 * 
	 * @param schema   The Schema name
	 * @param repoName The name of the repo table
	 * @return String containing valid SQL statement
	 */
	private String getUpdateRepoSql(String schema, String repoName) {
		return "UPDATE " + schema + "." + repoName + " SET dt_delete=? WHERE pkid=?";
	}

	/**
	 * Method to set the generated keys to the appropriate Object in the DbDataArray
	 * which is batched
	 * 
	 * @param dba         The batched DbDataArray
	 * @param pkid        The versioning id of the object
	 * @param objectId    The object id of the object
	 * @param objectIndex The index of the object in the batched array
	 */
	void setKeys(DbDataArray dba, Long pkid, Long objectId, int objectIndex) {
		dba.getItems().get(objectIndex).setObjectId(objectId);
		dba.getItems().get(objectIndex).setPkid(pkid);
		dba.getItems().get(objectIndex)
				.setUserId(this.saveAsUser != null ? this.saveAsUser.getObjectId() : this.instanceUser.getObjectId());
		if (log4j.isDebugEnabled()) {
			log4j.trace("Generated keys (pkid):" + pkid.toString());
			log4j.trace("Generated keys (object_id)" + objectId.toString());
		}
	}

	void addRepoBatchImpl(Long PKID, Long oldMetaPKID, Long objectId, Timestamp tsInsert, Timestamp tsDelete,
			Long parentId, Long objType, String objStatus, Long userId, PreparedStatement psInsert)
			throws SQLException {
		int paramCount = 1;
		if (objectId != 0)
			psInsert.setLong(paramCount++, objectId);
		psInsert.setTimestamp(paramCount++, tsInsert);
		psInsert.setTimestamp(paramCount++, tsDelete);
		psInsert.setLong(paramCount++, parentId);
		psInsert.setLong(paramCount++, objType);
		// if there's no meta update, use the existing META_PKID
		if (oldMetaPKID != 0)
			psInsert.setLong(paramCount++, oldMetaPKID);
		psInsert.setString(paramCount++, objStatus);
		psInsert.setLong(paramCount, userId);
		psInsert.addBatch();
	}

	/**
	 * Method for saving base object repository data.
	 * 
	 * @param dbt            Descriptor of the object type
	 * @param dbo            The object to be saved
	 * @param withMetaUpdate Should the object metadata be also updated or the
	 *                       update is repo only
	 * @param conn           SQL Connection to be used for execution of the SQL
	 *                       statements.
	 * @throws SQLException
	 * @throws SvException
	 * @throws Exception
	 */
	void addRepoBatch(DbDataObject dbt, DbDataObject dbo, Boolean withMetaUpdate, Object[] repoObjects,
			PreparedStatement psInvalidate, PreparedStatement psInsert, Timestamp dtInsert, Timestamp dtEndPrev,
			Object extendedRepoStruct, int rowIndex) throws SQLException, SvException {

		ISvDatabaseIO dbHandler = SvConf.getDbHandler();
		Timestamp tsInsert, tsDelete;

		if (dbo.getPkid() != 0) {
			psInvalidate.setTimestamp(1, dtEndPrev);
			psInvalidate.setLong(2, dbo.getPkid());
			psInvalidate.addBatch();
		}
		String objStatus = dbo.getStatus() != null ? dbo.getStatus() : getDefaultStatus(dbt);
		// make sure that the object type hasn't changed
		Long objType = repoObjects == null || repoObjects[0] == null ? dbt.getObjectId() : (Long) repoObjects[3];
		Long objParent = dbo.getParentId() == null ? 0 : dbo.getParentId();
		Long oldMetaPKID = repoObjects != null && withMetaUpdate == false ? (Long) repoObjects[4] : 0L;
		Long userId = this.saveAsUser != null ? this.saveAsUser.getObjectId() : this.instanceUser.getObjectId();

		if (SvConf.isOverrideTimeStamps()) {
			dbo.setDtInsert(new DateTime(dtInsert));
			dbo.setDtDelete(SvConf.MAX_DATE);
			tsInsert = dtInsert;
			tsDelete = SvConf.MAX_DATE_SQL;
		} else {
			tsInsert = new Timestamp(dbo.getDtInsert().getMillis());
			tsDelete = new Timestamp(dbo.getDtDelete().getMillis());
		}

		if (!dbHandler.getOverrideInsertRepo()) { // if the handler does not
			addRepoBatchImpl(0L, oldMetaPKID, dbo.getObjectId(), tsInsert, tsDelete, objParent, objType, objStatus,
					userId, psInsert);
		} else {
			// if the insert is overriden then call overrided add repo batch
			dbHandler.addRepoBatch(extendedRepoStruct, 0L, oldMetaPKID, dbo.getObjectId(), tsInsert, tsDelete,
					objParent, objType, objStatus, userId, rowIndex);
		}
	}

	Connection getConstraintsConn() throws SvException {
		if (log4j.isDebugEnabled())
			log4j.trace("Fetching internal reader to execute constraints");

		if (internalReader == null) {
			internalReader = new SvReader();
			Connection conn = internalReader.dbGetConn();
			internalReader.dbSetAutoCommit(true);
			// oracle doesn't support read uncommitted, but we need it to
			// prevent MSSQL to block!
			if (!SvConf.getDbType().equals(SvDbType.ORACLE))
				try {
					oldTrxIsolation = conn.getTransactionIsolation();
					conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				} catch (SQLException e) {
					log4j.error("Transaction isolation level can not be set to TRANSACTION_READ_UNCOMMITTED", e);
				}
		}
		return internalReader.dbGetConn();
	}

	@Override
	public void release() {
		try {
			if (internalReader != null) {
				internalReader.dbRollback();
				// oracle doesn't support read uncommitted, but we need it to
				// prevent MSSQL to block! so we need to return the original
				// transaction isolation see method getConstraintsConn()
				if (!SvConf.getDbType().equals(SvDbType.ORACLE)) {
					Connection conn = internalReader.dbGetConn();
					if (conn != null)
						conn.setTransactionIsolation(oldTrxIsolation);
				}
				internalReader.release();
				if (log4j.isDebugEnabled())
					log4j.trace("Released internal reader used to execute constraints");
			}
		} catch (Exception e) {
			log4j.error("Internal reader can't be properly released", e);
		}
		super.release();
	}

	/**
	 * Method that returns the default status for an Object Type
	 * 
	 * @param dbt References to the object configuration
	 * @return
	 */
	public static String getDefaultStatus(DbDataObject dbt) {
		// TODO Extend the function to get defaults for object workflow
		return svCONST.STATUS_VALID;
	}

	/**
	 * Method to execute all constraint checks on the object before save
	 * 
	 * @param dbo
	 * @throws SvException
	 */
	void executeConstraints(DbDataArray dba) throws SvException {
		SvObjectConstraints oConstr = SvCore.getObjectConstraints(dba.getItems().get(0).getObjectType());
		if (oConstr == null)
			return;
		StringBuilder sqlStrB = oConstr.getSQLQueryString(dba);
		if (log4j.isDebugEnabled())
			log4j.trace("Executing unique constraints check:" + sqlStrB.toString());

		try (PreparedStatement ps = getConstraintsConn().prepareStatement(sqlStrB.toString())) {
			bindQueryVals(ps, oConstr.getSQLParamVals(dba));
			try (ResultSet rs = ps.executeQuery()) {
				StringBuilder strb = new StringBuilder();
				String constraintName = null;
				if (rs.next()) {
					constraintName = rs.getString("constr_name");
					strb.append("Constraint violated:" + rs.getString("constr_name") + ", values:"
							+ rs.getString("existing_unq_vals") + ";");
				}
				if (strb.length() > 0) {
					strb.setLength(strb.length() - 1);
					throw (new SvException("system.error.unq_constraint_violated", instanceUser,
							oConstr.getConstraints().get(constraintName).getConstraintFields(),
							dba.toSimpleJson().toString() + strb.toString()));
				}
			}
		} catch (SQLException ex) {
			throw (new SvException("system.error.sql_statement_err", instanceUser, dba, sqlStrB.toString(), ex));
		}
	}

	/**
	 * Method executing core pre-save checks
	 * 
	 * @param dbt The Object Type descriptor
	 * @param dbo The data object subject of writing to the DB
	 * @return Array objects containing the old repo data
	 * @throws SvException
	 */
	HashMap<Long, Object[]> preSaveChecks(DbDataObject dbt, DbDataArray dba, Boolean isFirstOld,
			boolean skipPreSaveChecks) throws SvException {
		HashMap<Long, Object[]> oldRepoData = null;

		// TODO: why do we always hit the DB to get the repo data?
		// and not to mention that we are ignoring the JDBC batching!!!
		// if the object is in the cache and its not dirty, does it make sense
		// to hit the DB?
		// record locking seems the only feasible explanation, so it would be
		// deprecated when going
		// distributed anyway because we'll need to use the distibuted locking
		// mechanism from SvLock!
		if (isFirstOld)
			oldRepoData = getRepoData(dbt, dba, false, false, null);

		if (!skipPreSaveChecks) {
			executeConstraints(dba);

			if (dbt.getObjectId().equals(svCONST.OBJECT_TYPE_FORM)) {
				for (DbDataObject dbo : dba.getItems())
					canSaveForm(dbo);
			}
		}
		return oldRepoData;
	}

	/**
	 * Method for cleaning up the cache for a specific DbDataObject
	 * 
	 * @param objectId     The object id of the DbDataObject for which the cache
	 *                     should purged
	 * @param objectTypeId The object type id of the DbDataObject
	 * @throws SvException
	 */
	static void cacheCleanup(Long objectId, Long objectTypeId) throws SvException {
		DbDataObject dbo = DbCache.getObject(objectId, objectTypeId);
		if (dbo != null)
			cacheCleanup(dbo);
	}

	/**
	 * Method for cleaning up the cache for a specific DbDataObject
	 * 
	 * @param dbo The DbDataObject for which the cache should purged
	 * @throws SvException
	 */
	static void cacheCleanup(DbDataObject dbo) throws SvException {

		cacheCleanup(dbo.getObjectId(), dbo.getParentId(), dbo.getObjectType());

		if (isCfgInDb) {
			if (dbo.getObjectType().equals(svCONST.OBJECT_TYPE_LINK))
				removeLinkCache(dbo);
			if (dbo.getObjectType().equals(svCONST.OBJECT_TYPE_SYS_PARAMS))
				SvParameter.paramsCache.invalidate(dbo.getVal(Sv.PARAM_NAME));

		}

	}

	/**
	 * Method for cleaning up the cache for a specific repo object
	 * 
	 * @param dbo The DbDataObject for which the cache should purged
	 * @throws SvException
	 */
	static void cacheCleanup(Long objectId, Long parentId, Long objectType) throws SvException {

		if (isCfgInDb) {
			if (objectType != svCONST.OBJECT_TYPE_TABLE && objectType != svCONST.OBJECT_TYPE_FIELD) {
				DbCache.removeObject(objectId, objectType);
				DbCache.removeObjectSupport(parentId, objectType);
			} else {
				DbDataObject dboRefresh = null;
				try (SvReader svr = new SvReader()) {
					dboRefresh = svr.getObjectById(objectId, objectType, null);
				}
				if (dboRefresh != null)
					DbCache.addObject(dboRefresh);
			}
		}

	}

	/**
	 * Method to execute all registered After Save callbacks in Svarog.
	 * 
	 * @param dbo The object which was saved
	 * @throws SvException If any exception was raised by the callbacks, throw it
	 */
	void executeAfterSaveCallbacks(DbDataObject dbo) throws SvException {
		CopyOnWriteArrayList<ISvOnSave> globalCallback = onSaveCallbacks.get(0L);
		CopyOnWriteArrayList<ISvOnSave> localCallbacks = null;
		if (globalCallback != null)
			for (ISvOnSave onSave : globalCallback) {
				onSave.afterSave(this, dbo);
			}

		localCallbacks = onSaveCallbacks.get(dbo.getObjectType());
		if (localCallbacks != null)
			for (ISvOnSave onSave : localCallbacks) {
				onSave.afterSave(this, dbo);
			}

	}

	/**
	 * Root method to save data to the underlying table. This method will generate
	 * the INSERT query and bind the appropriate parameters as values.
	 * 
	 * @param arrayToSave Array of objects to save
	 * @param dbt         The table descriptor
	 * @param oldRepoObjs In case we are updating records, the old repo objects
	 * @param isUpdate    If this is update or new objects
	 * @throws SvException
	 */

	void saveTableData(DbDataArray arrayToSave, DbDataObject dbt, HashMap<Long, Object[]> oldRepoObjs, Boolean isUpdate)
			throws SvException {
		// get the table columns (object fields) to generate the insert
		DbDataArray objectFields = DbCache.getObjectsByParentId(dbt.getObjectId(), svCONST.OBJECT_TYPE_FIELD_SORT);

		DbDataObject dboFirst = arrayToSave.get(0);
		boolean hasNullGeometry = SvGeometry.getGeometry(dboFirst) == null;
		String sql = getQryInsertTableData(dbt, objectFields, isUpdate, true, hasNullGeometry);
		if (log4j.isDebugEnabled())
			log4j.trace("Executing SQL:" + sql);

		try (SvLob lob = new SvLob(this.dbGetConn()); PreparedStatement ps = this.dbGetConn().prepareStatement(sql)) {
			for (DbDataObject objToSave : arrayToSave.getItems()) {
				Object[] oldRepoData = isUpdate ? oldRepoObjs.get(objToSave.getObjectId()) : null;
				// always bind the PKID at position one
				ps.setBigDecimal(1, new BigDecimal(objToSave.getPkid()));
				if ((SvGeometry.getGeometry(objToSave) == null) != hasNullGeometry)
					throw (new SvException("system.error.null_geometry_mix", instanceUser, arrayToSave, dbt));
				bindColumnValues(objToSave, isUpdate, objectFields, oldRepoData, ps, lob);
			}
			int[] insertedRows = ps.executeBatch();
			if (arrayToSave.getItems().size() != insertedRows.length)
				throw (new SvException("system.error.batch_size_err", instanceUser, arrayToSave, dbt));
		} catch (BatchUpdateException e) {
			throw (new SvException("system.error.batch_err", instanceUser, arrayToSave, e.getUpdateCounts(),
					e.getNextException()));
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.SQL_ERR, instanceUser, arrayToSave, dbt, e));
		} catch (Exception e) {
			if (e instanceof SvException)
				throw ((SvException) e);
			else
				throw (new SvException("system.error.general_err", instanceUser, arrayToSave, dbt, e));
		}
	}

	/**
	 * Method to bind the object field values to the parameters of the prepared
	 * statement
	 * 
	 * @param objToSave    The DbDataObject object which is subject of inserting
	 *                     into the Database
	 * @param isUpdate     Is this a new object or we are updating and existing one
	 * @param objectFields The list of columns/fields of the object
	 * @param oldRepoData  The repo data from the previous object version
	 * @param ps           The PreparedStatement used for this insert
	 * @param lob          The SvLob instance used for binding BLOBs/CLOBs
	 * @throws SvException  Underlying SvException
	 * @throws SQLException Any SQLException thrown by the JDBC driver
	 * @throws Exception    Any other generic exception including SvException
	 */
	private void bindColumnValues(DbDataObject objToSave, Boolean isUpdate, DbDataArray objectFields,
			Object[] oldRepoData, PreparedStatement ps, SvLob lob) throws SQLException, SvException {
		String fName = "";
		int pCount = 2;
		for (DbDataObject dbf : objectFields.getItems()) {
			fName = (String) dbf.getVal("FIELD_NAME");
			if (fName.equals("PKID"))
				continue;

			Object value = objToSave.getVal(fName);
			if (log4j.isDebugEnabled())
				log4j.trace("Bind variable " + pCount + ", field:" + fName + ", value:" + value);

			// validate the field data and if exception happens re-throw
			try {
				validateFieldData(dbf, value, false);
			} catch (SvException ex) {
				ex.setUserData(objToSave);
				throw (ex);
			}
			bindInsertQueryVars(ps, dbf, pCount, value, lob);
			pCount++;
		}

		if (isUpdate) {
			ps.setBigDecimal(pCount, new BigDecimal((Long) oldRepoData[4]));
			if (log4j.isDebugEnabled()) {
				log4j.debug("Bind variable " + pCount + ", field: OLD_PKID, value:" + oldRepoData[4]);
			}

		}
		ps.addBatch();
	}

	/**
	 * Method to check if a Svarog field has the sv_multiselect flag
	 * 
	 * @param dbf The Svarog Field descriptor reference which should be checked
	 * @return True if the field has the multi select flag on
	 * @throws SvException if an exception is raised, its wrapped into SvException
	 *                     and re-thrown
	 */
	Boolean isDbfMultiSelect(DbDataObject dbf) throws SvException {
		Boolean isMulti = (Boolean) dbf.getVal("sv_multiselect");
		if (isMulti == null) {
			isMulti = false;
		}
		return isMulti;
	}

	/**
	 * cachCleanup Method for validating the data for a specific field
	 * 
	 * @param dbf   The descriptor of the field
	 * @param value The value to which the field is set
	 * @return
	 * @throws SvException
	 */
	void validateFieldData(DbDataObject dbf, Object value, Boolean overrideNullCheck) throws SvException {
		if (!overrideNullCheck)
			if ((!(Boolean) dbf.getVal("is_null") && (value == null || value.equals(""))))
				throw (new SvException("system.error.field_must_have_value", instanceUser, null, dbf));

		if (value != null && dbf.getVal("FIELD_TYPE").equals(DbFieldType.NVARCHAR.toString()))
			if (dbf.getVal("FIELD_SIZE") != null && ((Long) dbf.getVal("FIELD_SIZE")) < value.toString().length())
				throw (new SvException("system.error.field_value_too_long", instanceUser, null, dbf));

		if (dbf.getVal("CODE_LIST_ID") != null) {
			if (!(Boolean) dbf.getVal("is_null") && value != null) {
				HashMap<String, String> vals = getCodeList().getCodeList((Long) dbf.getVal("CODE_LIST_ID"), false);
				if (vals != null && value != null) {
					// multi select code lists
					if (value instanceof ArrayList<?> && isDbfMultiSelect(dbf)) {
						for (String o : (ArrayList<String>) value) {
							String code = o.toString();
							if (!vals.containsKey((String) code)) {
								if (log4j.isDebugEnabled())
									log4j.warn("Field value:" + value + ", is not in the list:" + vals.toString()
											+ ", for field:" + dbf.toSimpleJson().toString());

								throw (new SvException("system.error.field_value_out_of_range", instanceUser, dbf,
										value));
							}
						}
					} else {
						// old school single code
						value = value.toString();
						if (!vals.containsKey((String) value)) {
							if (log4j.isDebugEnabled())
								log4j.warn("Field value:" + value + ", is not in the list:" + vals.toString()
										+ ", for field:" + dbf.toSimpleJson().toString());

							throw (new SvException("system.error.field_value_out_of_range", instanceUser, dbf, value));
						}
					}

				}
			}
		}

	}

	/**
	 * A method which checks if a field in a table is writable for the User in the
	 * specified Status
	 * 
	 * @param dbf      The DbDataField object which should be checked
	 * @param userName The User Name of the user who tries to write
	 * @param status   The status in which is the parent object of the field
	 * @return
	 */
	private Boolean isFieldWriteable(DbDataObject dbf, String userName, String status) {

		// TODO add if the field is writeable
		return true;
	}

	/**
	 * Method to authorise the saving of List of objects in the database
	 * 
	 * @param dba The array of DbDataObjects which should be saved
	 * @throws SvException Exception is thrown if the user is not authorised to save
	 *                     the specific object
	 */
	void authoriseSave(DbDataArray dba) throws SvException {
		// we are guaranteed to have at least one Object
		DbDataObject dbt = getDbt(dba.get(0));
		for (DbDataObject dbo : dba.getItems()) {
			if (!dbt.getObjectId().equals(dbo.getObjectType()))
				dbt = getDbt(dbo);
			if (!hasDbtAccess(dbt, null, SvAccess.WRITE))
				throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, instanceUser, dbt, SvAccess.WRITE.toString()));
		}
	}

	/**
	 * The base saveObject method. This method is called by other method overloads.
	 * 
	 * @param dba               The array of objects which needs to be saved.
	 * @param skipPreSaveChecks Flag to enable skipping of pre-save checks
	 * @throws Exception
	 */
	protected void saveObjectImpl(DbDataArray dba, Boolean skipPreSaveChecks) throws SvException {

		if (dba == null || dba.getItems().size() < 1)
			throw (new SvException("system.error.no_obj2save_err", instanceUser, dba, null));
		// authorise the save operation
		if (!isAdmin() && !isSystem())
			authoriseSave(dba);

		DbDataObject dboFirst = dba.getItems().get(0);
		Boolean isUpdate = dboFirst.getPkid() != 0L;

		DbDataObject dbt = getDbt(dboFirst);

		if (dbt == null)
			throw (new SvException("system.error.cant_find_dbt", instanceUser, dba, dbt));
		if (((dbt.getVal("repo_table") != null && (Boolean) dbt.getVal("repo_table"))))
			throw (new SvException("system.error.repo_save_err", instanceUser, dba, dbt));

		HashMap<Long, Object[]> oldRepoObjs;
		try {
			oldRepoObjs = saveRepoData(dbt, dba, true, skipPreSaveChecks);
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.JDBC_CANT_RELEASE, instanceUser, dba, dbt, e));
		}

		// if we aren't saving a form
		if (!dboFirst.getObjectType().equals(svCONST.OBJECT_TYPE_FORM)) {
			if (isUpdate && dba.getItems().size() != oldRepoObjs.size())
				throw (new SvException("system.error.batch_size_update_err", instanceUser, dba, dbt));
			saveTableData(dba, dbt, oldRepoObjs, isUpdate);
		} else
			for (DbDataObject dbo : dba.getItems()) {
				try {
					// execute pre-save checks
					Object[] oldRepoObjects = oldRepoObjs != null ? oldRepoObjs.get(dbo.getObjectId()) : null;
					Long oldPkid = oldRepoObjects != null && oldRepoObjects[4] != null ? ((Long) oldRepoObjects[4]) : 0;
					dbo.setObjectType(dbt.getObjectId());
					this.saveFormData(dbo, dbt, oldPkid, dbo.getPkid(), dbo.getStatus());

				} catch (SvException e) {
					dbo.setObjectId(0L);
					dbo.setPkid(0L);
					throw (e);
				}
			}

		// finally invoce the cache clean up
		cacheCleanup(dba);

	}

	private void cacheCleanup(DbDataArray dba) throws SvException {
		DbDataObject dboFirst = dba.getItems().get(0);

		for (DbDataObject dbo : dba.getItems()) {
			cacheCleanup(dbo);
			executeAfterSaveCallbacks(dbo);
			dbo.setIsDirty(false);
		}
		// if we have geometry then clean the tile cache
		if (dboFirst.isGeometryType())
			SvGeometry.cacheCleanup(dba);

		// broadcast the dirty objects to the cluster
		// if we are coordinator, broadcast through the proxy otherwise
		// broadcast through the client
		if (SvCluster.getIsActive().get()) {
			if (!SvCluster.isCoordinator())
				SvClusterNotifierClient.publishDirtyArray(dba);
			else
				SvClusterNotifierProxy.publishDirtyArray(dba);

		}
	}

	/**
	 * A method that saves a JSON object to the Database. If the JSON object
	 * contains an Array of JSON Objects, each of them will be saved in the database
	 * using the same transaction. If saving one of the objects fails, the whole
	 * object will be rolled back.
	 * 
	 * @param jsonObject The JSON object which needs to be saved.
	 * @throws SvException
	 */
	public void saveObject(JsonObject jsonObject) throws SvException {

		JsonElement jItems = null;
		jItems = jsonObject.get(DbDataArray.class.getCanonicalName());

		if (jItems == null) {
			DbDataObject dbo = new DbDataObject();
			dbo.fromJson(jsonObject);
			saveObject(dbo);
		} else {
			DbDataArray dba = new DbDataArray();
			dba.fromJson(jsonObject);
			saveObject(dba);

		}
	}

	/**
	 * A method that saves a DbDataArray object to the Database. If saving one of
	 * the objects fails, the whole object will be rolled back. This method contains
	 * implicit transaction handling.
	 * 
	 * @param dbDataArray The DbDataArray object which needs to be saved.
	 * @param isBatch     Flag to signal if the inserts in the DB should be batched
	 * @throws SvException
	 */
	public void saveObject(DbDataArray dbDataArray, Boolean isBatch) throws SvException {
		saveObject(dbDataArray, isBatch, this.autoCommit);
	}

	/**
	 * A method that saves a DbDataArray object to the Database.
	 * 
	 * @param dbDataArray The DbDataArray object which needs to be saved.
	 * @param isBatch     Flag to signal if the inserts in the DB should be batched
	 * @param autoCommit  if autocommit is true, the connection will be committed if
	 *                    no exception occurs. if exception occurs, a rollback will
	 *                    be issued
	 * @throws SvException
	 */
	public void saveObject(DbDataArray dbDataArray, Boolean isBatch, Boolean autoCommit) throws SvException {
		int currentBatchSize = batchSize;
		if (!isBatch)
			currentBatchSize = 1;

		try {
			this.dbSetAutoCommit(false);
			int start = 0;
			int end = currentBatchSize;
			while (start < dbDataArray.size()) {
				if (end > dbDataArray.size())
					end = dbDataArray.size();
				saveObjectImpl(new DbDataArray(dbDataArray.getItems().subList(start, end)), false);
				start = end;
				end = start + currentBatchSize;
			}

			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * A method that saves a DbDataArray object to the Database. If saving one of
	 * the objects fails, the whole object will be rolled back.
	 * 
	 * Note: This overload does not use JDBC batching. Performance will be affected
	 * if the list of objects is of same type where batching is applicable
	 * 
	 * @param dbDataArray The DbDataArray object which needs to be saved.
	 * @throws SvException
	 */
	public void saveObject(DbDataArray dbDataArray) throws SvException {
		saveObject(dbDataArray, false);
	}

	/**
	 * A method that saves a DbDataObject object to the Database.
	 * 
	 * @param dbDataObject The DbDataObject object which needs to be saved.
	 * @throws SvException
	 */
	public void saveObject(DbDataObject dbDataObject) throws SvException {
		saveObject(dbDataObject, this.autoCommit);
	}

	/**
	 * A method that saves a DbDataObject object to the Database, using a explicitly
	 * specified SQLConnection.
	 * 
	 * @param dbDataObject The DbDataObject which needs to be saved.
	 * @param autoCommit   if autocommit is true, the connection will be committed
	 *                     if no exception occurs. if exception occurs, a rollback
	 *                     will be issued
	 * @throws SvException
	 */

	public void saveObject(DbDataObject dbDataObject, Boolean autoCommit) throws SvException {
		DbDataArray dba = new DbDataArray();
		dba.addDataItem(dbDataObject);
		saveObject(dba, false, autoCommit);
	}

	/**
	 * Method for deleting an object from database. The delete method acts depending
	 * on the deletion type of the reference type of the object which is deleted
	 * 
	 * @param dbo  The object which should be deleted
	 * @param conn The database connection which is used for the operation
	 * @throws SQLException
	 * @throws SvException
	 */
	protected void deleteObjectImpl(DbDataObject dbo) throws SvException {
		DbDataArray dba = new DbDataArray();
		dba.addDataItem(dbo);
		deleteImpl(dba, false, false, null, null);
	}

	/**
	 * Method for deleting list of object from database. The delete method acts
	 * depending on the deletion type of the reference type of the object which is
	 * deleted
	 * 
	 * @param dba The list of objects object which should be deleted
	 * @throws SQLException
	 * @throws SvException
	 */
	public void deleteObjects(DbDataArray dba) throws SvException {
		deleteImpl(dba, false, false, null, null);
	}

	/**
	 * Method for deleting list of object from database. The delete method acts
	 * depending on the deletion type of the reference type of the object which is
	 * deleted
	 * 
	 * @param dba            The list of objects object which should be deleted
	 * @param deleteChildren Flag to ensure the method will cascade the deletion to
	 *                       child objects
	 * @param deleteLinks    Flag to ensure the method will cascade the deletion to
	 *                       link objects (It delete only the link, not the linking
	 *                       object!)
	 * @throws SvException
	 */
	public void deleteObjects(DbDataArray dba, boolean deleteChildren, boolean deleteLinks) throws SvException {
		deleteImpl(dba, deleteChildren, deleteLinks, null, null);
	}

	/**
	 * Method for deleting list of object from database. The delete method acts
	 * depending on the deletion type of the reference type of the object which is
	 * deleted
	 * 
	 * @param dba            The list of objects object which should be deleted
	 * @param deleteChildren Flag to ensure the method will cascade the deletion to
	 *                       child objects
	 * @param deleteLinks    Flag to ensure the method will cascade the deletion to
	 *                       link objects (It delete only the link, not the linking
	 *                       object!)
	 * @throws SvException
	 */
	public void deleteObjects(DbDataArray dba, boolean deleteChildren, boolean deleteLinks, boolean autoCommit)
			throws SvException {

		deleteImpl(dba, deleteChildren, deleteLinks, null, null);
		if (autoCommit)
			dbCommit();
	}

	void validateList(DbDataArray dba) throws SvException {
		if (dba.size() < 1)
			throw (new SvException(Sv.Exceptions.NULL_OBJECT, instanceUser, dba, null));
		DbDataObject dbt = getDbt(dba.get(0));
		for (DbDataObject dbo : dba.getItems()) {
			if (log4j.isDebugEnabled())
				log4j.trace("Deleting object with ID:" + dbo.getObjectId());

			if (dbo.getPkid() == 0L)
				throw (new SvException(Sv.Exceptions.OBJECT_NOT_PERSISTENT, instanceUser, dbo, null));

			if (dbo.getObjectType() != dbt.getObjectId())
				throw (new SvException(Sv.Exceptions.MULTI_TYPES_NOT_ALLOWED, instanceUser, dba, null));

		}
	}

	/**
	 * Method to perform the actual deletion of objects in the database. It allows
	 * deletion of dependent objects, such as child objects and links
	 * 
	 * @param dba            The list of objects which is subject of deletion
	 * @param deleteChildren Flag to allow deletion of child objects
	 * @param deleteLinks    Flat to allow deletion of links to other objects
	 * @param childTypes     Filter to allow only certain types of child objects to
	 *                       be deleted (not working atm)
	 * @param linkTypes      Filter to delete only certain types of links
	 * @throws SvException Any underlying exception raised by the svarog core
	 */
	protected void deleteImpl(DbDataArray dba, boolean deleteChildren, boolean deleteLinks, ArrayList<Long> childTypes,
			ArrayList<Long> linkTypes) throws SvException {

		// perform basic validation on the deletion
		validateList(dba);
		try {
			this.dbSetAutoCommit(false);
			DbDataObject dbt = getDbt(dba.get(0));
			// authorise the save operation
			if (!isAdmin() && !isSystem() && !hasDbtAccess(dbt, null, SvAccess.MODIFY))
				throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, instanceUser, dbt, SvAccess.MODIFY.toString()));

			// if an existing object is updated, make sure the object
			// can still be updated and was not changed in mean time
			HashMap<Long, Object[]> repoData = getRepoData(dbt, dba, deleteChildren, deleteLinks, linkTypes);
			for (DbDataObject dbo : dba.getItems()) {
				if (!repoData.containsKey(dbo.getObjectId()))
					throw (new SvException(Sv.Exceptions.OBJECT_NOT_FOUND, instanceUser, dbo, null));

			}

			int invalidatedRows = executeRepoUpdate(repoData);
			if (invalidatedRows != repoData.size())
				throw (new SvException(Sv.Exceptions.OBJECT_COUNT_ERROR, instanceUser, dba, null));
			// now clean the cache
			cacheCleanup(repoData, deleteLinks);

		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.SQL_ERR, instanceUser, dba, null, e));
		}
	}

	int executeRepoUpdateSQL(String schema, String repoName, List<Long> pkids) throws SvException, SQLException {
		if (pkids.size() < 1)
			return 0;

		PreparedStatement ps = null;
		DateTime dt_insert = new DateTime();
		StringBuilder sqlDel = null;
		try {
			Connection conn = this.dbGetConn();
			sqlDel = new StringBuilder(50);
			sqlDel.append(UPDATE + " " + schema + "." + repoName + " SET dt_delete=? WHERE PKID in (");
			for (int i = 0; i < pkids.size(); i++) {
				sqlDel.append("?,");
			}
			sqlDel.setLength(sqlDel.length() - 1);
			sqlDel.append(")");

			ps = conn.prepareStatement(sqlDel.toString());
			ps.setTimestamp(1, new Timestamp(dt_insert.getMillis() - 1));
			int paramPos = 2;
			for (Long l : pkids) {
				ps.setLong(paramPos++, l);
			}
			return ps.executeUpdate();
		} finally {
			closeResource((AutoCloseable) ps, instanceUser);
		}

	}

	/**
	 * Method to perform the actual database update to set the DT_DELETE to current
	 * timestamp
	 * 
	 * @param dbt      The object type descriptor
	 * @param repoData
	 * @return
	 * @throws SQLException
	 * @throws SvException
	 */
	int executeRepoUpdate(HashMap<Long, Object[]> repoData) throws SQLException, SvException {

		int deletedRows = 0;

		DbDataObject dbt = null;

		Iterator<Object[]> repoIt = repoData.values().iterator();
		List<Long> pkids = new ArrayList<Long>(repoData.size());
		String schema = null;
		String repoName = null;

		while (repoIt.hasNext()) {
			Object[] repo = repoIt.next();
			if (dbt == null || !dbt.getObjectId().equals(repo[3])) {
				dbt = getDbt((Long) repo[3]);
				// if the repo table is the same but the dbt has changed, just leave it
				if (!(dbt.getVal("schema").equals(schema) && dbt.getVal("repo_name").equals(repoName))) {
					// repo has changed, first commit the old result to the DB
					deletedRows = deletedRows + executeRepoUpdateSQL(schema, repoName, pkids);
					// then change the repo and clear the list of PKIDs
					repoName = (String) dbt.getVal("repo_name");
					schema = (String) dbt.getVal("schema");
					pkids.clear();
				}
			}
			pkids.add((Long) repo[0]);
		}
		// final update of the DB
		deletedRows = deletedRows + executeRepoUpdateSQL(schema, repoName, pkids);

		return deletedRows;

	}

	void cacheCleanup(HashMap<Long, Object[]> repoData, boolean deleteLinks) throws SvException {
		Iterator<Entry<Long, Object[]>> it = repoData.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Long, Object[]> item = it.next();
			cacheCleanup((Long) item.getValue()[1], (Long) item.getValue()[2], (Long) item.getValue()[3]);
			if (deleteLinks && svCONST.OBJECT_TYPE_LINK != (Long) item.getValue()[3])
				it.remove();
		}
		// TODO
		// now cleanup the cached links info

	}

	/**
	 * A method that saves a DbDataObject object to the Database.
	 * 
	 * @param dbDataObject The DbDataObject object which needs to be saved.
	 * @param autoCommit   Auto commit flag. If its true, the method will commit the
	 *                     operation to the database
	 * @throws SvException Any underlying exception that is raised by Svarog
	 */
	public void deleteObject(DbDataObject dbDataObject, Boolean autoCommit) throws SvException {

		try {
			this.dbSetAutoCommit(false);
			deleteObjectImpl(dbDataObject);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * A method that saves a DbDataObject object to the Database.
	 * 
	 * @param dbDataObject The DbDataObject object which needs to be saved.
	 * @throws SvException
	 */
	public void deleteObject(DbDataObject dbDataObject) throws SvException {
		deleteObject(dbDataObject, this.autoCommit);

	}

	/**
	 * Method for mass deletion of objects according to their parent object.
	 * 
	 * All objects and their children which have a system object id ( see
	 * {@link svCONST.MAX_SYS_OBJECT_ID}) can not be deleted nor affected by this
	 * method.
	 * 
	 * @param parentDbo          The parent object for which we want to delete the
	 *                           children
	 * @param childrenObjectType The object type ID of the children
	 * @throws SQLException
	 */
	protected void deleteObjectsByParentImpl(DbDataObject parentDbo, Long childrenObjectType) throws SvException {
		if (parentDbo.getObjectId().compareTo(svCONST.MAX_SYS_OBJECT_ID) < 0)
			throw (new SvException("system.error.no_obj_to_del", instanceUser, parentDbo, null));

		if (childrenObjectType == null || childrenObjectType == 0)
			throw (new SvException("system.error.delete_child_type_err", instanceUser, parentDbo, null));

		PreparedStatement ps = null;
		try {

			Connection conn = dbGetConn();
			DbDataObject dbt = getDbt(childrenObjectType);

			// if an existing object is updated, make sure the object
			// can still be updated and was not changed in mean time
			String schema = dbt.getVal("schema").toString();
			String repo_name = dbt.getVal("repo_name").toString();
			DateTime dt_insert = new DateTime();

			String delByParentSQL = UPDATE + " " + schema + "." + repo_name + " SET dt_delete=? WHERE parent_id=?"
					+ " and ? between dt_insert and dt_delete and object_type=?";

			log4j.trace("Invalidate the old one first");
			ps = conn.prepareStatement(delByParentSQL);
			ps.setTimestamp(1, new Timestamp(dt_insert.getMillis() - 1));
			ps.setLong(2, parentDbo.getObjectId());
			ps.setTimestamp(3, new Timestamp(dt_insert.getMillis() - 1));
			ps.setLong(4, childrenObjectType);

			int invalidatedRows = ps.executeUpdate();
			if (invalidatedRows > 0)
				DbCache.removeByParentId(childrenObjectType, parentDbo.getObjectId());
		} catch (SQLException e) {
			throw (new SvException("system.error.delete_byparent_err", instanceUser, parentDbo, null, e));
		} finally {
			closeResource((AutoCloseable) ps, instanceUser);
		}

	}

	/**
	 * Method for deleting a list childred by parent with option to auto commit or
	 * not
	 * 
	 * @param parentDbo          Reference to the parent object
	 * @param childrenObjectType ID of the children object type
	 * @param autoCommit         Flag to auto commit/rollback or not
	 * @throws SvException
	 */
	public void deleteObjectsByParent(DbDataObject parentDbo, Long childrenObjectType, Boolean autoCommit)
			throws SvException {
		try {
			this.dbSetAutoCommit(false);
			deleteObjectsByParentImpl(parentDbo, childrenObjectType);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * Method for deleting a list childred by parent. Deletion will be committed to
	 * the db if successful.
	 * 
	 * @param parentDbo          Reference to the parent object
	 * @param childrenObjectType ID of the children object type
	 * @throws SvException
	 */
	public void deleteObjectsByParent(DbDataObject parentDbo, Long childrenObjectType) throws SvException {
		deleteObjectsByParent(parentDbo, childrenObjectType, this.autoCommit);
	}

	/**
	 * Method for saving form specific data
	 * 
	 * @param objToSave The object we are saving
	 * @param conn      SQL Connection that is used
	 * @param dbt       Descriptor of the table to which we are saving data
	 * @param oldPkid   If this is an update, this value needs to be set to the PKID
	 *                  which we are updating
	 * @param newPkid   The new PKID of the object after the update
	 * @param status    The new status of the object
	 * @return Standard Svarog svCONST error codes
	 * @throws Exception
	 * @throws SQLException
	 */
	void saveFormData(DbDataObject objToSave, DbDataObject dbt, Long oldPkid, Long newPkid, String status)
			throws SvException {
		Integer resultCnt = 0;

		try (SvLob lob = new SvLob(this.dbGetConn()); SvReader svr = new SvReader(this)) {

			svr.instanceUser = svCONST.systemUser;

			DbDataArray objectFields = DbCache.getObjectsByParentId(dbt.getObjectId(), svCONST.OBJECT_TYPE_FIELD_SORT);
			String sql = getQryInsertTableData(dbt, objectFields, oldPkid != null && oldPkid != 0, true, false);
			log4j.debug("Executing SQL:" + sql);

			Connection conn = this.dbGetConn();

			int pCount = 1;
			try (PreparedStatement ps = conn.prepareStatement(sql)) {

				ps.setBigDecimal(pCount, new BigDecimal(newPkid));
				String fName = "";
				pCount++;
				for (DbDataObject dbf : objectFields.getItems()) {
					fName = (String) dbf.getVal("FIELD_NAME");
					if (fName.equals("PKID"))
						continue;
					Object value = objToSave.getVal(fName);
					validateFieldData(dbf, value, false);
					bindInsertQueryVars(ps, dbf, pCount, value, lob);

					pCount++;
				}
				if (oldPkid != 0)
					ps.setBigDecimal(pCount, new BigDecimal(oldPkid));

				resultCnt = ps.executeUpdate();
			}
			if (resultCnt != 1)
				throw (new SvException("system.error.sql_update_err", instanceUser, objToSave, dbt));

			DbDataObject formType = svr.getObjectById((Long) objToSave.getVal("FORM_TYPE_ID"),
					getDbt(svCONST.OBJECT_TYPE_FORM_TYPE), null, false);

			DbDataArray formFields = svr.getObjectsByLinkedId(formType.getObjectId(), svCONST.OBJECT_TYPE_FORM_TYPE,
					"FORM_FIELD_LINK", svCONST.OBJECT_TYPE_FORM_FIELD_TYPE, false, null, 0, 0);

			DateTime dt_insert = new DateTime();
			if (oldPkid != null && !oldPkid.equals(0L)) {
				String sqlUpdate = UPDATE + " " + SvConf.getDefaultSchema() + "." + SvConf.getMasterRepo()
						+ " set DT_DELETE=? where pkid in (SELECT fields.PKID FROM " + SvConf.getDefaultSchema() + "."
						+ SvConf.getMasterRepo() + " fields where fields.parent_id=? "
						+ " and fields.object_type=? and fields.dt_delete=?)";
				if (log4j.isDebugEnabled())
					log4j.debug("Executing SQL:" + sqlUpdate);

				try (PreparedStatement psInvalidate = conn.prepareStatement(sqlUpdate)) {
					psInvalidate.setTimestamp(1, new Timestamp(dt_insert.getMillis() - 1));
					psInvalidate.setLong(2, objToSave.getObjectId());
					psInvalidate.setLong(3, svCONST.OBJECT_TYPE_FORM_FIELD);
					psInvalidate.setTimestamp(4, SvConf.MAX_DATE_SQL);
					psInvalidate.executeUpdate();
				}

			}
			saveFormFields(objToSave, formType, oldPkid, dt_insert, formFields);

		} catch (Exception ex) {
			if (ex instanceof SvException)
				throw ((SvException) ex);
			else
				throw (new SvException("system.error.form_data_save_err", instanceUser, objToSave, dbt, ex));

		}

	}

	/**
	 * Method to save the form fields of the object to save in the database
	 * 
	 * @param objToSave The object whose fields should be saved
	 * @param formDbt   The form descriptor
	 * @param oldPkid   The old version id (PKID) of the object
	 * @param dt_insert The timestamp of the insert
	 * @param svr       SvReader instance to used for fetching the linked field
	 *                  objects
	 * @throws SvException
	 */
	void saveFormFields(DbDataObject objToSave, DbDataObject formDbt, Long oldPkid, DateTime dt_insert,
			DbDataArray formFields) throws SvException {

		String fieldName;
		String fVal;
		String fVal_1st;
		String fVal_2nd;
		DbDataArray fieldVals = new DbDataArray();
		for (DbDataObject dbf : formFields.getItems()) {
			fieldName = (String) dbf.getVal("LABEL_CODE");
			validateFieldData(dbf, objToSave.getVal(fieldName), false);

			if (objToSave.getVal(fieldName) == null) {
				continue;
			}
			fVal = (String) objToSave.getVal(fieldName).toString();
			Object obj = objToSave.getVal(fieldName + "_1st");
			fVal_1st = (String) (obj != null ? obj.toString() : null);
			obj = objToSave.getVal(fieldName + "_2nd");
			fVal_2nd = (String) (obj != null ? obj.toString() : null);

			validateFieldData(dbf, fVal_1st, true);
			validateFieldData(dbf, fVal_2nd, true);

			DbDataObject dbfVal = new DbDataObject();
			dbfVal.setObjectType(svCONST.OBJECT_TYPE_FORM_FIELD);
			dbfVal.setParentId(objToSave.getObjectId());
			dbfVal.setVal("form_object_id", objToSave.getObjectId());
			dbfVal.setVal("field_type_id", dbf.getObjectId());
			dbfVal.setVal("value", fVal.toString());
			dbfVal.setVal("first_check", fVal_1st != null ? fVal_1st.toString() : null);
			dbfVal.setVal("second_check", fVal_2nd != null ? fVal_2nd.toString() : null);
			fieldVals.addDataItem(dbfVal);

		}
		if (fieldVals.getItems().size() > 0)
			this.saveObject(fieldVals, true, false);

	}

	/**
	 * Method to auto instantiate an array of form types.
	 * 
	 * @param formFields
	 * @param formTypeIds
	 * @param foundFormTypes
	 * @param formArray
	 * @param overrideAutoinstance
	 * @param parentId
	 * @throws SvException
	 */
	DbDataArray autoInstanceForms(ArrayList<Long> formTypeIds, Long parentId) throws SvException {
		if (log4j.isDebugEnabled())
			log4j.trace("Auto instance of form types " + formTypeIds.toString() + ", under parent id:" + parentId);
		DbDataObject currentObject = null;
		SvReader svr = null;
		DbDataArray formFields = null;
		DbDataArray dba = new DbDataArray();
		LinkedHashMap<String, Object> templateMap = null;
		ArrayList<DbDataObject> autoForms = new ArrayList<DbDataObject>();
		try {
			// move the instantiation of the reader out of the loop to fix the
			// connection leak
			svr = new SvReader(this);
			for (Long typeId : formTypeIds) {
				DbDataObject formTypeDbt = getFormType(typeId);
				if (formTypeDbt != null) {
					// auto instantiate a form if it doesn't exists

					if ((Boolean) formTypeDbt.getVal("multi_entry") == false
							&& (Boolean) formTypeDbt.getVal("AUTOINSTANCE_SINGLE")) {
						currentObject = new DbDataObject();
						currentObject.setObjectType(svCONST.OBJECT_TYPE_FORM);
						currentObject.setParentId(parentId);
						currentObject.setVal("LABEL_CODE", I18n.getText((String) formTypeDbt.getVal("LABEL_CODE")));
						currentObject.setVal("VALUE", null);
						currentObject.setVal("FIRST_CHECK", null);
						// currentObject.setVal("LABEL_CODE",
						// I18n.getText((String)
						// formTypeDbt.getVal("LABEL_CODE")));
						currentObject.setVal("FORM_TYPE_ID", typeId);
						if ((Boolean) formTypeDbt.getVal("mandatory_base_value"))
							currentObject.setVal("FORM_VALIDATION", false);
						else
							currentObject.setVal("FORM_VALIDATION", true);

						autoForms.add(currentObject);
						// switch this user to run under system in order to get
						// the form config
						svr.instanceUser = svCONST.systemUser;
						formFields = svr.getObjectsByLinkedId(typeId, svCONST.OBJECT_TYPE_FORM_TYPE, "FORM_FIELD_LINK",
								svCONST.OBJECT_TYPE_FORM_FIELD_TYPE, false, null, 0, 0);
						if (formFields != null) {
							for (DbDataObject field : formFields.getSortedItems("SORT_ORDER")) {
								currentObject.setVal((String) field.getVal("label_code"), null);
								currentObject.setVal((String) field.getVal("label_code") + "_1st", null);
								currentObject.setVal((String) field.getVal("label_code") + "_2nd", null);
							}

						}
					}

				} else {
					DbDataObject dbo = new DbDataObject();
					dbo.setVal("FORM_TYPE_IDS", formTypeIds);
					throw (new SvException("system.error.form_type_err", instanceUser, dbo, formFields));
				}
			}
			dba.setItems(autoForms);

			saveObject(dba, true, true);
		} finally {
			if (svr != null)
				svr.release();
		}
		return dba;
	}

	public HashMap<Long, Long> cloneForms(DbDataObject dbo, Long oldOID) throws SvException {
		HashMap<Long, Long> oldNewOIDPairs = new HashMap<Long, Long>();
		String sqlSelect = "select distinct (form_type_id) from vsvarog_form  sv where sv.parent_id=? and dt_delete=?";

		try (SvReader svr = new SvReader(this); PreparedStatement ps = this.dbGetConn().prepareStatement(sqlSelect)) {
			ps.setLong(1, oldOID);
			ps.setTimestamp(2, new Timestamp(SvConf.MAX_DATE.getMillis()));
			ArrayList<Long> formTypeIds = new ArrayList<Long>();
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					formTypeIds.add(rs.getLong(1));
				}
			}
			Long currentOID = 0L;
			DbDataArray forms = svr.getFormsByParentId(oldOID, formTypeIds, null, null, true);
			for (DbDataObject child : forms.getItems()) {
				currentOID = child.getObjectId();
				child.setObjectId(0L);
				child.setPkid(0L);
				child.setParentId(dbo.getObjectId());
				saveObject(child, false);
				oldNewOIDPairs.put(currentOID, child.getObjectId());
			}
		} catch (SQLException e) {
			throw (new SvException("system.error.children_clone_err", instanceUser, dbo, null, e));
		}
		return oldNewOIDPairs;
	}

	public HashMap<Long, Long> cloneObjects(DbDataObject dbo, Long oldOID, Long objectTypeToClone) throws SvException {

		HashMap<Long, Long> oldNewOIDPairs = new HashMap<Long, Long>();
		DbDataArray children = null;
		Long currentOID = 0L;
		SvReader svr = new SvReader(this);
		try {
			children = svr.getObjectsByParentId(oldOID, objectTypeToClone, null, 0, 0);
			for (DbDataObject child : children.getItems()) {
				currentOID = child.getObjectId();
				child.setObjectId(0L);
				child.setPkid(0L);
				child.setParentId(dbo.getObjectId());
				saveObject(child, false);
				oldNewOIDPairs.put(currentOID, child.getObjectId());
			}
		} finally {
			svr.release();
		}
		return oldNewOIDPairs;
	}

	/**
	 * Method to cleanup the linked objects cache based on link object.
	 * 
	 * @param linkDbo The link object
	 */
	static void removeLinkCache(DbDataObject linkDbo) {
		DbDataObject dbl = getLinkType((Long) linkDbo.getVal("LINK_TYPE_ID"));
		DbCache.invalidateLinkCache((Long) linkDbo.getVal("LINK_OBJ_ID_1"), dbl.getObjectId(),
				(Long) dbl.getVal("LINK_OBJ_TYPE_2"));
		DbCache.invalidateLinkCache((Long) linkDbo.getVal("LINK_OBJ_ID_2"), dbl.getObjectId(),
				(Long) dbl.getVal("LINK_OBJ_TYPE_1"));
	}

	/**
	 * Method clone the link objects from source to cloned child objects
	 * 
	 * @param objectId        The object id of the source object subject to cloning
	 * @param childObjectType The type of the children subject to cloning
	 * @param oldNewOIDPairs  The old/new ID pairs mapping the source to cloned
	 *                        child objects
	 * @throws SvException
	 */
	void cloneLinkObjects(Long objectId, Long childObjectType, HashMap<Long, Long> oldNewOIDPairs) throws SvException {

		DbDataObject linkDbt = getDbt(svCONST.OBJECT_TYPE_LINK);
		DbDataObject linkTypeDbt = getDbt(svCONST.OBJECT_TYPE_LINK_TYPE);
		DbDataObject childObjectDbt = getDbt(childObjectType);

		String findLinksSql = "select vl.*  from " + linkDbt.getVal("SCHEMA") + ".v" + linkDbt.getVal("TABLE_NAME")
				+ " vl join " + childObjectDbt.getVal("SCHEMA") + ".v" + childObjectDbt.getVal("TABLE_NAME")
				+ " app on  VL.LINK_OBJ_ID_1=app.object_id "
				+ "where vl.dt_delete=? and app.dt_delete=? and app.parent_id=? and "
				+ "link_type_id in (select object_id from " + linkTypeDbt.getVal("SCHEMA") + ".v"
				+ linkTypeDbt.getVal("TABLE_NAME") + " where" + "(link_obj_type_1=?) and dt_delete=?)" + "union "
				+ "select vl.*  from " + linkDbt.getVal("SCHEMA") + ".v" + linkDbt.getVal("TABLE_NAME") + " vl join "
				+ childObjectDbt.getVal("SCHEMA") + ".v" + childObjectDbt.getVal("TABLE_NAME")
				+ " app on  VL.LINK_OBJ_ID_2=app.object_id "
				+ "where vl.dt_delete=? and app.dt_delete=? and app.parent_id=? and "
				+ "link_type_id in (select object_id from " + linkTypeDbt.getVal("SCHEMA") + ".v"
				+ linkTypeDbt.getVal("TABLE_NAME") + " where" + "(link_obj_type_2=?) and dt_delete=?)";
		;

		DbDataArray dbarr = new DbDataArray();

		try (PreparedStatement ps = this.dbGetConn().prepareStatement(findLinksSql)) {
			ps.setTimestamp(1, new Timestamp(SvConf.MAX_DATE.getMillis()));
			ps.setTimestamp(2, new Timestamp(SvConf.MAX_DATE.getMillis()));
			ps.setLong(3, objectId);
			ps.setLong(4, childObjectType);
			ps.setTimestamp(5, new Timestamp(SvConf.MAX_DATE.getMillis()));
			ps.setTimestamp(6, new Timestamp(SvConf.MAX_DATE.getMillis()));
			ps.setTimestamp(7, new Timestamp(SvConf.MAX_DATE.getMillis()));
			ps.setLong(8, objectId);
			ps.setLong(9, childObjectType);
			ps.setTimestamp(10, new Timestamp(SvConf.MAX_DATE.getMillis()));
			log4j.debug("Executing SQL:" + findLinksSql);
			try (ResultSet rs = ps.executeQuery()) {
				Boolean isFound = false;
				while (rs.next()) {
					DbDataObject dbo = new DbDataObject();
					isFound = false;
					dbo.setObjectType(rs.getLong("OBJECT_TYPE"));
					dbo.setVal("LINK_TYPE_ID", rs.getLong("LINK_TYPE_ID"));
					Long oid2 = rs.getLong("LINK_OBJ_ID_2");
					Long oid1 = rs.getLong("LINK_OBJ_ID_1");
					
					if (oid1 != null && oldNewOIDPairs.containsKey(oid1)) {
						dbo.setVal("LINK_OBJ_ID_1", oldNewOIDPairs.get(oid1));
						dbo.setVal("LINK_OBJ_ID_2", oid2);
						isFound = true;
					} else if (oid2 != null && oldNewOIDPairs.containsKey(oid2)) {
						dbo.setVal("LINK_OBJ_ID_2", oldNewOIDPairs.get(oid2));
						dbo.setVal("LINK_OBJ_ID_1", oid1);
						isFound = true;
					}
					if (isFound)
						dbarr.addDataItem(dbo);
				}
			}
			if (dbarr != null && dbarr.getItems().size() > 0)
				this.saveObject(dbarr, true, false);
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.SQL_ERR, instanceUser, dbarr, null, e));
		}

	}

	/**
	 * Method that clones the child objects of the old parent under the newly cloned
	 * object
	 * 
	 * @param dbo                The newly cloned object
	 * @param oldOID             The object ID of the original object, from which we
	 *                           want to clone the child objects
	 * @param cloneChildrenLinks Flag to enable/disable cloning of the links between
	 *                           the child object and other objects
	 * @param ignoredObjectTypes List of Long value(s) of object type(s) that should
	 *                           be ignored through the cloning process
	 * @throws SvException
	 */
	void cloneChildren(DbDataObject dbo, Long oldOID, Boolean cloneChildrenLinks, ArrayList<Long> ignoredObjectTypes)
			throws SvException {

		String sqlSelect = "select distinct object_type from " + repoDbt.getVal("SCHEMA")
				+ ".svarog sv where sv.parent_id=? and dt_delete=?";
		try (PreparedStatement ps = this.dbGetConn().prepareStatement(sqlSelect);) {
			ps.setLong(1, oldOID);
			ps.setTimestamp(2, new Timestamp(SvConf.MAX_DATE.getMillis()));
			try (ResultSet rs = ps.executeQuery()) {
				ArrayList<Long> objectTypesToClone = new ArrayList<Long>();
				// hold the old/new OIDs, just in case we need to clone the links
				HashMap<Long, Long> oldNewOIDPairs = null;
				while (rs.next()) {
					Long tType = rs.getLong(1);
					if (ignoredObjectTypes == null)
						objectTypesToClone.add(tType);
					else if (!ignoredObjectTypes.contains(tType))
						objectTypesToClone.add(tType);

				}
				closeResource((AutoCloseable) rs, instanceUser);
				closeResource((AutoCloseable) ps, instanceUser);

				for (Long objectTypeToClone : objectTypesToClone) {
					if (objectTypeToClone.equals(svCONST.OBJECT_TYPE_FORM))
						oldNewOIDPairs = cloneForms(dbo, oldOID);
					else
						oldNewOIDPairs = cloneObjects(dbo, oldOID, objectTypeToClone);
					if (cloneChildrenLinks)
						cloneLinkObjects(oldOID, objectTypeToClone, oldNewOIDPairs);
				}
			}

		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.SQL_ERR, instanceUser, dbo, null));
		}
	}

	/**
	 * Method for cloning a DbDataObject into new instance.
	 * 
	 * @param dbo                The object to be cloned
	 * @param cloneChildren      Flag to enable/disable cloning of child objects
	 * @param cloneChildrenLinks Flag to enable/disable cloning of children links to
	 *                           other objects
	 * @param ignoredObjectTypes List of Long value(s) of object type(s) that should
	 *                           be ignored through the cloning process
	 * @return The cloned DbDataObject
	 * @throws SvException
	 */
	DbDataObject cloneObjectImpl(DbDataObject dbo, Boolean cloneChildren, Boolean cloneChildrenLinks,
			ArrayList<Long> ignoredObjectTypes) throws SvException {
		if (dbo.isReadOnly())
			throw (new SvException("system.error.read_only_clone_forbidden", instanceUser, dbo, null));

		SvReader svr = new SvReader(this);
		DbDataObject oldDbo = null;
		DbDataObject newObj = null;
		try {
			oldDbo = svr.getObjectById(dbo.getObjectId(), getDbt(dbo.getObjectType()), null);
			newObj = new DbDataObject(dbo.getObjectType());
			newObj.setValuesMap(oldDbo.getValuesMap());
			newObj.setParentId(oldDbo.getParentId());
			newObj.setStatus(oldDbo.getStatus());
			if (newObj != null) {

				saveObject(newObj, false);
				if (cloneChildren) {
					cloneChildren(newObj, oldDbo.getObjectId(), cloneChildrenLinks, ignoredObjectTypes);
				}
			}
		} finally {
			svr.release();
		}
		return newObj;
	}

	/**
	 * Method for cloning a DbDataObject into new instance. If there is no exception
	 * the change is committed, otherwise rolled back.
	 * 
	 * @param dbo                The object to be cloned
	 * @param cloneChildren      Flag to enable/disable cloning of child objects
	 * @param cloneChildrenLinks Flag to enable/disable cloning of children links to
	 *                           other objects
	 * @return The cloned DbDataObject
	 * @throws SvException
	 */
	public DbDataObject cloneObject(DbDataObject dbo, Boolean cloneChildren, Boolean cloneChildrenLinks)
			throws SvException {
		return cloneObject(dbo, cloneChildren, cloneChildrenLinks, this.autoCommit);
	}

	/**
	 * Method for cloning a DbDataObject into new instance. If there is no exception
	 * the change is committed, otherwise rolled back.
	 * 
	 * @param dbo                The object to be cloned
	 * @param cloneChildren      Flag to enable/disable cloning of child objects
	 * @param cloneChildrenLinks Flag to enable/disable cloning of children links to
	 *                           other objects
	 * @param ignoredObjectTypes List of Long value(s) of object type(s) that should
	 *                           be ignored through the cloning process
	 * @return The cloned DbDataObject
	 * @throws SvException
	 */
	public DbDataObject cloneObject(DbDataObject dbo, Boolean cloneChildren, Boolean cloneChildrenLinks,
			ArrayList<Long> ignoredObjectTypes) throws SvException {
		return cloneObject(dbo, cloneChildren, cloneChildrenLinks, ignoredObjectTypes, this.autoCommit);
	}

	/**
	 * Method for cloning a DbDataObject into new instance, with option to control
	 * the transaction
	 * 
	 * @param dbo                The object to be cloned
	 * @param cloneChildren      Flag to enable/disable cloning of child objects
	 * @param cloneChildrenLinks Flag to enable/disable cloning of children links to
	 *                           other objects
	 * @return The cloned DbDataObject
	 * @throws SvException
	 */
	public DbDataObject cloneObject(DbDataObject dbo, Boolean cloneChildren, Boolean cloneChildrenLinks,
			Boolean autoCommit) throws SvException {
		return cloneObject(dbo, cloneChildren, cloneChildrenLinks, null, autoCommit);
	}

	/**
	 * Method for cloning a DbDataObject into new instance, with option to control
	 * the transaction
	 * 
	 * @param dbo                The object to be cloned
	 * @param cloneChildren      Flag to enable/disable cloning of child objects
	 * @param cloneChildrenLinks Flag to enable/disable cloning of children links to
	 *                           other objects
	 * @param ignoredObjectTypes List of Long value(s) of object type(s) that should
	 *                           be ignored through the cloning process
	 * @return The cloned DbDataObject
	 * @throws SvException
	 */
	public DbDataObject cloneObject(DbDataObject dbo, Boolean cloneChildren, Boolean cloneChildrenLinks,
			ArrayList<Long> ignoredObjectTypes, Boolean autoCommit) throws SvException {

		DbDataObject cloneDbo = null;
		try {
			this.dbSetAutoCommit(false);
			cloneDbo = cloneObjectImpl(dbo, cloneChildren, cloneChildrenLinks, ignoredObjectTypes);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
		return cloneDbo;
	}

}
