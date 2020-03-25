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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataObject;

/**
 * Class to execute an action configured in the Rule Engine, which is in the
 * form of a SQL string
 * 
 * @author PR01
 *
 */
public class ActionSQL extends SvCore {

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system privileges.
	 * 
	 * @param sharedSvCore
	 *            The shared SvCore see {@link SvCore}
	 * 
	 * @throws SvException
	 *             Pass through underlying exceptions
	 */
	ActionSQL(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Method for executing an SQL based action of the rule engine
	 * 
	 * @param sql
	 *            SQL query to be executed
	 * @param dbo
	 *            Database object the rule action is executed on
	 * @param exec_id
	 *            Id of the rule execution saved in database.
	 * @param calltype
	 *            Execute query, get result or execute stored procedure.
	 * @param rettype
	 *            In case of stored procedure type of return parameter to be
	 *            binded.
	 * @param arrayType
	 *            The array type that should be used for returning values (SQL
	 *            type)
	 * @param params
	 *            Hashmap containing additional parameters passed to the
	 *            bindParams method
	 * @param autoCommit
	 *            Flag to enable/disable auto commit/rollback on
	 *            success/exception
	 * @return Object result of execution, can be of different type
	 * @throws SvException
	 *             Pass through underlying exceptions
	 */
	public Object execute(String sql, DbDataObject dbo, Long exec_id, String calltype, Long rettype, String arrayType,
			Map<Object, Object> params, Boolean autoCommit) throws SvException {
		Object retVal = null;
		try {
			this.dbSetAutoCommit(false);
			retVal = executeImpl(sql, dbo, exec_id, calltype, rettype, arrayType, params);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
		return retVal;
	}

	/**
	 * Prepare statement and execute appropriate actions depending on call type.
	 * 
	 * @param sql
	 *            SQL query to be executed
	 * @param dbo
	 *            Database object the rule action is executed on
	 * @param exec_id
	 *            Id of the rule execution saved in database.
	 * @param calltype
	 *            Execute query, get result or execute stored procedure.
	 * @param rettype
	 *            In case of stored procedure type of return parameter to be
	 *            binded.
	 * @param arrayType
	 *            The array type that should be used for returning values (SQL
	 *            type)
	 * @param params
	 *            Hashmap containing additional parameters passed to the
	 *            bindParams method
	 * @return Object result of execution, can be of different type
	 * @throws SvException
	 *             Throws system.error.re_action_sql_err exception to signal sql
	 *             failure
	 * 
	 */
	Object executeImpl(String sql, DbDataObject dbo, Long exec_id, String calltype, Long rettype, String arrayType,
			Map<Object, Object> params) throws SvException {
		Object result = null;
		CallableStatement cst = null;
		ResultSet rs = null;
		try {
			cst = this.dbGetConn().prepareCall(sql);
			prepareGet(sql, dbo, exec_id, rettype, arrayType, params, cst);
			if (calltype.equals("Execute")) {
				result = cst.executeUpdate();
			} else if (calltype.equals("Query")) {
				rs = cst.executeQuery();
				if (rs.next()) {
					result = rs.getObject(1);
				}
			} else if (calltype.equals("Procedure")) {
				cst.execute();
				result = cst.getObject(1);
			}
		} catch (SQLException e) {
			throw (new SvException("system.error.re_action_sql_err", instanceUser, dbo, sql, e));
		} finally {
			closeResource((AutoCloseable) rs, instanceUser);
			closeResource((AutoCloseable) cst, instanceUser);
		}
		return result;
	}

	/**
	 * Prepare statement using SQL query input string with DbDataObject
	 * properties as bind parameters.
	 * 
	 * @param sql
	 *            SQL query to be executed
	 * @param dbo
	 *            Database object the rule action is executed on
	 * @param exec_id
	 *            Id of the rule execution saved in database.
	 * @param rettype
	 *            In case of stored procedure type of return parameter to be
	 *            binded.
	 * @param arrayType
	 *            The array type that should be used for returning values (SQL
	 *            type)
	 * @param params
	 *            Hashmap containing additional parameters passed to the
	 *            bindParams method
	 * @return CallableStatement
	 * @throws SvException
	 *             Throws system.error.re_action_sql_err in case of SQL failure
	 * 
	 */
	private void prepareGet(String sql, DbDataObject dbo, Long exec_id, Long rettype, String arrayType,
			Map<Object, Object> params, CallableStatement cst) throws SvException {

		String pattern = "(\\{)(.*?)(\\})";
		ArrayList<String> fieldNames = getFieldNames(sql, pattern);
		sql = "{ ? = " + sql.replaceAll(pattern, "?") + "}";

		int offset;
		try {
			if (sql.toLowerCase().startsWith("call")) {
				if (rettype == java.sql.Types.ARRAY) {
					cst.registerOutParameter(1, rettype.intValue(), arrayType);
				} else {
					cst.registerOutParameter(1, rettype.intValue());
				}
				offset = 2;
			} else {
				offset = 1;
			}
			bindParams(cst, dbo, exec_id, fieldNames, offset, params);
		} catch (SQLException e) {
			throw (new SvException("system.error.re_action_sql_err", instanceUser, dbo, sql, e));
		}
	}

	/**
	 * Parse SQL query to extract field names to be binded in prepared
	 * statement.
	 * 
	 * @param sql
	 *            SQL query template
	 * @param pattern
	 *            Regular expression pattern for matching results
	 * @return String array list with results matched
	 * 
	 */
	private ArrayList<String> getFieldNames(String sql, String pattern) {
		ArrayList<String> fieldNames = new ArrayList<String>();
		Matcher matcher = Pattern.compile(pattern).matcher(sql);
		while (matcher.find()) {
			fieldNames.add(matcher.group(2));
		}
		return fieldNames;
	}

	/**
	 * Loop field names from list and bind CallableStatement parameters with
	 * appropriate predefined field values from database object
	 * 
	 * @param cst
	 *            CallableStatement to bind parameters for
	 * @param dbo
	 *            DbDataObject to get values from
	 * @param exec_id
	 *            Rule execution id
	 * @param fieldNames
	 *            List of field names for binding
	 * @param offset
	 *            Binding parameters start index
	 * @param params
	 *            The map of key/values used as parameters
	 * @return CallableStatement with parameters binded
	 * @throws SQLException
	 *             Exception which is result of JDBC parameter binding
	 * 
	 */
	private void bindParams(CallableStatement cst, DbDataObject dbo, Long exec_id, ArrayList<String> fieldNames,
			int offset, Map<Object, Object> params) throws SQLException {
		String currentField;
		for (int i = 0; i < fieldNames.size(); i++) {
			currentField = fieldNames.get(i).toLowerCase();
			if (currentField.equals("pkid"))
				cst.setLong(i + offset, dbo.getPkid());
			else if (currentField.equals("object_id"))
				cst.setLong(i + offset, dbo.getObjectId());
			else if (currentField.equals("parent_id"))
				cst.setLong(i + offset, dbo.getParentId());
			else if (currentField.equals("object_type"))
				cst.setLong(i + offset, dbo.getObjectType());
			else if (currentField.equals("dt_insert"))
				cst.setTimestamp(i + offset, new Timestamp(dbo.getDtInsert().getMillis() - 1));
			else if (currentField.equals("dt_delete"))
				cst.setTimestamp(i + offset, new Timestamp(dbo.getDtDelete().getMillis() - 1));
			else if (currentField.equals("status"))
				cst.setString(i + offset, dbo.getStatus());
			else if (currentField.equals("exec_id"))
				cst.setLong(i + offset, exec_id);
			else if (params.containsKey(currentField)) {
				bindParamByType(cst, i + offset, params.get(currentField));
			}
		}

	}

	/**
	 * Method to bind a parameter to the callable statement at index
	 * parameterIndex
	 * 
	 * @param cst
	 *            The callable statement
	 * @param parameterIndex
	 *            The index of parameter
	 * @param value
	 *            The object holding the value
	 * @throws SQLException
	 *             Pass through SQL excetpions related to the binding
	 */
	private void bindParamByType(CallableStatement cst, int parameterIndex, Object value) throws SQLException {
		if (value instanceof Long) {
			cst.setLong(parameterIndex, (long) value);
		}
		if (value instanceof String) {
			cst.setString(parameterIndex, (String) value);
		}
		if (value instanceof DateTime) {
			cst.setTimestamp(parameterIndex, new Timestamp(((DateTime) value).getMillis()));
		}
	}
}
