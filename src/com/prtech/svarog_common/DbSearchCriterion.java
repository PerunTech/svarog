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
package com.prtech.svarog_common;

import java.util.ArrayList;
import java.util.ResourceBundle;

import org.locationtech.jts.geom.Envelope;

import com.prtech.svarog.SvConf;
import com.prtech.svarog.SvCore;
import com.prtech.svarog.SvException;
import com.prtech.svarog.svCONST;
import com.prtech.svarog_interfaces.ISvDatabaseIO;

/**
 * Basic single criterion used to generate the WHERE clause in the final SQL
 * query string generated by the DbQueryObject
 * 
 * @author PR01
 *
 */
public class DbSearchCriterion extends DbSearch {

	@Deprecated
	ArrayList<String> orderByFields = new ArrayList<String>();

	/**
	 * Enumeration containing all different types of comparison operands.
	 * 
	 * @author XPS13
	 *
	 */
	public enum DbCompareOperand {
		GREATER, LESS, EQUAL, GREATER_EQUAL, LESS_EQUAL, NOTEQUAL, BETWEEN, LIKE, ILIKE, ISNULL, BBOX, DBLINK, DBLINK_REVERSE, IN_SUBQUERY, IN_LIST
	};

	/**
	 * Default logic operand between this and the next criterion
	 */
	DbLogicOperand nextCritOperand = DbLogicOperand.AND;

	/**
	 * Flag for logical negation
	 */
	Boolean notPrefix = false;

	/**
	 * The field name which should be used in the comparison
	 */
	String fieldName;

	/**
	 * If the type of operand is SUBQUERY
	 */
	DbQueryObject inSubQuery = null;

	/**
	 * If the type of operand is SUBQUERY
	 */
	ArrayList<Object> inList = null;

	/**
	 * The second fieldName is used only for the BETWEEN operand If there is a
	 * second field, only the first compare value will be used like:
	 * 
	 * VALUE BETWEEN FIELD1 and FIELD2
	 */
	String fieldName2;

	/**
	 * The right-hand value to run the comparison against
	 */
	Object compareValue;

	/**
	 * The logic operand to use for the comparison
	 */
	DbCompareOperand operand;

	/**
	 * Variable to store info if the criterion is using specific free text where
	 */
	String freeTextWhere = "";

	/**
	 * If the comparison is database column against another database column
	 */
	boolean isField2FieldJoin = false;

	/**
	 * In case the field2field flag is true, than the left and right fields are
	 * taken into account
	 */
	String leftField = null;

	/**
	 * The right hand side field
	 */
	String rightField = null;

	/**
	 * The second compare value is used only for the BETWEEN operand, only if
	 * the second field name is empty. Example:
	 * 
	 * FIELD1 BETWEEN VALUE1 and VALUE2
	 */
	Object compareValue2;

	public DbSearchCriterion() {
	}

	/**
	 * Constructor using left and right fields as part of the where. Useful for
	 * supporting a join
	 * 
	 * @param leftField
	 *            The left hand side svarog field
	 * @param operand
	 *            The comparison operand as per {@link #DbSearchCriterion
	 *            .DbCompareOperand}
	 * @param rightField
	 *            The right hand side svarog field
	 * @throws SvException
	 */
	public DbSearchCriterion(String leftField, DbCompareOperand operand, String rightField, boolean isField2FieldJoin)
			throws SvException {
		this.leftField = leftField;
		this.rightField = rightField;
		this.operand = operand;
		if (leftField == null || rightField == null)
			throw (new SvException("system.error.dbsearch_missing_value", svCONST.systemUser, null, this));
		this.isField2FieldJoin = isField2FieldJoin;
	}

	/**
	 * WARNING: This constructor will allow your code to be used as basis for
	 * SQL Injection. Avoid it unless you know what you are doing
	 * 
	 * @param freeTextWhere
	 *            Free text SQL string to be used as part of the where clause
	 * @param compareValue
	 *            The first value parameter to be used for the prepared
	 *            statement
	 * @param compareValue2
	 *            The second value parameter to be used for the prepared
	 *            statement
	 */
	public DbSearchCriterion(String freeTextWhere, Object compareValue, Object compareValue2) {
		this.freeTextWhere = freeTextWhere;
		this.compareValue = compareValue;
		this.compareValue2 = compareValue2;
	}

	/**
	 * Default constructor taking a field name, operand and compare value.
	 * 
	 * @param fieldName
	 *            The field name to be used in the where clause
	 * @param operand
	 *            The comparison operand as per {@link #DbSearchCriterion
	 *            .DbCompareOperand}
	 * @param compareValue
	 *            The value to be compared against
	 * @throws SvException
	 */
	public DbSearchCriterion(String fieldName, DbCompareOperand operand, Object compareValue) throws SvException {
		this(fieldName, operand);
		this.compareValue = compareValue;
		if (compareValue == null)
			throw (new SvException("system.error.dbsearch_missing_value", svCONST.systemUser, null, this));
	}

	/**
	 * Constructor without compare value, useful for ISNULL
	 * 
	 * @param fieldName
	 *            The field name to be used in the where clause
	 * @param operand
	 *            The comparison operand as per {@link #DbSearchCriterion
	 *            .DbCompareOperand}
	 * @throws SvException
	 */
	public DbSearchCriterion(String fieldName, DbCompareOperand operand) throws SvException {
		if (fieldName == null || operand == null)
			throw (new SvException("system.error.dbsearch_missing_value", svCONST.systemUser, null, this));
		this.fieldName = fieldName;
		this.operand = operand;
	}

	public DbSearchCriterion(String fieldName, DbCompareOperand operand, Object compareValue,
			DbLogicOperand nextCritOperand) throws SvException {
		this(fieldName, operand, compareValue);
		this.nextCritOperand = nextCritOperand;
	}

	public DbSearchCriterion(String fieldName, DbCompareOperand operand, Object compareValue, Object compareValue2)
			throws SvException {
		this(fieldName, operand, compareValue);
		this.compareValue2 = compareValue2;

	}

	public DbSearchCriterion(String fieldName, DbCompareOperand operand, Object compareValue, Object compareValue2,
			Boolean notPrefix) throws SvException {
		this(fieldName, operand, compareValue);
		this.compareValue2 = compareValue2;
		this.notPrefix = notPrefix;
	}

	@Override
	public ArrayList<Object> getSQLParamVals() throws SvException {
		ArrayList<Object> arr = new ArrayList<Object>();
		if (!isField2FieldJoin) {
			// TODO Auto-generated method stub
			if (freeTextWhere != null && !freeTextWhere.equals("")) {
				arr.add(compareValue);
				if (compareValue2 != null)
					arr.add(compareValue2);

			} else {
				if (operand != DbCompareOperand.ISNULL && operand != DbCompareOperand.BBOX
						&& operand != DbCompareOperand.IN_SUBQUERY)
					if (operand.equals(DbCompareOperand.ILIKE))
						arr.add(compareValue.toString().toUpperCase());
					else
						arr.add(compareValue);
				if ((operand == DbCompareOperand.BETWEEN && (fieldName2 == null || fieldName2.equals("")))
						|| operand == DbCompareOperand.DBLINK || operand == DbCompareOperand.DBLINK_REVERSE)
					arr.add(compareValue2);
				if (operand == DbCompareOperand.BBOX) {
					if (compareValue instanceof Envelope) {
						Envelope env = (Envelope) compareValue;
						arr.add(env.getMinX());
						arr.add(env.getMinY());
						arr.add(env.getMaxX());
						arr.add(env.getMaxY());
					} else
						throw (new SvException("system.error.dbsc_missing_fields", svCONST.systemUser, null, this));
				} else if (operand == DbCompareOperand.IN_SUBQUERY) {
					arr.addAll(inSubQuery.getSQLParamVals());
				}
			}
		}
		return arr;
	}

	@Override
	public String getSQLExpression() throws SvException {

		return getSQLExpression("", "");
	}

	@Override
	@Deprecated
	public String getSQLExpression(String repoPrefix, String tblPrefix) throws SvException {

		ResourceBundle sqlKw = SvConf.getSqlkw();
		String retval = "";
		if (freeTextWhere != null && !freeTextWhere.equals(""))
			return freeTextWhere;

		String tmpRPrefix = "";
		String tmpTPrefix = "";
		if ((this.fieldName == null || this.operand == null) && (!isField2FieldJoin))
			throw (new SvException("system.error.dbsc_missing_fields", svCONST.systemUser, null, this));

		DbCompareOperand tmpOperand = operand;

		if (repoPrefix != null && !repoPrefix.equals(""))
			tmpRPrefix = repoPrefix + ".";

		if (tblPrefix != null && !tblPrefix.equals(""))
			tmpTPrefix = tblPrefix + ".";

		String fullFieldName = fieldName != null ? fieldName.toUpperCase().trim() : null;
		// if we are using a search criterion via link, enforce fieldname as
		// Object id
		if (tmpOperand == DbCompareOperand.DBLINK || tmpOperand == DbCompareOperand.DBLINK_REVERSE)
			fullFieldName = "OBJECT_ID";

		if (repoPrefix != null && !repoPrefix.equals("") && svCONST.repoFieldNames.indexOf(fullFieldName) >= 0)
			fullFieldName = tmpRPrefix + fullFieldName;
		else
			fullFieldName = tmpTPrefix + fullFieldName;

		if (tmpOperand == DbCompareOperand.ILIKE) {
			fullFieldName = "upper(" + fullFieldName + ")";
			tmpOperand = DbCompareOperand.LIKE;
		}

		switch (tmpOperand) {
		case BETWEEN:
			if (fieldName2 != null && fieldName2.length() > 0) {
				String fullFieldName2 = fieldName2.toUpperCase().trim();
				if (repoPrefix != null && !repoPrefix.equals("") && svCONST.repoFieldNames.indexOf(fullFieldName2) >= 0)
					fullFieldName2 = tmpRPrefix + fullFieldName2;
				else
					fullFieldName2 = tmpTPrefix + fullFieldName2;

				retval = "( ? " + sqlKw.getString(tmpOperand.toString()) + " " + fullFieldName + " AND "
						+ fullFieldName2 + ")";
			} else
				retval = "(" + fullFieldName + " " + sqlKw.getString(tmpOperand.toString()) + " ? AND ?)";
			break;
		case ISNULL:
			retval = "(" + fullFieldName + " " + sqlKw.getString(tmpOperand.toString()) + ")";
			break;
		case BBOX:
			ISvDatabaseIO gio = SvConf.getDbHandler();
			retval = "(" + gio.getBBoxSQL(fullFieldName) + ")";
			break;
		case DBLINK:
		case DBLINK_REVERSE:
			retval = "(" + fullFieldName + " in (SELECT link_obj_id_"
					+ (tmpOperand == DbCompareOperand.DBLINK ? "1" : "2") + " " + "from " + SvConf.getDefaultSchema()
					+ ".V" + SvCore.getDbt(svCONST.OBJECT_TYPE_LINK).getVal("TABLE_NAME") + " where " + "link_obj_id_"
					+ (tmpOperand == DbCompareOperand.DBLINK ? "2" : "1") + "=? and link_type_id=?))";
			break;
		case IN_SUBQUERY:
			retval = "(" + fullFieldName + " IN (" + inSubQuery.getSQLExpression() + "))";
			break;
		default:
			if (isField2FieldJoin) {
				retval = "(" + leftField + " " + sqlKw.getString(tmpOperand.toString()) + " " + rightField + ")";
			} else
				retval = "(" + fullFieldName + " " + sqlKw.getString(tmpOperand.toString()) + " ?)";
		}

		if (notPrefix)
			retval = "NOT" + retval;
		return retval;
	}

	@Override
	public String getSQLExpression(String tblPrefix) throws SvException {
		return getSQLExpression(null, tblPrefix);
	}

	////// From this point on there's only classic getter/setter methods
	public Boolean getNotPrefix() {
		return notPrefix;
	}

	public void setNotPrefix(Boolean notPrefix) {
		this.notPrefix = notPrefix;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public Object getCompareValue() {
		return compareValue;
	}

	public void setCompareValue(Object compareValue) {
		this.compareValue = compareValue;
	}

	public DbCompareOperand getOperand() {
		return operand;
	}

	public void setOperand(DbCompareOperand operand) {
		this.operand = operand;
	}

	public Object getCompareValue2() {
		return compareValue2;
	}

	public void setCompareValue2(Object compareValue2) {
		this.compareValue2 = compareValue2;
	}

	public String getFieldName2() {
		return fieldName2;
	}

	public void setFieldName2(String fieldName2) {
		this.fieldName2 = fieldName2;
	}

	@Deprecated
	public ArrayList<String> getOrderByFields() {
		return orderByFields;
	}

	@Deprecated
	public void addOrderByField(String fieldName) {
		this.orderByFields.add(fieldName);
	}

	public String getNextCritOperand() {
		return nextCritOperand.toString();
	}

	public void setNextCritOperand(String nextCritOperand) {
		this.nextCritOperand = DbLogicOperand.valueOf(nextCritOperand);
	}

	public DbQueryObject getInSubQuery() {
		return inSubQuery;
	}

	public void setInSubQuery(DbQueryObject inSubQuery) {
		this.inSubQuery = inSubQuery;
	}

	public ArrayList<Object> getInList() {
		return inList;
	}

	public void setInList(ArrayList<Object> inList) {
		this.inList = inList;
	}

}
