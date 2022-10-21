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
import java.util.LinkedList;
import java.util.ResourceBundle;

import org.joda.time.DateTime;

import com.google.gson.JsonObject;
import com.prtech.svarog.SvConf;
import com.prtech.svarog.SvCore;
import com.prtech.svarog.SvException;
import com.prtech.svarog.svCONST;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_interfaces.ISvDatabaseIO;

public class DbQueryObject extends DbQuery {

	public enum DbJoinType {
		INNER, LEFT, RIGHT, FULL
	};

	/**
	 * The LinkType specifies how the two objects will be joined. If link type is
	 * DBLINK, svarog will try to join the objects based on a configuration of type
	 * svCONST.OBJECT_TYPE_LINK_TYPE.
	 * 
	 * If the link type if PARENT it will treat the next object as a parent object
	 * to this one If the link type if CHILD it will treat the next object as a
	 * object object to this one
	 * 
	 * @author PR01
	 * 
	 */
	public enum LinkType {
		DBLINK, DBLINK_REVERSE, PARENT, CHILD, CUSTOM, CUSTOM_FREETEXT, DENORMALIZED, DENORMALIZED_REVERSE,
		DENORMALIZED_FULL
	};

	boolean reverseRelation = false;
	private DbQueryObject parent = null;
	LinkedList<DbQueryObject> children = new LinkedList<DbQueryObject>();

	DbDataObject dbt;
	DbDataArray dbtFields;
	DbDataObject repo;
	DbDataArray repoFields;
	DbSearchExpression search;
	DbSearch searchExternal;

	String denormalizedFieldName;

	/**
	 * This is the name of the OTHER field on which we shall perform an SQL join. By
	 * other, we mean the field name in the Table/Object to which we perform the
	 * join. This field is not part of the current object/Table!
	 */
	String denormalizedJoinOnFieldName;
	LinkType linkToNextType;
	DbDataObject linkToNext;
	DbJoinType joinToNext;
	DateTime referenceDate;
	DateTime linkReferenceDate;
	ArrayList<String> orderByFields;

	ArrayList<String> linkStatusList;

	Boolean enableHistory = false;
	// String customJoinCol1;
	// String customJoinCol2;
	ArrayList<String> customJoinLeft = new ArrayList<String>();
	ArrayList<String> customJoinRight = new ArrayList<String>();

	String subQuery;
	String subQueryFields;
	ArrayList<Object> whereParamVals;
	ArrayList<Object> subParamVals;
	ArrayList<String> customFieldsList;
	String customFreeTextJoin;
	String sqlTablePrefix = null;
	Boolean isReturnType = false;

	Boolean returnLinkObjects = false;

	/**
	 * Overriden version of the method in order to verify if the DBT exists in the
	 * target configuration
	 */
	@Override
	public Boolean fromJson(JsonObject obj) {
		Boolean result = Jsonable.jsonIO.setMembersFromJson("", this, obj);
		if (dbt != null) {
			String tableName = (String) dbt.getVal("TABLE_NAME");
			DbDataObject newDbt = SvCore.getDbtByName(tableName);
			dbt = newDbt;
			if (newDbt != null)
				autoAssignDbt();
			else
				result = false;
		}
		return result;
	}

	/**
	 * Method to update the search criteria on change of search, refdate or join
	 * type
	 */
	private void updateSearch() {
		this.search = getDefaultRepoCriterion(referenceDate, joinToNext);
		if (this.searchExternal != null)
			((DbSearchExpression) this.search).addDbSearchItem(this.searchExternal);
	}

	/**
	 * Method to auto assign supporting data for the specific Dbt
	 */
	private void autoAssignDbt() {
		this.dbtFields = SvCore.getFields(dbt.getObjectId());
		this.repo = SvCore.getRepoDbt(dbt.getObjectId());
		this.repoFields = SvCore.getFields(repo.getObjectId());
		assert (dbtFields != null);
		assert (repoFields  != null);
		assert (repo  != null);
	}

	/**
	 * Default DQO constructor.
	 * 
	 * @param dbt           The object descriptor for the table based on which DQO
	 *                      will generate a query
	 * @param search        Search criteria
	 * @param linkToNext    The link descriptor for joining by link
	 * @param referenceDate The reference date for which we'll fetch the dataset
	 * @throws SvException If mandatory parameters are omitted an Exception is
	 *                     thrown
	 */
	public DbQueryObject(DbDataObject dbt, DbSearch search, DateTime referenceDate, DbJoinType joinToNext)
			throws SvException {
		if (dbt == null)
			throw (new SvException("system.error.dqo_missing_dbt", svCONST.systemUser, null, this));

		this.dbt = dbt;
		autoAssignDbt();
		this.referenceDate = referenceDate;
		this.searchExternal = search;
		this.joinToNext = joinToNext;

		updateSearch();
	}

	/**
	 * Constructor to construct a DQO based on dbt. This constructor is deprecated
	 * because the repo information is not needed. Avoid it in the future
	 * 
	 * @param repo           Repo Descriptor
	 * @param repoFields     Repo Fields list
	 * @param dbt            The Object Type descriptor for which this DQO will
	 *                       generate a query
	 * @param dbtFields      The fields of the object type
	 * @param search         Search criteria
	 * @param joinToNext     Criteria for joining to the next DQO
	 * @param linkToNext     The link descriptor for joining by link
	 * @param linkToNextType The type of join to the next DQO
	 * @param orderByFields  The list of fields in the order by clause
	 * @param referenceDate  The reference date of the DQO
	 * @throws SvException If mandatory parameters are omitted an Exception is
	 *                     thrown
	 */
	@Deprecated
	public DbQueryObject(DbDataObject repo, DbDataArray repoFields, DbDataObject dbt, DbDataArray dbtFields,
			DbSearch search, DbJoinType joinToNext, DbDataObject linkToNext, LinkType linkToNextType,
			ArrayList<String> orderByFields, DateTime referenceDate) throws SvException {
		this(dbt, search, joinToNext, linkToNext, linkToNextType, orderByFields, referenceDate);
	}

	/**
	 * Constructor to construct a DQO based on dbt.
	 * 
	 * @param dbt            The Object Type descriptor for which this DQO will
	 *                       generate a query
	 * @param search         Search criteria
	 * @param joinToNext     Criteria for joining to the next DQO
	 * @param linkToNext     The link descriptor for joining by link
	 * @param linkToNextType The type of join to the next DQO
	 * @param orderByFields  The list of fields in the order by clause
	 * @param referenceDate  The reference date of the DQO
	 * @throws SvException If mandatory parameters are omitted an Exception is
	 *                     thrown
	 */
	public DbQueryObject(DbDataObject dbt, DbSearch search, DbJoinType joinToNext, DbDataObject linkToNext,
			LinkType linkToNextType, ArrayList<String> orderByFields, DateTime referenceDate) throws SvException {
		this(dbt, search, referenceDate, joinToNext);
		this.orderByFields = orderByFields;
		this.linkToNextType = linkToNextType;
		this.linkToNext = linkToNext;
	}

	/**
	 * Constructor to create new DQO based on sub-query
	 * 
	 * @param subQuery        The SQL subquery
	 * @param whereParamVals  The array of parameters for the where clause
	 * @param inSubQParamVals The array of parameters to be replaced in the subquery
	 * @param fieldList       The list of fields this DQO will return
	 * @throws SvException If mandatory parameters are omitted an Exception is
	 *                     thrown
	 */
	public DbQueryObject(String subQuery, ArrayList<Object> whereParamVals, ArrayList<Object> inSubQParamVals,
			String fieldList) throws SvException {
		if (subQuery == null)
			throw (new SvException("system.error.dqo_missing_subquery", svCONST.systemUser, null, this));
		this.subQuery = subQuery;
		if (whereParamVals == null)
			this.whereParamVals = new ArrayList<Object>();
		else
			this.whereParamVals = whereParamVals;
		if (inSubQParamVals == null)
			this.subParamVals = new ArrayList<Object>();
		else
			this.subParamVals = inSubQParamVals;
		this.subQueryFields = fieldList;
	}

	/**
	 * Constructor to construct a DQO based on dbt. This constructor is deprecated
	 * because the repo information is not needed. Avoid it in the future
	 * 
	 * @param repo           Repo Descriptor
	 * @param repoFields     Repo Fields list
	 * @param dbt            The Object Type descriptor for which this DQO will
	 *                       generate a query
	 * @param dbtFields      The fields of the object type
	 * @param search         Search criteria
	 * @param joinToNext     Criteria for joining to the next DQO
	 * @param linkToNext     The link descriptor for joining by link
	 * @param linkToNextType The type of join to the next DQO
	 * @param orderByFields  The list of fields in the order by clause
	 * @throws SvException If mandatory parameters are omitted an Exception is
	 *                     thrown
	 */
	@Deprecated
	public DbQueryObject(DbDataObject repo, DbDataArray repoFields, DbDataObject dbt, DbDataArray dbtFields,
			DbSearch search, DbJoinType joinToNext, DbDataObject linkToNext, LinkType linkToNextType,
			ArrayList<String> orderByFields) throws SvException {
		this(dbt, search, null, joinToNext);
		this.orderByFields = orderByFields;
		this.linkToNextType = linkToNextType;
		this.linkToNext = linkToNext;
	}

	/**
	 * Another overload for backwards compatibility. Needs to be removed in Svarog
	 * v3.
	 * 
	 * @param repo
	 * @param repoFields
	 * @param dbt
	 * @param dbtFields
	 * @param search
	 * @param referenceDate
	 * @param orderByFields
	 * @throws SvException
	 */
	@Deprecated
	public DbQueryObject(DbDataObject repo, DbDataArray repoFields, DbDataObject dbt, DbDataArray dbtFields,
			DbSearch search, DateTime referenceDate, ArrayList<String> orderByFields) throws SvException {
		this(dbt, search, referenceDate, null);
		this.orderByFields = orderByFields;
	}

	/**
	 * Empty constructor to be used within package
	 */
	public DbQueryObject() {

	}

	/**
	 * Method that sets the default repo criterion
	 * 
	 * @param refDate The reference date for object validity
	 * @return
	 */
	DbSearchExpression getDefaultRepoCriterion(DateTime refDate, DbJoinType dbj) {
		DbSearchExpression expr = new DbSearchExpression();
		if (!enableHistory) {

			DbSearch crit = null;
			DbSearchCriterion critLeftJ = null;

			if (refDate != null) {
				DbSearchCriterion critRef = new DbSearchCriterion();
				critRef.setFieldName("DT_INSERT");
				critRef.setFieldName2("DT_DELETE");
				critRef.setCompareValue(refDate);
				critRef.setOperand(DbCompareOperand.BETWEEN);
				crit = critRef;
			} else {
				DbSearchExpression noRef = new DbSearchExpression();
				DbSearchCriterion critRef = new DbSearchCriterion();
				critRef.setFieldName("DT_DELETE");
				critRef.setCompareValue(SvConf.MAX_DATE);
				critRef.setOperand(DbCompareOperand.EQUAL);

				if (dbj != null && dbj.equals(DbJoinType.LEFT)) {
					critRef.nextCritOperand = DbLogicOperand.OR;
					critLeftJ = new DbSearchCriterion();
					critLeftJ.setFieldName("DT_DELETE");
					critLeftJ.setOperand(DbCompareOperand.ISNULL);
				}
				if (critLeftJ != null) {
					noRef.addDbSearchItem(critRef);
					noRef.addDbSearchItem(critLeftJ);
					crit = noRef;
				} else
					crit = critRef;
			}
			expr.addDbSearchItem(crit);
		}
		return expr;

	}

	/**
	 * Method returning a list of DB fields with their appropriate aliases
	 * 
	 * @param repoDbt The config object for the repository of the object
	 * @param dbt     The config object for the object type
	 * @return A string containing list of fields split by comma
	 * @throws SvException
	 */
	StringBuilder getFieldList(String repoPrefix, String tblPrefix, Boolean includeGeometries) throws SvException {
		assert (dbtFields != null);
		StringBuilder retval = null;
		String finalPrefix = (sqlTablePrefix != null) ? sqlTablePrefix : tblPrefix;

		if (repo != null && dbt != null) {
			retval = this.getFieldList(finalPrefix, repoFields, dbtFields, includeGeometries);
		} else if (this.subQueryFields != null && !this.subQueryFields.equals("")) {
			retval = new StringBuilder(400);
			retval.append(subQueryFields);
		}

		return retval;

	}

	/**
	 * Method returning a list of DB fields with their appropriate aliases
	 * 
	 * @param repoDbt The config object for the repository of the object
	 * @param dbt     The config object for the object type
	 * @return A string containing list of fields split by comma
	 * @throws SvException
	 */
	public StringBuilder getFieldList(String sqlTblAlias, DbDataArray repoFields, DbDataArray dbtFields,
			Boolean includeGeometries) throws SvException {
		return getFieldList(sqlTblAlias, repoFields, dbtFields, includeGeometries, true);
	}

	/**
	 * Method returning a list of DB fields with their appropriate aliases
	 * 
	 * @param dbt     The config object for the object type
	 * @return A string containing list of fields split by comma
	 * @throws SvException
	 */
	public StringBuilder getFieldList(String sqlTblAlias, DbDataArray repoFields, DbDataArray dbtFields,
			Boolean includeGeometries, boolean useColumnPrefix) throws SvException {
		assert (dbtFields != null);
		ResourceBundle sqlKw = SvConf.getSqlkw();
		StringBuilder retval = new StringBuilder(400);
		String tmpAlias = sqlTblAlias.length() > 0 ? sqlTblAlias + "." : "";
		String tmpPrefix = useColumnPrefix ? sqlTblAlias + "_" : "";

		if (customFieldsList == null) {
			// tuka ima null sto treba da se sredi
			for (DbDataObject obj : repoFields.getItems()) {
				// retval += (retval.equals("") ? "" : ",");
				retval.append("," + tmpAlias + sqlKw.getString("OBJECT_QUALIFIER_LEFT") + obj.getVal("field_name")
						+ sqlKw.getString("OBJECT_QUALIFIER_RIGHT") + " as " + tmpPrefix + obj.getVal("field_name"));
			}
			for (DbDataObject obj : dbtFields.getItems()) {
				// add specifics for GIS data
				if (((String) obj.getVal("field_type")).equals("GEOMETRY")) {
					if (includeGeometries) {
						ISvDatabaseIO gio = SvConf.getDbHandler();
						retval.append(","
								+ gio.getGeomReadSQL(
										tmpAlias + sqlKw.getString("OBJECT_QUALIFIER_LEFT") + obj.getVal("field_name"))
								+ sqlKw.getString("OBJECT_QUALIFIER_RIGHT") + " as " + tmpPrefix
								+ obj.getVal("field_name"));
					}

				} else if (!obj.getVal("field_name").equals("PKID"))
					retval.append("," + tmpAlias + sqlKw.getString("OBJECT_QUALIFIER_LEFT") + obj.getVal("field_name")
							+ sqlKw.getString("OBJECT_QUALIFIER_RIGHT") + " as " + tmpPrefix
							+ obj.getVal("field_name"));
				// TODO add geometry handling
			}
		} else {
			for (String fieldName : customFieldsList) {
				retval.append("," + tmpAlias + sqlKw.getString("OBJECT_QUALIFIER_LEFT") + fieldName
						+ sqlKw.getString("OBJECT_QUALIFIER_RIGHT") + " as " + tmpPrefix + fieldName);
			}

		}
		return retval.deleteCharAt(0);

	}

	String getTblJoin(String tblPrefix) {
		if (repo == null)
			return subQuery;
		String finalPrefix = (sqlTablePrefix != null) ? sqlTablePrefix : tblPrefix;
		return repo.getVal("schema") + ".v" + dbt.getVal("table_name") + " " + finalPrefix;
	}

	String getTblJoin(String repoPrefix, String tblPrefix) {
		if (repo == null)
			return subQuery;
		String finalPrefix = (sqlTablePrefix != null) ? sqlTablePrefix : tblPrefix;
		return repo.getVal("schema") + "." + repo.getVal("table_name") + " " + repoPrefix + " JOIN "
				+ dbt.getVal("schema") + "." + dbt.getVal("table_name") + " " + finalPrefix + " ON " + repoPrefix
				+ ".meta_pkid=" + finalPrefix + ".pkid";
	}

	String getTblJoin(String repoPrefix, String tblPrefix, String joinToNext) {
		if (repo == null)
			return subQuery;

		String finalPrefix = (sqlTablePrefix != null) ? sqlTablePrefix : tblPrefix;
		return repo.getVal("schema") + "." + repo.getVal("table_name") + " " + repoPrefix + " " + joinToNext + " JOIN "
				+ dbt.getVal("schema") + "." + dbt.getVal("table_name") + " " + finalPrefix + " ON " + repoPrefix
				+ ".meta_pkid=" + finalPrefix + ".pkid";
	}

	StringBuilder getTableSql(String repoPrefix, String tblPrefix, Boolean includeGeometries) throws SvException {
		return getTableSql(repoPrefix, tblPrefix, includeGeometries, sqlTablePrefix);
	}

	StringBuilder getTableSql(String repoPrefix, String tblPrefix, Boolean includeGeometries, String resultSetPrefix)
			throws SvException {

		StringBuilder retval = new StringBuilder(400);
		String finalRepoPrefix = repoPrefix == null || repoPrefix.equals("") ? null : repoPrefix;
		retval.append("SELECT " + getFieldList(finalRepoPrefix, tblPrefix, includeGeometries) + " FROM "
				+ getTblJoin(tblPrefix));
		return retval;
	}

	@Override
	public String getSQLExpression() throws SvException {
		return getSQLExpression(false).toString();
	}

	@Override
	public StringBuilder getSQLExpression(Boolean forcePhysicalTables) throws SvException {
		return getSQLExpression(forcePhysicalTables, false);
	}

	@Override
	public StringBuilder getSQLExpression(Boolean forcePhysicalTables, Boolean includeGeometries) throws SvException {
		if (subQuery != null && !subQuery.equals(""))
			return new StringBuilder().append(subQuery);

		if (repo == null || repoFields == null || dbt == null || dbtFields == null)
			throw (new SvException("system.error.dqo_missing_dbt", svCONST.systemUser, null, this));

		String prefix = this.getSqlTablePrefix() != null ? this.getSqlTablePrefix()
				: "tbl" + this.getReturnTypeSequence();

		StringBuilder sqlQry = null;
		if (forcePhysicalTables) {
			sqlQry = getTableSql("rep" + this.getReturnTypeSequence(), prefix, includeGeometries);
			if (search != null)
				sqlQry.append(" WHERE " + search.getSQLExpression(prefix));

		} else {
			sqlQry = getTableSql(null, prefix, includeGeometries);
			if (search != null)
				sqlQry.append(" WHERE " + search.getSQLExpression(prefix));

		}

		if (orderByFields != null && orderByFields.size() > 0) {
			sqlQry.append(" ORDER BY ");
			for (String fldName : orderByFields) {
				sqlQry.append(fldName + ",");
			}
			sqlQry.deleteCharAt(sqlQry.length() - 1);
		}
		return sqlQry;
	}

	@Override
	public ArrayList<Object> getSQLParamVals() throws SvException {
		if (subQuery != null)
			return whereParamVals;
		ArrayList<Object> paramVals = null;
		if (search != null)
			paramVals = search.getSQLParamVals();
		else
			paramVals = new ArrayList<Object>();
		return paramVals;
	}

	public ArrayList<Object> getSubSQLParamVals() {
		if (subQuery != null)
			return subParamVals;
		return null;
	}

	@Override
	public DbDataObject getReturnType() {
		return dbt;
	}

	@Override
	public void setReturnType(DbDataObject returnType) {
		dbt = returnType;
	}

	public DbDataObject getDbt() {
		return dbt;
	}

	public void setDbt(DbDataObject dbt) {
		this.dbt = dbt;
	}

	public DbDataArray getDbt_fields() {
		return dbtFields;
	}

	public void setDbtFields(DbDataArray dbtFields) {
		this.dbtFields = dbtFields;
	}

	public DbDataObject getRepo() {
		return repo;
	}

	public void setRepo(DbDataObject repo) {
		this.repo = repo;
	}

	public DbDataArray getRepoFields() {
		return repoFields;
	}

	public void setRepoFields(DbDataArray repoFields) {
		this.repoFields = repoFields;
	}

	public DbSearch getSearch() {
		return search;
	}

	public DbSearch getSearchExternal() {
		return searchExternal;
	}

	public void setSearch(DbSearch search) {
		this.searchExternal = search;
		updateSearch();
	}

	public LinkType getLinkToNextType() {
		return linkToNextType;
	}

	public void setLinkToNextType(LinkType linkToNextType) {
		this.linkToNextType = linkToNextType;
	}

	public DbDataObject getLinkToNext() {
		return linkToNext;
	}

	public void setLinkToNext(DbDataObject linkToNext) {
		this.linkToNext = linkToNext;
	}

	public DbJoinType getJoinToNext() {
		return joinToNext;
	}

	public void setJoinToNext(DbJoinType joinToNext) {
		this.joinToNext = joinToNext;
		updateSearch();
	}

	public DateTime getReferenceDate() {
		return referenceDate;
	}

	public void setReferenceDate(DateTime referenceDate) {
		this.referenceDate = referenceDate;
		updateSearch();
	}

	public ArrayList<String> getOrderByFields() {
		return orderByFields;
	}

	public void setOrderByFields(ArrayList<String> orderByFields) {
		this.orderByFields = orderByFields;
	}

	/**
	 * 
	 * @deprecated Instead this method use getCustomJoinCol2()
	 */
	@Deprecated()
	public String getCustomJoinCol1() {
		if (customJoinLeft.size() > 0)
			return customJoinLeft.get(0);
		else
			return null;
	}

	/**
	 * 
	 * @deprecated Instead this method use addCustomJoinLeft()
	 */
	@Deprecated()
	public void setCustomJoinCol1(String customJoinCol1) {
		if (customJoinLeft.size() > 0)
			customJoinLeft.set(0, customJoinCol1);
		else
			customJoinLeft.add(customJoinCol1);
	}

	/**
	 * 
	 * @deprecated Instead this method use getCustomJoinRight()
	 */
	@Deprecated()
	public String getCustomJoinCol2() {
		if (customJoinRight.size() > 0)
			return customJoinRight.get(0);
		else
			return null;
	}

	/**
	 * 
	 * @deprecated Instead this method use addCustomJoinRigh()
	 */
	@Deprecated()
	public void setCustomJoinCol2(String customJoinCol2) {
		if (customJoinRight.size() > 0)
			customJoinRight.set(0, customJoinCol2);
		else
			customJoinRight.add(customJoinCol2);
	}

	public void addCustomJoinRight(String customJoinCol2) {
		customJoinRight.add(customJoinCol2);
	}

	public void addCustomJoinLeft(String customJoinCol1) {
		customJoinLeft.add(customJoinCol1);
	}

	public ArrayList<String> getCustomJoinRight() {
		return customJoinRight;
	}

	public ArrayList<String> getCustomJoinLeft() {
		return customJoinLeft;
	}

	public String getCustomFreeTextJoin() {
		return customFreeTextJoin;
	}

	public void setCustomFreeTextJoin(String customFreeTextJoin) {
		this.customFreeTextJoin = customFreeTextJoin;
	}

	public Boolean getIsReturnType() {
		return isReturnType;
	}

	public void setIsReturnType(Boolean isReturnType) {
		this.isReturnType = isReturnType;
	}

	public Boolean getEnableHistory() {
		return enableHistory;
	}

	public void setEnableHistory(Boolean enableHistory) {
		this.enableHistory = enableHistory;

		this.search = getDefaultRepoCriterion(referenceDate, this.getJoinToNext());
		if (this.searchExternal != null)
			((DbSearchExpression) this.search).addDbSearchItem(this.searchExternal);

	}

	public String getSqlTablePrefix() {
		return sqlTablePrefix;
	}

	public void setSqlTablePrefix(String sqlTablePrefix) {
		this.sqlTablePrefix = sqlTablePrefix;
	}

	public String getDenormalizedFieldName() {
		return denormalizedFieldName;
	}

	public void setDenormalizedFieldName(String denormalizedFieldName) {
		this.denormalizedFieldName = denormalizedFieldName;
	}

	public boolean isReverseRelation() {
		return reverseRelation;
	}

	public void setReverseRelation(boolean reverseRelation) {
		this.reverseRelation = reverseRelation;
	}

	public DbQueryObject getParent() {
		return parent;
	}

	public void setParent(DbQueryObject parent) {
		this.parent = parent;
	}

	public void addChild(DbQueryObject dqo) {
		children.add(dqo);
		dqo.setParent(this);

	}

	public LinkedList<DbQueryObject> getChildren() {
		return this.children;
	}

	public void setChildren(LinkedList<DbQueryObject> children) {
		this.children = children;
	}

	public DateTime getLinkReferenceDate() {
		return linkReferenceDate;
	}

	public void setLinkReferenceDate(DateTime linkReferenceDate) {
		this.linkReferenceDate = linkReferenceDate;
	}

	public ArrayList<String> getLinkStatusList() {
		return linkStatusList;
	}

	public void setLinkStatusList(ArrayList<String> linkStatusList) {
		this.linkStatusList = linkStatusList;
	}

	public Boolean getReturnLinkObjects() {
		return returnLinkObjects;
	}

	public void setReturnLinkObjects(Boolean returnLinkObjects) {
		this.returnLinkObjects = returnLinkObjects;
	}

	public ArrayList<String> getCustomFieldsList() {
		return customFieldsList;
	}

	public void setCustomFieldsList(ArrayList<String> customFieldsList) {
		this.customFieldsList = customFieldsList;
	}

	public String getDenormalizedJoinOnFieldName() {
		return denormalizedJoinOnFieldName;
	}

	/**
	 * * This is the name of the OTHER field on which we shall perform an SQL join.
	 * By other, we mean the field name in the Table/Object to which we perform the
	 * join. This field is not part of the current object/Table!
	 * 
	 * @param denormalizedJoinOnFieldName The field name part of the previous
	 *                                    object/table in the SQL Expression
	 */

	public void setDenormalizedJoinOnFieldName(String denormalizedJoinOnFieldName) {
		this.denormalizedJoinOnFieldName = denormalizedJoinOnFieldName;
	}
}