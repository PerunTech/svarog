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
import java.util.ListIterator;

import com.prtech.svarog.SvConf;
import com.prtech.svarog.SvCore;
import com.prtech.svarog.SvException;
import com.prtech.svarog.svCONST;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;

public class DbQueryExpression extends DbQuery {

	/**
	 * The core link type
	 */
	static DbDataObject dblt = null;

	/**
	 * List of DbQueryObjects to be used for query generation. If the expression is
	 * reverse this list is ignored
	 */
	LinkedList<DbQueryObject> items = new LinkedList<DbQueryObject>();

	/**
	 * If the expression is reverse then expression must have a root object
	 */
	DbQueryObject rootQueryObject = null;

	/**
	 * If the expression is reverse, then the root object is traversed otherwise the
	 * list of items is traversed in forward maner
	 */
	Boolean isReverseExpression = false;

	/**
	 * Array of objects with params for the generated SQL Query string.
	 */
	ArrayList<Object> queryParamVals = new ArrayList<Object>();

	/**
	 * List holding the valid statuses of links
	 */
	ArrayList<String> linkStatusList = new ArrayList<String>();

	/**
	 * Default contructor to create an empty expression
	 */
	public DbQueryExpression() {

	}

	/**
	 * Default constructor to create a query expression with root query object and
	 * reverse execution
	 * 
	 * @param rootQueryObject
	 */
	public DbQueryExpression(DbQueryObject rootQueryObject) {
		this.rootQueryObject = rootQueryObject;
		isReverseExpression = true;
	}

	public ArrayList<String> getLinkStatusList() {
		return linkStatusList;
	}

	public void setLinkStatusList(ArrayList<String> linkList) {
		if (linkList != null)
			this.linkStatusList = linkList;
	}

	public void addLinkStatus(String linkStatus) {
		if (linkStatusList != null)
			this.linkStatusList.add(linkStatus);
	}

	public static void setDblt(DbDataObject dbLink) {
		dblt = dbLink;
	}

	public static DbDataObject getDblt() {
		return dblt;
	}

	public void addItem(DbQueryObject item) throws SvException {
		if (!isReverseExpression)
			items.add(item);
		else
			throw (new SvException("system.error.expression_is_reverse", svCONST.systemUser, null, this));
	}

	public LinkedList<DbQueryObject> getItems() {
		return items;
	}

	@Override
	public String getSQLExpression() throws SvException {
		return getSQLExpression(false).toString();
	}

	@Override
	public StringBuilder getSQLExpression(Boolean forcePhysicalTables) throws SvException {
		return getSQLExpression(forcePhysicalTables, false);
	}

	/**
	 * Method to get a DQO from the list of available QueryObjects
	 * 
	 * @param currentDqo The currently selected DQO
	 * @param sequence   The sequence inside the DQO or global sequence
	 * @return
	 */
	DbQueryObject getDqo(DbQueryObject currentDqo, Integer sequence) {
		DbQueryObject dqo = null;
		if (isReverseExpression) {
			if (currentDqo == null)
				dqo = rootQueryObject;
			else
				dqo = currentDqo.children.size() > sequence ? currentDqo.children.get(sequence) : null;
		} else
			dqo = items.size() > sequence ? items.get(sequence) : null;

		return dqo;
	}

	private void buildQueryString(DbQueryObject currentDqo, StringBuilder columnList, StringBuilder tblList,
			StringBuilder whereCriteria, boolean includeGeometries, ArrayList<Object> whereQueryParamVals)
			throws SvException {
		StringBuilder tmpWhere = new StringBuilder();
		// append the column list to the query string
		if (currentDqo.getIsReturnType())
			columnList.append(currentDqo.getFieldList(null, currentDqo.getSqlTablePrefix(), includeGeometries) + ",");
		boolean queryValsBound = false;
		// add the table to the join list
		String tblJoin = currentDqo.getTblJoin(currentDqo.getSqlTablePrefix());

		if (currentDqo.getLinkToNextType() != null && currentDqo.getParent() != null) {
			StringBuilder[] nextJoinStr = new StringBuilder[2];
			getPrevTableJoin(currentDqo.getParent(), currentDqo, nextJoinStr, false);
			if (currentDqo.joinToNext.equals(DbJoinType.LEFT)) {
				boolean isDbLink = (currentDqo.linkToNextType.equals(LinkType.DBLINK)
						|| currentDqo.linkToNextType.equals(LinkType.DBLINK_REVERSE));

				StringBuilder tmpFinal = new StringBuilder().append(" LEFT JOIN (SELECT ");
				// System.out.println("tmp str:" + tmp.toString());
				tmpFinal.append(currentDqo.getFieldList(currentDqo.getSqlTablePrefix(), SvCore.getRepoDbtFields(),
						SvCore.getFields(currentDqo.getDbt().getObjectId()), false, false));
				if (isDbLink)
					tmpFinal.append("," + currentDqo.getFieldList("lnk" + currentDqo.getSqlTablePrefix(),
							SvCore.getRepoDbtFields(), SvCore.getFields(svCONST.OBJECT_TYPE_LINK), false));
				tmpFinal.append(" FROM " + currentDqo.getTblJoin(currentDqo.getSqlTablePrefix()));

				StringBuilder[] leftJoinStr = new StringBuilder[10];
				getPrevTableJoin(currentDqo.getParent(), currentDqo, leftJoinStr, true);

				String lnkTbl = leftJoinStr[0].toString().replace(currentDqo.getSqlTablePrefix(),
						"lnk" + currentDqo.getSqlTablePrefix());

				if (isDbLink)
					tmpFinal.append(" INNER JOIN " + lnkTbl + leftJoinStr[1].toString());

				if (currentDqo.getSearch() != null)
					tmpFinal.append(
							" WHERE " + currentDqo.getSearch().getSQLExpression(currentDqo.getSqlTablePrefix()));

				tmpFinal.append(") " + currentDqo.getSqlTablePrefix());

				queryParamVals.addAll(currentDqo.getSQLParamVals());
				queryValsBound = true;
				int joinIndex = isDbLink ? 2 : 1;
				tblJoin = tmpFinal.toString() + leftJoinStr[joinIndex].toString();
			} else
				tblJoin = nextJoinStr[0] + tblJoin + nextJoinStr[1];

			if (currentDqo.linkToNextType != null
					&& (currentDqo.linkToNextType.equals(LinkType.DBLINK)
							|| currentDqo.linkToNextType.equals(LinkType.DBLINK_REVERSE))
					&& currentDqo.linkToNext != null && dblt != null) {

				// if the DQO required from to return the links, than we need to
				// append them in the list of columns
				if (currentDqo.getReturnLinkObjects())
					columnList.append(currentDqo.getFieldList("lnk" + currentDqo.getSqlTablePrefix(),
							SvCore.getRepoDbtFields(), SvCore.getFields(svCONST.OBJECT_TYPE_LINK), false) + ",");

				if (!currentDqo.joinToNext.equals(DbJoinType.LEFT)) {
					if (currentDqo.getReferenceDate() == null) {
						whereQueryParamVals.add(SvConf.MAX_DATE);
						tmpWhere.append(" (lnk" + currentDqo.getSqlTablePrefix() + ".dt_delete=?) ");
					} else {
						tmpWhere.append(" (? between lnk" + currentDqo.getSqlTablePrefix() + ".dt_insert and lnk"
								+ currentDqo.getSqlTablePrefix() + ".dt_delete) ");
						whereQueryParamVals
								.add(currentDqo.getLinkReferenceDate() != null ? currentDqo.getLinkReferenceDate()
										: currentDqo.getReferenceDate());

					}
				}
			}
		}

		if (currentDqo.getSQLParamVals() != null && !queryValsBound)
			whereQueryParamVals.addAll(currentDqo.getSQLParamVals());
		if (currentDqo.getSubSQLParamVals() != null)
			queryParamVals.addAll(currentDqo.getSubSQLParamVals());

		tblList.append(tblJoin);

		if (currentDqo.getParent() != null && currentDqo.getJoinToNext().equals(DbJoinType.LEFT)) {
			// tmpWhere.append(") OR " + currentDqo.getSqlTablePrefix() +
			// ".object_id is null )");
			// tmpWhere.insert(0, "((");
		} else if (currentDqo.getSearch() != null) {
			if (whereCriteria.length() > 2 && currentDqo.getParent() != null)
				whereCriteria.append(" " + currentDqo.getParent().search.getNextCritOperand() + " ");

			if (tmpWhere.length() > 1)
				tmpWhere.append(" AND ");
			tmpWhere.append(currentDqo.search.getSQLExpression(currentDqo.getSqlTablePrefix()));
		}

		whereCriteria.append(tmpWhere);
		// traverse all children
		if (currentDqo.children != null) {
			for (DbQueryObject dqo : currentDqo.children) {
				buildQueryString(dqo, columnList, tblList, whereCriteria, includeGeometries, whereQueryParamVals);
			}
		}

	}

	/**
	 * Method to build an sql expression for a reversed query expression
	 * 
	 * @param includeGeometries if geometries should be included in the resultset
	 * @return A SQL String to be executed using JDBC
	 * @throws SvException Any exception which occurred should be forwarded
	 */
	public StringBuilder getReverseSQLExpression(Boolean includeGeometries) throws SvException {

		StringBuilder queryString = new StringBuilder(400);
		queryString.append("SELECT "); // well it will always start with a
										// select :)
		StringBuilder whereCriteria = new StringBuilder();

		ArrayList<String> orderByFields = new ArrayList<String>();

		StringBuilder tblList = new StringBuilder(100);

		ArrayList<Object> whereQueryParamVals = new ArrayList<Object>();
		queryParamVals = new ArrayList<Object>();

		buildQueryString(rootQueryObject, queryString, tblList, whereCriteria, includeGeometries, whereQueryParamVals);

		queryParamVals.addAll(whereQueryParamVals);

		queryString.setLength(queryString.length() - 1);
		queryString.append(" FROM ").append(tblList).append(" WHERE ").append(whereCriteria);

		if (orderByFields != null && orderByFields.size() > 0) {
			queryString.append(" ORDER BY ");
			for (String fldName : orderByFields) {
				queryString.append(fldName + ",");
			}
			queryString.setLength(queryString.length() - 1);
		}

		return queryString;
	}

	@Override
	/**
	 * Method to generate SQL Query from an Expression which groups multiple Objects
	 */
	public StringBuilder getSQLExpression(Boolean forcePhysicalTables, Boolean includeGeometries) throws SvException {

		// TODO make sure to handle the forcePhysicalTables flag!!!
		if (items.size() <= 0 && rootQueryObject == null)
			return new StringBuilder(0);
		// make sure we are backwards compatible by setting the return type
		// based on setReturnType()
		getLegacyReturnType();

		if (isReverseExpression)
			return getReverseSQLExpression(includeGeometries);

		DbQueryObject nextDqo = null;
		Boolean hasCustomFromPrev = false;
		Boolean returnTypeMatched = false;

		Integer itemSeq = 0;

		StringBuilder queryString = new StringBuilder(400);
		queryString.append("SELECT "); // well it will always start with a
										// select :)

		StringBuilder joinCritFromPrev = new StringBuilder();
		StringBuilder whereCriteria = new StringBuilder();

		ArrayList<String> orderByFields = new ArrayList<String>();
		String currentRepoPrefix = null;
		String currentTblPrefix = null;

		StringBuilder tblList = new StringBuilder(100);

		ListIterator<DbQueryObject> dqoIterator = items.listIterator();
		while (dqoIterator.hasNext() || nextDqo != null) {
			DbQueryObject currentDqo = nextDqo == null ? dqoIterator.next() : nextDqo;
			// set the prefixes
			currentRepoPrefix = (forcePhysicalTables ? "REP" + itemSeq : null);
			currentTblPrefix = currentDqo.getSqlTablePrefix() != null ? currentDqo.getSqlTablePrefix()
					: "TBL" + itemSeq;

			// if there's orderBy fields then store them away
			if (currentDqo.getOrderByFields() != null)
				orderByFields.addAll(currentDqo.getOrderByFields());

			// if there is no return type, just return all columns
			// TODO update this to handle objects which has set isReturnType
			if (this.getReturnType() == null) {
				queryString.append(currentDqo.getFieldList(currentRepoPrefix, currentTblPrefix, includeGeometries));
				if (dqoIterator.hasNext())
					queryString.append(",");

			} else // find the objects with return type, make sure we just match
					// the first
			if (currentDqo.getIsReturnType() && !returnTypeMatched) {
				queryString.append(currentDqo.getFieldList(currentRepoPrefix, currentTblPrefix, includeGeometries));
				this.setReturnTypeSequence(itemSeq);
				returnTypeMatched = true;
			}

			if (!forcePhysicalTables)
				tblList.append(currentDqo.getTblJoin(currentTblPrefix));
			else
				tblList.append(currentDqo.getTblJoin(currentRepoPrefix, currentTblPrefix,
						currentDqo.joinToNext == null ? "" : currentDqo.joinToNext.toString()));

			if (!joinCritFromPrev.toString().equals("")) {
				if (hasCustomFromPrev) {
					tblList.append(joinCritFromPrev + " ");
					hasCustomFromPrev = false;
				} else {
					tblList.append(" " + joinCritFromPrev + " ");
				}
				joinCritFromPrev.setLength(0);
			}

			// if there is a next object get the next table join
			if (dqoIterator.hasNext()) {
				nextDqo = dqoIterator.next();
				StringBuilder[] nextJoinStr = new StringBuilder[2];
				hasCustomFromPrev = getNextTableJoin(currentDqo, itemSeq, nextJoinStr, currentRepoPrefix,
						currentTblPrefix, nextDqo);
				tblList.append(nextJoinStr[0]);
				joinCritFromPrev = nextJoinStr[1];

			} else
				nextDqo = null;

			if (nextDqo != null && currentDqo.linkToNextType != null
					&& (currentDqo.linkToNextType.equals(LinkType.DBLINK)
							|| currentDqo.linkToNextType.equals(LinkType.DBLINK_REVERSE))
					&& currentDqo.linkToNext != null && dblt != null) {

				String nextTableAlias = nextDqo.getSqlTablePrefix() != null ? nextDqo.getSqlTablePrefix()
						: "TBL" + (itemSeq + 1);
				if (currentDqo.getReferenceDate() == null)
					whereCriteria.append(" (lnk" + nextTableAlias + ".dt_delete=?) and ");
				else
					whereCriteria.append(" (? between lnk" + nextTableAlias + ".dt_insert and lnk" + nextTableAlias
							+ ".dt_delete) and ");
			}

			if (currentDqo.search != null) {
				whereCriteria.append(currentDqo.search.getSQLExpression(currentRepoPrefix, currentTblPrefix));
				if (nextDqo != null)
					whereCriteria.append(" " + currentDqo.search.getNextCritOperand() + " ");
			}
			itemSeq++;

		}
		String strWhere = whereCriteria.toString().trim();
		if (strWhere.endsWith("AND"))
			whereCriteria.setLength(whereCriteria.length() - 4);
		queryString.append(" FROM ").append(tblList).append(" WHERE ").append(whereCriteria);
		// if()
		if (orderByFields != null && orderByFields.size() > 0) {
			queryString.append(" ORDER BY ");
			for (String fldName : orderByFields) {
				queryString.append(fldName + ",");
			}
			queryString.setLength(queryString.length() - 1);
		}

		return queryString;
	}

	/**
	 * 
	 * @param prevDqo
	 * @param itemSeq
	 * @param nextJoinStr
	 * @return
	 * @throws Exception
	 */
	private Boolean getPrevTableJoin(DbQueryObject prevDqo, DbQueryObject currentDqo, StringBuilder[] nextJoinStr,
			boolean isLeft) throws SvException {
		StringBuilder joinCritFromPrev = new StringBuilder();
		StringBuilder tmpTbl = new StringBuilder();
		Boolean hasCustomFromPrev = false;
		StringBuilder nextLeftJoint = isLeft ? new StringBuilder() : tmpTbl;
		String nextLeftPrefix = isLeft ? "lnk" + currentDqo.getSqlTablePrefix() + "_" : "";
		String tmpjoin = isLeft ? "" : " " + prevDqo.joinToNext.toString() + " JOIN ";
		String lnkPrefix = isLeft ? currentDqo.getSqlTablePrefix() : "lnk" + currentDqo.getSqlTablePrefix();
		switch (currentDqo.linkToNextType) {
		case CHILD:
			joinCritFromPrev.append(" on " + currentDqo.getSqlTablePrefix() + ".object_id="
					+ prevDqo.getSqlTablePrefix() + ".parent_id ");
			break;
		case PARENT:
			joinCritFromPrev.append(" on " + currentDqo.getSqlTablePrefix() + ".parent_id="
					+ prevDqo.getSqlTablePrefix() + ".object_id ");
			break;
		case CUSTOM:
			if (currentDqo.getCustomJoinLeft().size() != currentDqo.getCustomJoinRight().size())
				throw (new SvException("system.error.dbqe_sql_join_err", svCONST.systemUser, null, this));

			for (int i = 0; i < currentDqo.getCustomJoinLeft().size(); i++) {

				joinCritFromPrev.append((i == 0 ? " on " : " and ") + currentDqo.getSqlTablePrefix() + "."
						+ currentDqo.getCustomJoinLeft().get(i) + "=" + currentDqo.getSqlTablePrefix() + "."
						+ currentDqo.getCustomJoinRight().get(i));

			}
			hasCustomFromPrev = true;
			break;
		case CUSTOM_FREETEXT:
			joinCritFromPrev.append(currentDqo.customFreeTextJoin);
			hasCustomFromPrev = true;
			break;

		case DBLINK: {
			if (currentDqo.linkToNext == null)
				throw (new SvException("system.error.dbqe_sql_dblink_err", svCONST.systemUser, null, this));

			if (dblt == null)
				throw (new SvException("system.error.dbqe_sql_dblinkconf_err", svCONST.systemUser, null, this));
			// first get the dblink table name
			tmpTbl.append(tmpjoin + dblt.getVal("schema") + ".v" + dblt.getVal("table_name") + " " + lnkPrefix);

			nextLeftJoint.append(" on " + prevDqo.getSqlTablePrefix() + ".object_id=" + lnkPrefix + "." + nextLeftPrefix
					+ "link_obj_id_1 and " + lnkPrefix + "." + nextLeftPrefix + "link_type_id="
					+ currentDqo.linkToNext.getObject_id() + " ");

			// legacy version
			ArrayList<String> tmpLnkStatus = (linkStatusList != null && linkStatusList.size() > 0) ? linkStatusList
					: null;
			// if legacy version is still null, try to get from current DQO
			if (tmpLnkStatus == null)
				tmpLnkStatus = (currentDqo.getLinkStatusList() != null && currentDqo.getLinkStatusList().size() > 0)
						? currentDqo.getLinkStatusList()
						: null;
			StringBuilder lnkStr = new StringBuilder();
			if (tmpLnkStatus != null) {
				boolean hasPrev = false;
				lnkStr.append(" and (");
				for (String currStatus : tmpLnkStatus) {
					if (hasPrev)
						lnkStr.append(" or ");
					lnkStr.append(lnkPrefix + ".status='" + currStatus + "'");
					hasPrev = true;
				}
				lnkStr.append(") ");
			}
			joinCritFromPrev.append(" on lnk" + currentDqo.getSqlTablePrefix() + ".link_obj_id_2="
					+ currentDqo.getSqlTablePrefix() + ".object_id ");
			if (!isLeft)
				tmpTbl.append(lnkStr);
			else
				joinCritFromPrev.append(lnkStr);
		}
			break;
		case DBLINK_REVERSE: {
			if (currentDqo.linkToNext == null)
				throw (new SvException("system.error.dbqe_sql_dblink_err", svCONST.systemUser, null, this));

			if (dblt == null)
				throw (new SvException("system.error.dbqe_sql_dblinkconf_err", svCONST.systemUser, null, this));
			// first get the dblink table name

			tmpTbl.append(tmpjoin + dblt.getVal("schema") + ".v" + dblt.getVal("table_name") + " " + lnkPrefix);

			nextLeftJoint.append(" on " + prevDqo.getSqlTablePrefix() + ".object_id=" + lnkPrefix + "." + nextLeftPrefix
					+ "link_obj_id_2 and " + lnkPrefix + "." + nextLeftPrefix + "link_type_id="
					+ currentDqo.linkToNext.getObject_id() + " ");

			// legacy version
			ArrayList<String> tmpLnkStatus = (linkStatusList != null && linkStatusList.size() > 0) ? linkStatusList
					: null;
			// if legacy version is still null, try to get from current DQO
			if (tmpLnkStatus == null)
				tmpLnkStatus = (currentDqo.getLinkStatusList() != null && currentDqo.getLinkStatusList().size() > 0)
						? currentDqo.getLinkStatusList()
						: null;

			StringBuilder lnkStr = new StringBuilder();
			if (tmpLnkStatus != null) {
				boolean hasPrev = false;
				lnkStr.append(" and (");
				for (String currStatus : tmpLnkStatus) {
					if (hasPrev)
						lnkStr.append(" or ");
					lnkStr.append(lnkPrefix + ".status='" + currStatus + "'");
					hasPrev = true;
				}
				lnkStr.append(") ");
			}

			joinCritFromPrev.append(" on lnk" + currentDqo.getSqlTablePrefix() + ".link_obj_id_1="
					+ currentDqo.getSqlTablePrefix() + ".object_id ");

			if (!isLeft)
				tmpTbl.append(lnkStr);
			else
				joinCritFromPrev.append(lnkStr);
			// linkSeq++;
		}
			break;
		case DENORMALIZED: {
			// first get the dblink table name
			joinCritFromPrev.append(" on " + prevDqo.getSqlTablePrefix() + ".object_id="
					+ currentDqo.getSqlTablePrefix() + "." + currentDqo.getDenormalizedFieldName() + " ");
		}
			break;
		case DENORMALIZED_REVERSE: {
			// first get the dblink table name
			joinCritFromPrev.append(" on " + prevDqo.getSqlTablePrefix() + "." + prevDqo.getDenormalizedFieldName()
					+ "=" + currentDqo.getSqlTablePrefix() + ".object_id ");
		}
			break;
		case DENORMALIZED_FULL: {
			// get the name of the previous field. If the current table has its own
			// specified field name use it, otherwise fall back to the previous DQO
			String prevTableField = currentDqo.getDenormalizedJoinOnFieldName() != null
					? currentDqo.getDenormalizedJoinOnFieldName()
					: prevDqo.getDenormalizedFieldName();

			// first get the dblink table name
			joinCritFromPrev.append(" on " + prevDqo.getSqlTablePrefix() + "." + prevTableField + "="
					+ currentDqo.getSqlTablePrefix() + "." + currentDqo.getDenormalizedFieldName() + " ");
		}
			break;
		}
		tmpTbl.append(tmpjoin);
		nextJoinStr[0] = tmpTbl;
		nextJoinStr[1] = joinCritFromPrev;
		if (isLeft)
			nextJoinStr[2] = nextLeftJoint;
		return hasCustomFromPrev;
	}

	/**
	 * 
	 * @param prevDqo
	 * @param itemSeq
	 * @param nextJoinStr
	 * @return
	 * @throws Exception
	 */
	private Boolean getNextTableJoin(DbQueryObject prevDqo, Integer itemSeq, StringBuilder[] nextJoinStr,
			String currentRepoPrefix, String currentTblPrefix, DbQueryObject currentDqo) throws SvException {
		StringBuilder joinCritFromPrev = new StringBuilder();
		StringBuilder tmpTbl = new StringBuilder();
		Boolean hasCustomFromPrev = false;

		if (currentRepoPrefix == null) {
			currentRepoPrefix = currentTblPrefix;
		}
		String currentTableAlias = currentDqo.getSqlTablePrefix() != null ? currentDqo.getSqlTablePrefix()
				: "TBL" + (itemSeq + 1);

		switch (prevDqo.linkToNextType) {
		case CHILD:
			joinCritFromPrev.append(" on " + currentRepoPrefix + ".object_id=" + currentTableAlias + ".parent_id ");
			break;
		case PARENT:
			joinCritFromPrev.append(" on " + currentRepoPrefix + ".parent_id=" + currentTableAlias + ".object_id ");
			break;
		case CUSTOM:
			if (prevDqo.getCustomJoinLeft().size() != prevDqo.getCustomJoinRight().size())
				throw (new SvException("system.error.dbqe_sql_join_err", svCONST.systemUser, null, this));

			for (int i = 0; i < prevDqo.getCustomJoinLeft().size(); i++) {

				joinCritFromPrev.append(
						(i == 0 ? " on " : " and ") + currentTblPrefix + "." + prevDqo.getCustomJoinLeft().get(i) + "="
								+ currentTableAlias + "." + prevDqo.getCustomJoinRight().get(i));

			}
			hasCustomFromPrev = true;
			break;
		case CUSTOM_FREETEXT:
			joinCritFromPrev.append(prevDqo.customFreeTextJoin);
			hasCustomFromPrev = true;
			break;

		case DBLINK: {
			if (prevDqo.linkToNext == null)
				throw (new SvException("system.error.dbqe_sql_dblink_err", svCONST.systemUser, null, this));

			if (dblt == null)
				throw (new SvException("system.error.dbqe_sql_dblinkconf_err", svCONST.systemUser, null, this));
			// first get the dblink table name
			tmpTbl.append(" " + prevDqo.joinToNext.toString() + " JOIN " + dblt.getVal("schema") + ".v"
					+ dblt.getVal("table_name") + " lnk" + currentTableAlias);

			tmpTbl.append(" on " + currentRepoPrefix + ".object_id=lnk" + currentTableAlias + ".link_obj_id_1 and lnk"
					+ currentTableAlias + ".link_type_id=" + prevDqo.linkToNext.getObject_id() + " ");

			if (linkStatusList != null && linkStatusList.size() > 0) {
				boolean hasPrev = false;
				tmpTbl.append("and (");
				for (String currStatus : linkStatusList) {
					if (hasPrev)
						tmpTbl.append(" or ");
					tmpTbl.append("lnk" + currentTableAlias + ".status='" + currStatus + "'");
					hasPrev = true;
				}
				tmpTbl.append(") ");
			}
			joinCritFromPrev
					.append(" on lnk" + currentTableAlias + ".link_obj_id_2=" + currentTableAlias + ".object_id ");
		}
			break;
		case DBLINK_REVERSE: {
			if (prevDqo.linkToNext == null)
				throw (new SvException("system.error.dbqe_sql_dblink_err", svCONST.systemUser, null, this));

			if (dblt == null)
				throw (new SvException("system.error.dbqe_sql_dblinkconf_err", svCONST.systemUser, null, this));
			// first get the dblink table name
			tmpTbl.append(" " + prevDqo.joinToNext.toString() + " JOIN " + dblt.getVal("schema") + ".v"
					+ dblt.getVal("table_name") + " lnk" + currentTableAlias);

			tmpTbl.append(" on " + currentRepoPrefix + ".object_id=lnk" + currentTableAlias + ".link_obj_id_2 and lnk"
					+ currentTableAlias + ".link_type_id=" + prevDqo.linkToNext.getObject_id() + " ");

			if (linkStatusList != null && linkStatusList.size() > 0) {
				boolean hasPrev = false;
				tmpTbl.append("and (");
				for (String currStatus : linkStatusList) {
					if (hasPrev)
						tmpTbl.append(" or ");
					tmpTbl.append("lnk" + currentTableAlias + ".status='" + currStatus + "'");
					hasPrev = true;
				}
				tmpTbl.append(") ");
			}

			joinCritFromPrev
					.append(" on lnk" + currentTableAlias + ".link_obj_id_1=" + currentTableAlias + ".object_id ");

			// linkSeq++;
		}
			break;
		case DENORMALIZED: {
			// first get the dblink table name
			joinCritFromPrev.append(" on " + currentRepoPrefix + ".object_id=" + currentTableAlias + "."
					+ prevDqo.getDenormalizedFieldName() + " ");
		}
			break;
		case DENORMALIZED_REVERSE: {
			// first get the dblink table name
			joinCritFromPrev.append(" on " + currentRepoPrefix + "." + prevDqo.getDenormalizedFieldName() + "="
					+ currentTableAlias + ".object_id ");
		}
			break;
		case DENORMALIZED_FULL: {
			// first get the dblink table name
			joinCritFromPrev.append(" on " + currentRepoPrefix + "." + prevDqo.getDenormalizedFieldName() + "="
					+ currentTableAlias + "." + currentDqo.getDenormalizedFieldName() + " ");
		}
			break;
		}
		tmpTbl.append(" " + prevDqo.joinToNext.toString() + " JOIN ");
		nextJoinStr[0] = tmpTbl;
		nextJoinStr[1] = joinCritFromPrev;
		return hasCustomFromPrev;
	}

	@Override
	public ArrayList<Object> getSQLParamVals() throws SvException {
		if (this.isReverseExpression && queryParamVals != null) {
			return queryParamVals;
		}
		ArrayList<Object> arr = new ArrayList<Object>();
		ArrayList<Object> sub = new ArrayList<Object>();

		for (DbQueryObject item : items) {
			if (item.linkToNextType != null
					&& (item.linkToNextType.equals(LinkType.DBLINK)
							|| item.linkToNextType.equals(LinkType.DBLINK_REVERSE))
					&& item.linkToNext != null && dblt != null) {
				if (item.getReferenceDate() == null)
					arr.add(SvConf.MAX_DATE);
				else
					arr.add(item.getReferenceDate());
			}

			if (item.getSQLParamVals() != null)
				arr.addAll(item.getSQLParamVals());
			if (item.getSubSQLParamVals() != null)
				sub.addAll(item.getSubSQLParamVals());
		}
		sub.addAll(arr);
		return sub;

	}

	private void getReverseReturnTypes(DbQueryObject queryObject, ArrayList<DbDataObject> returnTypes) {

		DbQueryObject currentDqo = queryObject;
		if (currentDqo.getIsReturnType())
			returnTypes.add(currentDqo.getDbt());

		for (DbQueryObject childDqo : currentDqo.children)
			getReverseReturnTypes(childDqo, returnTypes);
	}

	private void getLegacyReturnType() {
		if (this.getReturnType() != null) {
			// find if any of the DQO objects has isReturnType set.
			Boolean hasDqoIsReturnType = false;
			for (DbQueryObject dqo : items) {
				if (dqo.getIsReturnType()) {
					hasDqoIsReturnType = true; // we found it
					break;
				}
			}
			// none of the DQOs has isReturnType set to true, so we pin it to
			// the first instance
			if (!hasDqoIsReturnType) {
				for (DbQueryObject dqo : items) {
					if (dqo.getDbt() != null && dqo.getDbt().getObjectId().equals(this.getReturnType().getObjectId())) {
						dqo.setIsReturnType(true); // we found it
						break;
					}
				}

			}

		} else if (rootQueryObject != null && isReverseExpression) {
			// this is related to setting the legacy return of a reverse query
			returnTypes.clear();
			getReverseReturnTypes(rootQueryObject, returnTypes);
		} else {
			// this is related to setting the legacy return of a forward query
			for (DbQueryObject dqo : items) {
				if (dqo.getIsReturnType()) {
					returnTypes.add(dqo.getDbt());
				}
			}

		}
	}

	public DbQueryObject getRootQueryObject() {
		return rootQueryObject;
	}

	public void setRootQueryObject(DbQueryObject root) {
		this.rootQueryObject = root;
	}

	public Boolean getIsReverseExpression() {
		return (rootQueryObject != null) ? isReverseExpression : false;
	}

	public void setIsReverseExpression(Boolean isReverseExpression) {
		this.isReverseExpression = isReverseExpression;
	}

}