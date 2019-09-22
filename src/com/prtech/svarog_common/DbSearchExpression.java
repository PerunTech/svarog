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
import java.util.Iterator;

import com.google.gson.JsonObject;
import com.prtech.svarog.SvException;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;

public class DbSearchExpression extends DbSearch {
	DbLogicOperand nextCritOperand = DbLogicOperand.AND;

	public String getNextCritOperand() {
		return nextCritOperand.toString();
	}

	public void setNextCritOperand(String nextCritOperand) {
		this.nextCritOperand = DbLogicOperand.valueOf(nextCritOperand);
	}

	ArrayList<DbSearch> exprList = new ArrayList<DbSearch>();
	ArrayList<String> orderByFields = new ArrayList<String>();

	public DbSearchExpression addDbSearchItem(DbSearch item) {
		exprList.add(item);
		return this;
	}

	@Override
	public String getSQLExpression() throws SvException {
		// TODO Auto-generated method stub
		return getSQLExpression("", "");
	}

	public DbSearchExpression() {

	}

	@Deprecated
	public String getSQLExpression(String repoPrefix, String tblPrefix) throws SvException {
		return getSQLExpression(tblPrefix);
	}

	@Override
	public String getSQLExpression(String tblPrefix) throws SvException {
		StringBuilder retval = new StringBuilder();
		if (exprList.size() < 1)
			return retval.toString();

		Iterator<DbSearch> it = exprList.iterator();

		while (it.hasNext()) {
			DbSearch dbs = it.next();
			String strExpr = dbs.getSQLExpression(tblPrefix);
			if (strExpr != null && !strExpr.equals("")) {
				retval.append(" " + dbs.getSQLExpression(tblPrefix));
				if (it.hasNext()) {
					if (dbs instanceof DbSearchCriterion)
						retval.append(" " + ((DbSearchCriterion) dbs).getNextCritOperand());
					else
						retval.append(" " + ((DbSearchExpression) dbs).getNextCritOperand());
				}
			}

		}
		return "(" + retval + ")";
	}

	public ArrayList<DbSearch> getExprList() {
		return exprList;
	}

	public void setExprList(ArrayList<DbSearch> exprList) {
		this.exprList = exprList;
	}

	@Override
	public ArrayList<Object> getSQLParamVals() throws SvException {
		ArrayList<Object> arr = new ArrayList<Object>();
		for (DbSearch item : exprList) {
			arr.addAll(item.getSQLParamVals());
		}
		return arr;

	}

	public ArrayList<String> getOrderByFields() {
		return orderByFields;
	}

	public void addOrderByField(String fieldName) {
		this.orderByFields.add(fieldName);
	}

}
