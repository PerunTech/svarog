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

import java.util.ArrayList;
import java.util.HashMap;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvObjectConstraints {
	HashMap<String, SvConstraint> constraints = new HashMap<String, SvConstraint>();
	DbDataObject dbt;
	DbDataArray dbtFields;

	SvObjectConstraints(DbDataObject dbt) throws SvException {
		this.dbt = dbt;
		dbtFields = SvCore.getFields(dbt.getObjectId());
		if (dbtFields != null) {
			for (DbDataObject dbo : dbtFields.getItems()) {
				if ((Boolean) dbo.getVal("is_unique")) {
					String unqName = (String) dbo.getVal("unq_constraint_name");
					unqName = (unqName == null ? "DEFAULT_UNQ" : unqName);
					SvConstraint currentCons = constraints.get(unqName);
					if (currentCons == null) {
						currentCons = new SvConstraint(this, unqName);
						constraints.put(unqName, currentCons);
					}
					if (!currentCons.addField(dbo))
						throw (new SvException("system.error.bad_constraint_cfg", svCONST.systemUser, dbt, dbo));

				}
			}
		}
		for (SvConstraint svc : constraints.values())
			svc.initSQL();
	}

	public StringBuilder getSQLQueryString(DbDataArray dba) {
		StringBuilder retStrB = null;
		for (SvConstraint svc : constraints.values()) {
			if (retStrB == null)
				retStrB = svc.getSQLQueryString(dba);
			else {
				retStrB.append(" UNION ");
				retStrB.append(svc.getSQLQueryString(dba));
			}
		}
		return retStrB;
	}

	public ArrayList<Object> getSQLParamVals(DbDataArray dba) {
		ArrayList<Object> retVals = null;
		for (SvConstraint svc : constraints.values()) {
			if (retVals == null)
				retVals = svc.getSQLParamVals(dba);
			else {

				retVals.addAll(svc.getSQLParamVals(dba));
			}
		}
		return retVals;
	}

	public HashMap<String, SvConstraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(HashMap<String, SvConstraint> constraints) {
		this.constraints = constraints;
	}

	public DbDataObject getDbt() {
		return dbt;
	}

	public void setDbt(DbDataObject dbt) {
		this.dbt = dbt;
	}

	public DbDataArray getDbtFields() {
		return dbtFields;
	}

	public void setDbtFields(DbDataArray dbtFields) {
		this.dbtFields = dbtFields;
	}
}
