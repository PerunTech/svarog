/*******************************************************************************
 * Copyright (c) 2013, 2017 Perun Technologii DOOEL Skopje.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License
 * Version 2.0 or the Svarog License Agreement (the "License");
 * You may not use this file except in compliance with the License. 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See LICENSE file in the project root for the specific language governing 
 * permissions and limitations under the License.
 *
 *******************************************************************************/
package com.prtech.svarog;

import java.util.ArrayList;
import java.util.ResourceBundle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvConstraint {
	SvObjectConstraints parent = null;
	public static final ResourceBundle sqlKw = SvConf.getSqlkw();
	
	static final Logger log4j = LogManager.getLogger(SvConstraint.class.getName());

	ArrayList<Long> fields = new ArrayList<Long>();
	String uniqueLevel = null;
	String constraintName = null;
	StringBuilder strBase = null;
	StringBuilder singleCrit = null;

	public DbDataArray getConstraintFields()
	{
		DbDataArray retval=new DbDataArray();
		retval.setItems(getConstraintFieldsA());
		return retval;
		
	}
	private ArrayList<DbDataObject> getConstraintFieldsA()
	{
		ArrayList<DbDataObject> fieldObjs=new ArrayList<DbDataObject>();
		for(Long fieldId:fields)
		{
			
			DbDataObject fld = DbCache.getObject(fieldId, svCONST.OBJECT_TYPE_FIELD);
			fieldObjs.add(fld);
		}
		return fieldObjs;
	}
	
	
	StringBuilder getBase() {
		if (strBase == null) {
			strBase = new StringBuilder();
			DbDataObject dbt = parent.getDbt();
			strBase.append("SELECT '" + constraintName + "' as constr_name, ");
			String nvlFunction = sqlKw.getString("NVL");
			singleCrit = new StringBuilder();
			singleCrit.append("(" + (uniqueLevel.equals("PARENT") ? " "+sqlKw.getString("OBJECT_QUALIFIER_LEFT")+"PARENT_ID"+sqlKw.getString("OBJECT_QUALIFIER_RIGHT")+"=? AND " : ""));
			singleCrit.append(sqlKw.getString("OBJECT_QUALIFIER_LEFT")+"OBJECT_ID"+sqlKw.getString("OBJECT_QUALIFIER_RIGHT")+"!=? AND ");
			for (Long fieldId : fields) {
				DbDataObject fld =DbCache.getObject(fieldId, svCONST.OBJECT_TYPE_FIELD);
				boolean isNullable = (boolean)fld.getVal("IS_NULL");
				singleCrit.append((isNullable?nvlFunction+"(":"")+sqlKw.getString("OBJECT_QUALIFIER_LEFT")+(String) fld.getVal("FIELD_NAME")+sqlKw.getString("OBJECT_QUALIFIER_RIGHT")+ (isNullable?", 'NaN')":"")+"="+(isNullable?nvlFunction+"(":"")+"?"+(isNullable?", 'NaN')":"")+" AND ");
				strBase.append(sqlKw.getString("STRING_CAST").replace("{COLUMN_NAME}", sqlKw.getString("OBJECT_QUALIFIER_LEFT")+(String) fld.getVal("FIELD_NAME") +sqlKw.getString("OBJECT_QUALIFIER_RIGHT"))+sqlKw.getString("STRING_CONCAT")+"','"+sqlKw.getString("STRING_CONCAT"));
			}
			singleCrit.setLength(singleCrit.length() - 4);
			singleCrit.append(")");

			strBase.setLength(strBase.length() - ((sqlKw.getString("STRING_CONCAT").length()*2)+3));
			strBase.append(
					" as existing_unq_vals FROM " + dbt.getVal("schema") + ".v" + dbt.getVal("table_name") + " WHERE DT_DELETE=? AND");
		}
		return strBase;
	}
	StringBuilder getSingleCriterion() {
		if (singleCrit == null) {
			strBase = getBase();
		}
		return singleCrit;
	}

	public SvConstraint(SvObjectConstraints parent, String constraintName) {
		this.parent = parent;
		this.constraintName = constraintName;
	}

	public boolean addField(DbDataObject dbf) {
		fields.add(dbf.getObject_id());
		if (uniqueLevel == null)
			uniqueLevel = (String) dbf.getVal("unq_level");
		else if (!uniqueLevel.equals((String) dbf.getVal("unq_level")))
		{
			log4j.warn(
					"One or multiple fields configured under same unique constraint have different uniqueness Level! Expected:"+uniqueLevel+", got:"+(String) dbf.getVal("unq_level"));
			return false;
		}
		return true;
	}

	public StringBuilder getSQLQueryString(DbDataArray dba) {
		if (fields.size() < 1 || uniqueLevel == null)
			return null;
		StringBuilder sqlBuilder= new StringBuilder();
		sqlBuilder.append(getBase());
		sqlBuilder.append("(");
		for (int i=0;i<dba.getItems().size();i++)
			sqlBuilder.append(singleCrit).append(" OR ");
		sqlBuilder.setLength(sqlBuilder.length() - 4);
		sqlBuilder.append(")");
		return sqlBuilder;
	}
	
	public ArrayList<Object> getSQLParamVals(DbDataArray dba) {
		if (fields.size() < 1 || uniqueLevel == null)
			return null;
		ArrayList<Object> bindVals = new ArrayList<Object>();
		ArrayList<DbDataObject> fieldObjs=getConstraintFieldsA();

		bindVals.add(SvConf.MAX_DATE);
		for (DbDataObject dbo:dba.getItems())
		{
			if(uniqueLevel.equals("PARENT"))
				bindVals.add(dbo.getParent_id());
			bindVals.add(dbo.getObject_id());
			for(DbDataObject fld:fieldObjs)
			{
				bindVals.add(dbo.getVal((String)fld.getVal("FIELD_NAME")));
			}
		}
		return bindVals;
	}
}
