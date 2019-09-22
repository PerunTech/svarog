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

import com.prtech.svarog.SvException;
import com.prtech.svarog_common.DbDataObject;

/**
 * 
 * @author PR01
 * 
 *         The DbQuery abstract class is the basis for generating queries
 *         against the database
 *
 */
public abstract class DbQuery extends Jsonable {
	/**
	 * The method generates a ready to use SQL expression which will be executed
	 * against the DB
	 * 
	 * @return String containing the SQL query
	 * @throws Exception
	 */

	public abstract String getSQLExpression() throws SvException;

	/**
	 * The method generates a ready to use SQL expression which will be executed
	 * against the DB
	 * 
	 * @param Setting
	 *            forcePhysicalTables to true will render query based on joins
	 *            between physical tables rather than views Should be used with
	 *            care, especially when dealing with complex joins
	 * @return String containing the SQL query
	 * @throws Exception
	 */
	public abstract StringBuilder getSQLExpression(Boolean forcePhysicalTables) throws SvException;

	/**
	 * The method generates a ready to use SQL expression which will be executed
	 * against the DB
	 * 
	 * @param forcePhysicalTables
	 *            Setting this param to true will render query based on joins
	 *            between physical tables rather than views Should be used with
	 *            care, especially when dealing with complex joins
	 * @param includeGeometries
	 *            This parameter controls the inclusion/exclusion of any
	 *            GEOMETRY fields
	 * @return String containing the SQL query
	 * @throws Exception
	 */
	public abstract StringBuilder getSQLExpression(Boolean forcePhysicalTables, Boolean includeGeometries)
			throws SvException;

	/**
	 * The method generates an array of objects which are used as values to be
	 * bind to query parameters
	 * 
	 * @return
	 * @throws SvException
	 */
	public abstract ArrayList<Object> getSQLParamVals() throws SvException;

	protected ArrayList<DbDataObject> returnTypes = new ArrayList<DbDataObject>();
	/**
	 * Variable holding the return type sequence order in the select query
	 */
	protected ArrayList<Integer> returnTypeSequences = new ArrayList<Integer>();

	/*********************
	 * NOTICE ****************************** All methods and variables bellow
	 * are the legacy methods for Svarog versions which do not support multiple
	 * return types.
	 ***********************************************************/

	/**
	 * The variable holding the type of object which should be returned to by
	 * the query. If the returnType is null the query will return a complex type
	 * of object containing all columns in tabular form
	 */
	// protected DbDataObject returnType=null;
	/**
	 * Getter for the returnType
	 * 
	 * @return A DbDataObject describing the return type.
	 */

	// public abstract DbDataObject getReturnType();
	/**
	 * Method to set the return type of DbQuery
	 * 
	 * @param returnType
	 */
	// public abstract void setReturnType(DbDataObject returnType);
	public DbDataObject getReturnType() {
		if (this.returnTypes.size() == 1)
			return returnTypes.get(0);
		else
			return null;
	}

	public ArrayList<DbDataObject> getReturnTypes() {
		return returnTypes;
	}

	@Deprecated
	public void setReturnType(DbDataObject returnType) {
		this.returnTypes.clear();
		this.returnTypes.add(returnType);
	}

	public Integer getReturnTypeSequence() {
		if (this.returnTypeSequences.size() > 0)
			return returnTypeSequences.get(0);
		else
			return 0;
	}

	public void setReturnTypeSequence(Integer returnTypeSequence) {
		this.returnTypeSequences.clear();
		this.returnTypeSequences.add(returnTypeSequence);
	}

}
