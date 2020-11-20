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
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

/***
 * Class for managing parameters. It extends the {@link SvCore} class and
 * provides functionality for generating (
 * {@link #generateParameters(DbDataObject, DbDataObject, Boolean)}), setting
 * and getting parameters ( {@link #getParameters} ) from the system. The
 * functionality is accomplished through a link (LINK_CONFOBJ_WITH_PARAM_TYPE )
 * between a configuration object and a parameter_type(s). During the generation
 * of parameters we get the parameter types attached to the configuration object
 * and instantiate parameters which we attach to the implementation object with
 * a parent child relationship.
 * 
 * 
 * @see #getParameters
 * @see #generateParameters(DbDataObject, DbDataObject, Boolean)
 * @author Kristijan Ristovski
 *
 */
public class SvParameter extends SvCore {

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvParameter(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvParameter(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public SvParameter(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvParameter() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Method that returns the parameters attached to this object from the database.
	 * The object that we receive is a HashMap of a DbDataObject,DbDataArray type,
	 * where the DbDataObject is the SVAROG_PARAM and the DbDataArray is the list of
	 * SVAGOR_PARAM_VALUE objects for that SVAROG_PARAM object
	 * 
	 * @param dbo The object for which we will return the parameters
	 * @return Array objects containing parameter with its value(s)
	 * @throws SvException
	 */
	public HashMap<DbDataObject, DbDataArray> getParameters(DbDataObject dbo) throws SvException {

		
		HashMap<DbDataObject, DbDataArray> allParams = new HashMap<>();
		try(SvReader svr = new SvReader(this)) {
			// get all existing parameters for the object user of the parameters
			DbDataArray allParamsForObj = svr.getObjectsByParentId(dbo.getObjectId(), svCONST.OBJECT_TYPE_PARAM, null,
					0, 0);

			if (allParamsForObj != null && allParamsForObj.getItems().size() > 0) {
				for (DbDataObject dbParam : allParamsForObj.getItems()) {
					// get all existing values for the parameter
					DbDataArray allValueFopParam = svr.getObjectsByParentId(dbParam.getObjectId(),
							svCONST.OBJECT_TYPE_PARAM_VALUE, null, 0, 0);
					allParams.put(dbParam, allValueFopParam);
				}
			}

			return allParams;
		} 
	}

	/**
	 * Method that creates parameters for object. This overloaded version performs
	 * commit on success and rollback on exception
	 * 
	 * @param dbo       The implementation object for which we will generate the
	 *                  parameters
	 * @param dbConfObj The configuration object from which we will take the
	 *                  configuration for which parameters we need to generate
	 * @throws SvException Pass through underlying exceptions from
	 *                     {@link #generateParametersImpl(DbDataObject, DbDataObject)}
	 */
	public void generateParameters(DbDataObject dbo, DbDataObject dbConfObj) throws SvException {
		generateParameters(dbo, dbConfObj, this.autoCommit);
	}

	/**
	 * Method that generates parameters for an implementation object. It will
	 * generate the parameters for it according to the configuration attached to the
	 * configuration object ( through the following link
	 * LINK_CONFOBJ_WITH_PARAM_TYPE). It will search for any SVAROG_PARAM_TYPE
	 * objects attached to the configuration object and if it finds any it will
	 * instantiate objects of SVAROG_PARAM and SVAROG_PARAM_VALUE accordingly. Has
	 * an option to auto commit or rollback if exception occured.
	 * 
	 * @param dbo        The implementation object for which we will generate the
	 *                   parameters
	 * @param dbConfObj  The configuration object from which we will take the
	 *                   configuration for which parameters we need to generate
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException
	 */
	public void generateParameters(DbDataObject dbo, DbDataObject dbConfObj, Boolean autoCommit) throws SvException {

		try {
			this.dbSetAutoCommit(false);
			generateParametersImpl(dbo, dbConfObj);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * Implementation method that we generate parameters for object
	 * 
	 * @param dbo       The implementation object for which we will generate the
	 *                  parameters
	 * @param dbConfObj The configuration object from which we will take the
	 *                  configuration for which parameters we need to generate
	 * @throws SvException
	 */
	void generateParametersImpl(DbDataObject dbo, DbDataObject dbConfObj) throws SvException {

		try (SvReader svr = new SvReader(this); SvWriter svw = new SvWriter(svr)) {
			// get all existing parameters for the object user of the parameters
			HashMap<DbDataObject, DbDataArray> allParamsForObj = getParameters(dbo);

			// get all config parameters for the config object
			DbDataArray allConfParamsOfConfObj = svr.getObjectsByLinkedId(dbConfObj.getObjectId(),
					dbConfObj.getObjectType(), SvCore.getLinkType("LINK_CONFOBJ_WITH_PARAM_TYPE",
							dbConfObj.getObjectType(), svCONST.OBJECT_TYPE_PARAM_TYPE),
					svCONST.OBJECT_TYPE_PARAM_TYPE, false, null, 0, 0);

			DbDataObject paramValue;

			if (allParamsForObj != null && allParamsForObj.isEmpty()) {

				// Check if existing parameters for the config object
				if (allConfParamsOfConfObj != null && allConfParamsOfConfObj.getItems().size() > 0) {
					String value = "";
					for (DbDataObject dbConfParam : allConfParamsOfConfObj.getItems()) {
						DbDataObject dbParam = createParam(dbo, dbConfParam);
						svw.saveObject(dbParam, false);
						value = dbConfParam != null && dbConfParam.getVal("DEFAULT_VALUE") != null
								? dbConfParam.getVal("DEFAULT_VALUE").toString()
								: "";
						paramValue = createParamValue(dbParam, value);
						svw.saveObject(paramValue, false);
					}
				}
			}
		}

	}

	/**
	 * Method that sets parameters for an implementation object. This overloaded
	 * version performs commit on success and rollback on exception. See also
	 * {@link #setParameters(DbDataObject, HashMap, Boolean)})
	 * 
	 * 
	 * @param dbo    The implementation object for which we will set the parameters
	 * @param params Already existing parameters for implementation object which is
	 *               relationship with dbo. In case we can them with (
	 *               {@link #getParameters} ).
	 * @throws SvException
	 * 
	 */
	public void setParameters(DbDataObject dbo, HashMap<DbDataObject, ArrayList<DbDataObject>> params)
			throws SvException {
		setParameters(dbo, params, this.autoCommit);
	}

	/**
	 * Method that sets parameters for an implementation object. It will set the
	 * parameters for it according to the already existing parameters for its
	 * implementation object with which is a parent-child relationship. It will
	 * instantiate objects of SVAROG_PARAM and SVAROG_PARAM_VALUE accordingly. Has
	 * an option to auto commit or rollback if exception occured.
	 * 
	 * @param dbo        The implementation object for which we will set the
	 *                   parameters
	 * @param params     Already existing parameters for implementation object which
	 *                   is relationship with dbo. In case we can them with (
	 *                   {@link #getParameters} ).
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException
	 */

	public void setParameters(DbDataObject dbo, HashMap<DbDataObject, ArrayList<DbDataObject>> params,
			Boolean autoCommit) throws SvException {

		try {
			this.dbSetAutoCommit(false);
			setParametersImpl(dbo, params);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * Implementation method that we will set parameters for an implementation
	 * object.
	 * 
	 * @param dbo    The implementation object for which we will set the parameters
	 * @param params Already existing parameters for implementation object which is
	 *               relationship with dbo. In case we can them with (
	 *               {@link #getParameters} ).
	 * @throws SvException
	 */
	void setParametersImpl(DbDataObject dbo, HashMap<DbDataObject, ArrayList<DbDataObject>> params) throws SvException {

		try (SvReader svr = new SvReader(this); SvWriter svw = new SvWriter(svr)) {

			DbDataObject paramValue;
			DbDataObject param;
			String value;
			if (params != null && !params.isEmpty()) {

				for (DbDataObject dbParam : params.keySet()) {
					DbDataObject confParam = svr.getObjectById((Long) dbParam.getVal(Sv.PARAM_TYPE_ID),
							svCONST.OBJECT_TYPE_PARAM_TYPE, null);
					param = createParam(dbo, confParam);
					svw.saveObject(param, false);
					ArrayList<DbDataObject> paramValues = params.get(dbParam);
					Iterator<DbDataObject> it = paramValues.iterator();
					while (it.hasNext()) {
						DbDataObject dbValue = it.next();

						value = dbValue.getVal(Sv.VALUE) != null ? dbValue.getVal(Sv.VALUE).toString() : "";
						paramValue = createParamValue(param, value);
						svw.saveObject(paramValue, false);
					}

				}
			}
		}

	}

	/**
	 * Method that creates instance of parameter
	 * 
	 * @param dbObj     The object for which creates parameter
	 * @param confParam The configuration type of parameter
	 * @return parameterObject
	 */
	private DbDataObject createParam(DbDataObject dbObj, DbDataObject confParam) {
		DbDataObject dbParam = new DbDataObject();

		dbParam.setObjectType(svCONST.OBJECT_TYPE_PARAM);
		dbParam.setParentId(dbObj.getObjectId());
		dbParam.setVal(Sv.PARAM_TYPE_ID, confParam != null ? confParam.getObjectId() : 0L);
		dbParam.setVal(Sv.PARAM_ORDER, confParam != null ? confParam.getVal(Sv.PARAM_ORDER) : 0L);

		return dbParam;
	}

	/**
	 * Method that creates instance of value of parameter
	 * 
	 * @param objParam The object for which creates value
	 * @param value    The value which will be set as value of the object
	 * @return parameterValueObject
	 */
	private DbDataObject createParamValue(DbDataObject objParam, String value) {
		DbDataObject dbParamVal = new DbDataObject();
		dbParamVal.setObjectType(svCONST.OBJECT_TYPE_PARAM_VALUE);
		dbParamVal.setParentId(objParam.getObjectId());
		dbParamVal.setVal(Sv.VALUE, value);

		return dbParamVal;
	}

	/**
	 * Method that returns DbDataObject of PARAM_TYPE according labelCode.
	 * 
	 * @param labelCode The label code of the parameter type
	 * @return DbDataObject
	 */
	public DbDataObject getParamType(String labelCode) throws SvException {
		DbDataObject paramType = DbCache.getObject(labelCode, svCONST.OBJECT_TYPE_PARAM_TYPE);

		if (paramType == null) {
			try (SvReader svr = new SvReader(this)) {

				DbSearchExpression dbSearch = new DbSearchExpression();
				DbSearchCriterion dbcrit = new DbSearchCriterion(Sv.LABEL_CODE.toString(), DbCompareOperand.EQUAL, labelCode);
				dbSearch.addDbSearchItem(dbcrit);

				DbDataArray dbarr = svr.getObjects(dbSearch, svCONST.OBJECT_TYPE_PARAM_TYPE, new DateTime(), 0, 0);
				if (dbarr != null && dbarr.getItems().size() > 0) {
					paramType = dbarr.getItems().get(0);
				}

				if (paramType == null) {
					return null;
				} else
					DbCache.addObject(paramType, labelCode);
			}
		}
		return paramType;

	}

	/**
	 * Method that returns the DbDataObject attached to default repo object
	 * according labelCode from the database.
	 * 
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first object.
	 */
	public DbDataObject getParamObject(String labelCode) throws SvException {
		return getParamImpl(SvCore.repoDbt, labelCode);
	}

	/**
	 * Method that returns the DbDataArray attached to default repo object according
	 * labelCode from the database.
	 * 
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It returns @DbDataArray.
	 */
	public DbDataArray getParamObjects(String labelCode) throws SvException {
		return getParamValuesImpl(SvCore.repoDbt, labelCode);
	}

	/**
	 * Method that returns the DbDataObject attached to dbo according labelCode from
	 * the database.
	 * 
	 * @param dbo       The object for which we will return one.
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first object.
	 */
	public DbDataObject getParamObject(DbDataObject dbo, String labelCode) throws SvException {
		return getParamImpl(dbo, labelCode);
	}

	/**
	 * Method that returns the DbDataArray attached to dbo according labelCode from
	 * the database.
	 * 
	 * @param dbo       The object for which we will return one.
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It returns @DbDataArray.
	 */
	public DbDataArray getParamObjects(DbDataObject dbo, String labelCode) throws SvException {
		return getParamValuesImpl(dbo, labelCode);
	}

	/**
	 * Method that returns value of DbDataObject which is attached to default repo
	 * object from the database.
	 * 
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public DateTime getParamDateTime(String label) throws SvException {
		return getParamDateTime(SvCore.repoDbt, label);
	}

	/**
	 * Method that returns value of DbDataObject which is attached to dbo from the
	 * database.
	 * 
	 * @param dbo   The object for which we will return the parameter value
	 *              according to label.
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public DateTime getParamDateTime(DbDataObject dbo, String label) throws SvException {
		DateTime result = null;
		DbDataObject dbParamValue = getParamImpl(dbo, label);

		if (dbParamValue != null) {
			DateTimeFormatter df = DateTimeFormat.forPattern("MM.dd.yyyy HH:mm:ss");
			result = df.parseDateTime(dbParamValue.getVal(Sv.VALUE).toString());

		}

		return result;
	}

	/**
	 * Method that returns value of DbDataObject which is attached to default repo
	 * object from the database.
	 * 
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public Long getParamLong(String label) throws SvException {
		return getParamLong(SvCore.repoDbt, label);
	}

	/**
	 * Method that returns value of DbDataObject which is attached to dbo from the
	 * database.
	 * 
	 * @param dbo   The object for which we will return the parameter value
	 *              according to label.
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public Long getParamLong(DbDataObject dbo, String label) throws SvException {
		Long result = null;
		DbDataObject dbParamValue = getParamImpl(dbo, label);

		if (dbParamValue != null) {
			result = Long.valueOf(dbParamValue.getVal(Sv.VALUE).toString());
		}

		return result;
	}

	/**
	 * Method that returns value of DbDataObject which is attached to default repo
	 * object from the database.
	 * 
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public String getParamString(String label) throws SvException {
		return getParamString(SvCore.repoDbt, label);
	}

	/**
	 * Method that returns value of DbDataObject which is attached to dbo from the
	 * database.
	 * 
	 * @param dbo   The object for which we will return the parameter value
	 *              according to label.
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first value of object.
	 */
	public String getParamString(DbDataObject dbo, String label) throws SvException {
		String result = null;
		DbDataObject dbParamValue = getParamImpl(dbo, label);

		if (dbParamValue != null) {
			result = dbParamValue.getVal(Sv.VALUE).toString();
		}

		return result;
	}

	/**
	 * The implementation method that returns DbDataObject of PARAM_VALUE attached
	 * to dbo from the database.
	 * 
	 * @param dbo   The object for which we will return the parameters according to
	 *              label.
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It only returns the first object.
	 */
	DbDataObject getParamImpl(DbDataObject dbo, String label) throws SvException {

		try (SvReader svr = new SvReader(this)) {

			DbDataObject result = null;

			DbDataObject paramType = getParamType(label);

			DbDataArray allParams = svr.getObjectsByParentId(dbo.getObjectId(), svCONST.OBJECT_TYPE_PARAM, null, 0, 0);

			if (allParams.getItems().size() > 0) {
				for (DbDataObject dbParam : allParams.getItems()) {
					if (paramType == null || dbParam.getVal(Sv.PARAM_TYPE_ID).equals(paramType.getObjectId())) {
						result = svr.getObjectsByParentId(dbParam.getObjectId(), svCONST.OBJECT_TYPE_PARAM_VALUE, null,
								0, 0).getItems().get(0);
						break;
					}
				}
			}
			return result;
		}

	}

	/**
	 * The implementation method that returns DbDataArray of PARAM_VALUE attached to
	 * dbo from the database.
	 * 
	 * @param dbo   The object for which we will return the parameters according to
	 *              label.
	 * @param label label is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * 
	 * @return It returns all objects.
	 */
	DbDataArray getParamValuesImpl(DbDataObject dbo, String label) throws SvException {

		try (SvReader svr = new SvReader(this)) {
			DbDataArray result = null;

			DbDataObject paramType = getParamType(label);

			DbDataArray allParams = svr.getObjectsByParentId(dbo.getObjectId(), svCONST.OBJECT_TYPE_PARAM, null, 0, 0);

			if (allParams.getItems().size() > 0) {
				for (DbDataObject dbParam : allParams.getItems()) {
					if (paramType == null || dbParam.getVal(Sv.PARAM_TYPE_ID).equals(paramType.getObjectId())) {
						result = svr.getObjectsByParentId(dbParam.getObjectId(), svCONST.OBJECT_TYPE_PARAM_VALUE, null,
								0, 0);
						break;
					}
				}
			}
			return result;
		}

	}

	/**
	 * Method that sets the value of parameter attached to default repo object or
	 * create new according to labelCode.
	 * 
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 */

	public void setParamDateTime(String labelCode, DateTime value) throws SvException {
		setParamDateTime(labelCode, value, this.autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to default repo object or
	 * create new according to labelCode, with option to skip existence check as
	 * well as disable the auto commit.
	 * 
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 */
	public void setParamDateTime(String labelCode, DateTime value, Boolean autoCommit) throws SvException {

		DateTimeFormatter df = DateTimeFormat.forPattern("MM.dd.yyyy HH:mm:ss");
		setParamImpl(SvCore.repoDbt, labelCode, df.print(value), autoCommit, true);
	}

	/**
	 * Method that sets the value of parameter attached to dbo or create new
	 * according to labelCode.
	 * 
	 * @param dbo       The object for which we will set the value of parameters.
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 */
	public void setParamDateTime(DbDataObject dbo, String labelCode, DateTime value) throws SvException {

		setParamDateTime(dbo, labelCode, value, this.autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to dbo or create new
	 * according to labelCode, with option to skip existence check as well as
	 * disable the auto commit.
	 * 
	 * @param dbo        The object for which we will set the value of parameters.
	 * @param labelCode  labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit Flag to enable/disable auto commit
	 * 
	 */
	public void setParamDateTime(DbDataObject dbo, String labelCode, DateTime value, Boolean autoCommit)
			throws SvException {

		DateTimeFormatter df = DateTimeFormat.forPattern("MM.dd.yyyy HH:mm:ss");
		setParamImpl(dbo, labelCode, df.print(value), autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to default repo object or
	 * create new according to labelCode.
	 * 
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 */

	public void setParamLong(String labelCode, Long value) throws SvException {
		setParamLong(labelCode, value, this.autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to default repo object or
	 * create new according to labelCode, with option to skip existence check as
	 * well as disable the auto commit.
	 * 
	 * @param labelCode  labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException
	 * 
	 */

	public void setParamLong(String labelCode, Long value, Boolean autoCommit) throws SvException {

		setParamImpl(SvCore.repoDbt, labelCode, String.valueOf(value), autoCommit, true);
	}

	/**
	 * Method that sets the value of parameter attached to dbo or create new
	 * according to labelCode.
	 * 
	 * @param dbo       The object for which we will set the value of parameters.
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 * @throws SvException
	 * 
	 */
	public void setParamLong(DbDataObject dbo, String labelCode, Long value) throws SvException {

		setParamLong(dbo, labelCode, value, this.autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to dbo or create new
	 * according to labelCode, with option to skip existence check as well as
	 * disable the auto commit.
	 * 
	 * @param dbo        The object for which we will set the value of parameters.
	 * @param labelCode  labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException
	 * 
	 */
	public void setParamLong(DbDataObject dbo, String labelCode, Long value, Boolean autoCommit) throws SvException {

		setParamImpl(dbo, labelCode, String.valueOf(value), autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to default repo object or
	 * create new according to labelCode.
	 * 
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 * @throws SvException
	 * 
	 */

	public void setParamString(String labelCode, String value) throws SvException {
		setParamString(labelCode, value, this.autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to default repo object or
	 * create new according to labelCode, with option to skip existence check as
	 * well as disable the auto commit.
	 * 
	 * @param labelCode  labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException
	 * 
	 */

	public void setParamString(String labelCode, String value, Boolean autoCommit) throws SvException {
		setParamImpl(SvCore.repoDbt, labelCode, value, autoCommit, true);
	}

	/**
	 * Method that sets the value of parameter attached to dbo or create new
	 * according to labelCode.
	 * 
	 * @param dbo       The object for which we will set the value of parameters.
	 * @param labelCode labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * 
	 * @throws SvException
	 * 
	 */
	public void setParamString(DbDataObject dbo, String labelCode, String value) throws SvException {

		setParamString(dbo, labelCode, value, this.autoCommit);
	}

	/**
	 * Method that sets the value of parameter attached to dbo or create new
	 * according to labelCode, with option to skip existence check as well as
	 * disable the auto commit.
	 * 
	 * @param dbo        The object for which we will set the value of parameters.
	 * @param labelCode  labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit Flag to enable/disable auto commit
	 * @throws SvException
	 * 
	 */
	public void setParamString(DbDataObject dbo, String labelCode, String value, Boolean autoCommit)
			throws SvException {

		setParamImpl(dbo, labelCode, value, autoCommit);
	}

	/**
	 * The implementation method that sets the value of parameter attached to dbo or
	 * create new according to labelCode.
	 * 
	 * @param dbo        The object for which we will set the value of parameters.
	 * @param labelCode  labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit Flag to disable the auto commit on success. In case the
	 *                   linking is part of a transaction
	 * 
	 */
	void setParamImpl(DbDataObject dbo, String labelCode, String value, Boolean autoCommit) throws SvException {
		setParamImpl(dbo, labelCode, value, autoCommit, false);
	}

	/**
	 * The implementation method that sets the value of parameter attached to dbo or
	 * create new according to labelCode.
	 * 
	 * @param dbo            The object for which we will set the value of
	 *                       parameters.
	 * @param labelCode      labelCode is LABEL_CODE of SVAROG_PARAM_TYPE object.
	 * @param value
	 * @param autoCommit     Flag to disable the auto commit on success. In case the
	 *                       linking is part of a transaction
	 * @param autoCreateType Flag to autocreate an empty param type. Useful for
	 *                       anonymous params.
	 * 
	 */
	void setParamImpl(DbDataObject dbo, String labelCode, String value, Boolean autoCommit, Boolean autoCreateType)
			throws SvException {

		try (SvReader svr = new SvReader(this); SvWriter svw = new SvWriter(svr)) {
			DbDataObject dbParamValue = getParamObject(dbo, labelCode);
			if (dbParamValue != null) {
				dbParamValue.setVal(Sv.VALUE, value);
				svw.saveObject(dbParamValue, autoCommit);
			} else {
				DbDataObject paramType = null;
				paramType = getParamType(labelCode);
				if (paramType == null) {
					if (!autoCreateType)
						throw (new SvException("system.error.paramType_not_exsist", instanceUser, null, null));
					else
						paramType = createParamType(labelCode, autoCommit);
				}

				DbDataObject dbParam = createParam(dbo, paramType);
				svw.saveObject(dbParam, autoCommit);
				dbParamValue = createParamValue(dbParam, value);
				svw.saveObject(dbParamValue, autoCommit);

			}
		}

	}

	public DbDataObject createParamType(String labelCode, Boolean autoCommit) throws SvException {
		
		DbDataObject paramTypeObj = null;
		try (SvWriter svw = new SvWriter(this)){
			
			paramTypeObj = new DbDataObject();
			paramTypeObj.setObjectType(svCONST.OBJECT_TYPE_PARAM_TYPE);
			paramTypeObj.setStatus(svCONST.STATUS_VALID);
			paramTypeObj.setParentId(0L);
			paramTypeObj.setDtInsert(new DateTime());
			paramTypeObj.setDtDelete(SvConf.MAX_DATE);
			paramTypeObj.setVal(Sv.LABEL_CODE, labelCode);
			paramTypeObj.setVal(Sv.DATA_TYPE, Sv.NVARCHAR);
			paramTypeObj.setVal(Sv.INPUT_TYPE, Sv.TEXT_AREA);
			svw.saveObject(paramTypeObj, autoCommit);
		}

		return paramTypeObj;
	}

}
