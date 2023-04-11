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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
	private static final Logger log4j = SvConf.getLogger(SvParameter.class);

	static Cache<String, DbDataObject> paramsCache = initParamCache();

	static int decodeType(Class<?> clazz) {
		int code = 0;
		if (clazz == String.class)
			code = 0;
		else if (clazz == DateTime.class)
			code = 1;
		else if (clazz == Boolean.class)
			code = 2;
		else if (clazz == Integer.class)
			code = 3;
		else if (clazz == Long.class)
			code = 4;
		else if (clazz == Double.class)
			code = 5;
		return code;

	}

	static Class<?> typeByCode(int type) {
		Class<?> clazz = String.class;
		switch (type) {
		case 0:
			clazz = String.class;
			break;
		case 1:
			clazz = DateTime.class;
			break;
		case 2:
			clazz = Boolean.class;
			break;
		case 3:
			clazz = Integer.class;
			break;
		case 4:
			clazz = Long.class;
			break;
		case 5:
			clazz = Double.class;
			break;

		}
		return clazz;
	}

	/**
	 * Method to initialise the params cache.
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	static Cache<String, DbDataObject> initParamCache() {
		@SuppressWarnings("rawtypes")
		CacheBuilder builder = CacheBuilder.newBuilder();
		builder = builder.maximumSize(Sv.DEFAULT_CACHE_SIZE);
		builder = builder.expireAfterAccess(Sv.DEFAULT_CACHE_TTL, TimeUnit.MINUTES);
		return (Cache<String, DbDataObject>) builder.<Long, DbDataObject>build();
	}

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
		try (SvReader svr = new SvReader(this)) {
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
				DbSearchCriterion dbcrit = new DbSearchCriterion(Sv.LABEL_CODE.toString(), DbCompareOperand.EQUAL,
						labelCode);
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
		try (SvWriter svw = new SvWriter(this)) {

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

	/**
	 * The implementation method that sets the value of parameter in the sys params
	 * object
	 * 
	 * @param key            the system parameter key.
	 * @param value
	 * @param autoCommit     Flag to disable the auto commit on success. In case the
	 *                       linking is part of a transaction
	 * @param autoCreateType Flag to autocreate an empty param type. Useful for
	 *                       anonymous params.
	 * 
	 */
	static void setSysParamImpl(String key, Object value, Boolean autoCommit) throws SvException {

		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr)) {
			DbDataObject dbt = SvCore.getDbt(svCONST.OBJECT_TYPE_SYS_PARAMS);
			DbDataObject dbParamValue = svr.getObjectByUnqConfId(key, dbt);
			if (dbParamValue == null) {
				dbParamValue = new DbDataObject(svCONST.OBJECT_TYPE_SYS_PARAMS);
				dbParamValue.setVal(Sv.PARAM_NAME, key);
			}
			dbParamValue.setVal(Sv.PARAM_TYPE, value.getClass().getName());
			dbParamValue.setVal(Sv.PARAM_VALUE, value);
			svw.saveObject(dbParamValue, autoCommit);
			paramsCache.put(Sv.PARAM_NAME, dbParamValue);
		}
	}

	private static void prepareParam(DbDataObject dbParamValue) {
		String value = (String) dbParamValue.getVal(Sv.PARAM_VALUE);
		try {
			Class<?> c = null;
			Object type = dbParamValue.getVal(Sv.PARAM_TYPE);
			if (type instanceof String)
				c = Class.forName((String) dbParamValue.getVal(Sv.PARAM_TYPE));
			else
				c = typeByCode(((Long) dbParamValue.getVal(Sv.PARAM_TYPE)).intValue());
			Constructor<?> ctor = c.getConstructor(String.class);
			Object object = ctor.newInstance(new Object[] { value });
			dbParamValue.setVal(Sv.PARAM_VALUE, object);
		} catch (Exception e) {
			log4j.error("Casting to class not possible", e);
		}
	}

	/**
	 * The implementation method that gets the value of parameter in the User params
	 * by parent object object
	 * 
	 * @param key the system parameter key.
	 * 
	 * 
	 */
	static Object getUserParamImpl(String key, Object defaultValue, Class<?> paramType, Long parentId)
			throws SvException {
		if (key.length() > 10)
			throw (new SvException(Sv.KEY_TOO_LONG, svCONST.systemUser));

		Object value = null;
		DbDataObject dbParamValue = null;
		try (SvReader svr = new SvReader()) {
			DbDataObject dbt = SvCore.getDbtByName(Sv.USER_PARAMS);
			DbDataArray dbParams = svr.getObjectsByParentId(parentId, dbt.getObjectId(), null);
			for (DbDataObject dbo : dbParams.getItems())
				if (dbo.getVal(Sv.PARAM_NAME).equals(key)) {
					dbParamValue = dbo;
					break;

				}

			if (dbParamValue == null) {
				if (SvConf.getParam(key) != null && !SvConf.getParam(key).isEmpty())
					defaultValue = SvConf.getParam(key);
				dbParamValue = new DbDataObject(dbt.getObjectId());
				dbParamValue.setParentId(parentId);
				dbParamValue.setVal(Sv.PARAM_NAME, key);
				dbParamValue.setVal(Sv.PARAM_VALUE, defaultValue);
				dbParamValue.setVal(Sv.PARAM_TYPE, decodeType(paramType));
				try (SvWriter svw = new SvWriter()) {
					svw.saveObject(dbParamValue, true);
				}
			} else
				prepareParam(dbParamValue);
		}
		value = dbParamValue.getVal(Sv.PARAM_VALUE);
		return value;
	}

	/**
	 * The implementation method that sets the value of parameter in the sys params
	 * object
	 * 
	 * @param key        the system parameter key.
	 * @param value
	 * @param autoCommit Flag to disable the auto commit on success. In case the
	 *                   linking is part of a transaction
	 * @param parentId   Id of the parent obhect
	 * 
	 */
	static void setUserParamImpl(String key, Object value, Boolean autoCommit, Long parentId) throws SvException {
		if (key.length() > 10)
			throw (new SvException(Sv.KEY_TOO_LONG, svCONST.systemUser));
		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr)) {
			DbDataObject dbParamValue = null;
			DbDataObject dbt = SvCore.getDbtByName(Sv.USER_PARAMS);
			DbDataArray dbParams = svr.getObjectsByParentId(parentId, dbt.getObjectId(), null);
			for (DbDataObject dbo : dbParams.getItems())
				if (dbo.getVal(Sv.PARAM_NAME).equals(key)) {
					dbParamValue = dbo;
					break;

				}
			if (dbParamValue == null) {
				dbParamValue = new DbDataObject(dbt.getObjectId());
				dbParamValue.setVal(Sv.PARAM_NAME, key);
			}
			dbParamValue.setVal(Sv.PARAM_TYPE, decodeType(value.getClass()));
			dbParamValue.setVal(Sv.PARAM_VALUE, value);
			svw.saveObject(dbParamValue, autoCommit);
		}
	}

	/**
	 * The implementation method that gets the value of parameter in the sys params
	 * object
	 * 
	 * @param key the system parameter key.
	 * 
	 * 
	 */
	static Object getSysParamImpl(String key, Object defaultValue, Class<?> paramType) throws SvException {
		return getSysParamImpl(key, defaultValue, paramType, true);
	}

	/**
	 * The implementation method that gets the value of parameter in the sys params
	 * object
	 * 
	 * @param key the system parameter key.
	 * 
	 * 
	 */
	private static Object getSysParamImpl(String key, Object defaultValue, Class<?> paramType, boolean autoCreate)
			throws SvException {
		Object value = null;
		DbDataObject dbParamValue = paramsCache.getIfPresent(key);
		if (dbParamValue == null) {
			try (SvReader svr = new SvReader()) {
				DbDataObject dbt = SvCore.getDbt(svCONST.OBJECT_TYPE_SYS_PARAMS);
				dbParamValue = svr.getObjectByUnqConfId(key, dbt);
				if (dbParamValue == null) {
					if (SvConf.getParam(key) != null && !SvConf.getParam(key).isEmpty())
						defaultValue = SvConf.getParam(key);
					dbParamValue = new DbDataObject(svCONST.OBJECT_TYPE_SYS_PARAMS);
					dbParamValue.setVal(Sv.PARAM_NAME, key);
					dbParamValue.setVal(Sv.PARAM_VALUE, defaultValue);
					dbParamValue.setVal(Sv.PARAM_TYPE, paramType.getName());
					if (autoCreate)
						try (SvWriter svw = new SvWriter()) {
							svw.saveObject(dbParamValue, true);
						}
				} else
					prepareParam(dbParamValue);
				dbParamValue.setIsDirty(false);
				paramsCache.put(key, dbParamValue);
			}
		}
		value = dbParamValue.getVal(Sv.PARAM_VALUE);
		return value;
	}

	/**
	 * Method that sets the value of system parameter of type string, which
	 * autocommits
	 * 
	 * @param key   of the parameter
	 * @param value of the parameter
	 * 
	 * @throws SvException
	 * 
	 */

	public static void setSysParam(String key, String value) throws SvException {
		setSysParamImpl(key, value, true);
	}

	public static void setSysParam(String key, DateTime value) throws SvException {
		setSysParamImpl(key, value, true);
	}

	public static void setSysParam(String key, Boolean value) throws SvException {
		setSysParamImpl(key, value, true);
	}

	public static void setSysParam(String key, Integer value) throws SvException {
		setSysParamImpl(key, value, true);
	}

	public static void setSysParam(String key, Long value) throws SvException {
		setSysParamImpl(key, value, true);
	}

	public static void setSysParam(String key, Double value) throws SvException {
		setSysParamImpl(key, value, true);
	}

	/**
	 * Method that gets the value of system parameter as type Object
	 * 
	 * @param paramName The name of the parameter
	 * @return The value of the parameter as string
	 * 
	 * @throws SvException
	 * 
	 */
	public Object getSysParam(String paramName) throws SvException {
		return getSysParamImpl(paramName, null, Object.class);
	}

	/**
	 * Method that gets the value of system parameter as type Object
	 * 
	 * @param paramName The name of the parameter
	 * @return The value of the parameter as string
	 * 
	 * @throws SvException
	 * 
	 */
	public static Object getSysParam(String paramName, boolean autoCreate) throws SvException {
		return getSysParamImpl(paramName, null, Object.class, autoCreate);
	}

	/**
	 * Method that gets the value of system parameter as type long
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static String getSysParam(String paramName, String defaultValue) throws SvException {
		return (String) getSysParamImpl(paramName, defaultValue, String.class);

	}

	/**
	 * Method that gets the value of system parameter as type long
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static Long getSysParam(String paramName, Long defaultValue) throws SvException {
		return (Long) getSysParamImpl(paramName, defaultValue, Long.class);
	}

	/**
	 * Method that gets the value of system parameter as type Date
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static DateTime getSysParam(String paramName, DateTime defaultValue) throws SvException {
		return (DateTime) getSysParamImpl(paramName, defaultValue, DateTime.class);
	}

	/**
	 * Method that gets the value of system parameter as boolean
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static Boolean getSysParam(String paramName, Boolean defaultValue) throws SvException {
		return (Boolean) getSysParamImpl(paramName, defaultValue, Boolean.class);
	}

	/**
	 * Method to get a system system parameter under the specified name, and parse
	 * it using the assigned date format.
	 * 
	 * @param paramName  The param name to be
	 * @param dateFormat
	 * @return
	 * @throws Exception
	 */
	public static Integer getSysParam(String paramName, Integer defaultValue) throws SvException {
		return (Integer) getSysParamImpl(paramName, defaultValue, Integer.class);
	}

	/**
	 * Method to get a system system parameter under the specified name, and parse
	 * it as Doubles.
	 * 
	 * @param paramName  The param name to be
	 * @param dateFormat
	 * @return
	 * @throws Exception
	 */

	public static Double getSysParam(String paramName, Double defaultValue) throws SvException {
		return (Double) getSysParamImpl(paramName, defaultValue, Double.class);
	}

	/**
	 * Method that sets the value of system parameter of type string, which
	 * autocommits
	 * 
	 * @param key   of the parameter
	 * @param value of the parameter
	 * 
	 * @throws SvException
	 * 
	 */

	public static void setUserParam(String key, String value, Long parentId) throws SvException {
		setUserParamImpl(key, value, true, parentId);
	}

	public static void setUserParam(String key, DateTime value, Long parentId) throws SvException {
		setUserParamImpl(key, value, true, parentId);
	}

	public static void setUserParam(String key, Boolean value, Long parentId) throws SvException {
		setUserParamImpl(key, value, true, parentId);
	}

	public static void setUserParam(String key, Integer value, Long parentId) throws SvException {
		setUserParamImpl(key, value, true, parentId);
	}

	public static void setUserParam(String key, Long value, Long parentId) throws SvException {
		setUserParamImpl(key, value, true, parentId);
	}

	public static void setUserParam(String key, Double value, Long parentId) throws SvException {
		setUserParamImpl(key, value, true, parentId);
	}

	/**
	 * Method that gets the value of system parameter as type Object
	 * 
	 * @param paramName The name of the parameter
	 * @return The value of the parameter as string
	 * 
	 * @throws SvException
	 * 
	 */
	public Object getUserParam(String paramName, Long parentId) throws SvException {
		return getUserParamImpl(paramName, null, Object.class, parentId);
	}

	/**
	 * Method that gets the value of system parameter as type long
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static String getUserParam(String paramName, String defaultValue, Long parentId) throws SvException {
		return (String) getUserParamImpl(paramName, defaultValue, String.class, parentId);

	}

	/**
	 * Method that gets the value of system parameter as type long
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static Long getUserParam(String paramName, Long defaultValue, Long parentId) throws SvException {
		return (Long) getUserParamImpl(paramName, defaultValue, Long.class, parentId);
	}

	/**
	 * Method that gets the value of system parameter as type Date
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static DateTime getUserParam(String paramName, DateTime defaultValue, Long parentId) throws SvException {
		return (DateTime) getUserParamImpl(paramName, defaultValue, DateTime.class, parentId);
	}

	/**
	 * Method that gets the value of system parameter as boolean
	 * 
	 * @param paramName The name of the parameter
	 * 
	 * @throws SvException
	 * 
	 */
	public static Boolean getUserParam(String paramName, Boolean defaultValue, Long parentId) throws SvException {
		return (Boolean) getUserParamImpl(paramName, defaultValue, Boolean.class, parentId);
	}

	/**
	 * Method to get a system system parameter under the specified name, and parse
	 * it using the assigned date format.
	 * 
	 * @param paramName  The param name to be
	 * @param dateFormat
	 * @return
	 * @throws Exception
	 */
	public static Integer getUserParam(String paramName, Integer defaultValue, Long parentId) throws SvException {
		return (Integer) getUserParamImpl(paramName, defaultValue, Integer.class, parentId);
	}

	/**
	 * Method to get a system system parameter under the specified name, and parse
	 * it as Doubles.
	 * 
	 * @param paramName  The param name to be
	 * @param dateFormat
	 * @return
	 * @throws Exception
	 */

	public static Double getUserParam(String paramName, Double defaultValue, Long parentId) throws SvException {
		return (Double) getUserParamImpl(paramName, defaultValue, Double.class, parentId);
	}

}
