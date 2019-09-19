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
import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.joda.time.DateTime;

import com.prtech.svarog.SvConf.SvDbType;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQuery;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;

public class SvReader extends SvCore {

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public SvReader(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public SvReader(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public SvReader(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvReader() throws SvException {
		super();
	}

	/**
	 * To be removed in Svarog v2.0. Simplified version of getObjects, which
	 * takes a DbSearch and Object Type Descriptor instead
	 * 
	 * @param dbSearch
	 *            A {@link DbSearch} instance containing the search criteria
	 * @param typeDescriptor
	 * @param refDate
	 *            Reference date to be used for the query
	 * @param errorCode
	 *            standard svCONST error code
	 * @param rowLimit
	 *            maximum number of objects to be returned
	 * @param offset
	 *            offset from which the objects should be returned
	 * @return A {@link DbDataArray} object containing all returned data in
	 *         DbDataObject format
	 */
	@Deprecated
	DbDataArray getObjects(DbSearch dbSearch, DbDataObject typeDescriptor, DateTime refDate, long[] errorCode,
			Integer rowLimit, Integer offset) {

		try {

			DbQueryObject query = new DbQueryObject(repoDbt, repoDbtFields, typeDescriptor,
					getFields(typeDescriptor.getObject_id()), dbSearch, refDate,
					dbSearch != null ? dbSearch.getOrderByFields() : null);

			return getObjects(query, errorCode, rowLimit, offset);
		} catch (Exception e) {
			log4j.error("Error in getObjects!", e);
		}
		return null;

	}

	/**
	 * Simplified version of getObjects, which takes a DbSearch and Object Type
	 * Descriptor instead
	 * 
	 * @param dbSearch
	 *            A {@link DbSearch} instance containing the search criteria
	 * @param typeDescriptor
	 *            A descriptor object of the type we need to fetch.
	 * @param refDate
	 *            Reference date to be used for the query
	 * @param rowLimit
	 *            maximum number of objects to be returned
	 * @param offset
	 *            offset from which the objects should be returned
	 * @return A {@link DbDataArray} object containing all returned data in
	 *         DbDataObject format
	 * @throws SvException
	 */
	DbDataArray getObjects(DbSearch dbSearch, DbDataObject typeDescriptor, DateTime refDate, Integer rowLimit,
			Integer offset) throws SvException {
		// create the query object based on the DbSearch and DBT
		DbQueryObject query = new DbQueryObject(repoDbt, repoDbtFields, typeDescriptor,
				getFields(typeDescriptor.getObject_id()), dbSearch, refDate,
				dbSearch != null ? dbSearch.getOrderByFields() : null);

		return getObjects(query, rowLimit, offset);
	}

	/**
	 * Simplified version of getObjects, which takes a DbSearch and Object Type
	 * ID
	 * 
	 * @param dbSearch
	 *            A {@link DbSearch} instance containing the search criteria
	 * @param objectType
	 *            ID of the object type to be fetched.
	 * @param refDate
	 *            Reference date to be used for the query
	 * @param rowLimit
	 *            maximum number of objects to be returned
	 * @param offset
	 *            offset from which the objects should be returned
	 * @return A {@link DbDataArray} object containing all returned data in
	 *         DbDataObject format
	 * @throws SvException
	 */
	public DbDataArray getObjects(DbSearch dbSearch, Long objectType, DateTime refDate, Integer rowLimit,
			Integer offset) throws SvException {
		DbDataArray retval = null;
		retval = getObjects(dbSearch, getDbt(objectType), refDate, rowLimit, offset);
		return retval;
	}

	/**
	 * Method that returns an array of DbDataObjects according to specified
	 * DbQuery object.
	 * 
	 * @param query
	 *            The DbQuery object which actually describes the query to the
	 *            DB
	 * @param errorCode
	 *            Error codes from the operation
	 * @param rowLimit
	 *            The number of rows to be returned
	 * @param offset
	 *            The offset in the recordset from which the rowset will be
	 *            returned
	 * @return The returning object of type DbDataArray
	 * @throws SvException
	 */
	public DbDataArray getObjects(DbQuery query, Integer rowLimit, Integer offset) throws SvException {

		DbDataArray retval = null;
		retval = super.getObjects(query, rowLimit, offset);
		return retval;
	}

	/**
	 * Method to return the version history for a specific list of objects
	 * satifying the search criteria
	 * 
	 * @param search
	 *            The search criteria to be matched
	 * @param objectType
	 *            The object type Id
	 * @param rowLimit
	 *            Row limit of the dataset
	 * @param offset
	 *            The offset to start at
	 * @return The array of objects
	 * @throws SvException
	 */
	public DbDataArray getObjectsHistory(DbSearch search, Long objectType, Integer rowLimit, Integer offset)
			throws SvException {
		DbQueryObject query = new DbQueryObject(SvCore.getDbt(objectType), search, null, null);
		query.setEnableHistory(true);
		return getObjects(query, rowLimit, offset);

	}

	/**
	 * Method that creates a sorted map of form fields according to the SORT
	 * ORDER
	 * 
	 * @param currentFormTypeId
	 *            The form type ID for which we want to create a map
	 * @return A map containing the form field labels as keys
	 * @throws SvException
	 */
	private LinkedHashMap<String, Object> formTypeFields(Long formTypeId) throws SvException {
		LinkedHashMap<String, Object> templateMap = null;
		SvReader svr = null;
		try {
			svr = new SvReader(this);
			// switch this instance under the system user to ensure we get the
			// config
			svr.instanceUser = svCONST.systemUser;

			DbDataObject dbl = getLinkType("FORM_FIELD_LINK", svCONST.OBJECT_TYPE_FORM_TYPE,
					svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);

			DbDataArray formFields = svr.getObjectsByLinkedId(formTypeId, svCONST.OBJECT_TYPE_FORM_TYPE, dbl,
					svCONST.OBJECT_TYPE_FORM_FIELD_TYPE, false, null, 0, 0);

			// create the table map according to the sort order
			templateMap = new LinkedHashMap<String, Object>();
			templateMap.put("LABEL_CODE", null);
			templateMap.put("VALUE", null);
			templateMap.put("FIRST_CHECK", null);
			for (DbDataObject ffDbo : formFields.getSortedItems("SORT_ORDER")) {
				templateMap.put(((String) ffDbo.getVal("label_code")).toUpperCase(), null);
				templateMap.put(((String) ffDbo.getVal("label_code") + "_1st").toUpperCase(), null);
				templateMap.put(((String) ffDbo.getVal("label_code") + "_2nd").toUpperCase(), null);
			}
		} finally {
			if (svr != null)
				svr.release();
		}
		return templateMap;
	}

	/**
	 * Method to return the array of form fields in plain format, ready for
	 * re-formatting
	 * 
	 * @param parentId
	 *            The Id of the parent object of the forms
	 * @param formTypeIds
	 *            The list of form types to be fetched for the specified parent
	 * @param search
	 *            Optional DbSearch criteria
	 * @param refDate
	 *            The reference date on which the form should be read
	 * @return DbDataArray containing the form fields
	 * @throws SvException
	 */
	private DbDataArray getFormPlainFields(Long parentId, ArrayList<Long> formTypeIds, DbSearch search,
			DateTime refDate) throws SvException {
		// get the basic DBTs as well as fields
		DbDataObject formFieldsDbt = getDbt(svCONST.OBJECT_TYPE_FORM_FIELD);

		// do we effing need to rebuild the index everytime we fetch an object?
		DbDataObject formDbt = getDbt(svCONST.OBJECT_TYPE_FORM);

		DbSearchExpression dbe = new DbSearchExpression();
		dbe.addDbSearchItem(new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, parentId));

		// create search expression for each form type
		if (formTypeIds != null) {
			DbSearchExpression dbsTypes = new DbSearchExpression();
			for (Long formTypeId : formTypeIds) {
				dbsTypes.addDbSearchItem(
						new DbSearchCriterion("FORM_TYPE_ID", DbCompareOperand.EQUAL, formTypeId, DbLogicOperand.OR));
			}
			dbe.addDbSearchItem(dbsTypes);
		}
		if (search != null) {
			dbe.addDbSearchItem(search);
		}

		ArrayList<String> sortField = new ArrayList<String>();
		sortField.add("FORM_OBJECT_ID");
		sortField.add("FIELD_TYPE_ID");
		DbQueryObject dqoForm = new DbQueryObject(formDbt, dbe, DbJoinType.LEFT, null, LinkType.CHILD, null, refDate);

		dqoForm.setSqlTablePrefix("TBL0");
		dqoForm.setIsReturnType(true);
		DbQueryObject dqoField = new DbQueryObject(formFieldsDbt, null, DbJoinType.LEFT, null, LinkType.PARENT,
				sortField, refDate);
		dqoField.setSqlTablePrefix("TBL1");
		dqoForm.addChild(dqoField);
		dqoField.setIsReturnType(true);

		DbQueryExpression dqe = new DbQueryExpression();
		dqe.setIsReverseExpression(true);
		dqe.setRootQueryObject(dqoForm);

		// get the form data from the database
		return getObjects(dqe, 0, 0);

	}

	/**
	 * Method to initialise a form instance for a non-structured DbDataObject
	 * 
	 * @param obj
	 *            The non structured object
	 * @param templateMap
	 *            The template map to initialise the form fields
	 * @return
	 * @throws SvException
	 */
	@SuppressWarnings("unchecked")
	DbDataObject initFormInstance(DbDataObject obj, LinkedHashMap<String, Object> templateMap) throws SvException {
		DbDataObject currentObject = new DbDataObject();
		currentObject.setValues((LinkedHashMap<String, Object>) templateMap.clone());

		DbDataObject formTypeDbt = getFormType((Long) obj.getVal("TBL0_FORM_TYPE_ID"));
		if (formTypeDbt == null) {
			throw (new SvException("system.err.form_dbt_not_found", this.instanceUser, obj, templateMap));
		}
		currentObject.setVal("FORM_TYPE_ID", formTypeDbt.getObject_id());
		// TODO for oracle do the standard boolean
		// transformation
		if (SvConf.getDbType().equals(SvDbType.ORACLE)) {
			obj.setVal("TBL0_FORM_VALIDATION",
					new Boolean((obj.getVal("TBL0_FORM_VALIDATION")).equals("0") ? false : true));
		}

		currentObject.setVal("FORM_VALIDATION", obj.getVal("TBL0_FORM_VALIDATION"));
		String labelText = "";
		String longLabelText = I18n.getLongText((String) formTypeDbt.getVal("LABEL_CODE"));
		if (longLabelText == null || longLabelText.length() == 0) {
			labelText = I18n.getText((String) formTypeDbt.getVal("LABEL_CODE"));
		} else {
			labelText = longLabelText;
		}
		currentObject.setVal("LABEL_CODE", labelText);
		currentObject.setObject_id((Long) obj.getVal("TBL0_OBJECT_ID"));
		currentObject.setObject_type(svCONST.OBJECT_TYPE_FORM);
		currentObject.setDt_delete((DateTime) obj.getVal("TBL0_DT_DELETE"));
		currentObject.setDt_insert((DateTime) obj.getVal("TBL0_DT_INSERT"));
		currentObject.setPkid((Long) obj.getVal("TBL0_PKID"));
		currentObject.setParent_id((Long) obj.getVal("TBL0_PARENT_ID"));
		currentObject.setStatus((String) obj.getVal("TBL0_STATUS"));
		currentObject.setUser_id((Long) obj.getVal("TBL0_USER_ID"));
		currentObject.setVal("VALUE", (Long) obj.getVal("TBL0_VALUE"));
		currentObject.setVal("FIRST_CHECK", (Long) obj.getVal("TBL0_FIRST_CHECK"));
		// TODO return the secondary checks when Roles are
		// applied
		// currentObject.setVal("FIRST_CHECK",(Long)obj.getVal("TBL0_FIRST_CHECK"));
		// currentObject.setVal("SECOND_CHECK",(Long)obj.getVal("TBL0_SECOND_CHECK"));
		return currentObject;

	}

	/**
	 * Loads form implementations based on a parent id and list of type of form.
	 * If the type of form you are trying to load has auto "instantiate single"
	 * flag to true it will create the forms before fetching them. Auto create
	 * will not create additional fields but just the base form object.
	 * 
	 * @param parentId
	 *            The parent object of the form
	 * @param formTypeId
	 *            The type of the form
	 * @param search
	 *            Additional search criteria, via class implementing DbSearch
	 * @param refDate
	 *            Reference date to use
	 * @param disableAutoinstance
	 *            Flag to enable or disable auto instance
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getFormsByParentId(Long parentId, ArrayList<Long> formTypeIds, DbSearch search, DateTime refDate,
			Boolean disableAutoinstance) throws SvException {
		String lockKey = "FORMS-" + parentId.toString();
		DbDataArray formArray = null;
		ReentrantLock lock = null;
		SvReader svrConfig = null;
		try {
			// lock the form parent before creating a new form with autoinstance
			// to avoid race-conditions
			if (!disableAutoinstance)
				lock = SvLock.getLock(lockKey, false, SvConf.getMaxLockTimeout());
			// the return form array
			if (disableAutoinstance || (!disableAutoinstance && lock != null)) {
				formArray = new DbDataArray();
				Long currentFormId = 0L;
				Long currentFormTypeId = 0L;
				DbDataObject currentObject = null;
				// chain it to share the connection
				svrConfig = new SvReader(this);
				// make sure we run as system to read the configuration
				svrConfig.instanceUser = svCONST.systemUser;
				// list of form types found in the result set. The rest are
				// subject
				// of auto-instance if configured
				@SuppressWarnings("unchecked")
				ArrayList<Long> notFoundFormTypes = (ArrayList<Long>) formTypeIds.clone();
				// template map of form fields to ensure proper field sort in
				// the
				// result set
				LinkedHashMap<String, Object> templateMap = null;
				// the resulting data set containing the form fields from the DB
				DbDataArray plainFields = getFormPlainFields(parentId, formTypeIds, search, refDate);
				Long fldDescId = null;
				if (plainFields != null) { // the re-formatting loop
					for (DbDataObject obj : plainFields.getItems()) {
						// if the form type has changed
						if (!currentFormTypeId.equals((Long) obj.getVal("TBL0_FORM_TYPE_ID"))) {
							currentFormTypeId = (Long) obj.getVal("TBL0_FORM_TYPE_ID");
							templateMap = formTypeFields(currentFormTypeId);
							// if the new form type is found, remove it from the
							// not
							// found list
							if (notFoundFormTypes.contains(currentFormTypeId))
								notFoundFormTypes.remove(currentFormTypeId);
						}
						// get the form id for the current row
						Long newFormId = (Long) obj.getVal("TBL0_OBJECT_ID");

						// if the row belongs to a new form then initialise the
						// form
						if (!(currentFormId.equals(newFormId))) {
							currentFormId = (Long) obj.getVal("TBL0_OBJECT_ID");
							if (currentObject != null)
								formArray.addDataItem(currentObject);
							currentObject = initFormInstance(obj, templateMap);
						}
						// get the current field descriptor
						fldDescId = (Long) obj.getVal("tbl1_FIELD_TYPE_ID");
						if (fldDescId != null) {
							DbDataObject fldDesc = svrConfig.getObjectById(fldDescId,
									svCONST.OBJECT_TYPE_FORM_FIELD_TYPE, null);
							if (fldDesc != null) {
								currentObject.setVal((String) fldDesc.getVal("label_code"), obj.getVal("tbl1_VALUE"));
								currentObject.setVal((String) fldDesc.getVal("label_code") + "_1st",
										obj.getVal("tbl1_FIRST_CHECK"));
								currentObject.setVal((String) fldDesc.getVal("label_code") + "_2nd",
										obj.getVal("tbl1_SECOND_CHECK"));
							}
						}
						// TODO return the secondary checks when Roles are
						// applied
						// currentObject.setVal((String)fldDesc.getVal("label_code")+"_1st",obj.getVal("tbl1_FIRST_CHECK"));
						// currentObject.setVal((String)fldDesc.getVal("label_code")+"_2nd",obj.getVal("tbl1_SECOND_CHECK"));

					}
				}
				// the record set iteration finished, but we need to add the
				// last
				// form
				if (currentObject != null)
					formArray.addDataItem(currentObject);

				// if there are form type which aren't stored in the db then
				// invoke
				// auto init on the not found
				if (notFoundFormTypes.size() > 0 && refDate == null && !disableAutoinstance) {
					SvWriter svw = null;
					DbDataArray autoForms = null;
					try {
						svw = new SvWriter(this);
						// get a writer with the same SvCore to autoinstance the
						// forms and add the returning array
						autoForms = svw.autoInstanceForms(notFoundFormTypes, parentId);
					} finally {
						if (svw != null)
							svw.release();
					}
					if (autoForms != null)
						formArray.getItems().addAll(autoForms.getItems());
				}
			} else {
				log4j.warn("Can't acquire lock for key " + lockKey);
				throw (new SvException("system.err.cant_acquire_lck", this.instanceUser, null, lockKey));
			}
		} finally {
			if (lock != null)
				SvLock.releaseLock(lockKey, lock);
			if (svrConfig != null)
				svrConfig.release();

		}
		return formArray;

	}

	/**
	 * Loads form implementations based on a parent id and type of form. Its
	 * overloaded version of the base method which uses an array of Type Ids
	 * 
	 * @param parentId
	 *            The parent object of the form
	 * @param formTypeId
	 *            The type of the form
	 * @param search
	 *            Additional search criteria, via class implementing DbSearch
	 * @param refDate
	 *            Reference date to use
	 * @param errorCode
	 *            Standard Svarog error codes as per {@link svCONST}
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getFormsByParentId(Long parentId, Long formTypeId, DbSearch search, DateTime refDate)
			throws SvException {

		ArrayList<Long> ids = new ArrayList<Long>();
		ids.add(formTypeId);
		return getFormsByParentId(parentId, ids, search, refDate, false);
	}

	/**
	 * Method for fetching an object from the system by using a numeric Object
	 * Type.
	 * 
	 * @param object_id
	 *            Id of the object which should be fetched
	 * @param typeDescriptor
	 *            Configuration for the type of the object fetched (also known
	 *            as DBT)
	 * @param refDate
	 *            Reference date on which the data should be fetched
	 * @param errorCode
	 *            An error code of the operation
	 * @return A DbDataObject if one is located in the DB or the Cache system.
	 * @throws SvException
	 */
	public DbDataObject getObjectById(Long object_id, DbDataObject typeDescriptor, DateTime refDate)
			throws SvException {
		return getObjectById(object_id, typeDescriptor, refDate, false);
	}

	/**
	 * Method for fetching an object from the system by using a numeric Object
	 * Type.
	 * 
	 * @param object_id
	 *            Id of the object which should be fetched
	 * @param object_type
	 *            Id of the type of object which should be fetched
	 * @param refDate
	 *            Reference date on which the data should be fetched
	 * @param errorCode
	 *            An error code of the operation
	 * @return A DbDataObject if one is located in the DB or the Cache system.
	 * @throws SvException
	 */
	public DbDataObject getObjectById(Long object_id, Long object_type, DateTime refDate) throws SvException {

		DbDataObject dbt = getDbt(object_type);
		if (object_id == null) {
			log4j.warn("Don't be stupid, can't load null object");
			return null;
		}
		return getObjectById(object_id, dbt, refDate, false);
	}

	/**
	 * Base getObjectById method for fetching an object from the system.
	 * 
	 * @param object_id
	 *            Id of the object which should be fetched
	 * @param dbt
	 *            Configuration for the type of the object fetched
	 * @param refDate
	 *            Reference date on which the data should be fetched
	 * @param errorCode
	 *            An error code of the operation
	 * @return A DbDataObject if one is located in the DB or the Cache system.
	 * @throws SvException
	 */
	DbDataObject getObjectById(Long object_id, DbDataObject dbt, DateTime refDate, Boolean forceDbQuery)
			throws SvException {
		DbDataObject object = null;

		if (!forceDbQuery && dbt.getVal("use_cache") != null && refDate == null && isCfgInDb
				&& (Boolean) dbt.getVal("use_cache"))
			object = DbCache.getObject(object_id, dbt.getObject_id());

		if (object == null || (object.isGeometryType() && !object.getHasGeometry())) {
			object = getObjectByIdImpl(object_id, dbt, refDate);
		}
		return object;
	}

	/**
	 * Svarog internal method to get an object by ID
	 * 
	 * @param object_id
	 *            The Id of the object
	 * @param dbt
	 *            The object type descriptor
	 * @param refDate
	 *            The reference date at which we want to fetch the object
	 * @return The loaded DbDataObject
	 * @throws SvException
	 */
	DbDataObject getObjectByIdImpl(Long object_id, DbDataObject dbt, DateTime refDate) throws SvException {
		DbDataObject object = null;
		DbDataArray arr = getObjects(new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, object_id), dbt,
				refDate, 1, 0);
		if (arr != null) {
			if (arr.getItems().size() == 1)
				object = arr.getItems().get(0);
			else if (arr.getItems().size() > 1)
				throw (new SvException("system.error.multiple_object_instances", instanceUser, null, dbt));
		}

		if (object != null)
			DbCache.addObject(object);
		return object;
	}

	/**
	 * Svarog internal method to get all data for certain SVAROG_OBJECT type
	 * 
	 * @param type
	 *            The Id of the SVAROG_OBJECT
	 * @param refDate
	 *            The reference date at which we want to fetch the object
	 * @param rowLimit
	 *            Maximum number of objects
	 * @param offset
	 *            The offset at which to start returning objects
	 * @return all data that currently exists for certain SVAROG_OBJECT type
	 * @throws SvException
	 */
	public DbDataArray getObjectsByTypeId(Long type_id, DateTime refDate, Integer rowLimit, Integer offset)
			throws SvException {
		DbDataArray arr = null;
		DbSearch search = new DbSearchCriterion("OBJECT_TYPE", DbSearchCriterion.DbCompareOperand.EQUAL, type_id);
		arr = getObjects(search, SvReader.getDbt(type_id), refDate, rowLimit, offset);

		return arr;
	}

	/**
	 * Overloaded version of getObjectsByLinkedId to return objects according to
	 * link code. Not so smart, issues with reversing.
	 * 
	 * @param LinkObjectId
	 *            The id of the linked object
	 * @param linkObjectTypeId1
	 *            The type of the linked object
	 * @param linkCode
	 *            The link label code
	 * @param linkObjectTypeId2
	 *            The type of result set object link by the link descriptor. Not
	 *            used.
	 * @param isReverse
	 *            Is the link configuration reverse according to the parameter
	 *            order?
	 * @param refDate
	 *            Reference date on which to get the data
	 * @param rowLimit
	 *            Maximum number of objects
	 * @param offset
	 *            The offset at which to start returning objects
	 * @return
	 * @throws SvException
	 */
	@Deprecated
	public DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId1, String linkCode,
			Long linkObjectTypeId2, Boolean isReverse, DateTime refDate, Integer rowLimit, Integer offset)
			throws SvException {
		DbDataArray ret = null;
		ret = getObjectsByLinkedId(LinkObjectId, linkObjectTypeId1, linkCode, linkObjectTypeId2, isReverse, refDate,
				rowLimit, offset, null);
		return ret;

	}

	/**
	 * Overloaded version of getObjectsByLinkedId to return objects according to
	 * link code with added filtering per status. Not so smart, issues with
	 * reversing. Please use the version which uses a proper Link descriptor
	 * 
	 * @param LinkObjectId
	 *            The id of the linked object
	 * @param linkObjectTypeId1
	 *            The type of the linked object
	 * @param linkCode
	 *            The link label code
	 * @param linkObjectTypeId2
	 *            The type of result set object link by the link descriptor. Not
	 *            used.
	 * @param isReverse
	 *            Is the link configuration reverse according to the parameter
	 *            order?
	 * @param refDate
	 *            Reference date on which to get the data
	 * @param rowLimit
	 *            Maximum number of objects
	 * @param offset
	 *            The offset at which to start returning objects
	 * @param linkStatus
	 *            The status of the link which is linking the two objects
	 * @return
	 * @throws SvException
	 */
	@Deprecated
	public DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId1, String linkCode,
			Long linkObjectTypeId2, Boolean isReverse, DateTime refDate, Integer rowLimit, Integer offset,
			String linkStatus) throws SvException {
		DbDataArray ret = null;
		DbDataObject dbl = getLinkType(linkCode, linkObjectTypeId1, linkObjectTypeId2);
		// TODO: remember to refactor the DbLink also.
		if (dbl != null)
			ret = getObjectsByLinkedId(LinkObjectId, linkObjectTypeId1, dbl, linkObjectTypeId2, isReverse, refDate,
					rowLimit, offset, linkStatus);
		return ret;

	}

	/**
	 * Another legacy overload. The linkObjectTypeId2 parameter is not used at
	 * all, but the dbLink data is.
	 * 
	 * @param LinkObjectId
	 *            The id of the linked object
	 * @param linkObjectTypeId1
	 *            The type of the linked object
	 * @param dbLink
	 *            Link descriptor
	 * @param linkObjectTypeId2
	 *            The type of result set object link by the link descriptor. Not
	 *            used.
	 * @param isReverse
	 *            Is the link configuration reverse according to the parameter
	 *            order?
	 * @param refDate
	 *            Reference date on which to get the data
	 * @param rowLimit
	 *            Maximum number of objects
	 * @param offset
	 *            The offset at which to start returning objects
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId1, DbDataObject dbLink,
			Long linkObjectTypeId2, Boolean isReverse, DateTime refDate, Integer rowLimit, Integer offset)
			throws SvException {
		return getObjectsByLinkedId(LinkObjectId, linkObjectTypeId1, dbLink, linkObjectTypeId2, isReverse, refDate,
				rowLimit, offset, null);
	}

	/**
	 * Getting linked objects by deducting the second object type from the link
	 * 
	 * @param LinkObjectId
	 *            The object to which the result set is linked
	 * @param linkObjectTypeId1
	 *            The type of LinkObjectId
	 * @param dbLink
	 *            The link descriptor
	 * @param refDate
	 *            The reference date on which we want to get the dataset
	 * @param rowLimit
	 *            Number of rows
	 * @param offset
	 *            Starting form row number
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getObjectsByLinkedId(Long LinkObjectId, DbDataObject dbLink, DateTime refDate, Integer rowLimit,
			Integer offset) throws SvException {

		DbDataArray arr = null; // to replace with db cache
		if (dbLink != null) {
			arr = getObjectsByLinkedId(LinkObjectId, (Long) dbLink.getVal("link_obj_type_1"), dbLink,
					(Long) dbLink.getVal("link_obj_type_2"), false, refDate, rowLimit, offset, null);
		}
		return arr;
	}

	/**
	 * The root method for fetching linked objects. It will first try to load
	 * from the cache, than try the database if there is no such collection in
	 * the cache.
	 * 
	 * @param LinkObjectId
	 *            The object to which the result set is linked
	 * @param linkObjectTypeId1
	 *            The type of LinkObjectId
	 * @param dbLink
	 *            The link descriptor
	 * @param linkObjectTypeId2
	 *            The type of objects which we want in the result set
	 * @param isReverse
	 *            If the link descriptor direction is reversed
	 * @param refDate
	 *            The reference date on which we want to get the dataset
	 * @param rowLimit
	 *            Number of rows
	 * @param offset
	 *            Starting form row number
	 * @param linkStatus
	 *            The status of the link between the two objects
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId1, DbDataObject dbLink,
			Long linkObjectTypeId2, Boolean isReverse, DateTime refDate, Integer rowLimit, Integer offset,
			String linkStatus) throws SvException {
		DbDataArray object = null; // to replace with db cache
		if (dbLink != null) {
			if (refDate == null)
				object = DbCache.getObjectsByLinkedId(LinkObjectId, linkObjectTypeId1, dbLink.getObject_id(),
						linkObjectTypeId2, linkStatus);
			// sync at some point with getObjects by parentId
			boolean shouldForceDbQuery = false;
			if (object == null)
				shouldForceDbQuery = true;
			else {
				if (object.size() < 1)
					shouldForceDbQuery = true;
				else {
					DbDataObject firstObj = object.get(0);

					if (firstObj.isGeometryType() && !firstObj.getHasGeometry())
						shouldForceDbQuery = true;
				}
			}

			if (shouldForceDbQuery)
				object = getObjectsByLinkedIdFromDb(LinkObjectId, linkObjectTypeId1, dbLink, linkObjectTypeId2,
						isReverse, refDate, rowLimit, offset, linkStatus);
		}
		return object;
	}

	/**
	 * The root method for fetching linked objects from the Database.
	 * 
	 * @param LinkObjectId
	 *            The object to which the result set is linked
	 * @param linkObjectTypeId1
	 *            The type of LinkObjectId
	 * @param dbLink
	 *            The link descriptor
	 * @param linkObjectTypeId2
	 *            The type of objects which we want in the result set
	 * @param isReverse
	 *            If the link descriptor direction is reversed
	 * @param refDate
	 *            The reference date on which we want to get the dataset
	 * @param rowLimit
	 *            Number of rows
	 * @param offset
	 *            Starting form row number
	 * @param linkStatus
	 *            The status of the link between the two objects
	 * @return
	 * @throws SvException
	 */
	DbDataArray getObjectsByLinkedIdFromDb(Long LinkObjectId, Long linkObjectTypeId1, DbDataObject dbLink,
			Long linkObjectTypeId2, Boolean isReverse, DateTime refDate, Integer rowLimit, Integer offset,
			String linkStatus) throws SvException {
		DbDataArray ret = null;

		// get descriptors for both types
		DbDataObject dbt = getDbt(linkObjectTypeId1);
		DbDataObject dbt1 = getDbt(linkObjectTypeId2);

		// Forward search: The return type is the first object type while
		// the linked object ID
		// is considered to be on the second object type
		//
		// Reverse searching: If reverse is needed the linked object ID
		// comes first
		// while the return type is set to the second type.
		//
		DbSearch dbs1 = (new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, LinkObjectId));

		DbDataObject dbl1 = (!isReverse ? dbLink : null);
		DbDataObject dbl2 = (!isReverse ? null : dbLink);

		// get the first table and join it with links
		DbQueryObject it = new DbQueryObject(repoDbt, repoDbtFields, dbt, getFields(dbt.getObject_id()), dbs1,
				DbJoinType.INNER, dbl1, LinkType.DBLINK, null, refDate);

		DbQueryObject it1 = new DbQueryObject(repoDbt, repoDbtFields, dbt1, getFields(dbt1.getObject_id()), null,
				DbJoinType.INNER, dbl2, LinkType.DBLINK, null, refDate);

		DbQueryExpression q = new DbQueryExpression();

		if (linkStatus != null)
			q.addLinkStatus(linkStatus);

		if (!isReverse) {
			q.addItem(it);
			q.addItem(it1);
			if (linkObjectTypeId1.equals(linkObjectTypeId2))
				it1.setIsReturnType(true);
		} else {
			q.addItem(it1);
			q.addItem(it);
			if (linkObjectTypeId1.equals(linkObjectTypeId2))
				it1.setIsReturnType(true);
		}

		q.setReturnType(dbt1);

		ret = this.getObjects(q, rowLimit, offset);
		DbCache.addArrayByLinkedId(ret, LinkObjectId, linkObjectTypeId1, dbLink.getObject_id(), linkObjectTypeId2,
				linkStatus);

		return ret;
	}

	/**
	 * Overridden method for fetching records based on parent child
	 * relationship, without sortOrder (legacy)
	 * 
	 * @param parent_id
	 *            The ID of the parent object
	 * @param object_type
	 *            The Id of the type of the child objects
	 * @param refDate
	 *            Reference date for which the fetch should be executed
	 * @param rowLimit
	 *            Limit on the number of rows
	 * @param offset
	 *            Offset at which to start returning rows
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getObjectsByParentId(Long parent_id, Long object_type, DateTime refDate, Integer rowLimit,
			Integer offset) throws SvException {
		return getObjectsByParentId(parent_id, object_type, refDate, rowLimit, offset, null);
	}

	/**
	 * Based method for fetching records based on parent child relationship
	 * 
	 * @param parent_id
	 *            The ID of the parent object
	 * @param object_type
	 *            The Id of the type of the child objects
	 * @param refDate
	 *            Reference date for which the fetch should be executed
	 * @param rowLimit
	 *            Limit on the number of rows
	 * @param offset
	 *            Offset at which to start returning rows
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getObjectsByParentId(Long parent_id, Long object_type, DateTime refDate, Integer rowLimit,
			Integer offset, String sortByField) throws SvException {
		DbDataArray object = null;

		if (refDate == null && isCfgInDb)
			object = DbCache.getObjectsByParentId(parent_id, object_type);

		// think about pulling this infron of the brackets #link
		// getObjectsByLinkId
		boolean shouldForceDbQuery = false;
		if (object == null || object.size() == 0)
			shouldForceDbQuery = true;
		else {
			if (object.size() < 1)
				shouldForceDbQuery = true;
			else {
				DbDataObject firstObj = object.get(0);

				if (firstObj.isGeometryType() && !firstObj.getHasGeometry())
					shouldForceDbQuery = true;
			}
		}
		if (shouldForceDbQuery) {
			object = getObjects(new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, parent_id),
					getDbt(object_type), refDate, rowLimit, offset);
			DbCache.addArrayByParentId(object, object_type, parent_id);
		}
		if (sortByField != null && !sortByField.equals("")) {
			object.getSortedItems(sortByField);
		}
		return object;
	}

	/**
	 * Method for searching object by its type unique id (expecting to return
	 * one object, if no always return the first one)
	 * 
	 * @param srchVlaue
	 *            The value we search for
	 * @param tableName
	 *            The name of the table we are searching in
	 * @return
	 * @throws SvException
	 */
	public DbDataObject getObjectByUnqConfId(String srchValue, String tableName) throws SvException {
		// get the unique field id from svarog_tables
		DbDataObject tableObj = getDbtByName(tableName);
		return getObjectByUnqConfId(srchValue, tableObj);
	}

	/**
	 * Method for searching object by its type unique id (expecting to return
	 * one object, if no always return the first one). This one is backwards
	 * compatible using case sensitive search.
	 * 
	 * @param srchVlaue
	 *            The value we search for
	 * @param tableObj
	 *            The object descriptor of the table we are searching in
	 * @return A DbDataObject describing the configuration row from a config
	 *         table identified by tableObject
	 * @throws SvException
	 */
	public DbDataObject getObjectByUnqConfId(String srchValue, DbDataObject tableObj) throws SvException {
		return getObjectByUnqConfId(srchValue, tableObj, false);
	}

	/**
	 * Method for searching object by its type unique id (expecting to return
	 * one object, if no always return the first one), allowing you to search by
	 * case insensitive or case sensitive manner.
	 * 
	 * @param srchVlaue
	 *            The value we search for
	 * @param tableObj
	 *            The object descriptor of the table we are searching in
	 * @param caseSensitive
	 *            Flag to denote if the search should be executed in case
	 *            insensitive manner
	 * @return A DbDataObject describing the configuration row from a config
	 *         table identified by tableObject
	 * @throws SvException
	 */
	public DbDataObject getObjectByUnqConfId(String srchValue, DbDataObject tableObj, boolean caseSensitive)
			throws SvException {
		DbDataObject result = null;

		if (tableObj == null || srchValue == null || tableObj.getObject_id() == 0) {
			log4j.error("Method SvReader.getObjectByUnqConfId can't find object with id:'" + srchValue
					+ "' for tableName: " + tableObj.getVal("TABLE_NAME"));
			return result;
		}
		// get name of the field which is unique key for the tableObj
		String unqFieldName = null;
		if (tableObj.getVal("CONFIG_UNQ_ID") != null) {
			unqFieldName = tableObj.getVal("CONFIG_UNQ_ID").toString();
		}

		DbCompareOperand dbc = caseSensitive ? DbCompareOperand.EQUAL : DbCompareOperand.ILIKE;
		// get the srchValue for the found unqFieldName in the found tableObj
		DbSearchCriterion cr1 = new DbSearchCriterion(unqFieldName, dbc, srchValue);
		DbSearchExpression expr1 = new DbSearchExpression().addDbSearchItem(cr1);
		DbDataArray results = getObjects(expr1, tableObj.getObject_id(), null, 0, 0);

		if (results.getItems().size() > 0) {
			result = results.getItems().get(0);
		}

		return result;
	}

}
