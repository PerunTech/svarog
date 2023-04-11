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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

import com.prtech.svarog_common.DbSearchExpression;

public class SvParametersTest {

	@Test
	public void testSetAndGetParamString() {

		SvReader svr = null;
		SvWriter svw = null;
		SvParameter svp = null;

		try {
			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			svp = new SvParameter(svw);

			DbDataObject paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", "param.module", svr);
			// if it doesn't exist, than it will create new type of parameter
			if (paramType == null) {
				createParamType(0L, "param.module", "String", "DROP_DOWN", null, null, null, null, 1L, "Кампања",
						"mk_MK", svr, svw);
				// check if paramType created
				paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", "param.module", svr);
			}

			if (paramType != null) {

				svp.setParamString("param.module", "TEST1");
				svp.setParamString("param.module", "TEST2");
				String value = svp.getParamString("param.module");
				if (value == null)
					fail("Can't get value of parameters module for repo object");
				if (!value.equals("TEST2")) {
					fail("Param value not as expected");
				}
			}
			svp.dbRollback();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
			if (svp != null)
				svp.release();
		}
	}

	@Test
	public void testSetAndGetParamPublicString() {

		SvReader svr = null;
		SvWriter svw = null;
		SvParameter svp = null;
		SvSecurity svs = null;

		try {
			svr = new SvReader();
			svs = new SvSecurity(svr);
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			svp = new SvParameter(svw);
			String paramLabel = "param.test" + svCONST.OBJECT_TYPE_SECURITY_LOG;

			DbDataObject parentDbt = SvCore.getDbt(svCONST.OBJECT_TYPE_SECURITY_LOG);
			DbDataObject paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", paramLabel, svr);
			// if it doesn't exist, than it will create new type of parameter
			if (paramType == null) {
				createParamType(svCONST.OBJECT_TYPE_SECURITY_LOG, paramLabel, "String", "DROP_DOWN", null, null, null,
						null, 1L, "Кампања", "mk_MK", svr, svw);
				// check if paramType created
				paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", paramLabel, svr);
			}

			if (paramType != null) {

				svp.setParamString(parentDbt, paramLabel, "TEST1");
				String val1 = svp.getParamString(parentDbt, paramLabel);
				if (val1 == null)
					fail("Can't get value of parameters module for repo object");
				String value = svs.getPublicParam(paramLabel);
				if (value == null)
					fail("Can't get value of parameters module for repo object");
				if (!value.equals("TEST1")) {
					fail("Param value not as expected");
				}
			}
			svp.dbRollback();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
			if (svp != null)
				svp.release();
		}
	}

	@Test
	public void testSetAndGetParamLong() {
		SvReader svr = null;
		SvWriter svw = null;
		SvParameter svp = null;

		try {
			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			svp = new SvParameter(svw);

			DbDataObject paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", "param.module.long",
					svr);
			// if it doesn't exist, than it will create new type of parameter
			if (paramType == null) {
				createParamType(0L, "param.module.long", "Long", "DROP_DOWN", null, null, null, null, 1L, "Кампања",
						"mk_MK", svr, svw);
				// check if paramType created
				paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", "param.module.long", svr);
				svr.dbCommit();
			}

			if (paramType != null) {
				svp.setParamLong("param.module.long", 14124L);
				svp.setParamLong("param.module.long", 34121L);
				Long value = svp.getParamLong("param.module.long");
				if (value == null)
					fail("Can't get value of parameters module for repo object");
				if (!value.equals(Long.valueOf(34121))) {
					fail("Param value not as expected");
				}
			}
			svp.dbCommit();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
			if (svp != null)
				svp.release();
		}
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testSetAndGetParamDateTime() {
		SvReader svr = null;
		SvWriter svw = null;
		SvParameter svp = null;

		try {
			svr = new SvReader();
			svw = new SvWriter(svr);
			svw.setAutoCommit(false);
			svp = new SvParameter(svw);

			DbDataObject paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", "param.module", svr);
			// if it doesn't exist, than it will create new type of parameter
			if (paramType == null) {
				createParamType(0L, "param.module", "DateTime", "DROP_DOWN", null, null, null, null, 1L, "Кампања",
						"mk_MK", svr, svw);
				// check if paramType created
				paramType = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", "param.module", svr);
			}

			if (paramType != null) {

				DateTime date1 = DateTime.parse("10.01.2016 19:29:05",
						DateTimeFormat.forPattern("dd.MM.yyyy HH:mm:ss"));
				svp.setParamDateTime("param.module", date1);
				DateTime date2 = DateTime.parse("10.01.2016 19:30:05",
						DateTimeFormat.forPattern("dd.MM.yyyy HH:mm:ss"));
				svp.setParamDateTime("param.module", date2);
				DateTime value = svp.getParamDateTime("param.module");
				if (value == null)
					fail("Can't get value of parameters module for repo object");
				if (!value.toDateMidnight().isEqual(date2.toDateMidnight())) {
					fail("Param value not as expected");
				}
			}
			svp.dbCommit();
		} catch (SvException ex) {
			ex.printStackTrace();
			fail(ex.getFormattedMessage());
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
			if (svp != null)
				svp.release();
		}
	}

	public void createParamType(Long parentId, String labelCode, String dataType, String inputType, String defaultValue,
			Long codeListId, String isMultipleValueOfEntry, String guiMetadata, Long paramOrder,
			String paramTypeLabelText, String labelLocaleId, SvReader svr, SvWriter svw) throws SvException {

		// check if paramTypeObject already exists
		DbDataObject paramTypeObj = searchForObject(svCONST.OBJECT_TYPE_PARAM_TYPE, "LABEL_CODE", labelCode, svr);
		// if not exist then create it
		if (paramTypeObj == null) {
			// prepare paramTypeObject and save it
			svw.saveObject(createParamTypeObj(parentId, labelCode, dataType, inputType, defaultValue, codeListId,
					isMultipleValueOfEntry, guiMetadata, paramOrder), false);
			// save label for paramTypeObject
			createLabel(labelCode, paramTypeLabelText, labelLocaleId, svw);
			System.out.println("ParamTypeObject with labelCode:" + labelCode + " saved with autoCommit:false.");
		} else {
			System.out.println("ParamTypeObject with labelCode:" + labelCode + " already exists.");

		}

	}

	public DbDataObject searchForObject(long objToSearch, String columnToSearch, String valueToSearch,
			SvReader svReader) {
		DbDataArray reg_office = new DbDataArray();
		DbDataObject result = null;
		try {
			com.prtech.svarog_common.DbSearchExpression expr = new com.prtech.svarog_common.DbSearchExpression();
			expr.addDbSearchItem(new com.prtech.svarog_common.DbSearchCriterion(columnToSearch, DbCompareOperand.EQUAL,
					valueToSearch));
			/*
			 * expr.addDbSearchItem( new DbSearchCriterion("PARENT_CODE_VALUE",
			 * DbCompareOperand.EQUAL, "POPULATED_AREAS_IPARD"));
			 */
			reg_office = svReader.getObjects(expr, objToSearch, null, 0, 0);
			ArrayList<DbDataObject> list = (ArrayList<DbDataObject>) reg_office.getItems();
			if (list.size() > 0) {
				result = list.get(0);
			}

		} catch (Exception ex) {
			System.out.println("Error in it.getSQLExpression()");
			ex.printStackTrace();
		}
		return result;

	}

	public DbDataObject createParamTypeObj(Long parentId, String labelCode, String dataType, String inputType,
			String defaultValue, Long codeListId, String isMultipleValueOfEntry, String guiMetadata, Long paramOrder) {
		DbDataObject paramTypeObj = new DbDataObject();
		paramTypeObj.setObject_type(svCONST.OBJECT_TYPE_PARAM_TYPE);
		paramTypeObj.setStatus("VALID");
		paramTypeObj.setParent_id(parentId);
		paramTypeObj.setDt_insert(new DateTime());
		paramTypeObj.setDt_delete(SvConf.MAX_DATE);
		if (labelCode != null)
			paramTypeObj.setVal("LABEL_CODE", labelCode);
		if (dataType != null)
			paramTypeObj.setVal("DATA_TYPE", dataType);
		if (inputType != null)
			paramTypeObj.setVal("INPUT_TYPE", inputType);
		if (defaultValue != null)
			paramTypeObj.setVal("DEFAULT_VALUE", defaultValue);
		if (codeListId != null)
			paramTypeObj.setVal("CODE_LIST_ID", codeListId);
		if (isMultipleValueOfEntry != null)
			paramTypeObj.setVal("IS_MULTIPLE_VALUE_OF_ENTRY", isMultipleValueOfEntry);
		if (guiMetadata != null)
			paramTypeObj.setVal("GUI_METADATA", guiMetadata);
		if (paramOrder != null)
			paramTypeObj.setVal("PARAM_ORDER", paramOrder);

		return paramTypeObj;
	}

	public void createLabel(String labelCode, String labelDescr, String localeId, SvWriter svWriter)
			throws SvException {

		SvReader svReader = new SvReader(svWriter);
		try {
			DbDataObject labelObj = getLabel(labelCode, localeId, svReader);
			if (labelObj == null) {
				labelObj = new DbDataObject();
				labelObj.setStatus("VALID");
				labelObj.setDt_insert(new DateTime());
				labelObj.setDt_delete(SvConf.MAX_DATE);
				labelObj.setVal("label_code", labelCode);
				labelObj.setVal("label_text", labelDescr);
				labelObj.setObject_type(svCONST.OBJECT_TYPE_LABEL);
				labelObj.setVal("locale_id", localeId);
				svWriter.saveObject(labelObj, false);
			} else {
				String currLabelDescr = labelObj.getVal("LABEL_TEXT").toString();
				if (!currLabelDescr.equals(labelDescr)) {
					labelObj.setVal("LABEL_TEXT", labelDescr);
					svWriter.saveObject(labelObj, false);
					System.out.println("Object of type LABEL updated with label_code" + labelCode);
				} else {
					System.out.println("Object of type LABEL with label_code:" + labelCode + " already exists.");
				}
			}

		} finally {
			if (svReader != null) {
				svReader.release();
			}
		}
	}

	public DbDataObject getLabel(String labelCode, String localeId, SvReader svReader) throws SvException {

		DbDataObject result = null;

		DbSearchExpression getFormType = new DbSearchExpression();
		DbSearchCriterion filterByLabel = new DbSearchCriterion("LABEL_CODE", DbCompareOperand.EQUAL, labelCode);
		DbSearchCriterion filterByLocaleId = new DbSearchCriterion("LOCALE_ID", DbCompareOperand.EQUAL, localeId);
		getFormType.addDbSearchItem(filterByLabel);
		getFormType.addDbSearchItem(filterByLocaleId);
		DbDataArray searchResult = svReader.getObjects(getFormType, svCONST.OBJECT_TYPE_LABEL, null, 0, 0);

		if (searchResult.getItems().size() > 0) {
			result = searchResult.getItems().get(0);
		}
		return result;
	}

	@Test
	public void testSysParamInt() throws SvException {
		try (SvParameter svp = new SvParameter()) {
			String paramName = "TEST_PARAM_INT3";
			Integer paramValue = 123;
			Integer val = SvParameter.getSysParam(paramName, paramValue);
			if (!val.equals(paramValue))
				fail("Non existent param shall return null");

			SvParameter.setSysParam(paramName, paramValue);
			Integer retVal = (Integer) svp.getSysParam(paramName);
			if (!retVal.equals(paramValue))
				fail("Value returned is not the same");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testSysParams() throws SvException {
		try (SvParameter svp = new SvParameter()) {
			String paramName = "TEST_PARAM";
			String paramValue = "TEST_PARAM_VALUE";
			String val = svp.getSysParam(paramName, paramValue);
			if (!val.equals(paramValue))
				fail("Non existent param shall return null");

			svp.setSysParam(paramName, paramValue);
			String retVal = (String) svp.getSysParam(paramName);
			if (!retVal.equals(paramValue))
				fail("Value returned is not the same");
		}
	}

	@Test
	public void testUserParams() throws SvException {
		try (SvParameter svp = new SvParameter()) {
			String paramName = "TEST_PARAM";
			String paramValue = "TEST_PARAM_VALUE";
			String val = svp.getUserParam(paramName, paramValue, 0L);
			if (!val.equals(paramValue))
				fail("Non existent param shall return null");

			svp.getUserParam(paramName, paramValue, 0L);
			String retVal = (String) svp.getUserParam(paramName, 0L);
			if (!retVal.equals(paramValue))
				fail("Value returned is not the same");
		}
	}

	@Test
	public void testSysParamNoAuto() throws SvException {
		String paramName = "TEST_PARAM_DONTCREATE" + DateTime.now().toString();
		Object val = SvParameter.getSysParam(paramName, false);
		if (val != null)
			fail("Non existent param shall return null");
	}
}
