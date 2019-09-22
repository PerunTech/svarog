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

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.ISvCodeList;
import com.prtech.svarog.svCONST;

public class CodeList extends SvCore implements ISvCodeList {
	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public CodeList(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public CodeList(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public CodeList(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}
	
	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	CodeList() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = LogManager.getLogger(CodeList.class.getName());

	public HashMap<Long, String> getCodeCategoriesId() {
		return getCodeCategoriesId(SvConf.getDefaultLocale());
	}

	public HashMap<Long, String> getCodeListId(String languageId, Long codeListObjectId) {
		String langId = languageId != null ? languageId : SvConf.getDefaultLocale();

		HashMap<Long, String> catList = new HashMap<Long, String>();
		DbDataArray object = getCodeListBase(codeListObjectId);
		for (DbDataObject dbo : object.getItems()) {
			String label = I18n.getText(langId, (String) dbo.getVal("label_code"));
			catList.put(dbo.getObject_id(), label);
		}

		return catList;

	}

	public HashMap<Long, String> getCodeListId(Long codeListObjectId) {
		return getCodeListId(SvConf.getDefaultLocale(), codeListObjectId);
	}

	public DbDataArray getCodeListBase(Long codeListObjectId) {
		SvReader svr = null;
		DbDataArray object = null;
		try {
			svr = new SvReader(this);
			object = svr.getObjectsByParentId(codeListObjectId.longValue(), svCONST.OBJECT_TYPE_CODE, null, null, null,
					"SORT_ORDER");
		} catch (SvException e) {
			log4j.error("Error loading the code list " + codeListObjectId + ":" + e.getFormattedMessage());
		} finally {
			if (svr != null)
				svr.release();
		}
		return object;

	}


	public HashMap<Long, String> getCodeCategoriesId(String languageId) {
		return getCodeListId(languageId, 0L);
	}

	// od tuka za refaktor
	public HashMap<String, String> getCodeCategories() {
		return getCodeCategories(SvConf.getDefaultLocale(), true);
	}

	public HashMap<String, String> getCodeList(String languageId, Long codeListObjectId, Boolean includeLabels) {
		String langId = languageId != null ? languageId : SvConf.getDefaultLocale();

		HashMap<String, String> catList = new HashMap<String, String>();
		DbDataArray object = getCodeListBase(codeListObjectId);
		for (DbDataObject dbo : object.getItems()) {
			String label = "";
			if (includeLabels)
				label = I18n.getText(langId, (String) dbo.getVal("label_code"));
			catList.put((String) dbo.getVal("CODE_VALUE"), label);
		}

		return catList;

	}

	public HashMap<String, String> getCodeList(Long codeListObjectId, Boolean includeLabels) {
		return getCodeList(SvConf.getDefaultLocale(), codeListObjectId, includeLabels);
	}

	public HashMap<String, String> getCodeCategories(String languageId, Boolean includeLabels) {
		return getCodeList(languageId, 0L, includeLabels);
	}

}
