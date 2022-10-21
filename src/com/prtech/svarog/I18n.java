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

import com.prtech.svarog_common.*;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_interfaces.II18n;

/**
 * Class used for internationalisation of text strings (Labels). Provides means
 * for switching between different languages.
 * 
 * @author PR01
 *
 */
public class I18n implements II18n {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(I18n.class);

	/**
	 * Array holding the list of labels which were not successfully loaded
	 */
	private static ArrayList<String> ignoredLabels = new ArrayList<String>();

	private static ConcurrentHashMap<String, DbDataArray> labelsCache = new ConcurrentHashMap<String, DbDataArray>(200);

	static final Boolean initCall = initOnSaveCallBack();

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system privileges.
	 * 
	 * @throws SvException
	 */
	/*I18n() throws SvException {
		super(svCONST.systemUser, null);
	}*/

	/**
	 * The method loadLabels
	 * 
	 * @param localeId
	 * @return
	 * @throws SvException
	 */
	static Boolean loadLabels(String localeId) throws SvException {
		DbDataArray labels = null;
		try (SvReader svr = new SvReader()) {
			labels = svr.getObjects(new DbSearchCriterion("LOCALE_ID", DbCompareOperand.EQUAL, localeId),
					SvCore.getDbt(svCONST.OBJECT_TYPE_LABEL), null, null, null);
			DbCache.addArrayByParentId(labels, svCONST.OBJECT_TYPE_LABEL, getLocaleId(localeId).getObjectId());
			for (DbDataObject dbo : labels.getItems())
				DbCache.addObject(dbo, dbo.getVal("label_code") + "." + dbo.getVal("locale_id"));
			return true;
		}
	}

	/**
	 * Method to ensure the cache key for labels is always the same
	 * 
	 * @param localeId  The string version of the locale ID
	 * @param labelCode The code of the label (ASCII mnemonic)
	 * @return The label object key
	 */
	static String getKey(String localeId, String labelCode) {
		return localeId + "." + labelCode;
	}

	/**
	 * Method to ensure the cache key for labels is always the same
	 * 
	 * @param label The DbDataObject version of the label
	 * @return The label object key
	 */
	static String getKey(DbDataObject label) {
		return getKey((String) label.getVal("locale_id"), (String) label.getVal("label_code"));
	}

	/**
	 * Method to ensure the group code for labels is always the same
	 * 
	 * @param labelCode The code of the label (ASCII mnemonic)
	 * @return The label group code
	 */
	static public String getGroupCode(String labelCode) {
		String labelGroupCode = labelCode;
		if (labelCode.lastIndexOf('.') > 0) {
			labelGroupCode = labelCode.substring(0, labelCode.lastIndexOf('.') + 1);
		}
		return labelGroupCode;
	}

	/**
	 * Method to ensure the group code for labels is always the same
	 * 
	 * @param label The DbDataObject version of the label
	 * @return The label group code
	 */
	static public String getGroupCode(DbDataObject label) {
		return getGroupCode((String) label.getVal("label_code"));
	}

	/**
	 * Method to return an array of labels for specific group code (label code up to
	 * the last point.)
	 * 
	 * @param localeId       The requested locale
	 * @param labelGroupCode The group code of the labels
	 * @return The resulting array of labels in the same group code
	 * @throws SvException Underlying exception
	 */
	public static DbDataArray getLabels(String localeId, String labelGroupCode) throws SvException {
		DbDataArray labels = null;

		labels = labelsCache.get(getKey(localeId, labelGroupCode));
		if (labels == null) {
			labels = loadLabels(localeId, labelGroupCode);
			if (labels != null && labels.size() > 0) {
				labels.rebuildIndex("LABEL_CODE");
				labelsCache.put(getKey(localeId, labelGroupCode), labels);
			}
		}
		return labels;

	}

	/**
	 * Method to return an array of labels for specific group code (label code up to
	 * the last point.)
	 * 
	 * @param localeId  The requested locale
	 * @param labelCode The label code of the label
	 * @return The resulting array of labels in the same group code
	 * @throws SvException Underlying exception
	 */
	private static DbDataArray loadLabels(String localeId, String labelCode) throws SvException {
		DbDataArray labels = null;
		try (SvReader svr = new SvReader()) {

			DbSearchExpression dbs = new DbSearchExpression();
			dbs.addDbSearchItem(
					new DbSearchCriterion("LOCALE_ID", DbCompareOperand.EQUAL, localeId, DbLogicOperand.AND));
			dbs.addDbSearchItem(new DbSearchCriterion("LABEL_CODE", DbCompareOperand.LIKE, labelCode + '%'));
			labels = svr.getObjects(dbs, SvCore.getDbt(svCONST.OBJECT_TYPE_LABEL), null, null, null);
		}
		return labels;
	}

	/**
	 * This method is dead! Use getLabels(languageId, labelCategory)!!
	 * 
	 * The method returns a HashMap<String, String> with pairs of label codes and
	 * localised text for the requested locale
	 * 
	 * @param languageId the id of the locale for which we would like to get a table
	 *                   of labels
	 * @return HashMap<String, String> containing label/text pairs
	 */
	@Deprecated
	public static DbDataArray getLabels(String languageId) {
		log4j.error("This method is dead! Use getLabels(languageId, labelCategory)");
		return null;

	}

	/**
	 * This method is dead! Use getLabels(languageId, labelCategory)!!
	 * 
	 * The method returns a HashMap<String, String> with pairs of label codes and
	 * localised text for the requested locale
	 * 
	 * @param languageId the id of the locale for which we would like to get a table
	 *                   of labels
	 * @return HashMap<String, String> containing label/text pairs
	 */
	@Deprecated
	public static DbDataArray getLabels(Long languageId) {
		log4j.error("This method is dead! Use getLabels(languageId, labelCategory)");
		return null;
	}

	/**
	 * The method returns a DbDataObject descriptor for the locale
	 * 
	 * @param languageId the string id of the locale for which we would like to get
	 *                   a table of labels
	 * @return HashMap<String, String> containing label/text pairs
	 */
	public static DbDataObject getLocaleId(String languageId) {
		return SvarogInstall.getLocaleList().getItemByIdx(languageId);
	}

	/**
	 * The method getText returns a text representation in the specified locale for
	 * the requested label code
	 * 
	 * @param languageId the locale which i18n use to localise the label
	 * @param labelCode  the label code for which i18n will return a localised
	 *                   string
	 * @return String representation of the label
	 * @throws SvException
	 */

	public static String getText(String languageId, String labelCode) {
		try {
			return getBaseText(languageId, labelCode, "label_text");
		} catch (SvException e) {
			log4j.error("Error fetching text for lang:label '" + languageId + ":" + labelCode + "':"
					+ e.getFormattedMessage());
			return labelCode;
		}
	}

	/**
	 * The method returns a text representation for a label code in the default
	 * configured locale
	 * 
	 * @param languageId the locale which i18n use to localise the label
	 * @param labelCode  the label code for which i18n will return a localised
	 *                   string
	 * @return String representation of the label
	 */
	@Override
	public String getI18nText(String languageId, String labelCode) {
		if (languageId == null)
			languageId = (String) SvCore.getDefaultLocale().getVal("LOCALE_ID");
		return getText(languageId, labelCode);
	}

	/**
	 * The method getLongText returns a text description in the specified locale for
	 * the requested label code
	 * 
	 * @param languageId the locale which i18n use to localise the label
	 * @param labelCode  the label code for which i18n will return a localised
	 *                   string
	 * @return String representation of the label
	 * @throws SvException
	 */
	public static String getLongText(String languageId, String labelCode) {
		try {
			return getBaseText(languageId, labelCode, "label_descr");
		} catch (SvException e) {
			log4j.error("Error fetching text for lang:label '" + languageId + ":" + labelCode + "':"
					+ e.getFormattedMessage());
			return labelCode;
		}
	}

	/**
	 * The method returns a text description for a label code in the default
	 * configured locale
	 * 
	 * @param languageId the locale which i18n use to localise the label
	 * @param labelCode  the label code for which i18n will return a localised
	 *                   string
	 * @return String representation of the label
	 * @throws SvException
	 */
	@Override
	public String getI18nLongText(String languageId, String labelCode) {
		if (languageId == null)
			languageId = (String) SvCore.getDefaultLocale().getVal("LOCALE_ID");
		return getLongText(languageId, labelCode);
	};

	static DbDataObject getLabel(String localeId, String labelCode) throws SvException {
		DbDataObject locale = SvarogInstall.getLocaleList().getItemByIdx(localeId);
		DbDataObject dbo = null;
		if (ignoredLabels.indexOf(getKey(localeId, labelCode)) > -1) {
			return null;
		}
		if (locale != null) {

			String labelGroupCode = getGroupCode(labelCode);

			DbDataArray lblGroup = getLabels(localeId, labelGroupCode);

			if (lblGroup == null || lblGroup.size() < 1) {
				ignoredLabels.add(getKey(localeId, labelCode));
				log4j.error("Label: " + labelCode + " and locale:" + localeId + " are added to ignore list. "
						+ "Translate the label and restart the application server. "
						+ "Svarog will not try to load the object again!");

			} else {
				dbo = lblGroup.getItemByIdx(labelCode, locale.getObjectId());
			}
		}
		return dbo;
	}

	private static String getBaseText(String languageId, String labelCode, String textFieldName) throws SvException {
		DbDataObject dbo = getLabel(languageId, labelCode);
		if (dbo == null)
			return labelCode;
		else
			return (String) dbo.getVal(textFieldName);

	}

	/**
	 * The method getText returns a text representation for a label code in the
	 * default configured locale
	 * 
	 * @param labelCode the label code for which i18n will return a localised string
	 * @return String representation of the label
	 * @throws SvException
	 */
	public static String getText(String labelCode) {
		return getText(SvConf.getDefaultLocale(), labelCode);
	}

	/**
	 * Method to provide I18 through the standard interface
	 * 
	 * @param labelCode the label code for which i18n will return a localised string
	 * @return String representation of the label
	 * @throws SvException
	 */
	@Override
	public String getI18nText(String labelCode) {

		return I18n.getText(labelCode);
	}

	/**
	 * The method getLongText returns a text description for a label code in the
	 * default configured locale
	 * 
	 * @param labelCode the label code for which i18n will return a localised string
	 * @return String representation of the label
	 * @throws SvException
	 */
	public static String getLongText(String labelCode) {
		return getLongText(SvConf.getDefaultLocale(), labelCode);
	}

	/**
	 * The method returns a text description for a label code in the default
	 * configured locale
	 * 
	 * @param labelCode the label code for which i18n will return a localised string
	 * @return String representation of the label
	 * @throws SvException
	 */
	@Override
	public String getI18nLongText(String labelCode) {

		return I18n.getLongText(labelCode);
	}

	public static void invalidateLabelsCache(DbDataObject label) throws SvException {
		DbDataArray labels = null;

		if (ignoredLabels.indexOf(getKey(label)) > -1) {
			ignoredLabels.remove(getKey(label));
		}

		String groupCode = getGroupCode(label);
		String key = getKey((String) label.getVal("LOCALE_ID"), groupCode);
		labels = labelsCache.get(key);
		if (labels != null) {
			labelsCache.remove(key);
		}
	}

	/**
	 * Register invalidate labels cache after save
	 */
	static Boolean initOnSaveCallBack() {
		OnSaveCallBackI18n call = new OnSaveCallBackI18n();
		SvCore.registerOnSaveCallback(call, svCONST.OBJECT_TYPE_LABEL);
		return true;
	}

}

class OnSaveCallBackI18n implements ISvOnSave {

	@Override
	public boolean beforeSave(SvCore parentCore, DbDataObject dbo) throws SvException {

		DbDataObject locale = SvarogInstall.getLocaleList().getItemByIdx((String) dbo.getVal("LOCALE_ID"));
		dbo.setParentId(locale.getObjectId());
		return true;
	}

	@Override
	public void afterSave(SvCore parentCore, DbDataObject dbo) throws SvException {
		I18n.invalidateLabelsCache(dbo);

	}
}
