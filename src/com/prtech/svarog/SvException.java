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

import org.joda.time.DateTime;

import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.Jsonable;
/**
 * Svarog specific exception class. 
 * @author XPS13
 *
 */
public class SvException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	DbDataObject instanceUser=null;
	Jsonable userData=null;
	Object configData=null;
	
	DateTime timeStamp=new DateTime();
	private String i18nLabelCode;

	public SvException(String svarogLabelCode, DbDataObject instanceUser)
	{
		super(svarogLabelCode);
		this.i18nLabelCode=svarogLabelCode;
		if(instanceUser==null)
			this.instanceUser=svCONST.systemUser;
		else
			this.instanceUser=instanceUser;
	}

	/** 
	 * Default constructor to be used with the Svarog Exception. 
	 * @param svarogLabelCode
	 */
	public SvException(String svarogLabelCode, DbDataObject instanceUser, Throwable cause)
	{
		this(svarogLabelCode,instanceUser);
		initCause(cause);
		this.i18nLabelCode=svarogLabelCode;
		this.instanceUser=instanceUser;
		
	}

	
	public String getFormattedMessage()
	{
		String errMessage = "ERROR_ID:" + this.getInstanceUser().getVal("USER_NAME") + "."
				+ this.getTimeStamp().getMillis() + ", \n";
		errMessage += "Error Code:" + this.getLabelCode()+ ", \n";
		errMessage += "Error Message:" + I18n.getText(this.getLabelCode())+ ", \n";
		String confStr = (this.getConfigData() != null ? (this.getConfigData() instanceof  Jsonable?((Jsonable)this.getConfigData()).toJson().toString():this.getConfigData().toString()): "N/A. ");
		errMessage += "Config Data:" + confStr + ", \n";
		errMessage += "User Data:" + (this.getUserData() != null ? this.getUserData().toJson() : "N/A. ")+ "";
		return errMessage;

	}
	
	public String getJsonMessage()
	{
		JsonObject obj = new JsonObject();
		obj.addProperty("ERROR_ID", this.getInstanceUser().getVal("USER_NAME") + "."
				+ this.getTimeStamp().getMillis());
		obj.addProperty("Error_Message", (this.getLabelCode()));
		String confStr = (this.getConfigData() != null ? (this.getConfigData() instanceof  Jsonable?((Jsonable)this.getConfigData()).toJson().toString():this.getConfigData().toString()): "N/A. ");
		obj.addProperty("Config_Data", confStr);
		obj.addProperty("User_Data" , (String) (this.getUserData() != null ? this.getUserData().toJson().toString() : "N/A. "));
		return obj.toString();

	}

	/**
	 * Constructor that accepts a label code as well as DbDataObject to support with debugging.
	 * @param svarogLabelCode
	 * @param dbo
	 */
	public SvException(String svarogLabelCode, DbDataObject instanceUser, Jsonable userData, Object configData, Throwable cause)
	{
		this(svarogLabelCode, instanceUser, userData,configData);
		initCause(cause);
	}


	public SvException(String svarogLabelCode, DbDataObject instanceUser, Jsonable userData, Object configData)
	{
		this(svarogLabelCode, instanceUser);
		this.userData=userData;
		this.configData=configData;
	}

	/**
	 * Getter method to return the svarog Label Code to be used for getting
	 * a I18n message.
	 * 
	 * @return String label code to be used with {@link I18n}
	 */
	public String getLabelCode()
	{
		return i18nLabelCode;
	}

	public DbDataObject getInstanceUser() {
		return instanceUser;
	}

	public void setInstanceUser(DbDataObject instanceUser) {
		this.instanceUser = instanceUser;
	}

	public Jsonable getUserData() {
		return userData;
	}

	public void setUserData(DbDataObject userData) {
		this.userData = userData;
	}

	public Object getConfigData() {
		return configData;
	}

	public void setConfigData(Object configData) {
		this.configData = configData;
	}

	public DateTime getTimeStamp() {
		return timeStamp;
	}

}
