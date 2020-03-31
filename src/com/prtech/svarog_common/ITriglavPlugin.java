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

import java.util.LinkedHashMap;

/**
* Interface that needs to be implemented in order to use the more advanced functions of the PluginExtender.
* The SvarogPluginExtender checks if the class that is being processed implements ITriglavPluginClass and if it does uses the inherited methods to 
* get the data necessary in order to build a feature rich customizable web service.
* @author Gjorgji Pejov
* @deprecated
*/
@Deprecated
public interface ITriglavPlugin {
	
	/**
	 * Method that gets the representational name of the class. Will be used for building the path for the webservice
	 * 
	 * @return String - The representational webservice name
	 */
	public String getWebServiceName ();
	/**
	 * Method which returns all the @Produces annotations for all the webservice methods that need to be generated from this class.
	 * 
	 * @return LinkedHashMap<String,String> - All the @Produces annotations for all the webservice
	 */
	public LinkedHashMap<String,String> getMethodProduces();
	/**
	 * Method which returns all the @Consumes annotations for all the webservice methods that need to be generated from this class.
	 * 
	 * @return LinkedHashMap<String,String> - All the @Consumes annotations for all the webservice
	 */
	public LinkedHashMap<String,String> getMethodConsumes();
	/**
	 * Method which returns all the representational web service names for the methods. The pair should be {String methodName,String WebserviceName}. 
	 * If a name is found here for a method that is the name that is used henceforth.
	 * 
	 * @return LinkedHashMap<String,String> - All the Webservice name combinations for the methods in this class
	 */
	public LinkedHashMap<String,String> getMethodWebServiceNames();
	/**
	 * Method which returns all the httpMethods for the methods in this class.
	 * 
	 * @return LinkedHashMap<String,String> - All the httpMethods for the methods in this class.
	 */
	public LinkedHashMap<String,String> getMethodHttpMethods();
	/**
	 * Method which returns all the default paramValues per method if they exist
	 * The first String is the methodName,  while the HashMap contains the ParamName/ParamValue combinations
	 * @return LinkedHashMap<String,String> - All the default paramValues for the methods in this class.
	 */
	public LinkedHashMap<String,LinkedHashMap<String,Object>> getDefaultMethodParams();
	
}
