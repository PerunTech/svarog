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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.prtech.svarog_common.Jsonable;

public class SvActionResult extends Jsonable {

	

	public enum ReturnType {
		PASS, FAIL, WARNING, ERROR
	}

	 ReturnType result;

	 String resultMessage;
	 LinkedHashMap<String, Object> values = new LinkedHashMap<String, Object>();


	public Object getVal(String key) {
		return values.get(key.toUpperCase());
	}

	public void setVal(String key, Object obj) {

		values.put(key.toUpperCase(), obj);
	}

	public String getResult() {
		return this.result.toString();

	}

	public void setResult(String result) throws Exception {
		this.result = ReturnType.valueOf(result);

	}

	public String getResultMessage() {
		return resultMessage;
	}

	public void setResultMessage(String resultMessage) {
		this.resultMessage = resultMessage;
	}

	public Set<Map.Entry<String, Object>> getValues() {
		return values.entrySet();
	}

	public void setValues(LinkedHashMap<String, Object> values) {
		this.values = values;
	}

}
