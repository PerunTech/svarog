/*******************************************************************************
 * Copyright (c) 2013, 2016 Perun Technologii DOOEL Skopje.
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
package com.prtech.svarog_common;


import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class MembersMap {

	public LinkedHashMap<String, Object> getMembersToMap(String startsWith, Object obj)
	{
		LinkedHashMap<String, Object> retMap = new LinkedHashMap<String, Object>();
		Class<?> baseClass = obj.getClass();
		Field[] fields = baseClass.getDeclaredFields();
		
		for(int i=0;i<fields.length;i++)
		{
			String fieldName = fields[i].getName();
			if(!fields[i].getType().equals(this.getClass())  && fieldName.startsWith(startsWith))
			{
				Object fieldValue = null;
				try {
					fields[i].setAccessible(true);
					fieldValue = fields[i].get(obj);
				} catch (Exception e) {
					e.printStackTrace();
				} 
				
				retMap.put(fieldName,fieldValue);
			}
		}
		return retMap;
	}
	public void setMembersFromMap(String startsWith, Object obj,HashMap<String, Object> inMap )
	{

		Class<?> baseClass = obj.getClass();
		for (Iterator<String> it = inMap.keySet().iterator(); it.hasNext(); ) 
		{
			  String fieldName = it.next();
			  try
			  {
				  Field field = baseClass.getDeclaredField(fieldName);
						try {
							field.setAccessible(true);
							field.set(obj,inMap.get(fieldName));
						} catch (Exception e) {
							System.out.print("Type mismatch. The class type in the map " +
							  		"is not the same as the class member for member: "+fieldName);
							e.printStackTrace();
						} 

			  }catch(Exception ex)
			  {
				  System.out.print("Failed to set field value from map: "+ex.getMessage()+"\n");
			  }
			  
		}
		
	}
}
