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

import com.prtech.svarog.SvCore;
import com.prtech.svarog.SvException;

/**
 * Interface for implementing object validations or post save cleanup tasks
 * @author XPS13
 *
 */
public interface ISvOnSave {
	/**
	 * The beforeSave callback is aimed at providing external validation to objects.
	 * If the invoked method returns false, svarog will abort the objectSave method
	 * @param parentCore The SvCore under which the save operations are pending
	 * @param dbo The object being subject of saving
	 * @return Boolean result of the before save validation
	 * @throws SvException 
	 */
	public boolean beforeSave(SvCore parentCore, DbDataObject dbo) throws SvException;
	
	/**
	 * The afterSave callback is aimed at cleanup functions which arise after successful saving of the 
	 * object in the database
	 * @param parentCore The SvCore under which the save operation was executed
	 * @param dbo The object being subject of saving
	 */
	public void afterSave(SvCore parentCore, DbDataObject dbo) throws SvException;
}
