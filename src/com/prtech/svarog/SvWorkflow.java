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
package com.prtech.svarog;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvWorkflow extends SvCore {

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public SvWorkflow (String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws Exception
	 */
	public SvWorkflow(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	public SvWorkflow(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvWorkflow() throws SvException {
		super(svCONST.systemUser, null);
	}


	/**
	 * Method to change the status of a DbDataObject and move it to a new state.
	 * This overloaded version performs commit on success and rollback on exception
	 * @param dbo
	 *            The DbDataObject which is subject of change
	 * @param newStatus
	 *            The status to which the object will be moved
	 * @throws SvException 
	 */
	public void moveObject(DbDataObject dbo, String newStatus) throws SvException {
		moveObject(dbo, newStatus, this.autoCommit);
	}

	/**
	 * Method that moves an object from one status to another status, with
	 * option to auto commit or rollback if exception occured.
	 * 
	 * @param dbo
	 *            The object to be moved to another status
	 * @param newStatus
	 *            The status to which the object should be moved
	 * @param autoCommit
	 *            Flag to enable/disable auto commit
	 * @throws Exception
	 */
	public void moveObject(DbDataObject dbo, String newStatus, Boolean autoCommit) throws SvException {
		try {
			this.dbSetAutoCommit(false);
			moveObjectImpl(dbo, newStatus);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
	}

	/**
	 * Implementation method that moves an object from one status to another
	 * status
	 * 
	 * @param dbo
	 *            The object to be moved to another status
	 * @param newStatus
	 *            The status to which the object should be moved
	 * @throws Exception
	 */
	 void moveObjectImpl(DbDataObject dbo, String newStatus) throws SvException {

		DbDataObject dbt = getDbt(dbo);
		dbo.setStatus(newStatus);
		DbDataArray dba = new DbDataArray();
		dba.addDataItem(dbo);

		SvWriter svw = new SvWriter(this);
		try
		{
		svw.saveRepoData(dbt, dba, false, false);

		// in case we are moving a link, we need to invalidate the link cache
		if (dbo.getObject_type().equals(svCONST.OBJECT_TYPE_LINK))
			svw.removeLinkCache(dbo);
		}finally
		{
			svw.release();
		}
		// TODO add movement specific business rules and other things
	}

}
