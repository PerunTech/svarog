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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog.svCONST;

/**
 * This class provides methods to link two DbDataObjects via a specific link
 * type. It uses the basic link types managed by the SvCore to perform
 * persistence of link information based on different link types.
 * 
 * @author ristepejov
 *
 */
public class SvLink extends SvCore {

	/**
	 * Default log4j instance
	 */
	private static final Logger log4j = LogManager.getLogger(SvLink.class.getName());

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id String UID of the user session under which the SvCore
	 *                   instance will run
	 * @throws SvException Pass through of any exception from the super class
	 */
	public SvLink(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id   String UID of the user session under which the SvCore
	 *                     instance will run
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through of any exception from the super class
	 */
	public SvLink(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through of any exception from the super class
	 */
	public SvLink(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException Pass through of any exception from the super class
	 */
	SvLink() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Private method to check existence of objects subject to linking.
	 * 
	 * @param objectId1 The object id of the first object
	 * @param objectId2 The object id of the second object
	 * @param dbt1      The object type descriptor of the first object
	 * @param dbt2      The object type descriptor of the second object
	 * @return
	 * @throws SvException
	 */
	private boolean objectsExist(Long objectId1, Long objectId2, DbDataObject dbt1, DbDataObject dbt2)
			throws SvException {

		boolean retVal = false;

		String sqlQry = "SELECT pkid,object_id,status FROM " + dbt1.getVal("schema") + "." + dbt1.getVal("repo_name")
				+ " WHERE OBJECT_ID=? AND CURRENT_TIMESTAMP<DT_DELETE" + " UNION ALL SELECT pkid,object_id,status FROM "
				+ dbt2.getVal("schema") + "." + dbt2.getVal("repo_name")
				+ " WHERE OBJECT_ID=? AND CURRENT_TIMESTAMP<DT_DELETE";

		try (PreparedStatement ps = this.dbGetConn().prepareStatement(sqlQry)) {
			if (log4j.isDebugEnabled())
				log4j.debug(sqlQry);
			ps.setLong(1, objectId1);
			ps.setLong(2, objectId2);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					if (rs.next()) {
						retVal = true;
					}
				}
			}

			return retVal;
		} catch (SQLException e) {
			throw (new SvException("system.error.sql_err", instanceUser, dbt1, dbt2));
		}
	}

	/**
	 * Public method to link two objects by previously configured link type
	 * 
	 * @param objectId1  The object id of the first object
	 * @param objectId2  The object id of the second object
	 * @param linkTypeId The object id of the link type
	 * @param linkNote   The note associated with the link
	 * @throws SvException Passthrough of underlying exception from
	 *                     {@link #linkObjects(Long, Long, Long, String, boolean, boolean)}
	 */
	public void linkObjects(Long objectId1, Long objectId2, Long linkTypeId, String linkNote) throws SvException {
		linkObjects(objectId1, objectId2, linkTypeId, linkNote, true, this.autoCommit);
	}

	/**
	 * Public method to link two objects by previously configured link type, with
	 * option to skip existence check
	 * 
	 * @param obj1       The object reference of the first object
	 * @param obj2       The object reference of the second object
	 * @param linkCode   The string code uniquely identifying the link type
	 * @param linkNote   The note associated with the link
	 * @param autoCommit Flag to disable the auto commit on success. In case the
	 *                   linking is part of a transaction
	 * @throws SvException Pass through of underlying exception from
	 *                     {@link #linkObjects(DbDataObject, DbDataObject, String, String, boolean)}
	 */
	public void linkObjects(DbDataObject obj1, DbDataObject obj2, String linkCode, String linkNote, boolean autoCommit)
			throws SvException {
		linkObjects(obj1, obj2, linkCode, linkNote, true, autoCommit);
	}

	/**
	 * Public method to link two objects by previously configured link type, with
	 * option to skip existence check as well as disable the auto commit
	 * 
	 * 
	 * @param obj1                The object reference of the first object
	 * @param obj2                The object reference of the second object
	 * @param linkCode            The unique code of of the link type
	 * @param linkNote            The note associated with the link
	 * @param checkIfObjectsExist Flag to disable the object existence checks.
	 *                            Usefull for mass processing and importing
	 * @param autoCommit          Flag to disable the auto commit on success. In
	 *                            case the linking is part of a transaction
	 * @throws SvException Throws "system.error.invalid_link_type" if it can't find
	 *                     the link type. Pass through of any exception from
	 *                     underlying SvReader methods or link implementation
	 *                     {@link #linkObjectsImpl(DbDataObject, DbDataObject, DbDataObject, String, boolean, Boolean)}
	 */
	public void linkObjects(DbDataObject obj1, DbDataObject obj2, String linkCode, String linkNote,
			boolean checkIfObjectsExist, boolean autoCommit) throws SvException {
		DbDataObject dbl = getLinkType(linkCode, obj1.getObjectType(), obj2.getObjectType());

		if (dbl == null)
			throw (new SvException("system.error.invalid_link_type", instanceUser, null, null));

		SvReader svr = new SvReader(this);
		try {
			this.linkObjectsImpl(obj1, obj2, dbl, linkNote, checkIfObjectsExist, autoCommit);

		} finally {
			svr.release();
		}
	}

	/**
	 * Public method to link two objects by previously configured link type, with
	 * option to skip existence check as well as disable the auto commit
	 * 
	 * @param objectId1           The object id of the first object
	 * @param objectId2           The object id of the second object
	 * @param linkTypeId          The object id of the link type
	 * @param linkNote            The note associated with the link
	 * @param checkIfObjectsExist Flag to disable the object existence checks.
	 *                            Usefull for mass processing and importing
	 * @param autoCommit          Flag to disable the auto commit on success. In
	 *                            case the linking is part of a transaction
	 * @throws SvException Throws "system.error.invalid_link_type" if it can't find
	 *                     the link type. Pass through of any exception from
	 *                     underlying SvReader methods or link implementation
	 *                     {@link #linkObjectsImpl(DbDataObject, DbDataObject, DbDataObject, String, boolean, Boolean)}
	 */
	public void linkObjects(Long objectId1, Long objectId2, Long linkTypeId, String linkNote,
			boolean checkIfObjectsExist, boolean autoCommit) throws SvException {
		DbDataObject dbl = getLinkType(linkTypeId);

		if (dbl == null)
			throw (new SvException("system.error.invalid_link_type", instanceUser, null, null));

		try (SvReader svr = new SvReader(this)) {
			svr.isInternal = true;
			DbDataObject obj1 = svr.getObjectById(objectId1, getDbt((Long) dbl.getVal("LINK_OBJ_TYPE_1")), null);
			DbDataObject obj2 = svr.getObjectById(objectId2, getDbt((Long) dbl.getVal("LINK_OBJ_TYPE_2")), null);
			this.linkObjectsImpl(obj1, obj2, dbl, linkNote, checkIfObjectsExist, autoCommit);
		}
	}

	/**
	 * The implementation method of linkObjects. It will link two objects based on a
	 * pre-configured link
	 * 
	 * @param obj1                The first DbDataObject
	 * @param obj2                The second DbDataObject
	 * @param dbl                 The link descriptor DbDataObject
	 * @param linkNote            The free text link note
	 * @param checkIfObjectsExist Flag to disable the object existence checks.
	 *                            Usefull for mass processing and importing
	 * @param autoCommit          Flag to disable the auto commit on success. In
	 *                            case the linking is part of a transaction
	 * @throws SvException Throws "system.error.invalid_objects2link" if there's no
	 *                     object references. If the object types are not
	 *                     corresponding to the link type then
	 *                     "system.error.invalid_link_type". If checkIfObjectsExist
	 *                     flag is true then "system.error.invalid_objects2link" is
	 *                     thrown in case objects are not valid in the database.
	 */
	protected void linkObjectsImpl(DbDataObject obj1, DbDataObject obj2, DbDataObject dbl, String linkNote,
			boolean checkIfObjectsExist, Boolean autoCommit) throws SvException {
		if (dbl == null || obj1 == null || obj2 == null)
			throw (new SvException("system.error.invalid_objects2link", instanceUser, null, null));

		DbDataObject dbt1 = SvCore.getDbt((Long) dbl.getVal("LINK_OBJ_TYPE_1"));
		DbDataObject dbt2 = SvCore.getDbt((Long) dbl.getVal("LINK_OBJ_TYPE_2"));

		if (!obj1.getObject_type().equals((Long) dbl.getVal("LINK_OBJ_TYPE_1"))
				|| !obj2.getObject_type().equals((Long) dbl.getVal("LINK_OBJ_TYPE_2")))
			throw (new SvException("system.error.invalid_link_type", instanceUser, dbt1, dbt2));

		if (dbt1 == null || dbt2 == null)
			throw (new SvException("system.error.invalid_link_obj_descriptor", instanceUser, dbl,
					(dbt2 != null ? dbt2 : dbt1)));

		if (checkIfObjectsExist) {
			if (!objectsExist(obj1.getObject_id(), obj2.getObject_id(), dbt1, dbt2))
				throw (new SvException("system.error.invalid_objects2link", instanceUser, dbt1, dbt2));
		}

		DbDataObject dbo = new DbDataObject();
		dbo.setObject_type(svCONST.OBJECT_TYPE_LINK);
		dbo.setVal("LINK_TYPE_ID", dbl.getObject_id());
		dbo.setVal("LINK_OBJ_ID_1", obj1.getObject_id());
		dbo.setVal("LINK_OBJ_ID_2", obj2.getObject_id());
		dbo.setVal("LINK_NOTES", linkNote);
		SvWriter svw = new SvWriter(this);
		try {
			svw.saveObject(dbo, autoCommit);
		} finally {
			svw.release();
		}

	}
}
