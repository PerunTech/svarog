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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class SvNote extends SvCore {

	/**
	 * Default log4j instance
	 */
	private static final Logger log4j = LogManager.getLogger(SvNote.class.getName());

	/**
	 * Constructor to create a SvNote object according to a user session. This
	 * is the default constructor available to the public, in order to enforce
	 * the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvNote(String session_id) throws SvException {
		super(session_id);
	}


	/**
	 * SvCore chained constructor. This constructor will re-use the JDBC
	 * connection from the chained SvCore
	 * 
	 * @param sharedSvCore The shared SvCore object from which we reuse the connection 
	 * @throws SvException
	 */
	public SvNote(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvNote() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * The implementation method that sets the note attached to dbo or create
	 * new according to note_name.
	 * 
	 * @param objectId
	 *            The Id of object to which we will attach the note.
	 * @param note_name
	 *            The name of the note to be attached.
	 * @param note_text
	 *            The content of the note itself
	 * 
	 */
	public void setNote(Long objectId, String note_name, String note_text) throws SvException {

		setNoteImpl(objectId, note_name, note_text, autoCommit);
	}

	/**
	 * The implementation method that sets the note attached to dbo or create
	 * new according to note_name.
	 * 
	 * @param objectId
	 *            The Id of object to which we will attach the note.
	 * @param note_name
	 *            The name of the note to be attached.
	 * @param note_text
	 *            The content of the note itself
	 * @param autoCommit
	 *            Flag to disable the auto commit on success. In case the
	 *            linking is part of a transaction
	 * 
	 */
	public void setNote(Long objectId, String note_name, String note_text, Boolean autoCommit) throws SvException {

		setNoteImpl(objectId, note_name, note_text, autoCommit);
	}

	/**
	 * The implementation method that sets the note attached to dbo or create
	 * new according to note_name.
	 * 
	 * @param objectId
	 *            The Id of object to which we will attach the note.
	 * @param note_name
	 *            The name of the note to be attached.
	 * @param note_text
	 *            The content of the note itself
	 * @param autoCommit
	 *            Flag to disable the auto commit on success. In case the
	 *            linking is part of a transaction
	 * 
	 */
	void setNoteImpl(Long objectId, String note_name, String note_text, Boolean autoCommit) throws SvException {

		SvReader svr = null;
		SvWriter svw = null;
		try {
			svr = new SvReader(this);
			svw = new SvWriter(svr);
			DbDataArray notes = svr.getObjectsByParentId(objectId, svCONST.OBJECT_TYPE_NOTES, null, 0, 0);
			notes.rebuildIndex("NOTE_NAME");
			DbDataObject note = notes.getItemByIdx(note_name, objectId);
			if (note == null) {
				note = new DbDataObject(svCONST.OBJECT_TYPE_NOTES);
				note.setVal("NOTE_NAME", note_name);			
				note.setParent_id(objectId);
			}
			note.setVal("NOTE_TEXT", note_text);
			svw.saveObject(note, autoCommit);
		} finally {
			if (svr != null)
				svr.release();
			if (svw != null)
				svw.release();
		}

	}

	/**
	 * The implementation method that gets the note attached to dbo with object
	 * ID or create new according to note_name.
	 * 
	 * @param objectId
	 *            The Id of object for which we will get the note.
	 * @param note_name
	 *            The name of the note to be retrieved.
	 */
	public String getNote(Long objectId, String note_name) throws SvException {
		SvReader svr = null;
		try {
			svr = new SvReader(this);
			DbDataArray notes = svr.getObjectsByParentId(objectId, svCONST.OBJECT_TYPE_NOTES, null, 0, 0);
			notes.rebuildIndex("NOTE_NAME");
			DbDataObject note = notes.getItemByIdx(note_name, objectId);
			if(note!=null)
				return (String)note.getVal("NOTE_TEXT");
			return "";
		} finally {
			if (svr != null)
				svr.release();
		}

	}

}
