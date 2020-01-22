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

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

/**
 * SvSequence is a class providing basic sequence generation with version
 * fencing. It shall allow in transaction generation of sequence as well as
 * generation of sequences outside of a transaction scope. The sequences can be
 * guaranteed to be sequential if used in transaction scope.
 * 
 * @author ristepejov
 *
 */
public class SvSequence extends SvCore {

	/**
	 * Constructor to create a SvSequence object according to a user session.
	 * This is the default constructor available to the public, in order to
	 * enforce the svarog security mechanisms based on the logged on user.
	 * 
	 * @throws SvException
	 *             Pass through of underlying exceptions
	 */
	public SvSequence(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException
	 */
	SvSequence() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Method to get the current sequence value
	 * 
	 * @param sequenceyKey
	 *            The key for which we want to get sequence
	 * @return The sequence value
	 * @throws SvException
	 */
	long getSeqCurrValImpl(String sequenceyKey) throws SvException {
		SvReader svr = new SvReader(this);
		try {
			DbDataArray dba = svr.getObjects(
					new DbSearchCriterion("SEQUENCE_KEY", DbCompareOperand.EQUAL, sequenceyKey),
					svCONST.OBJECT_TYPE_SEQUENCE, null, null, null);
			if (dba.getItems().size() < 1)
				return 0;
			else
				return (Long) dba.getItems().get(0).getVal("SEQUENCE_VALUE");
		} finally {
			svr.release();
		}
	}

	/**
	 * Public static method for generating a new sequence value. The method is
	 * static for the purpose of using next val without any instantiating of
	 * SvCore or similar. This method will be run out of the normal transaction.
	 * The core parameter is used only to validate if this is used by
	 * ServiceUser
	 * 
	 * @param sequenceyKey
	 *            The sequence key
	 * @param core
	 *            The SvCore instance to be used for load/save
	 * @return The next sequence value
	 * @throws SvException
	 *             Any underlying exception
	 */
	public static Long getSeqNextVal(String sequenceyKey, SvCore core) throws SvException {
		Boolean isService = false;
		SvCore currentCore = core;
		while (currentCore != null) {
			isService = currentCore.isService();
			if (isService)
				break;
			currentCore = currentCore.getParentSvCore();
		}

		// if(!isService)
		// throw (new SvException("system.error.core_isnot_service",
		// core.instanceUser));

		Long returnValue = null;
		SvReader svr = new SvReader();
		try {
			svr.setAutoCommit(false);
			returnValue = getSeqNextValImplCore(sequenceyKey, svr);
			svr.dbCommit();
		} finally {
			svr.release();
		}
		return returnValue;

	}

	/**
	 * Main static method for generating a new sequence value. The method is
	 * static for the purpose of using next val without any instantiating of
	 * SvCore or similar. This method will not commit nor rollback.
	 * 
	 * @param sequenceyKey
	 *            The sequence key
	 * @param core
	 *            The SvCore instance to be used for load/save
	 * @return The next sequence value
	 * @throws SvException
	 *             Any underlying exception
	 */
	private static Long getSeqNextValImplCore(String sequenceyKey, SvCore core) throws SvException {
		DbDataObject dbo = null;
		SvReader svr = new SvReader(core);
		SvWriter svw = new SvWriter(core);
		try {
			DbDataArray dba = svr.getObjects(
					new DbSearchCriterion("SEQUENCE_KEY", DbCompareOperand.EQUAL, sequenceyKey),
					svCONST.OBJECT_TYPE_SEQUENCE, null, null, null);

			if (dba.getItems().size() < 1) {
				dbo = new DbDataObject();
				dbo.setObject_type(svCONST.OBJECT_TYPE_SEQUENCE);
				dbo.setVal("SEQUENCE_KEY", sequenceyKey);
				dbo.setVal("SEQUENCE_VALUE", 1L);
				dba.addDataItem(dbo);
			} else {
				if (dba.getItems().size() != 1)
					throw (new SvException("system.error.double_seq", core.instanceUser, dba, null));

				dbo = dba.getItems().get(0);
				dbo.setVal("SEQUENCE_VALUE", ((Long) dbo.getVal("SEQUENCE_VALUE")).longValue() + 1L);
			}
			svw.isInternal = true;
			svw.saveObjectImpl(dba, false);
			return (Long) dbo.getVal("SEQUENCE_VALUE");
		} finally {
			svr.release();
			svw.release();
		}

	}

	/**
	 * Method that increases the sequence value
	 * 
	 * @param sequenceyKey
	 *            The key of the sequence
	 * @return The next value
	 * @throws SvException
	 *             Any underlying exception
	 */
	Long getSeqNextValImpl(String sequenceyKey) throws SvException {
		return getSeqNextValImplCore(sequenceyKey, this);
	}

	/**
	 * Method to return the current sequence value
	 * 
	 * @param sequenceyKey
	 *            The sequence key
	 * @return The current sequence value
	 * @throws SvException
	 *             Any underlying exception
	 */
	public Long getSeqCurrVal(String sequenceyKey) throws SvException {
		return getSeqCurrValImpl(sequenceyKey);
	}

	/**
	 * Method to increase the sequence, with auto-commit turned on.
	 * 
	 * @param sequenceyKey
	 *            The sequence key for which we want the next val.
	 * @return The next value of the sequence
	 * @throws SvException
	 *             Any underlying exception
	 */
	public Long getSeqNextVal(String sequenceyKey) throws SvException {
		return getSeqNextVal(sequenceyKey, true);
	}

	/**
	 * Method to increase the sequence, with option to commit or rollback
	 * manually.
	 * 
	 * @param sequenceyKey
	 *            The sequence key for which we want the next val.
	 * @param autoCommit
	 *            Flag to enable disable auto-commit
	 * @return The next value of the sequence
	 * @throws SvException
	 *             Any underlying exception
	 */
	public Long getSeqNextVal(String sequenceyKey, Boolean autoCommit) throws SvException {
		Long seqVal = 0L;
		try {
			this.dbSetAutoCommit(false);
			seqVal = getSeqNextValImpl(sequenceyKey);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}
		return seqVal;

	}

}
