package com.prtech.svarog_common;

import com.prtech.svarog.SvCore;

/**
 * Class name is underground to support operations which should not be publicly
 * used. These should support the svarog internals.
 * 
 * @author ristepejov
 *
 */
public class DboUnderground {
	/**
	 * Revert the protected member DbDataObject.isReadOnly to true
	 * 
	 * @param dbo
	 *            The DbDataObject instance which will be flagged as read-only
	 */
	static public void revertReadOnly(DbDataObject dbo, SvCore svc) {
		if (svc.isSystem())
			dbo.isReadOnly = false;
	}

}
