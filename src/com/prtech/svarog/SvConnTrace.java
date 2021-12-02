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
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class SvConnTrace {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvConnTrace.class);

	/**
	 * The tracked JDBC connection
	 */
	Connection connection = null;

	/**
	 * Usage count of the current connection
	 */
	int usageCount = 0;

	/**
	 * Method to increase the usageCount to mark another instance using the
	 * connection
	 * 
	 * @throws SvException
	 */
	Connection acquire() throws SvException {
		synchronized (this) {
			try {
				if (connection != null && connection.isClosed())
					connection = null;

				if (connection == null) {
					if (log4j.isDebugEnabled())
						log4j.trace("Current connection is null. Fetching a new connection from SvConf");
					connection = SvConf.getDBConnection();
				}
			} catch (SQLException e) {
				throw (new SvException("system.error.db_conn_err", svCONST.systemUser, e));

			}
			if (log4j.isDebugEnabled())
				log4j.trace("Acquiring tracked connection. Usage count:" + usageCount);
			return connection;
		}
	}

	/**
	 * Method to release the usage of connection. If connection is shared
	 * between multiple SvCore instances, then just decrease the usage count.
	 * Close the connection when usage count reaches zero
	 */
	void release(Boolean isManual) {
		synchronized (this) {
			if (log4j.isDebugEnabled())
				log4j.trace("Releasing a tracked connection. Usage count:" + usageCount);
			usageCount--;
			if (usageCount == 0) {
				if (log4j.isDebugEnabled())
					log4j.trace("Usage count is zero. Performing physical rollback/close on the connection");
				try {
					if (this.connection != null && !this.connection.isClosed()) {
						if (!this.connection.getAutoCommit())
							this.connection.rollback();
					} else
						log4j.error("Can't ROLLBACK a " + (this.connection == null ? "NULL" : "")
								+ (this.connection != null && this.connection.isClosed() ? "CLOSED" : "")
								+ " connection for a deleted SvCore object");
				} catch (SQLException e) {
					log4j.error("Can't ROLLBACK connection for a deleted SvCore object", e);
				}
				try {
					if (this.connection != null) {
						if (!isManual) {
							log4j.error(
									"There is a connection leak. Not all database connections are closed properly.");
							log4j.error(
									"This connection was closed because the SvCore that owned it got garbage collected");
							log4j.error("Every dbGetConn call must end with finalize block containing dbReleaseConn");
						}
						if (!connection.isClosed()) {
							this.connection.close();
						}
						this.connection = null;
					} else
						log4j.error("Can't release a NULL or CLOSED connection for a deleted SvCore object");
				} catch (SQLException e) {
					log4j.error("Can't release connection for a deleted SvCore object", e);
				}
			}
		}
	}

}