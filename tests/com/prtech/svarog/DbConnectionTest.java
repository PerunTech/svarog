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

import static org.junit.Assert.*;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionTest {
	@Test
	public void testGetDBConnection() {
		Connection conn = null;
		try {
			conn = SvConf.getDBConnection();
			if (conn == null)
				fail("Connection is null");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO Auto-generated catch block
			fail("DbConnection.getDBConnection() raised an exception");
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					fail(e.getMessage());
					e.printStackTrace();
				}
		}

	}

	/*
	 * @Test public void testMSSQL() { Connection con = null; if
	 * (!SvConf.getDBType().equals("MSSQL")) return;
	 * 
	 * try { con = SvConf.getDBConnection();
	 * 
	 * } catch (SvException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } try {
	 * 
	 * SQLServerDataTable stuTypeDT = new SQLServerDataTable();
	 * stuTypeDT.addColumnMetadata("PKID", java.sql.Types.NUMERIC);
	 * stuTypeDT.addColumnMetadata("META_PKID", java.sql.Types.NUMERIC);
	 * stuTypeDT.addColumnMetadata("OBJECT_ID", java.sql.Types.NUMERIC);
	 * stuTypeDT.addColumnMetadata("DT_INSERT", java.sql.Types.TIMESTAMP);
	 * stuTypeDT.addColumnMetadata("DT_DELETE", java.sql.Types.TIMESTAMP);
	 * stuTypeDT.addColumnMetadata("PARENT_ID", java.sql.Types.NUMERIC);
	 * stuTypeDT.addColumnMetadata("OBJECT_TYPE", java.sql.Types.NUMERIC);
	 * stuTypeDT.addColumnMetadata("STATUS", java.sql.Types.NVARCHAR);
	 * stuTypeDT.addColumnMetadata("USER_ID", java.sql.Types.NUMERIC);
	 * 
	 * stuTypeDT.addRow(0L, 0L, 0L, new Timestamp((new DateTime()).getMillis()),
	 * new Timestamp(svCONST.MAX_DATE.getMillis()), 0, 50, "VALID", -11);
	 * stuTypeDT.addRow(0L, 0L, 0L, new Timestamp((new DateTime()).getMillis()),
	 * new Timestamp(svCONST.MAX_DATE.getMillis()), 0, 50, "VALID", -11);
	 * 
	 * String ececStoredProc = "EXEC [insRepoData] ?";
	 * SQLServerPreparedStatement pStmt = (SQLServerPreparedStatement)
	 * con.prepareStatement(ececStoredProc); pStmt.setStructured(1,
	 * "[dbo].[RepoTableType]", stuTypeDT); ResultSet rs = pStmt.executeQuery();
	 * 
	 * ResultSetMetaData rsmd = rs.getMetaData(); int columnCount =
	 * rsmd.getColumnCount(); if (rs.next()) { do { for (int i = 1; i <=
	 * columnCount; i++) { // Object key = // rs.getObject("GENERATED_KEYS");
	 * Object key1 = rs.getObject(i); System.out.println("KEY " + i + " = " +
	 * key1); } } while (rs.next()); } else {
	 * System.out.println("NO KEYS WERE GENERATED."); } rs.close();
	 * 
	 * pStmt.close(); con.commit(); con.close();
	 * 
	 * } catch (Exception e) { e.printStackTrace(); }
	 * 
	 * }
	 */
	@Test
	public void testGetsDriverName() {
		if (!(SvConf.getDriverName().equals("org.postgresql.Driver")
				|| SvConf.getDriverName().equals("oracle.jdbc.driver.OracleDriver")
				|| SvConf.getDriverName().equals("com.microsoft.sqlserver.jdbc.SQLServerDriver")))
			fail("DbConnection.getDriverName() didn't return one of the three supported driver types");
	}

	@Test
	public void testGetsConnectionString() {
		if (!(SvConf.getConnectionString().startsWith("jdbc:postgresql://")
				|| SvConf.getConnectionString().startsWith("jdbc:oracle:thin:@")
				|| SvConf.getConnectionString().startsWith("jdbc:sqlserver://")))
			fail("DbConnection.getConnectionString() didn't return the default connection string setup");

	}

	@Test
	public void testGetsUserName() {
		if (!(SvConf.getUserName() != null && SvConf.getUserName().length() > 2))
			fail("DbConnection.getsUserName() didn't return the default type 'postgres'");

	}

	@Test
	public void testGetsPassword() {
		if (!(SvConf.getPassword() != null && SvConf.getPassword().length() > 2))
			fail("DbConnection.getsPassword() didn't return any password configured");
	}

	@Test
	public void testGetsDBType() {
		testGetDBConnection();
		String sType = SvConf.getDbType().toString();
		if (!(sType.equals("POSTGRES") || sType.equals("ORACLE") || sType.equals("MSSQL")))
			fail("DbConnection.getDBType() didn't return the default type POSTGRES");

	}

}
