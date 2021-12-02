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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog.SvConf.SvDbType;
import com.prtech.svarog_interfaces.ISvDatabaseIO;

public class SvPostgresIO implements ISvDatabaseIO {

	static final String sqlScriptsPath = "/com/prtech/svarog/sql/";
	static final String sqlScriptsPackage = "DEFAULT/";
	static final String sqlKeywordsBundle = sqlScriptsPath + sqlScriptsPackage + "sql_keywords.properties";

	private static final Logger logger = LogManager.getLogger(SvPostgresIO.class.getName());
	static String systemSrid;

	@Override
	public String getGeomReadSQL(String fieldName) {
		return "ST_AsEWKB(" + fieldName + ")::bytea";
		// SDO_UTIL.TO_WKBGEOMETRY(geometry IN SDO_GEOMETRY)
	}

	@Override
	public String getGeomWriteSQL() {
		return "ST_SetSRID(ST_GeomFromWKB(?)," + systemSrid + ")";
	}

	@Override
	public String getBBoxSQL(String geomName) {
		return geomName + " && ST_MakeEnvelope(?,?, ?, ?, " + systemSrid + ")";
	}

	@Override
	public Class<?> getTimeStampClass() {
		return java.sql.Timestamp.class;
	}

	@Override
	public void prepareArrayType(Connection conn, String arrayType) {
		// no special preps are needed for postgres
		return;
	}

	@Override
	public boolean getOverrideInsertRepo() {
		// Postgres is the default implementation so we don't need to override
		// the repo insert
		return false;
	}

	@Override
	public PreparedStatement getInsertRepoStatement(Connection conn, String defaultStatement, String schema,
			String repoName) {
		// Postgres is the default implementation so we don't need to override
		// the repo insert
		return null;
	}

	@Override
	public Object getInsertRepoStruct(Connection conn, int maxSize) {
		// Postgres is the default implementation so we don't need to override
		// the repo insert
		return null;
	}

	@Override
	public void addRepoBatch(Object insertRepoStruct, Long PKID, Long oldMetaPKID, Long objectId, Timestamp dtInsert,
			Timestamp dtDelete, Long parentId, Long objType, String objStatus, Long userId, int rowIndex) {
		// Postgres is the default implementation so we don't need to override
		// the repo insert
	}

	@Override
	public Map<Long, Long> repoSaveGetKeys(PreparedStatement repoInsert, Object insertRepoStruct) {
		// Postgres is the default implementation so we don't need to override
		// the repo insert
		return null;
	}

	@Override
	public String getHandlerType() {
		return SvDbType.POSTGRES.toString();
	}

	@Override
	public String getDbScriptDelimiter() {
		// for postgres as default database, script delimiter is semicolon
		return ";";
	}

	@Override
	public String getSQLScript(String scriptName) {
		String script = "";
		InputStream fis = null;
		try {
			fis = SvPostgresIO.class.getResourceAsStream(sqlScriptsPath + sqlScriptsPackage + scriptName);
			if (fis != null)
				script = IOUtils.toString(fis, "UTF-8");
			else
				logger.error(sqlScriptsPath + sqlScriptsPackage + scriptName + " is not available");
		} catch (IOException e) {
			logger.error("Can't read stream, disk access error maybe?", e);
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					logger.error("Can't close stream, disk access error maybe?", e);
				}
		}
		return script;
	}

	@Override
	public ResourceBundle getSQLKeyWordsBundle() {
		ResourceBundle rb = null;
		InputStream fis = null;
		try {
			fis = SvPostgresIO.class.getResourceAsStream(sqlKeywordsBundle);
			if (fis != null)
				rb = new PropertyResourceBundle(fis);
			else
				logger.error(sqlKeywordsBundle + " is not available");
		} catch (Exception e) {
			logger.error("Error loading SQL key words bundle {}", sqlKeywordsBundle, e);
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					logger.error("Can't close stream, disk access error maybe?", e);
				}
		}
		return rb;

	}

	@Override
	public void initSrid(String srid) {
		systemSrid = srid;

	}

	@Override
	public byte[] getGeometry(ResultSet resultSet, int columnIndex) throws SQLException {
		return (byte[]) resultSet.getObject(columnIndex);
	}

	@Override
	public void setGeometry(PreparedStatement preparedStatement, int position, byte[] value) throws SQLException {
		if (value == null)
			preparedStatement.setNull(position, java.sql.Types.NULL);
		else {
			preparedStatement.setObject(position, value);
		}

	}

	@Override
	public String beforeInstall(Connection conn, String schema) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String afterInstall(Connection conn, String schema) {
		// TODO Auto-generated method stub
		return null;
	}

}
