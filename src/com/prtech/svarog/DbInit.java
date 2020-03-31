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

import com.prtech.svarog.SvCore.SvAccess;
import com.prtech.svarog_common.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.IOUtils;
import org.apache.felix.main.AutoProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataField.DbFieldType;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonReader;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonWriter;

public class DbInit {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(DbInit.class);

	private static JsonObject getDefaultSDIMetadata() {
		JsonObject meta = new JsonObject();
		meta.addProperty("ShowInGeoJSONProps", true);
		return meta;

	}

	//
	private static DbDataTable getMasterExecutors() {
		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_executors");
		dbt.setDbRepoName("{MASTER_REPO}");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_EXECUTORS);
		dbt.setIsRepoTable(false);
		dbt.setLabel_code("master_repo.executors");
		dbt.setUse_cache(false);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("CATEGORY");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(100);
		dbf2.setIsNull(false);
		dbf2.setIndexName("exec_name_cat");
		dbf2.setIsUnique(true);
		dbf2.setLabel_code("master_repo.executor_category");

		// f3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("NAME");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(100);
		dbf3.setIndexName("exec_name_cat");
		dbf3.setIsUnique(true);
		dbf3.setIsNull(false);
		dbf3.setLabel_code("master_repo.executor_name");
		// f3
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("JAVA_TYPE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(100);
		dbf4.setIsNull(false);
		dbf4.setLabel_code("master_repo.executor_type");

		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("DESCRIPTION");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(300);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("master_repo.executor_description");

		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("START_DATE");
		dbf6.setDbFieldType(DbFieldType.TIMESTAMP);
		dbf6.setIsUnique(true);
		dbf6.setDbFieldSize(3);
		dbf6.setIsNull(false);
		dbf6.setLabel_code("master_repo.executor_start_date");

		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("END_DATE");
		dbf7.setDbFieldType(DbFieldType.TIMESTAMP);
		dbf7.setIsUnique(true);
		dbf7.setDbFieldSize(3);
		dbf7.setIsNull(false);
		dbf7.setLabel_code("master_repo.executor_end_date");
		// f1
		DbDataField dbf8 = new DbDataField();
		dbf8.setDbFieldName("VERSION");
		dbf8.setIsPrimaryKey(true);
		dbf8.setDbFieldType(DbFieldType.NUMERIC);
		dbf8.setDbFieldSize(3);
		dbf8.setDbFieldScale(0);
		dbf8.setIsNull(false);
		dbf8.setLabel_code("master_repo.executor_version");

		DbDataField[] dbTableFields = new DbDataField[8];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;
		dbTableFields[7] = dbf8;

		dbt.setDbTableFields(dbTableFields);

		return dbt;
	}

	// RULE ENGINE
	private static DbDataTable getMasterNotes() {
		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_notes");
		dbt.setDbRepoName("{MASTER_REPO}");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_NOTES);
		dbt.setIsRepoTable(false);
		dbt.setLabel_code("master_repo.notes");
		dbt.setUse_cache(false);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("NOTE_NAME");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(100);
		dbf2.setIsNull(false);
		dbf2.setIsUnique(true);
		dbf2.setUnique_level("PARENT");
		dbf2.setLabel_code("master_repo.note_name");

		// f3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("NOTE_TEXT");
		dbf3.setDbFieldType(DbFieldType.TEXT);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("master_repo.note_text");

		DbDataField[] dbTableFields = new DbDataField[3];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;

		dbt.setDbTableFields(dbTableFields);

		return dbt;
	}

	// RULE ENGINE
	private static DbDataTable getMasterRules() {
		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_rules");
		dbt.setDbRepoName("{MASTER_REPO}");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_RULE);
		dbt.setIsRepoTable(false);
		dbt.setCacheType("PERM");
		dbt.setLabel_code("master_repo.rule");
		dbt.setUse_cache(false);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("RULE_NAME");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(100);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("master_repo.rule_name");

		// f3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("IS_STOPPABLE");
		dbf3.setDbFieldType(DbFieldType.BOOLEAN);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("master_repo.is_stoppable");

		// f4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("IS_TRANSACTIONAL");
		dbf4.setDbFieldType(DbFieldType.BOOLEAN);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("master_repo.is_transactional");

		// f5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("IS_ROLLING_BACK");
		dbf5.setDbFieldType(DbFieldType.BOOLEAN);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("master_repo.is_rolling_back");

		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("RULE_LABEL");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(100);
		dbf6.setIsNull(false);
		dbf6.setLabel_code("master_repo.rule_label");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf6;
		dbTableFields[3] = dbf3;
		dbTableFields[4] = dbf4;
		dbTableFields[5] = dbf5;

		dbt.setDbTableFields(dbTableFields);

		return dbt;
	}

	private static DbDataTable getMasterActions() {

		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_actions");
		dbt.setDbRepoName("{MASTER_REPO}");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_ACTION);
		dbt.setIsRepoTable(false);
		dbt.setCacheType("PERM");
		dbt.setLabel_code("master_repo.action");
		dbt.setUse_cache(false);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("ACTION_NAME");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(100);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("master_repo.action_name");

		// f3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("ACTION_TYPE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(50);
		dbf3.setIsNull(false);
		dbf3.setLabel_code("master_repo.action_type");

		// f4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("CODE_TYPE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(50);
		dbf4.setIsNull(false);
		dbf4.setLabel_code("master_repo.code_type");

		// f5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("CODE_SUBTYPE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(50);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("master_repo.code_subtype");

		// f6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("RETURN_TYPE");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(50);
		dbf6.setIsNull(false);
		dbf6.setLabel_code("master_repo.return_type");

		// f7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("CLASS_NAME");
		dbf7.setDbFieldType(DbFieldType.NVARCHAR);
		dbf7.setDbFieldSize(50);
		dbf7.setIsNull(true);
		dbf7.setLabel_code("master_repo.class_name");

		// f8
		DbDataField dbf8 = new DbDataField();
		dbf8.setDbFieldName("METHOD_NAME");
		dbf8.setDbFieldType(DbFieldType.NVARCHAR);
		dbf8.setDbFieldSize(50);
		dbf8.setIsNull(true);
		dbf8.setLabel_code("master_repo.method_name");

		// f9
		DbDataField dbf9 = new DbDataField();
		dbf9.setDbFieldName("SORT_ORDER");
		dbf9.setDbFieldType(DbFieldType.NUMERIC);
		dbf9.setDbFieldSize(9);
		dbf9.setIsNull(false);
		dbf9.setLabel_code("master_repo.sort_order");

		// f10
		DbDataField dbf10 = new DbDataField();
		dbf10.setDbFieldName("ACTION_LABEL");
		dbf10.setDbFieldType(DbFieldType.NVARCHAR);
		dbf10.setDbFieldSize(100);
		dbf10.setIsNull(false);
		dbf10.setLabel_code("master_repo.action_label");

		DbDataField[] dbTableFields = new DbDataField[10];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf10;
		dbTableFields[3] = dbf3;
		dbTableFields[4] = dbf4;
		dbTableFields[5] = dbf5;
		dbTableFields[6] = dbf6;
		dbTableFields[7] = dbf7;
		dbTableFields[8] = dbf8;
		dbTableFields[9] = dbf9;

		dbt.setDbTableFields(dbTableFields);

		return dbt;
	}

	private static DbDataTable getMasterExecutions() {

		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_executions");
		dbt.setDbRepoName("{MASTER_REPO}_re");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_EXECUTION);
		dbt.setIsRepoTable(false);
		dbt.setLabel_code("master_repo.execution");
		dbt.setUse_cache(false);
		dbt.setIsConfigTable(false);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("IS_SUCCESSFUL");
		dbf2.setDbFieldType(DbFieldType.BOOLEAN);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("master_repo.is_successful");

		// f2
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("OBJ_EXEC_ON");
		dbf3.setDbFieldType(DbFieldType.NUMERIC);
		dbf3.setDbFieldSize(18);
		dbf3.setDbFieldScale(0);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("master_repo.object_exec_on");

		// f3
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("EXEC_STATE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(20);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("master_repo.exec_state");

		DbDataField[] dbTableFields = new DbDataField[4];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;

		dbt.setDbTableFields(dbTableFields);

		return dbt;
	}

	private static DbDataTable getMasterResults() {
		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_results");
		dbt.setDbRepoName("{MASTER_REPO}_re");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_RESULT);
		dbt.setIsRepoTable(false);
		dbt.setLabel_code("master_repo.result");
		dbt.setUse_cache(false);
		dbt.setIsConfigTable(false);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("ACTION_ID");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("master_repo.action_id");

		// f3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("IS_SUCCESSFUL");
		dbf3.setDbFieldType(DbFieldType.BOOLEAN);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("master_repo.is_successful");

		// f4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("RESULT");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("master_repo.result");

		// f5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("ERRORS");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("master_repo.errors");

		// f6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("EXEC_STATE");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(20);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("master_repo.exec_state");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbt.setDbTableFields(dbTableFields);

		return dbt;
	}

	// SUPPORT_TYPE_FIELD_TYPE
	private static DbDataTable createFormFieldType() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_form_field_type");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
		dbe.setLabel_code("FIELD_TYPE");
		dbe.setUse_cache(false);
		dbe.setIsConfigTable(true);
		dbe.setConfigColumnName("LABEL_CODE");

		// Column 1N
		DbDataField dbe1 = new DbDataField();
		dbe1.setDbFieldName("PKID");
		dbe1.setIsPrimaryKey(true);
		dbe1.setDbFieldType(DbFieldType.NUMERIC);
		dbe1.setDbFieldSize(18);
		dbe1.setDbFieldScale(0);
		dbe1.setIsNull(false);
		dbe1.setLabel_code("field.pkid");

		// Column 2
		DbDataField dbe2 = new DbDataField();
		dbe2.setDbFieldName("LABEL_CODE");
		dbe2.setDbFieldType(DbFieldType.NVARCHAR);
		dbe2.setDbFieldSize(100);
		dbe2.setIsNull(false);
		dbe2.setIsUnique(true);
		dbe2.setIndexName("FORMFIELD_LABEL_IDX");
		dbe2.setLabel_code("field.description");

		// Column 3
		DbDataField dbe3 = new DbDataField();
		dbe3.setDbFieldName("FIELD_TYPE");
		dbe3.setDbFieldType(DbFieldType.NVARCHAR);
		dbe3.setDbFieldScale(0);
		dbe3.setDbFieldSize(30);
		dbe3.setIsNull(false);
		dbe3.setCode_list_user_code("FIELD_TYPES");
		dbe3.setLabel_code("field.value_type");

		// Column 4
		DbDataField dbe4 = new DbDataField();
		dbe4.setDbFieldName("CODE_LIST_ID");
		dbe4.setDbFieldType(DbFieldType.NUMERIC);
		dbe4.setDbFieldSize(18);
		dbe4.setLabel_code("field.code_list_id");

		DbDataField dbe7 = new DbDataField();
		dbe7.setDbFieldName("ACL_OBJECT_ID");
		dbe7.setDbFieldType(DbFieldType.NUMERIC);
		dbe7.setDbFieldSize(18);
		dbe7.setLabel_code("form_type.access_group_over_type");

		// f6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("IS_NULL");
		dbf6.setDbFieldType(DbFieldType.BOOLEAN);
		dbf6.setIsNull(false);
		dbf6.setLabel_code("form_type.field_isnull");

		// f7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("IS_UNIQUE");
		dbf7.setDbFieldType(DbFieldType.BOOLEAN);
		dbf7.setIsNull(true);
		dbf7.setLabel_code("form_type.field_isunique");

		DbDataField dbf7_1 = new DbDataField();
		dbf7_1.setDbFieldName("FIELD_SIZE");
		dbf7_1.setDbFieldType(DbFieldType.NUMERIC);
		dbf7_1.setDbFieldSize(18);
		dbf7_1.setIsNull(true);
		dbf7_1.setLabel_code("form_type.field_size");

		DbDataField dbf12 = new DbDataField();
		dbf12.setDbFieldName("GUI_METADATA");
		dbf12.setDbFieldType(DbFieldType.NVARCHAR);
		dbf12.setDbFieldSize(2000);
		dbf12.setIsNull(true);
		dbf12.setLabel_code("master_repo.is_visible_ui");

		DbDataField dbf14 = new DbDataField();
		dbf14.setDbFieldName("SORT_ORDER");
		dbf14.setDbFieldType(DbFieldType.NUMERIC);
		dbf14.setDbFieldSize(18);
		dbf14.setDbFieldScale(0);
		dbf14.setIsNull(true);
		dbf14.setLabel_code("master_repo.field_sort_order");

		DbDataField[] dbTableFields = new DbDataField[10];
		dbTableFields[0] = dbe1;
		dbTableFields[1] = dbe2;
		dbTableFields[2] = dbe3;
		dbTableFields[3] = dbe4;

		dbTableFields[4] = dbe7;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;
		dbTableFields[7] = dbf12;
		dbTableFields[8] = dbf14;
		dbTableFields[9] = dbf7_1;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// SUPPORT_TYPE_FIELD_TYPE
	private static DbDataTable createFormField() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_form_field");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_FORM_FIELD);
		dbe.setLabel_code("FIELD_TYPE");
		dbe.setUse_cache(false);
		dbe.setParent_id(svCONST.OBJECT_TYPE_FORM);

		// Column 1N
		DbDataField dbe1 = new DbDataField();
		dbe1.setDbFieldName("PKID");
		dbe1.setDbFieldType(DbFieldType.NUMERIC);
		dbe1.setDbFieldSize(18);
		dbe1.setDbFieldScale(0);
		dbe1.setIsNull(false);
		dbe1.setIsPrimaryKey(true);
		dbe1.setLabel_code("master_repo.table_meta_pkid");

		// Column 1N
		DbDataField dbe1_0 = new DbDataField();
		dbe1_0.setDbFieldName("FORM_OBJECT_ID");
		dbe1_0.setDbFieldType(DbFieldType.NUMERIC);
		dbe1_0.setDbFieldSize(18);
		dbe1_0.setDbFieldScale(0);
		dbe1_0.setIsNull(false);
		dbe1_0.setIndexName("FOID_IDX");
		// dbe1_0.setIsPrimaryKey(true);
		dbe1_0.setLabel_code("form.field_type_id");

		// Column 1N
		DbDataField dbe1_1 = new DbDataField();
		dbe1_1.setDbFieldName("FIELD_TYPE_ID");
		dbe1_1.setDbFieldType(DbFieldType.NUMERIC);
		// dbe1_1.setIsPrimaryKey(true);
		dbe1_1.setDbFieldSize(18);
		dbe1_1.setDbFieldScale(0);
		dbe1_1.setIsNull(false);
		dbe1_1.setIndexName("FIELD_TYPE_ID_IDX");
		dbe1_1.setLabel_code("form.field_type_id");

		// Column 5
		DbDataField dbe5 = new DbDataField();
		dbe5.setDbFieldName("VALUE");
		dbe5.setDbFieldType(DbFieldType.NVARCHAR);
		dbe5.setDbFieldSize(200);
		dbe5.setLabel_code("form.registered_value");

		// Column 6
		DbDataField dbe6 = new DbDataField();
		dbe6.setDbFieldName("FIRST_CHECK");
		dbe6.setDbFieldType(DbFieldType.NVARCHAR);
		dbe6.setDbFieldSize(200);
		dbe6.setLabel_code("field.first_check");

		// Column 6
		DbDataField dbe7 = new DbDataField();
		dbe7.setDbFieldName("SECOND_CHECK");
		dbe7.setDbFieldType(DbFieldType.NVARCHAR);
		dbe7.setDbFieldSize(200);
		dbe7.setLabel_code("field.second_check");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbe1;
		dbTableFields[1] = dbe1_0;
		dbTableFields[2] = dbe1_1;
		dbTableFields[3] = dbe5;
		dbTableFields[4] = dbe6;
		dbTableFields[5] = dbe7;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// SUPPORT_TYPE_DOCUMENT
	private static DbDataTable createMasterFormType() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_form_type");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_FORM_TYPE);
		dbe.setLabel_code("master_repo.form_types");
		dbe.setUse_cache(true);
		dbe.setCacheType("PERM");
		dbe.setIsConfigTable(true);
		dbe.setConfigColumnName("LABEL_CODE");

		// Column 1N
		DbDataField dbe1 = new DbDataField();
		dbe1.setDbFieldName("PKID");
		dbe1.setIsPrimaryKey(true);
		dbe1.setDbFieldType(DbFieldType.NUMERIC);
		dbe1.setDbFieldSize(18);
		dbe1.setDbFieldScale(0);
		dbe1.setIsNull(false);
		dbe1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbe2 = new DbDataField();
		dbe2.setDbFieldName("LABEL_CODE");
		dbe2.setDbFieldType(DbFieldType.NVARCHAR);
		dbe2.setIsNull(false);
		dbe2.setDbFieldScale(0);
		dbe2.setDbFieldSize(100);
		dbe2.setIsUnique(true);
		dbe2.setUnique_level("TABLE");
		dbe2.setLabel_code("form_type.short_description");

		// Column 3
		DbDataField dbe4 = new DbDataField();
		dbe4.setDbFieldName("FORM_CATEGORY");
		dbe4.setDbFieldType(DbFieldType.NVARCHAR);
		dbe4.setDbFieldScale(0);
		dbe4.setDbFieldSize(50);
		dbe4.setIsNull(false);
		dbe4.setCode_list_user_code("FORM_CATEGORY");
		dbe4.setLabel_code("form_type.category");

		// Column 4
		DbDataField dbe5 = new DbDataField();
		dbe5.setDbFieldName("MULTI_ENTRY");
		dbe5.setDbFieldType(DbFieldType.BOOLEAN);
		dbe5.setLabel_code("form_type.multiple_entries");
		dbe5.setIsNull(false);

		// Column 4
		DbDataField dbe5_0 = new DbDataField();
		dbe5_0.setDbFieldName("AUTOINSTANCE_SINGLE");
		dbe5_0.setDbFieldType(DbFieldType.BOOLEAN);
		dbe5_0.setLabel_code("form_type.autoinstance_singleentry");
		dbe5_0.setIsNull(false);

		// Column 4
		DbDataField dbe5_1 = new DbDataField();
		dbe5_1.setDbFieldName("MANDATORY_BASE_VALUE");
		dbe5_1.setDbFieldType(DbFieldType.BOOLEAN);
		dbe5_1.setLabel_code("form_type.mandatory_base_value");
		dbe5_1.setIsNull(false);

		DbDataField dbe6 = new DbDataField();
		dbe6.setDbFieldName("SORT_ORDER");
		dbe6.setDbFieldType(DbFieldType.NUMERIC);
		dbe6.setDbFieldSize(18);
		dbe5.setIsNull(true);
		dbe6.setLabel_code("form_type.sort_order");

		DbDataField dbe7 = new DbDataField();
		dbe7.setDbFieldName("ACL_OBJECT_ID");
		dbe7.setDbFieldType(DbFieldType.NUMERIC);
		dbe7.setDbFieldSize(18);
		dbe7.setLabel_code("form_type.access_group_over_type");

		DbDataField dbe8 = new DbDataField();
		dbe8.setDbFieldName("ACL_VAL_OBJECT_ID");
		dbe8.setDbFieldType(DbFieldType.NUMERIC);
		dbe8.setDbFieldSize(18);
		dbe8.setLabel_code("form_type.access_group_over_val");

		DbDataField dbe9 = new DbDataField();
		dbe9.setDbFieldName("ACL_1CHK_OBJECT_ID");
		dbe9.setDbFieldType(DbFieldType.NUMERIC);
		dbe9.setDbFieldSize(18);
		dbe9.setLabel_code("form_type.access_group_over_1st_check");

		DbDataField dbe10 = new DbDataField();
		dbe10.setDbFieldName("ACL_2CHK_OBJECT_ID");
		dbe10.setDbFieldType(DbFieldType.NUMERIC);
		dbe10.setDbFieldSize(18);
		dbe10.setLabel_code("form_type.access_group_over_2nd_check");

		DbDataField dbf11 = new DbDataField();
		dbf11.setDbFieldName("GUI_METADATA");
		dbf11.setDbFieldType(DbFieldType.NVARCHAR);
		dbf11.setDbFieldSize(2000);
		dbf11.setIsNull(true);
		dbf11.setLabel_code("master_repo.is_visible_ui");

		DbDataField dbe15 = new DbDataField();
		dbe15.setDbFieldName("MAX_INSTANCES");
		dbe15.setDbFieldType(DbFieldType.NUMERIC);
		dbe15.setDbFieldSize(18);
		dbe15.setLabel_code("form_type.maximum_number_of_entries");
		dbe15.setIsNull(true);

		DbDataField[] dbTableFields = new DbDataField[13];
		dbTableFields[0] = dbe1;
		dbTableFields[1] = dbe2;
		dbTableFields[2] = dbe4;
		dbTableFields[3] = dbe5;
		dbTableFields[4] = dbe5_0;
		dbTableFields[5] = dbe5_1;
		dbTableFields[6] = dbe6;
		dbTableFields[7] = dbe7;
		dbTableFields[8] = dbe8;
		dbTableFields[9] = dbe9;
		dbTableFields[10] = dbe10;
		dbTableFields[11] = dbf11;
		dbTableFields[12] = dbe15;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// SUPPORT_TYPE_DOCUMENT_CLAIM
	private static DbDataTable createMasterForm() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_form");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_FORM);
		dbe.setLabel_code("master_repo.form");
		dbe.setUse_cache(false);
		// make sure we set the configuration table info
		dbe.setConfigTypeName("{REPO_TABLE_NAME}_form_type");
		dbe.setConfigRelationType("FIELD");
		dbe.setConfigRelatedTypeName("FORM_TYPE_ID");

		// Column 1N
		DbDataField dbe1 = new DbDataField();
		dbe1.setDbFieldName("PKID");
		dbe1.setIsPrimaryKey(true);
		dbe1.setDbFieldType(DbFieldType.NUMERIC);
		dbe1.setDbFieldSize(18);
		dbe1.setDbFieldScale(0);
		dbe1.setIsNull(false);
		dbe1.setLabel_code("master_repo.table_meta_pkid");

		// Column 1N
		DbDataField dbe1_1 = new DbDataField();
		dbe1_1.setDbFieldName("FORM_TYPE_ID");
		dbe1_1.setDbFieldType(DbFieldType.NUMERIC);
		dbe1_1.setDbFieldSize(18);
		dbe1_1.setDbFieldScale(0);
		dbe1_1.setIsNull(false);
		dbe1_1.setIndexName("FORM_TYPE_ID_IDX");
		dbe1_1.setLabel_code("form.form_type_id");

		// Column 1N
		DbDataField dbe1_2 = new DbDataField();
		dbe1_2.setDbFieldName("FORM_VALIDATION");
		dbe1_2.setDbFieldType(DbFieldType.BOOLEAN);
		dbe1_2.setIsNull(false);
		dbe1_2.setLabel_code("form.form_validation");

		// Column 5
		DbDataField dbe5 = new DbDataField();
		dbe5.setDbFieldName("VALUE");
		dbe5.setDbFieldType(DbFieldType.NUMERIC);
		dbe5.setDbFieldSize(18);
		dbe5.setDbFieldScale(0);
		dbe5.setLabel_code("form.registered_value");

		// Column 6
		DbDataField dbe6 = new DbDataField();
		dbe6.setDbFieldName("FIRST_CHECK");
		dbe6.setDbFieldType(DbFieldType.NUMERIC);
		dbe6.setDbFieldSize(18);
		dbe6.setDbFieldScale(0);
		dbe6.setLabel_code("form.first_check");

		// Column 6
		DbDataField dbe7 = new DbDataField();
		dbe7.setDbFieldName("SECOND_CHECK");
		dbe7.setDbFieldType(DbFieldType.NUMERIC);
		dbe7.setDbFieldSize(18);
		dbe7.setDbFieldScale(0);
		dbe7.setLabel_code("form.second_check");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbe1;
		dbTableFields[1] = dbe1_1;
		dbTableFields[2] = dbe1_2;
		dbTableFields[3] = dbe5;
		dbTableFields[4] = dbe6;
		dbTableFields[5] = dbe7;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// JOB_TYPE
	private static DbDataTable createJobType() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_job_type");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_JOB_TYPE);
		dbe.setLabel_code("master_repo.job_type");
		dbe.setUse_cache(false);
		dbe.setIsConfigTable(true);
		dbe.setConfigColumnName("LABEL_CODE");

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("LABEL_CODE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(100);
		dbf2.setIsNull(false);
		dbf2.setIsUnique(true);
		dbf2.setUnique_level("TABLE");
		dbf2.setLabel_code("job_type.label_code");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("JOB_TYPE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(100);
		dbf3.setIsNull(false);
		dbf3.setCode_list_user_code("JOB_TYPE");
		dbf3.setLabel_code("job_type.job_type");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("QUERY");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("job_type.query");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("EXECUTION_TYPE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setCode_list_user_code("JOB_EXEC_TYPE");
		dbf5.setLabel_code("job_type.exec_type");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("NOTE");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(2000);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("job_type.note");

		// TODO rolled back so Stribog 1.0 can work
		// Column 7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("PARAMS");
		dbf7.setDbFieldType(DbFieldType.NVARCHAR);
		dbf7.setDbFieldSize(2000);
		dbf7.setIsNull(true);
		dbf7.setLabel_code("job_type.params");

		DbDataField[] dbTableFields = new DbDataField[7];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// TASK_TYPE
	private static DbDataTable createTaskType() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_task_type");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_TASK_TYPE);
		dbe.setLabel_code("master_repo.task_type");
		dbe.setUse_cache(false);
		dbe.setIsConfigTable(true);
		dbe.setConfigColumnName("LABEL_CODE");

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("LABEL_CODE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(200);
		dbf2.setIsNull(false);
		dbf2.setIsUnique(true);
		dbf2.setUnique_level("TABLE");
		dbf2.setLabel_code("task_type.label_code");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("TASK_OBJECT_TYPE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(100);
		dbf3.setIsNull(false);
		dbf3.setCode_list_user_code("TASK_TYPE");
		dbf3.setLabel_code("task_type.task_object_type");

		// TODO rolled back so Stribog 1.0 can work
		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("PARAMS");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("task_type.params");

		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("JAVA_CLASS");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(500);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("task_type.java_class");

		DbDataField[] dbTableFields = new DbDataField[5];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// PARAM_TYPE
	private static DbDataTable createParamType() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_param_type");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_PARAM_TYPE);
		dbe.setLabel_code("master_repo.param_type");
		dbe.setUse_cache(true);
		dbe.setCacheType("PERM");
		dbe.setIsConfigTable(true);
		dbe.setConfigColumnName("LABEL_CODE");
		dbe.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("LABEL_CODE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(200);
		dbf2.setIsNull(false);
		dbf2.setIsUnique(true);
		dbf2.setUnique_level("TABLE");
		dbf2.setLabel_code("master_repo.label_code");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("DATA_TYPE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(100);
		dbf3.setIsNull(false);
		dbf3.setCode_list_user_code("DATA_TYPE");
		dbf3.setLabel_code("param_type.param_data_type");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("INPUT_TYPE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(100);
		dbf4.setIsNull(false);
		dbf4.setCode_list_user_code("INPUT_TYPE");
		dbf4.setLabel_code("param_type.param_input_type");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("DEFAULT_VALUE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("master_repo.default_value");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("CODE_LIST_ID");
		dbf6.setDbFieldType(DbFieldType.NUMERIC);
		dbf6.setDbFieldSize(18);
		dbf6.setDbFieldScale(0);
		dbf6.setIsNull(true);
		// dbf6.setCode_list_user_code("CODE_LIST");
		dbf6.setLabel_code("param_type.code_list");

		// Column 7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("IS_MULTI_VALUE_ENTRY");
		dbf7.setDbFieldType(DbFieldType.BOOLEAN);
		dbf7.setIsNull(true);
		dbf7.setLabel_code("param_type.is_multi_value_of_entry");

		// Column 8
		DbDataField dbf8 = new DbDataField();
		dbf8.setDbFieldName("GUI_METADATA");
		dbf8.setDbFieldType(DbFieldType.NVARCHAR);
		dbf8.setDbFieldSize(2000);
		dbf8.setIsNull(true);
		dbf8.setLabel_code("param_type.gui_metadata");

		// Column 9
		DbDataField dbf9 = new DbDataField();
		dbf9.setDbFieldName("PARAM_ORDER");
		dbf9.setDbFieldType(DbFieldType.NUMERIC);
		dbf9.setDbFieldSize(18);
		dbf9.setIsNull(true);
		dbf9.setLabel_code("param_type.sort_order");

		// Column 10
		DbDataField dbf10 = new DbDataField();
		dbf10.setDbFieldName("IS_MANDATORY");
		dbf10.setDbFieldType(DbFieldType.BOOLEAN);
		dbf10.setIsNull(true);
		dbf10.setLabel_code("param_type.is_mandatory");

		// Column 11
		DbDataField dbf11 = new DbDataField();
		dbf11.setDbFieldName("CRIT_FIELD");
		dbf11.setDbFieldType(DbFieldType.NVARCHAR);
		dbf11.setDbFieldSize(500);
		dbf11.setIsNull(true);
		dbf11.setLabel_code("param_type.crit_field");

		DbDataField[] dbTableFields = new DbDataField[11];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;
		dbTableFields[7] = dbf8;
		dbTableFields[8] = dbf10;
		dbTableFields[9] = dbf11;
		dbTableFields[10] = dbf9;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// PARAM
	private static DbDataTable createParam() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_param");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_PARAM);
		dbe.setLabel_code("master_repo.param");
		// dbe.setParentId(svCONST.OBJECT_TYPE_JOB);
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("PARAM_TYPE_ID");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("param_object.param_type_id");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("NOTE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("param_object.note");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("PARAM_ORDER");
		dbf4.setDbFieldType(DbFieldType.NUMERIC);
		dbf4.setDbFieldSize(18);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("param_type.sort_order");

		DbDataField[] dbTableFields = new DbDataField[4];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// PARAM_VALUE
	private static DbDataTable createParamValue() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_param_value");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_PARAM_VALUE);
		dbe.setLabel_code("master_repo.param_value");
		// dbe.setParentId(svCONST.OBJECT_TYPE_PARAM);
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("CODE_LIST_OBJ_ID");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("param_value.value");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("VALUE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("param_value.value");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("NOTE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("param_value.note");

		DbDataField[] dbTableFields = new DbDataField[4];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// JOB
	private static DbDataTable createJobTask() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_job_task");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_JOB_TASK);
		dbe.setLabel_code("master_repo.job_task");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("TASK_TYPE");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("job_task.task_type");

		// TODO rolled back so Stribog 1.0 can work
		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("PARAMS");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("job_task.params");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("EXEC_ORDER");
		dbf4.setDbFieldType(DbFieldType.NUMERIC);
		dbf4.setDbFieldSize(18);
		dbf4.setDbFieldScale(0);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("job_task.exec_order");
		dbf4.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("NOTE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("job_task.note");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("IS_DEPENDENT");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(10);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("job_task.is_dependent");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// JOB
	private static DbDataTable createJob() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_job");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_JOB);
		dbe.setLabel_code("master_repo.job");
		dbe.setParent_id(svCONST.OBJECT_TYPE_JOB_TASK);
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("TITLE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(200);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("job.job_title");

		// TODO rolled back so Stribog 1.0 can work
		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("PARAMS");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("job.params");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("JOB_OBJECT_TYPE");
		dbf4.setDbFieldType(DbFieldType.NUMERIC);
		dbf4.setDbFieldSize(18);
		dbf4.setDbFieldScale(0);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("job.job_object_type");
		dbf4.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("NOTE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("job.note");

		DbDataField[] dbTableFields = new DbDataField[5];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// JOB_OBJECT
	private static DbDataTable createJobObject() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_job_object");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_JOB_OBJECT);
		dbe.setLabel_code("master_repo.job_object");
		dbe.setParent_id(svCONST.OBJECT_TYPE_JOB);
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("SVAROG_OBJ_ID");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("job_object.svarog_obj_id");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("RESULT");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("job_object.result");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("NOTE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("job_object.note");

		DbDataField[] dbTableFields = new DbDataField[4];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// TASK
	private static DbDataTable createTask() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_task");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_TASK);
		dbe.setLabel_code("master_repo.task");
		dbe.setParent_id(svCONST.OBJECT_TYPE_JOB_TASK);
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("TASK_OBJ_ID");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("task.task_obj_id");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("RESULT");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(false);
		dbf3.setLabel_code("task.result");

		// TODO rolled back so Stribog 1.0 can work
		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("PARAMS");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("task.params");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("NOTE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("task.note");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("DEPENDENT_TASK_ID");
		dbf6.setDbFieldType(DbFieldType.NUMERIC);
		dbf6.setDbFieldSize(18);
		dbf6.setDbFieldScale(0);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("task.dependent_task_id");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// TASK_DETAIL
	private static DbDataTable createTaskDetail() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_task_detail");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_TASK_DETAIL);
		dbe.setLabel_code("master_repo.task_detail");
		dbe.setParent_id(svCONST.OBJECT_TYPE_TASK);
		dbe.setUse_cache(false);

		// Column 1N
		DbDataField dbf0 = new DbDataField();
		dbf0.setDbFieldName("PKID");
		dbf0.setIsPrimaryKey(true);
		dbf0.setDbFieldType(DbFieldType.NUMERIC);
		dbf0.setDbFieldSize(18);
		dbf0.setDbFieldScale(0);
		dbf0.setIsNull(false);
		dbf0.setLabel_code("master_repo.table_meta_pkid");

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("RESULT_TYPE");
		dbf1.setDbFieldType(DbFieldType.NVARCHAR);
		dbf1.setDbFieldSize(500);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("task_details.result_type");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("RESULT");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(2000);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("task_details.result");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("NOTE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(2000);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("task_details.note");

		DbDataField[] dbTableFields = new DbDataField[4];
		dbTableFields[0] = dbf0;
		dbTableFields[1] = dbf1;
		dbTableFields[2] = dbf2;
		dbTableFields[3] = dbf3;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// RENDER_ENGINE object
	private static DbDataTable createRenderEngine() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_render_engine");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_RENDER_ENGINE);
		dbe.setLabel_code("master_repo.render_engine");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("NAME");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("render_engine.name");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("LABEL_CODE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(200);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("render_engine.label_code");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("VERSION");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(50);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("render_engine.version");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("CODE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(200);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("render_engine.code");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("NOTE");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(2000);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("render_engine.note");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// UI_STRUCTURE_TYPE
	private static DbDataTable createUIStructureType() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_ui_struct_type");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_UI_STRUCTURE_TYPE);
		dbe.setLabel_code("master_repo.ui_struct_type");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("ui_struct_type.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("NAME");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("ui_struct_type.name");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("LABEL_CODE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(200);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("ui_struct_type.label_code");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("CAN_SAVE");
		dbf4.setDbFieldType(DbFieldType.BOOLEAN);
		dbf4.setDbFieldSize(18);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("ui_struct_type.can_save");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("RENDER_ENGINE_OBJ_ID");
		dbf5.setDbFieldType(DbFieldType.NUMERIC);
		dbf5.setDbFieldSize(18);
		dbf5.setDbFieldScale(0);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("ui_struct_type.renger_engine_obj_id");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("OUTPUT_FORMAT");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(100);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("ui_struct_type.output_format");

		// Column 7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("CATEGORY");
		dbf7.setDbFieldType(DbFieldType.NVARCHAR);
		dbf7.setDbFieldSize(100);
		dbf7.setIsNull(true);
		dbf7.setLabel_code("ui_struct_type.category");

		// Column 8
		DbDataField dbf8 = new DbDataField();
		dbf8.setDbFieldName("STRUCTURE_TYPE_ORDER");
		dbf8.setDbFieldType(DbFieldType.NUMERIC);
		dbf8.setDbFieldSize(9);
		dbf8.setIsNull(false);
		dbf8.setLabel_code("ui_struct_type.sort_order");

		DbDataField[] dbTableFields = new DbDataField[8];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;
		dbTableFields[7] = dbf8;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// UI_STRUCTURE_SOURCE
	private static DbDataTable createUIStructureSource() {

		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_ui_struct_source");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_UI_STRUCTURE_SOURCE);
		dbe.setParent_id(svCONST.OBJECT_TYPE_UI_STRUCTURE_TYPE);
		dbe.setLabel_code("master_repo.ui_struct_source");
		dbe.setUse_cache(false);
		dbe.setConfigColumnName("NAME");

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("ui_struct_source.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("NAME");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(200);
		dbf2.setIsNull(true);
		dbf2.setIsUnique(true);
		dbf2.setLabel_code("ui_struct_source.name");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("IS_MASTER");
		dbf3.setDbFieldType(DbFieldType.BOOLEAN);
		dbf3.setDbFieldSize(18);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("ui_struct_source.is_master");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("STRUCTURE_SOURCE_ORDER");
		dbf4.setDbFieldType(DbFieldType.NUMERIC);
		dbf4.setDbFieldSize(9);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("ui_struct_source.sort_order");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("NOTE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(2000);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("ui_struct_source.note");

		DbDataField[] dbTableFields = new DbDataField[5];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// CONTACT_DATA
	private static DbDataTable createContactData() {
		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_contact_data");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_CONTACT_DATA);
		dbe.setLabel_code("master_repo.contact_data");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("contact_data.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("STREET_TYPE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(20);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("contact_data.streeet_type");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("STREET_NAME");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(200);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("contact_data.street_name");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("HOUSE_NUMBER");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(50);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("contact_data.house_number");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("POSTAL_CODE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(50);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("contact_data.postal_code");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("CITY");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(200);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("contact_data.city");

		// Column 7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("STATE");
		dbf7.setDbFieldType(DbFieldType.NVARCHAR);
		dbf7.setDbFieldSize(200);
		dbf7.setIsNull(true);
		dbf7.setLabel_code("contact_data.state");

		// Column 7
		DbDataField dbf8 = new DbDataField();
		dbf8.setDbFieldName("PHONE_NUMBER");
		dbf8.setDbFieldType(DbFieldType.NVARCHAR);
		dbf8.setDbFieldSize(50);
		dbf8.setIsNull(true);
		dbf8.setLabel_code("contact_data.phone_number");

		// Column 8
		DbDataField dbf9 = new DbDataField();
		dbf9.setDbFieldName("MOBILE_NUMBER");
		dbf9.setDbFieldType(DbFieldType.NVARCHAR);
		dbf9.setDbFieldSize(50);
		dbf9.setIsNull(true);
		dbf9.setLabel_code("contact_data.mobile_number");

		// Column 9
		DbDataField dbf10 = new DbDataField();
		dbf10.setDbFieldName("FAX");
		dbf10.setDbFieldType(DbFieldType.NVARCHAR);
		dbf10.setDbFieldSize(100);
		dbf10.setIsNull(true);
		dbf10.setLabel_code("contact_data.fax");

		// Column 11
		DbDataField dbf11 = new DbDataField();
		dbf11.setDbFieldName("EMAIL");
		dbf11.setDbFieldType(DbFieldType.NVARCHAR);
		dbf11.setDbFieldSize(100);
		dbf11.setIsNull(true);
		dbf11.setLabel_code("contact_data.email");

		DbDataField[] dbTableFields = new DbDataField[11];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;
		dbTableFields[7] = dbf8;
		dbTableFields[8] = dbf9;
		dbTableFields[9] = dbf10;
		dbTableFields[10] = dbf11;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// EVENT
	private static DbDataTable createEvent() {
		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_event");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_EVENT);
		dbe.setLabel_code("master_repo.event");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("event.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("CATEGORY");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(50);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("event.category");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("TITLE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(250);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("event.title");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("DESCRIPTION");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("event.description");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("DT_START");
		dbf5.setDbFieldType(DbFieldType.TIMESTAMP);
		dbf5.setDbFieldSize(3);
		dbf5.setIsNull(false);
		dbf5.setLabel_code("event.dt_start");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("DT_END");
		dbf6.setDbFieldType(DbFieldType.TIMESTAMP);
		dbf6.setDbFieldSize(3);
		dbf6.setIsNull(false);
		dbf6.setLabel_code("event.dt_end");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// NOTIFICATION
	private static DbDataTable createNotification() {
		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_notification");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_NOTIFICATION);
		dbe.setLabel_code("master_repo.notification");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("notification.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("TYPE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(50);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("notification.type");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("TITLE");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(250);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("notification.title");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("MESSAGE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("notification.message");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("SENDER");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(50);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("notification.sender");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("EVENT_ID");
		dbf6.setDbFieldType(DbFieldType.NUMERIC);
		dbf6.setDbFieldSize(18);
		dbf6.setDbFieldScale(0);
		dbf5.setIsNull(true);
		dbf6.setLabel_code("notification.event_id");

		DbDataField[] dbTableFields = new DbDataField[6];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	// message and conversation / tracker
	private static DbDataTable createConversation() {
		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_conversation");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_CONVERSATION);
		dbe.setLabel_code("master_repo.conversation");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("conversation.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("ID");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setDbFieldScale(0);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("conversation.id");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("MODULE_NAME");
		dbf3.setDbFieldType(DbFieldType.NVARCHAR);
		dbf3.setDbFieldSize(70);
		dbf3.setCode_list_user_code("MODULES");
		dbf3.setIsNull(true);
		dbf3.setLabel_code("conversation.module_name");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("CATEGORY");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(70);
		dbf4.setCode_list_user_code("LIST_CATEGORIES");
		dbf4.setIsNull(true);
		dbf4.setLabel_code("conversation.category");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("TITLE");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(250);
		dbf5.setIsNull(false);
		dbf5.setLabel_code("conversation.title");

		// Column 6
		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("PRIORITY");
		dbf6.setDbFieldType(DbFieldType.NVARCHAR);
		dbf6.setDbFieldSize(50);
		dbf6.setCode_list_user_code("LIST_PRIORITY");
		dbf6.setIsNull(false);
		dbf6.setLabel_code("conversation.priority");

		// Column 7
		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("CREATED_BY");
		dbf7.setDbFieldType(DbFieldType.NUMERIC);
		dbf7.setDbFieldSize(18);
		dbf7.setIsNull(false);
		dbf7.setLabel_code("conversation.created_by");
		dbf7.setGui_metadata(
				"{'editoptions':{'readonly':true},'width':72,'react':{'filterable':true,'width':120,'visible':false,'resizable':true,'editable':false,'uischema':{'ui:readonly':true}}}");

		// Column 8
		DbDataField dbf8 = new DbDataField();
		dbf8.setDbFieldName("ASSIGNED_TO");
		dbf8.setDbFieldType(DbFieldType.NUMERIC);
		dbf8.setDbFieldSize(18);
		dbf8.setIsNull(false);
		dbf8.setLabel_code("conversation.assigned_to");
		dbf8.setGui_metadata(
				"{'editoptions':{'readonly':true},'width':72,'react':{'filterable':true,'width':120,'visible':false,'resizable':true,'editable':false,'uischema':{'ui:readonly':true}}}");

		// Column 9
		DbDataField dbf9 = new DbDataField();
		dbf9.setDbFieldName("CREATED_BY_USERNAME");
		dbf9.setDbFieldType(DbFieldType.NVARCHAR);
		dbf9.setDbFieldSize(100);
		dbf9.setIsNull(false);
		dbf9.setLabel_code("conversation.created_by_username");

		// Column 10
		DbDataField dbf10 = new DbDataField();
		dbf10.setDbFieldName("ASSIGNED_TO_USERNAME");
		dbf10.setDbFieldType(DbFieldType.NVARCHAR);
		dbf10.setDbFieldSize(100);
		dbf10.setIsNull(false);
		dbf10.setLabel_code("conversation.assigned_to_username");

		// Column 11
		DbDataField dbf11 = new DbDataField();
		dbf11.setDbFieldName("IS_READ");
		dbf11.setDbFieldType(DbFieldType.BOOLEAN);
		dbf11.setIsNull(false);
		dbf11.setLabel_code("conversation.is_read");

		// Column 12
		DbDataField dbf12 = new DbDataField();
		dbf12.setDbFieldName("CONVERSATION_STATUS");
		dbf12.setDbFieldType(DbFieldType.NVARCHAR);
		dbf12.setCode_list_user_code("CONVERSATION_STATUS");
		dbf12.setDbFieldSize(50);
		dbf12.setIsNull(true);
		dbf12.setLabel_code("conversation.status");

		// Column 13
		DbDataField dbf13 = new DbDataField();
		dbf13.setDbFieldName("CONTACT_INFO");
		dbf13.setDbFieldType(DbFieldType.NVARCHAR);
		dbf13.setDbFieldSize(70);
		dbf13.setIsNull(true);
		dbf13.setLabel_code("conversation.contact");
		dbf13.setGui_metadata(
				"{'editoptions':{'readonly':true},'width':72,'react':{'filterable':true,'visible':false,'resizable':true,'editable':false,'uischema':{'ui:readonly':false}}}");

		DbDataField[] dbTableFields = new DbDataField[13];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;
		dbTableFields[5] = dbf6;
		dbTableFields[6] = dbf7;
		dbTableFields[7] = dbf8;
		dbTableFields[8] = dbf9;
		dbTableFields[9] = dbf10;
		dbTableFields[10] = dbf11;
		dbTableFields[11] = dbf12;
		dbTableFields[12] = dbf13;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	private static DbDataTable createMessage() {
		DbDataTable dbe = new DbDataTable();
		dbe.setDbTableName("{REPO_TABLE_NAME}_message");
		dbe.setDbRepoName("{MASTER_REPO}");
		dbe.setDbSchema("{DEFAULT_SCHEMA}");
		dbe.setIsSystemTable(true);
		dbe.setIsRepoTable(false);
		dbe.setObjectId(svCONST.OBJECT_TYPE_MESSAGE);
		dbe.setLabel_code("master_repo.message");
		dbe.setUse_cache(false);

		// Column 1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("message.table_meta_pkid");

		// Column 2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("CREATED_BY");
		dbf2.setDbFieldType(DbFieldType.NUMERIC);
		dbf2.setDbFieldSize(18);
		dbf2.setIsNull(true);
		dbf2.setLabel_code("message.created_by");

		// Column 3
		DbDataField dbf3 = new DbDataField();
		dbf3.setDbFieldName("ASSIGNED_TO");
		dbf3.setDbFieldType(DbFieldType.NUMERIC);
		dbf3.setDbFieldSize(18);
		dbf3.setIsNull(true);
		dbf3.setLabel_code("message.assigned_to");

		// Column 4
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("MESSAGE_TEXT");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(2000);
		dbf4.setIsNull(true);
		dbf4.setLabel_code("message.message_text");

		// Column 5
		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("REPLY_TO");
		dbf5.setDbFieldType(DbFieldType.NUMERIC);
		dbf5.setDbFieldSize(18);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("message.reply_to");

		DbDataField[] dbTableFields = new DbDataField[5];
		dbTableFields[0] = dbf1;
		dbTableFields[1] = dbf2;
		dbTableFields[2] = dbf3;
		dbTableFields[3] = dbf4;
		dbTableFields[4] = dbf5;

		dbe.setDbTableFields(dbTableFields);
		return dbe;
	}

	private static DbDataTable getMasterLink() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_link");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_LINK);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.link");
			dbt.setUse_cache(false);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("LINK_TYPE_ID");
			dbf2.setIsUnique(true);
			dbf2.setDbFieldType(DbFieldType.NUMERIC);
			dbf2.setDbFieldSize(18);
			dbf2.setDbFieldScale(0);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.link_type");

			// f3
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("LINK_OBJ_ID_1");
			dbf3.setDbFieldType(DbFieldType.NUMERIC);
			dbf3.setDbFieldSize(18);
			dbf3.setIsNull(false);
			dbf3.setIsUnique(true);
			dbf3.setLabel_code("master_repo.link_object_id_1");

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("LINK_OBJ_ID_2");
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setIsUnique(true);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.link_object_id_2");

			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("LINK_NOTES");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(2000);
			dbf2_1.setIsNull(true);
			dbf2_1.setLabel_code("master_repo.link_notes");

			dbt.setDbTableFields(new DbDataField[5]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf4;
			dbt.getDbTableFields()[4] = dbf2_1;
			return dbt;
		}
	}

	private static DbDataTable getMasterSequence() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_sequence");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SEQUENCE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sequence");
			dbt.setUse_cache(false);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("SEQUENCE_VALUE");
			// dbf2.setIsUnique(true);
			dbf2.setDbFieldType(DbFieldType.NUMERIC);
			dbf2.setDbFieldSize(18);
			dbf2.setDbFieldScale(0);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.sequence_value");

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("SEQUENCE_DB_NAME");
			dbf4.setDbFieldType(DbFieldType.NVARCHAR);
			dbf4.setDbFieldSize(100);
			dbf4.setIsNull(true);
			dbf4.setLabel_code("master_repo.sequence_db_name");

			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("SEQUENCE_KEY");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(60);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.sequence_key");
			dbf2_1.setIsUnique(true);

			dbt.setDbTableFields(new DbDataField[4]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2_1;
			dbt.getDbTableFields()[2] = dbf2;
			dbt.getDbTableFields()[3] = dbf4;
			return dbt;
		}
	}

	private static JsonObject getUiWidth(JsonObject obj, Integer uiWidth) {
		if (obj == null)
			obj = new JsonObject();

		obj.addProperty("width", uiWidth);

		return obj;

	}

	/*
	 * Added 3 conf. values for fields hide, editable and readonly... R.P & F.R
	 */
	/* configure order: editoptions, hidden, editable with (true-false) F.R */
	private static JsonObject getDefaultUiMeta(Boolean isRepo, Boolean isHidden, Boolean isEditable,
			Boolean isEditrules) {
		JsonObject obj = new JsonObject();
		// if(isViewable)
		// obj.addProperty("viewable",true);
		if (isEditable)
			obj.addProperty("editable", true);

		if (isHidden)
			obj.addProperty("hidden", true);
		if (isRepo)

		{
			JsonObject subObj = new JsonObject();
			subObj.addProperty("readonly", true);
			obj.add("editoptions", subObj);
		}

		if (isEditrules) {
			JsonObject subObj = new JsonObject();
			subObj.addProperty("edithidden", true);
			subObj.addProperty("required", true);
			obj.add("editrules", subObj);
		}

		return obj;

	}

	private static DbDataTable getMasterLinkType() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_link_type");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_LINK_TYPE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.link_type");
			dbt.setUse_cache(true);
			dbt.setCacheType("PERM");
			dbt.setIsConfigTable(false);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("LINK_TYPE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(50);
			dbf2.setIsNull(false);
			dbf2.setIsUnique(true);
			dbf2.setUnique_level("TABLE");
			dbf2.setCode_list_user_code("LINK_TYPE");
			dbf2.setLabel_code("master_repo.link_type");

			// f2
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("LINK_TYPE_DESCRIPTION");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(200);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.link_type_description");

			// f3
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("LINK_OBJ_TYPE_1");
			dbf3.setDbFieldType(DbFieldType.NUMERIC);
			dbf3.setDbFieldSize(18);
			dbf3.setIsNull(false);
			dbf3.setIsUnique(true);
			dbf3.setUnique_level("TABLE");
			dbf3.setLabel_code("master_repo.link_object_type_1");

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("LINK_OBJ_TYPE_2");
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setIsNull(false);
			dbf4.setIsUnique(true);
			dbf4.setUnique_level("TABLE");
			dbf4.setLabel_code("master_repo.link_object_type_2");

			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("AS_USER_GROUP");
			dbf5.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(18);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.link_as_user_group");

			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("DEFER_SECURITY");
			dbf6.setDbFieldType(DbFieldType.BOOLEAN);
			dbf6.setDbFieldSize(18);
			dbf6.setIsNull(true);
			dbf6.setLabel_code("master_repo.link_defer_security");

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("IS_MANDATORY");
			dbf7.setDbFieldType(DbFieldType.BOOLEAN);
			dbf7.setDbFieldSize(18);
			dbf7.setIsNull(true);
			dbf7.setLabel_code("master_repo.link_is_mandatory");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("IS_REVERSE");
			dbf8.setDbFieldType(DbFieldType.BOOLEAN);
			dbf8.setDbFieldSize(18);
			dbf8.setIsNull(true);
			dbf8.setLabel_code("master_repo.link_is_reverse");

			dbt.setDbTableFields(new DbDataField[7]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_1;
			dbt.getDbTableFields()[3] = dbf3;
			dbt.getDbTableFields()[4] = dbf4;
			dbt.getDbTableFields()[5] = dbf5;
			dbt.getDbTableFields()[6] = dbf6;

			return dbt;
		}
	}

	private static DbDataTable getMasterSecurityLog() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_security_log");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SECURITY_LOG);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.security_log");
			dbt.setUse_cache(true);
			dbt.setCacheSize(9999);
			dbt.setCacheTTL(15);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("USER_OBJECT_ID");
			// dbf2.setIsPrimaryKey(true);
			dbf2.setDbFieldType(DbFieldType.NUMERIC);
			dbf2.setDbFieldSize(18);
			dbf2.setDbFieldScale(0);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.user_object_id");

			// f2
			DbDataField dbf2_0 = new DbDataField();
			dbf2_0.setDbFieldName("SESSION_ID");
			dbf2_0.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_0.setDbFieldSize(50);
			dbf2_0.setIsNull(false);
			dbf2_0.setIsUnique(true);
			dbf2_0.setLabel_code("master_repo.session_id");

			// f2
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("ACTIVITY_TYPE");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(100);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.activity_type");

			dbt.setDbTableFields(new DbDataField[4]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_0;
			dbt.getDbTableFields()[3] = dbf2_1;

			return dbt;
		}
	}

	private static DbDataTable getMasterUsers() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_users");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_USER);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.users");
			dbt.setUse_cache(true);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("USER_TYPE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(50);
			dbf2.setIsNull(false);
			dbf2.setCode_list_user_code("USER_TYPE");
			dbf2.setLabel_code("master_repo.user_type");
			dbf2.setIsUnique(true);
			dbf2.setUnique_constraint_name("unq_usr_type");
			dbf2.setUnique_level("TABLE");

			// f2
			DbDataField dbf2_0 = new DbDataField();
			dbf2_0.setDbFieldName("USER_UID");
			dbf2_0.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_0.setDbFieldSize(50);
			dbf2_0.setIsNull(true);
			dbf2_0.setIndexName("user_uid");
			// dbf2_0.setCode_list_user_code("USER_UID");
			dbf2_0.setLabel_code("master_repo.user_uid");
			dbf2_0.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

			// f2
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("USER_NAME");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(100);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.user_name");
			dbf2_1.setIs_updateable(false);
			dbf2_1.setIsUnique(true);
			dbf2_1.setUnique_level("TABLE");
			dbf2_1.setUnique_constraint_name("unq_usr_type");
			dbf2_1.setIndexName("unq_usr_pass");
			dbf2_1.setGui_metadata(getDefaultUiMeta(false, false, true, true).toString());

			// f3
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("FIRST_NAME");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(200);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.first_name");
			dbf3.setGui_metadata(getDefaultUiMeta(false, false, false, true).toString());

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("LAST_NAME");
			dbf4.setDbFieldType(DbFieldType.NVARCHAR);
			dbf4.setDbFieldSize(200);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.last_name");
			dbf4.setGui_metadata(getDefaultUiMeta(false, false, false, true).toString());

			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("E_MAIL");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(200);
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.e_mail");
			dbf5.setGui_metadata(getDefaultUiMeta(false, false, false, true).toString());

			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("PASSWORD_HASH");
			dbf6.setDbFieldType(DbFieldType.NVARCHAR);
			dbf6.setDbFieldSize(200);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.password_hash");
			dbf6.setIndexName("unq_usr_pass");
			dbf6.setGui_metadata(getDefaultUiMeta(false, true, false, true).toString());

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("CONFIRM_PASSWORD_HASH");
			dbf7.setDbFieldType(DbFieldType.NVARCHAR);
			dbf7.setDbFieldSize(200);
			dbf7.setIsNull(true);
			dbf7.setLabel_code("master_repo.confirm_password_hash");
			dbf7.setGui_metadata(getDefaultUiMeta(false, true, false, true).toString());

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("PIN");
			dbf8.setDbFieldType(DbFieldType.NVARCHAR);
			dbf8.setDbFieldSize(50);
			dbf8.setIsNull(true);
			dbf8.setLabel_code("master_repo.pin");
			dbf8.setGui_metadata(getDefaultUiMeta(false, false, false, true).toString());

			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("TAX_ID");
			dbf9.setDbFieldType(DbFieldType.NVARCHAR);
			dbf9.setDbFieldSize(50);
			dbf9.setIsNull(true);
			dbf9.setLabel_code("master_repo.tax_id");
			dbf9.setGui_metadata(getDefaultUiMeta(false, false, false, true).toString());

			dbt.setDbTableFields(new DbDataField[11]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_0;
			dbt.getDbTableFields()[3] = dbf2_1;
			dbt.getDbTableFields()[4] = dbf3;
			dbt.getDbTableFields()[5] = dbf4;
			dbt.getDbTableFields()[6] = dbf5;
			dbt.getDbTableFields()[7] = dbf6;
			dbt.getDbTableFields()[8] = dbf7;
			dbt.getDbTableFields()[9] = dbf8;
			dbt.getDbTableFields()[10] = dbf9;

			return dbt;
		}
	}

	private static DbDataTable getMasterGroups() {

		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_user_groups");
		dbt.setDbRepoName("{MASTER_REPO}");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_GROUP);
		dbt.setIsRepoTable(false);
		dbt.setLabel_code("master_repo.user_groups");
		dbt.setUse_cache(true);
		dbt.setIsConfigTable(true);
		dbt.setConfigColumnName("GROUP_NAME");

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("GROUP_TYPE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(50);
		dbf2.setIsNull(false);
		dbf2.setCode_list_user_code("GROUP_TYPE");
		dbf2.setLabel_code("master_repo.group_type");

		// f2
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("GROUP_SECURITY_TYPE");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(50);
		dbf4.setIsNull(false);
		dbf4.setCode_list_user_code("GROUP_SECURITY_TYPE");
		dbf4.setLabel_code("master_repo.group_security_type");

		// f2
		DbDataField dbf2_0 = new DbDataField();
		dbf2_0.setDbFieldName("GROUP_UID");
		dbf2_0.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2_0.setDbFieldSize(50);
		dbf2_0.setIsNull(false);
		dbf2_0.setCode_list_user_code("GROUP_UID");
		dbf2_0.setLabel_code("master_repo.group_uid");

		// f2
		DbDataField dbf2_1 = new DbDataField();
		dbf2_1.setDbFieldName("GROUP_NAME");
		dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2_1.setDbFieldSize(100);
		dbf2_1.setIsNull(false);
		dbf2_1.setIsUnique(true);
		dbf2_1.setLabel_code("master_repo.group_label_code");
		dbf2_1.setIs_updateable(false);

		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("E_MAIL");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(200);
		dbf5.setIsNull(false);
		dbf5.setLabel_code("master_repo.e_mail");

		dbt.setDbTableFields(new DbDataField[6]);
		dbt.getDbTableFields()[0] = dbf1;
		dbt.getDbTableFields()[1] = dbf2;
		dbt.getDbTableFields()[2] = dbf4;
		dbt.getDbTableFields()[3] = dbf2_0;
		dbt.getDbTableFields()[4] = dbf2_1;
		dbt.getDbTableFields()[5] = dbf5;
		// dbt.getDbTableFields()[6] = dbf6;

		return dbt;

	}

	/**
	 * Method for generating the master Organisational Units.
	 * 
	 * @return A DbDataTable descriptor for Org Units
	 */
	private static DbDataTable getMasterOU() {

		DbDataTable dbt = new DbDataTable();
		dbt.setDbTableName("{REPO_TABLE_NAME}_org_units");
		dbt.setDbRepoName("{MASTER_REPO}");
		dbt.setDbSchema("{DEFAULT_SCHEMA}");
		dbt.setIsSystemTable(true);
		dbt.setObjectId(svCONST.OBJECT_TYPE_ORG_UNITS);
		dbt.setIsRepoTable(false);
		dbt.setLabel_code("master_repo.organisational_units");
		dbt.setUse_cache(true);
		dbt.setCacheType("PERM");
		// dbt.setIsConfigTable(true);

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setLabel_code("master_repo.table_meta_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("ORG_UNIT_TYPE");
		dbf2.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2.setDbFieldSize(50);
		dbf2.setIsNull(false);
		dbf2.setCode_list_user_code("ORG_UNIT_TYPE");
		dbf2.setLabel_code("master_repo.group_type");

		// f2
		DbDataField dbf4 = new DbDataField();
		dbf4.setDbFieldName("NAME");
		dbf4.setDbFieldType(DbFieldType.NVARCHAR);
		dbf4.setDbFieldSize(200);
		dbf4.setIsNull(false);
		dbf4.setLabel_code("master_repo.org_unit");

		// f2
		DbDataField dbf2_0 = new DbDataField();
		dbf2_0.setDbFieldName("ADDRESS");
		dbf2_0.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2_0.setDbFieldSize(200);
		dbf2_0.setIsNull(true);
		dbf2_0.setLabel_code("master_repo.org_address");

		// f2
		DbDataField dbf2_1 = new DbDataField();
		dbf2_1.setDbFieldName("PHONE");
		dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
		dbf2_1.setDbFieldSize(50);
		dbf2_1.setIsNull(true);
		dbf2_1.setLabel_code("master_repo.phone");

		DbDataField dbf5 = new DbDataField();
		dbf5.setDbFieldName("E_MAIL");
		dbf5.setDbFieldType(DbFieldType.NVARCHAR);
		dbf5.setDbFieldSize(200);
		dbf5.setIsNull(true);
		dbf5.setLabel_code("master_repo.e_mail");

		DbDataField dbf6 = new DbDataField();
		dbf6.setDbFieldName("PARENT_OU_ID");
		dbf6.setDbFieldType(DbFieldType.NUMERIC);
		dbf6.setDbFieldSize(18);
		dbf6.setIsNull(true);
		dbf6.setLabel_code("master_repo.parent_ou");

		DbDataField dbf7 = new DbDataField();
		dbf7.setDbFieldName("EXTERNAL_ID");
		dbf7.setDbFieldType(DbFieldType.NUMERIC);
		dbf7.setLabel_code("master_repo.external_id");

		dbt.setDbTableFields(new DbDataField[8]);
		dbt.getDbTableFields()[0] = dbf1;
		dbt.getDbTableFields()[1] = dbf6;
		dbt.getDbTableFields()[2] = dbf4;
		dbt.getDbTableFields()[3] = dbf2_0;
		dbt.getDbTableFields()[4] = dbf2_1;
		dbt.getDbTableFields()[5] = dbf5;
		dbt.getDbTableFields()[6] = dbf2;
		dbt.getDbTableFields()[7] = dbf7;

		return dbt;

	}

	/**
	 * DataTable for storing workflow configurations This table should store all
	 * workflows for objects in the Svarog eco-system
	 * 
	 * @return A DbDataTable descriptor
	 */
	private static DbDataTable getMasterWorkflow() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_workflow");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_WORKFLOW);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.workflow");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(false);
			dbt.setParentName("TABLES");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);

			// dbf1.setSort_order(900);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("WORKFLOW_TYPE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(50);
			dbf2.setIsNull(false);
			dbf2.setCode_list_user_code("WORKFLOW_TYPE");
			dbf2.setLabel_code("master_repo.workflow_type");

			// f2
			DbDataField dbf2_0 = new DbDataField();
			dbf2_0.setDbFieldName("WORKFLOW_UID");
			dbf2_0.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_0.setDbFieldSize(50);
			dbf2_0.setIsNull(false);
			dbf2_0.setLabel_code("master_repo.workflow_uid");

			// f2
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("WORKFLOW_LABEL_CODE");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(100);
			dbf2_1.setIsNull(false);
			dbf2_1.setIsUnique(true);
			dbf2_1.setUnique_constraint_name("uq_object_from_to_status");
			dbf2_1.setUnique_level("PARENT");
			dbf2_1.setLabel_code("master_repo.workflow_name");
			dbf2_1.setIs_updateable(false);

			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("OBJECT_SUB_CODE");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(100);
			dbf5.setIsUnique(true);
			dbf5.setUnique_level("PARENT");
			dbf5.setUnique_constraint_name("uq_object_from_to_status");
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.object_type_id");

			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("ORIGINATING_STATUS");
			dbf6.setDbFieldType(DbFieldType.NVARCHAR);
			dbf6.setDbFieldSize(10);
			dbf6.setIsNull(true);
			dbf6.setUnique_constraint_name("uq_object_from_to_status");
			dbf6.setUnique_level("PARENT");
			dbf6.setIsUnique(true);
			dbf6.setCode_list_user_code("OBJ_STATUS");
			dbf6.setLabel_code("master_repo.from_status");

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("DESTINATION_STATUS");
			dbf7.setDbFieldType(DbFieldType.NVARCHAR);
			dbf7.setDbFieldSize(10);
			dbf7.setIsNull(false);
			dbf7.setIsUnique(true);
			dbf7.setUnique_level("PARENT");
			dbf7.setUnique_constraint_name("uq_object_from_to_status");
			dbf7.setCode_list_user_code("OBJ_STATUS");
			dbf7.setLabel_code("master_repo.object_status");
			dbf7.setGui_metadata(getUiWidth(getDefaultUiMeta(true, false, false, false), 72).toString());

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("CHECKIN_RULE");
			dbf8.setDbFieldType(DbFieldType.NUMERIC);
			dbf8.setDbFieldSize(18);
			dbf8.setDbFieldScale(0);
			dbf8.setIsNull(true);
			dbf8.setLabel_code("master_repo.checkin_rule");

			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("CHECKOUT_RULE");
			dbf9.setDbFieldType(DbFieldType.NUMERIC);
			dbf9.setDbFieldSize(18);
			dbf9.setDbFieldScale(0);
			dbf9.setIsNull(true);
			dbf9.setLabel_code("master_repo.checkout_rule");

			DbDataField dbf10 = new DbDataField();
			dbf10.setDbFieldName("IS_DEFAULT_WF_ROUTE");
			dbf10.setDbFieldType(DbFieldType.BOOLEAN);
			dbf10.setIsNull(true);
			dbf10.setLabel_code("master_repo.is_default_route");

			dbt.setDbTableFields(new DbDataField[10]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_0;
			dbt.getDbTableFields()[3] = dbf2_1;
			dbt.getDbTableFields()[4] = dbf5;
			dbt.getDbTableFields()[5] = dbf6;
			dbt.getDbTableFields()[6] = dbf7;
			dbt.getDbTableFields()[7] = dbf8;
			dbt.getDbTableFields()[8] = dbf9;
			dbt.getDbTableFields()[9] = dbf10;
			return dbt;
		}
	}

	private static DbDataTable getMasterACL() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_acl");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_ACL);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.acl");
			dbt.setConfigColumnName("LABEL_CODE");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(true);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2_0 = new DbDataField();
			dbf2_0.setDbFieldName("ACL_EXTERNAL_ID");
			dbf2_0.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_0.setDbFieldSize(50);
			dbf2_0.setIsNull(true);
			dbf2_0.setLabel_code("master_repo.acl_external_id");

			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("ACL_CODE");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(2);
			dbf3.setIsUnique(false);
			// dbf3.setUnique_level("TABLE");
			// dbf3.setIs_updateable(false);
			dbf3.setIsNull(true);
			dbf3.setLabel_code("master_repo.access_code");

			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("ACCESS_TYPE");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(50);
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.access_level_id");
			dbf5.setCode_list_user_code("ACCESS_CONTROL_LEVEL");
			dbf5.setIsUnique(true);
			dbf5.setUnique_constraint_name("TYPE_OID_CFG_ID");

			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("ACL_OBJECT_ID");
			dbf6.setDbFieldType(DbFieldType.NUMERIC);
			dbf6.setDbFieldSize(18);
			dbf6.setDbFieldScale(0);
			dbf6.setIsNull(false);
			dbf6.setIsUnique(true);
			dbf6.setUnique_constraint_name("TYPE_OID_CFG_ID");

			dbf6.setLabel_code("master_repo.acl_object_id");

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("ACL_OBJECT_TYPE");
			dbf7.setDbFieldType(DbFieldType.NUMERIC);
			dbf7.setDbFieldSize(18);
			dbf7.setDbFieldScale(0);
			dbf7.setIsNull(false);
			dbf7.setIsUnique(true);
			dbf7.setUnique_constraint_name("TYPE_OID_CFG_ID");

			dbf7.setLabel_code("master_repo.acl_object_type");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("ACL_CONFIG_UNQ");
			dbf8.setDbFieldType(DbFieldType.NVARCHAR);
			dbf8.setDbFieldSize(100);
			dbf8.setIsNull(true);
			dbf8.setIsUnique(true);
			dbf8.setUnique_constraint_name("TYPE_OID_CFG_ID");

			dbf8.setLabel_code("master_repo.acl_config_unq_id");

			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("LABEL_CODE");
			dbf9.setDbFieldType(DbFieldType.NVARCHAR);
			dbf9.setDbFieldSize(100);
			dbf9.setIsNull(false);
			dbf9.setIndexName("FORMFIELD_LABEL_IDX");
			dbf9.setIsUnique(true);
			dbf9.setLabel_code("master_repo.acl_label");

			dbt.setDbTableFields(new DbDataField[8]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2_0;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf5;
			dbt.getDbTableFields()[4] = dbf6;
			dbt.getDbTableFields()[5] = dbf7;
			dbt.getDbTableFields()[6] = dbf8;
			dbt.getDbTableFields()[7] = dbf9;
			return dbt;
		}
	}

	private static DbDataTable getMasterSIDACLs() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_sid_acl");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SID_ACL);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sid_acl");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(false);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("SID_OBJECT_ID");
			dbf2.setDbFieldType(DbFieldType.NUMERIC);
			dbf2.setDbFieldSize(18);
			dbf2.setIsNull(false);
			dbf2.setIsUnique(true);
			dbf2.setLabel_code("master_repo.sid_oid");

			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("SID_TYPE_ID");
			dbf3.setDbFieldType(DbFieldType.NUMERIC);
			dbf3.setDbFieldSize(18);
			dbf3.setIsNull(false);
			dbf3.setIsUnique(true);
			dbf3.setLabel_code("master_repo.sid_type_id");

			// f2
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("ACL_OBJECT_ID");
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setIsNull(false);
			dbf4.setIsUnique(true);
			dbf4.setLabel_code("master_repo.acl_id");

			dbt.setDbTableFields(new DbDataField[4]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf4;
			return dbt;
		}
	}

	private static DbDataTable getMasterFiles() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_files");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_FILE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.files");
			dbt.setUse_cache(false);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("FILE_TYPE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(50);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.file_type");
			dbf2.setCode_list_user_code("FILE_TYPES");
			// f2
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("FILE_NAME");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(200);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.file_name");
			dbf2_1.setIsUnique(false);
			dbf2_1.setUnique_level("PARENT");

			// f3
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("FILE_SIZE");
			dbf3.setDbFieldType(DbFieldType.NUMERIC);
			dbf3.setDbFieldSize(9);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.file_size");

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("FILE_DATE");
			dbf4.setDbFieldType(DbFieldType.TIMESTAMP);
			dbf4.setDbFieldSize(3);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.file_date");

			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("FILE_NOTES");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(2000);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.file_notes");

			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("FILE_ID");
			dbf6.setDbFieldType(DbFieldType.NUMERIC);
			dbf6.setDbFieldSize(18);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.physical_file_id");

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("FILE_STORE_ID");
			dbf7.setDbFieldType(DbFieldType.NUMERIC);
			dbf7.setDbFieldSize(18);
			dbf7.setIsNull(true);
			dbf7.setLabel_code("master_repo.file_store_id");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("CONTENT_TYPE");
			dbf8.setDbFieldType(DbFieldType.NVARCHAR);
			dbf8.setDbFieldSize(100);
			dbf8.setIsNull(false);
			dbf8.setLabel_code("master_repo.file_content_type");

			dbt.setDbTableFields(new DbDataField[8]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_1;
			dbt.getDbTableFields()[3] = dbf3;
			dbt.getDbTableFields()[4] = dbf4;
			dbt.getDbTableFields()[5] = dbf5;
			dbt.getDbTableFields()[6] = dbf6;
			dbt.getDbTableFields()[7] = dbf7;

			return dbt;
		}
	}

	private static DbDataTable getMasterCodes() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_codes");
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_CODE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.codes");
			dbt.setUse_cache(true);
			dbt.setCacheType("PERM");
			dbt.setIsConfigTable(false);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("CODE_TYPE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(3);
			dbf2.setIsNull(true);
			dbf2.setLabel_code("master_repo.code_type");
			// f2
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("CODE_VALUE");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(50);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.code_value");
			dbf2_1.setIsUnique(true);
			dbf2_1.setUnique_level("PARENT");

			// f3
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("LABEL_CODE");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(100);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.label_id");

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("SORT_ORDER");
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(9);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.sort_order");

			// f2
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("PARENT_CODE_VALUE");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(50);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.parent_code_value");

			dbt.setDbTableFields(new DbDataField[6]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_1;
			dbt.getDbTableFields()[3] = dbf3;
			dbt.getDbTableFields()[4] = dbf4;
			dbt.getDbTableFields()[5] = dbf5;

			return dbt;
		}
	}

	private static DbDataTable getLocales() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_locales");

			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_LOCALE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.locales");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(true);
			dbt.setConfigColumnName("LOCALE_ID");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("LANGUAGE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(100);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.locale_language");

			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("LOCALE_ID");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(10);
			dbf2_1.setIsNull(false);
			dbf2_1.setIsUnique(true);

			dbf2_1.setLabel_code("master_repo.locale_id");
			// f3

			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("COUNTRY");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(100);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.locale_country");

			dbt.setDbTableFields(new DbDataField[4]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf2_1;

			return dbt;
		}
	}

	private static DbDataTable getLabels() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_labels");

			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_LABEL);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.labels");
			dbt.setUse_cache(true);
			dbt.setCacheType("PERM");
			dbt.setIsConfigTable(false);
			dbt.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("LABEL_CODE");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(100);
			dbf2.setIsNull(false);
			dbf2.setIsUnique(true);
			dbf2.setLabel_code("master_repo.label_code");
			dbf2.setUnique_level("TABLE");

			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("LOCALE_ID");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(10);
			dbf2_1.setIsNull(false);
			dbf2_1.setIsUnique(true);
			dbf2_1.setLabel_code("master_repo.localed_id");
			dbf2_1.setUnique_level("TABLE");
			// f3

			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("LABEL_TEXT");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(200);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.label_text");

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("LABEL_DESCR");
			dbf4.setDbFieldType(DbFieldType.NVARCHAR);
			dbf4.setDbFieldSize(2000);
			dbf4.setLabel_code("master_repo.label_desc");

			dbt.setDbTableFields(new DbDataField[5]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf2_1;
			dbt.getDbTableFields()[4] = dbf4;
			return dbt;
		}
	}

	private static DbDataTable getMasterFields() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}_fields");

			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_FIELD);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.fields");
			dbt.setUse_cache(true);
			dbt.setCacheType("PERM");
			dbt.setIsConfigTable(true);
			dbt.setConfigColumnName("FIELD_NAME");
			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("FIELD_NAME");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(25);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.field_name");
			dbf2.setIsUnique(true);
			dbf2.setUnique_level("PARENT");
			;

			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("FIELD_TYPE");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(50);
			dbf2_1.setIsNull(false);
			dbf2_1.setLabel_code("master_repo.field_type");
			dbf2_1.setCode_list_user_code("FIELD_TYPES");

			// f3

			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("FIELD_SIZE");
			dbf3.setDbFieldType(DbFieldType.NUMERIC);
			dbf3.setDbFieldSize(18);
			dbf3.setDbFieldScale(0);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.field_size");

			// f4

			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("FIELD_SCALE");
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setDbFieldScale(0);
			dbf4.setIsNull(true);
			dbf4.setLabel_code("master_repo.field_scale");

			// f5
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("SEQUENCE_NAME");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(50);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.sequence_name");

			// f6
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("IS_NULL");
			dbf6.setDbFieldType(DbFieldType.BOOLEAN);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.field_isnull");

			// f7
			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("IS_UNIQUE");
			dbf7.setDbFieldType(DbFieldType.BOOLEAN);
			dbf7.setIsNull(false);
			dbf7.setLabel_code("master_repo.field_isunique");

			DbDataField dbf7_1 = new DbDataField();
			dbf7_1.setDbFieldName("UNQ_CONSTRAINT_NAME");
			dbf7_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf7_1.setDbFieldSize(50);
			dbf7_1.setIsNull(true);
			dbf7_1.setLabel_code("master_repo.unq_constraint");

			DbDataField dbf7_2 = new DbDataField();
			dbf7_2.setDbFieldName("UNQ_LEVEL");
			dbf7_2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf7_2.setDbFieldSize(50);
			dbf7_2.setIsNull(true);
			dbf7_2.setLabel_code("master_repo.unq_level");
			dbf7_2.setCode_list_user_code("UNQ_LEVEL");

			// f8
			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("IS_PRIMARY_KEY");
			dbf8.setDbFieldType(DbFieldType.BOOLEAN);
			dbf8.setIsNull(false);
			dbf8.setLabel_code("master_repo.field_ispk");

			// f9
			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("INDEX_NAME");
			dbf9.setDbFieldType(DbFieldType.NVARCHAR);
			dbf9.setDbFieldSize(50);
			dbf9.setIsNull(true);
			dbf9.setLabel_code("master_repo.unq_constraint");

			// f9

			DbDataField dbf10 = new DbDataField();
			dbf10.setDbFieldName("LABEL_CODE");
			dbf10.setDbFieldType(DbFieldType.NVARCHAR);
			dbf10.setDbFieldSize(100);
			dbf10.setIsNull(false);
			dbf10.setLabel_code("master_repo.label_code");

			DbDataField dbf11 = new DbDataField();
			dbf11.setDbFieldName("CODE_LIST_ID");
			dbf11.setDbFieldType(DbFieldType.NUMERIC);
			dbf11.setDbFieldSize(18);
			dbf11.setDbFieldScale(0);
			dbf11.setIsNull(true);
			dbf11.setLabel_code("master_repo.code_list_id");

			DbDataField dbf12 = new DbDataField();
			dbf12.setDbFieldName("GUI_METADATA");
			dbf12.setDbFieldType(DbFieldType.NVARCHAR);
			dbf12.setDbFieldSize(2000);
			dbf12.setIsNull(true);
			dbf12.setLabel_code("master_repo.is_visible_ui");

			// f8
			DbDataField dbf13 = new DbDataField();
			dbf13.setDbFieldName("IS_UPDATEABLE");
			dbf13.setDbFieldType(DbFieldType.BOOLEAN);
			dbf13.setIsNull(true);
			dbf13.setLabel_code("master_repo.field_is_updateable");

			DbDataField dbf14 = new DbDataField();
			dbf14.setDbFieldName("SORT_ORDER");
			dbf14.setDbFieldType(DbFieldType.NUMERIC);
			dbf14.setDbFieldSize(18);
			dbf14.setDbFieldScale(0);
			dbf14.setIsNull(true);
			dbf14.setLabel_code("master_repo.field_sort_order");

			DbDataField dbf15 = new DbDataField();
			dbf15.setDbFieldName("CODE_LIST_MNEMONIC");
			dbf15.setDbFieldType(DbFieldType.NVARCHAR);
			dbf15.setDbFieldSize(50);
			dbf15.setIsNull(true);
			dbf15.setLabel_code("master_repo.code_list_mnemonic");

			DbDataField dbf16 = new DbDataField();
			dbf16.setDbFieldName("REFERENTIAL_TABLE");
			dbf16.setDbFieldType(DbFieldType.NVARCHAR);
			dbf16.setDbFieldSize(50);
			dbf16.setIsNull(true);
			dbf16.setLabel_code("master_repo.referential_table");

			DbDataField dbf17 = new DbDataField();
			dbf17.setDbFieldName("REFERENTIAL_FIELD");
			dbf17.setDbFieldType(DbFieldType.NVARCHAR);
			dbf17.setDbFieldSize(50);
			dbf17.setIsNull(true);
			dbf17.setLabel_code("master_repo.referential_field");

			DbDataField dbf18 = new DbDataField();
			dbf18.setDbFieldName("EXTENDED_PARAMS");
			dbf18.setDbFieldType(DbFieldType.TEXT);
			dbf18.setIsNull(true);
			dbf18.setLabel_code("master_repo.ext_params");

			dbt.setDbTableFields(new DbDataField[21]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_1;
			dbt.getDbTableFields()[3] = dbf3;
			dbt.getDbTableFields()[4] = dbf4;
			dbt.getDbTableFields()[5] = dbf5;
			dbt.getDbTableFields()[6] = dbf6;
			dbt.getDbTableFields()[7] = dbf7;
			dbt.getDbTableFields()[8] = dbf7_1;
			dbt.getDbTableFields()[9] = dbf7_2;

			dbt.getDbTableFields()[10] = dbf8;
			dbt.getDbTableFields()[11] = dbf9;
			dbt.getDbTableFields()[12] = dbf10;
			dbt.getDbTableFields()[13] = dbf11;
			dbt.getDbTableFields()[14] = dbf12;

			dbt.getDbTableFields()[15] = dbf13;
			dbt.getDbTableFields()[16] = dbf14;
			dbt.getDbTableFields()[17] = dbf15;
			dbt.getDbTableFields()[18] = dbf16;
			dbt.getDbTableFields()[19] = dbf17;
			dbt.getDbTableFields()[20] = dbf18;
			return dbt;
		}
	}

	private static DbDataTable getMasterTable() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_tables");

			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_TABLE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.tables");
			dbt.setUse_cache(true);
			dbt.setCacheType("PERM");
			dbt.setIsConfigTable(true);
			dbt.setConfigColumnName("TABLE_NAME");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("REPO_NAME");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(50);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.repo_name");

			// f3

			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("TABLE_NAME");
			dbf3.setIsUnique(true);
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(25);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.table_name");
			dbf3.setUnique_level("TABLE");

			// f4
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("SCHEMA");
			dbf4.setIsUnique(true);
			dbf4.setDbFieldType(DbFieldType.NVARCHAR);
			dbf4.setDbFieldSize(50);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.table_schema");
			dbf4.setUnique_level("TABLE");

			// f5
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("SYSTEM_TABLE");
			dbf5.setDbFieldType(DbFieldType.BOOLEAN);
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.system_table");

			// f5
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("REPO_TABLE");
			dbf6.setDbFieldType(DbFieldType.BOOLEAN);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.repo_table");

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("LABEL_CODE");
			dbf7.setDbFieldType(DbFieldType.NVARCHAR);
			dbf7.setDbFieldSize(100);
			dbf7.setIsNull(false);
			dbf7.setLabel_code("master_repo.label_code");

			// f5
			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("USE_CACHE");
			dbf8.setDbFieldType(DbFieldType.BOOLEAN);
			dbf8.setIsNull(true);
			dbf8.setLabel_code("master_repo.use_cache");

			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("CACHE_TYPE");
			dbf9.setDbFieldType(DbFieldType.NVARCHAR);
			dbf9.setIsNull(false);
			dbf9.setDbFieldSize(10);
			dbf9.setLabel_code("master_repo.cache_type");
			dbf9.setCode_list_user_code("CACHE_TYPE");

			DbDataField dbf10 = new DbDataField();
			dbf10.setDbFieldName("CACHE_SIZE");
			dbf10.setDbFieldType(DbFieldType.NUMERIC);
			dbf10.setIsNull(true);
			dbf10.setDbFieldSize(18);
			dbf10.setLabel_code("master_repo.cache_size");

			DbDataField dbf11 = new DbDataField();
			dbf11.setDbFieldName("CACHE_EXPIRY");
			dbf11.setDbFieldType(DbFieldType.NUMERIC);
			dbf11.setIsNull(true);
			dbf11.setDbFieldSize(18);
			dbf11.setLabel_code("master_repo.cache_expiry");

			DbDataField dbf12 = new DbDataField();
			dbf12.setDbFieldName("IS_CONFIG_TABLE");
			dbf12.setDbFieldType(DbFieldType.BOOLEAN);
			dbf12.setIsNull(false);
			dbf12.setLabel_code("master_repo.is_config_table");

			DbDataField dbf13 = new DbDataField();
			dbf13.setDbFieldName("CONFIG_UNQ_ID");
			dbf13.setDbFieldType(DbFieldType.NVARCHAR);
			dbf13.setIsNull(true);
			dbf13.setDbFieldSize(50);
			dbf13.setLabel_code("master_repo.config_table_uq_column");

			DbDataField dbf14 = new DbDataField();
			dbf14.setDbFieldName("CONFIG_TYPE_ID");
			dbf14.setDbFieldType(DbFieldType.NUMERIC);
			dbf14.setIsNull(true);
			dbf14.setDbFieldSize(18);
			dbf14.setLabel_code("master_repo.config_type_id");

			DbDataField dbf15 = new DbDataField();
			dbf15.setDbFieldName("CONFIG_RELATION_TYPE");
			dbf15.setDbFieldType(DbFieldType.NVARCHAR);
			dbf15.setIsNull(true);
			dbf15.setDbFieldSize(50);
			dbf15.setLabel_code("master_repo.config_relation_type");
			dbf15.setCode_list_user_code("CONFIG_RELATION_TYPE");

			DbDataField dbf16 = new DbDataField();
			dbf16.setDbFieldName("CONFIG_RELATION_ID");
			dbf16.setDbFieldType(DbFieldType.NUMERIC);
			dbf16.setIsNull(true);
			dbf16.setDbFieldSize(18);
			dbf16.setLabel_code("master_repo.config_relation_id");

			dbt.setDbTableFields(new DbDataField[16]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf4;
			dbt.getDbTableFields()[4] = dbf5;
			dbt.getDbTableFields()[5] = dbf6;
			dbt.getDbTableFields()[6] = dbf7;
			dbt.getDbTableFields()[7] = dbf8;
			dbt.getDbTableFields()[8] = dbf9;
			dbt.getDbTableFields()[9] = dbf10;
			dbt.getDbTableFields()[10] = dbf11;
			dbt.getDbTableFields()[11] = dbf12;
			dbt.getDbTableFields()[12] = dbf13;
			dbt.getDbTableFields()[13] = dbf14;
			dbt.getDbTableFields()[14] = dbf15;
			dbt.getDbTableFields()[15] = dbf16;
			return dbt;
		}
	}

	private static DbDataTable getMasterSDIUnits() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_sdi_units");

			dbt.setDbRepoName("{REPO_TABLE_NAME}_sdi");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SDI_UNITS);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sdi_units");
			dbt.setUse_cache(true);
			dbt.setCacheType("PERM");
			dbt.setIsConfigTable(false);
			dbt.setParentName("SDI_UNITS");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("UNIT_NAME");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(100);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.unit_name");
			dbf2.setGui_metadata(getDefaultSDIMetadata().toString());

			// f2
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("UNIT_ID");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(100);
			dbf3.setIsNull(true);
			dbf3.setLabel_code("master_repo.unit_id");

			// f4
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("AREA");
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setDbFieldScale(2);
			dbf4.setLabel_code("master_repo.unit_area");

			// f5
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("PERIMETER");
			dbf5.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(18);
			dbf5.setDbFieldScale(2);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.unit_perimeter");

			// f5
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("UNIT_CLASS");
			dbf6.setDbFieldType(DbFieldType.NVARCHAR);
			dbf6.setDbFieldSize(50);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.bounds_class");
			dbf6.setCode_list_user_code("SDI_UNIT_CLASS");
			dbf6.setGui_metadata(getDefaultSDIMetadata().toString());

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("CENTROID");
			dbf7.setDbFieldType(DbFieldType.GEOMETRY);
			dbf7.setIsNull(true);
			dbf7.setGeometryType("POINT");
			dbf7.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf7.setIndexName("cent_idx");
			dbf7.setLabel_code("master_repo.centroid");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("GEOM");
			dbf8.setDbFieldType(DbFieldType.GEOMETRY);
			dbf8.setIsNull(true);
			dbf8.setGeometryType("MULTIPOLYGON");
			dbf8.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf8.setIndexName("geom_idx");
			dbf8.setLabel_code("master_repo.geometry");

			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("EXTERNAL_UNIT_ID");
			dbf9.setDbFieldType(DbFieldType.NVARCHAR);
			dbf9.setDbFieldSize(100);
			dbf9.setIsNull(true);
			dbf9.setLabel_code("master_repo.external_unit_id");

			DbDataField dbf10 = new DbDataField();
			dbf10.setDbFieldName("UNIT_LABEL");
			dbf10.setDbFieldType(DbFieldType.NVARCHAR);
			dbf10.setDbFieldSize(100);
			dbf10.setIsNull(true);
			dbf10.setLabel_code("master_repo.unit_label");
			dbf10.setGui_metadata(getDefaultSDIMetadata().toString());

			dbt.setDbTableFields(new DbDataField[10]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf4;
			dbt.getDbTableFields()[4] = dbf5;
			dbt.getDbTableFields()[5] = dbf6;
			dbt.getDbTableFields()[6] = dbf7;
			dbt.getDbTableFields()[7] = dbf8;
			dbt.getDbTableFields()[8] = dbf9;
			dbt.getDbTableFields()[9] = dbf10;

			return dbt;
		}
	}

	private static DbDataTable getMasterSDIUse() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_sdi_use");

			dbt.setDbRepoName("{REPO_TABLE_NAME}_sdi");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SDI_USE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sdi_use");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(false);
			dbt.setParentName("SDI_COVER");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f4
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("AREA");
			dbf4.setIsUnique(false);
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setDbFieldScale(2);
			dbf4.setLabel_code("master_repo.cover_area");

			DbDataField dbf4_1 = new DbDataField();
			dbf4_1.setDbFieldName("AREA_HA");
			dbf4_1.setIsUnique(false);
			dbf4_1.setDbFieldType(DbFieldType.NUMERIC);
			dbf4_1.setDbFieldSize(18);
			dbf4_1.setDbFieldScale(2);
			dbf4_1.setLabel_code("master_repo.bounds_area_ha");
			dbf4_1.setGui_metadata(getDefaultSDIMetadata().toString());

			// f5
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("PERIMETER");
			dbf5.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(18);
			dbf5.setDbFieldScale(2);
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.cover_perimeter");

			// f5
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("USE_CLASS");
			dbf6.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(9);
			dbf5.setDbFieldScale(0);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.use_class");
			dbf6.setCode_list_user_code("SDI_USE_CLASS");
			dbf6.setGui_metadata(getDefaultSDIMetadata().toString());

			// f5
			DbDataField dbf6_1 = new DbDataField();
			dbf6_1.setDbFieldName("USE_CLASS_2ND");
			dbf6_1.setDbFieldType(DbFieldType.NUMERIC);
			dbf6_1.setDbFieldSize(9);
			dbf6_1.setDbFieldScale(0);
			dbf6_1.setIsNull(false);
			dbf6_1.setLabel_code("master_repo.use_class_2nd");
			dbf6_1.setCode_list_user_code("SDI_USE_CLASS");
			dbf6_1.setGui_metadata(getDefaultSDIMetadata().toString());

			// f5
			DbDataField dbf6_2 = new DbDataField();
			dbf6_2.setDbFieldName("USE_CLASS_3RD");
			dbf6_2.setDbFieldType(DbFieldType.NUMERIC);
			dbf6_2.setDbFieldSize(9);
			dbf6_2.setDbFieldScale(0);
			dbf6_2.setIsNull(false);
			dbf6_2.setLabel_code("master_repo.use_class_3rd");
			dbf6_2.setCode_list_user_code("SDI_USE_CLASS");
			dbf6_2.setGui_metadata(getDefaultSDIMetadata().toString());

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("CENTROID");
			dbf7.setDbFieldType(DbFieldType.GEOMETRY);
			dbf7.setIsNull(true);
			dbf7.setGeometryType("POINT");
			dbf7.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf7.setIndexName("cent_idx");
			dbf7.setLabel_code("master_repo.centroid");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("GEOM");
			dbf8.setDbFieldType(DbFieldType.GEOMETRY);
			dbf8.setIsNull(true);
			dbf8.setGeometryType("MULTIPOLYGON");
			dbf8.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf8.setIndexName("geom_idx");
			dbf8.setLabel_code("master_repo.geometry");

			dbt.setDbTableFields(new DbDataField[9]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf4;
			dbt.getDbTableFields()[2] = dbf4_1;
			dbt.getDbTableFields()[3] = dbf5;
			dbt.getDbTableFields()[4] = dbf6;
			dbt.getDbTableFields()[5] = dbf6_1;
			dbt.getDbTableFields()[6] = dbf6_2;
			dbt.getDbTableFields()[7] = dbf7;
			dbt.getDbTableFields()[8] = dbf8;

			return dbt;
		}
	}

	private static DbDataTable getMasterSDICover() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_sdi_cover");

			dbt.setDbRepoName("{REPO_TABLE_NAME}_sdi");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SDI_COVER);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sdi_cover");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(false);
			dbt.setParentName("SDI_BOUNDS");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f4
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("AREA");
			dbf4.setIsUnique(false); // Why??
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setDbFieldScale(2);
			dbf4.setLabel_code("master_repo.cover_area");

			DbDataField dbf4_1 = new DbDataField();
			dbf4_1.setDbFieldName("AREA_HA");
			dbf4_1.setIsUnique(false); // Why??
			dbf4_1.setDbFieldType(DbFieldType.NUMERIC);
			dbf4_1.setDbFieldSize(18);
			dbf4_1.setDbFieldScale(2);
			dbf4_1.setLabel_code("master_repo.bounds_area_ha");
			dbf4_1.setGui_metadata(getDefaultSDIMetadata().toString());

			// f5
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("PERIMETER");
			dbf5.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(18);
			dbf5.setDbFieldScale(2);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.cover_perimeter");

			// f5
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("COVER_CLASS");
			dbf6.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(9);
			dbf5.setDbFieldScale(0);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.bounds_class");
			dbf6.setCode_list_user_code("SDI_COVER_CLASS");
			dbf6.setGui_metadata(getDefaultSDIMetadata().toString());

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("CENTROID");
			dbf7.setDbFieldType(DbFieldType.GEOMETRY);
			dbf7.setIsNull(true);
			dbf7.setGeometryType("POINT");
			dbf7.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf7.setIndexName("cent_idx");
			dbf7.setLabel_code("master_repo.centroid");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("GEOM");
			dbf8.setDbFieldType(DbFieldType.GEOMETRY);
			dbf8.setIsNull(true);
			dbf8.setGeometryType("MULTIPOLYGON");
			dbf8.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf8.setIndexName("geom_idx");
			dbf8.setLabel_code("master_repo.geometry");

			dbt.setDbTableFields(new DbDataField[7]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf4;
			dbt.getDbTableFields()[2] = dbf4_1;
			dbt.getDbTableFields()[3] = dbf5;
			dbt.getDbTableFields()[4] = dbf6;
			dbt.getDbTableFields()[5] = dbf7;
			dbt.getDbTableFields()[6] = dbf8;

			return dbt;
		}
	}

	private static DbDataTable getMasterSDIBounds() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_sdi_bounds");

			dbt.setDbRepoName("{REPO_TABLE_NAME}_sdi");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SDI_BOUNDS);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sdi_bounds");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(false);
			dbt.setParentName("SDI_UNITS");

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("BOUNDS_NAME");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(100);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.bounds_name");
			dbf2.setGui_metadata(getDefaultSDIMetadata().toString());

			// f2
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("BOUNDS_ID");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(100);
			dbf3.setIsNull(true);
			dbf3.setLabel_code("master_repo.bounds_id");

			// f4
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("AREA");
			dbf4.setIsUnique(false);
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setDbFieldScale(2);
			dbf4.setLabel_code("master_repo.bounds_area");

			// f4
			DbDataField dbf4_1 = new DbDataField();
			dbf4_1.setDbFieldName("AREA_HA");
			dbf4_1.setIsUnique(false);
			dbf4_1.setDbFieldType(DbFieldType.NUMERIC);
			dbf4_1.setDbFieldSize(18);
			dbf4_1.setDbFieldScale(2);
			dbf4_1.setLabel_code("master_repo.bounds_area_ha");
			dbf4_1.setGui_metadata(getDefaultSDIMetadata().toString());

			// f5
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("PERIMETER");
			dbf5.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(18);
			dbf5.setDbFieldScale(2);
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.bounds_perimeter");

			// f5
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("BOUNDS_CLASS");
			dbf6.setDbFieldType(DbFieldType.NUMERIC);
			dbf6.setDbFieldSize(9);
			dbf6.setDbFieldScale(0);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.bounds_class");
			dbf6.setCode_list_user_code("SDI_BOUNDS_CLASS");
			dbf6.setGui_metadata(getDefaultSDIMetadata().toString());

			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("CENTROID");
			dbf7.setDbFieldType(DbFieldType.GEOMETRY);
			dbf7.setIsNull(false);
			dbf7.setGeometryType("POINT");
			dbf7.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf7.setIndexName("cent_idx");
			dbf7.setLabel_code("master_repo.centroid");

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("GEOM");
			dbf8.setDbFieldType(DbFieldType.GEOMETRY);
			dbf8.setIsNull(false);
			dbf8.setGeometryType("MULTIPOLYGON");
			dbf8.setGeometrySrid(SvConf.getParam("sys.gis.default_srid"));
			dbf8.setIndexName("geom_idx");
			dbf8.setLabel_code("master_repo.geometry");

			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("COVER_CLASS");
			dbf9.setDbFieldType(DbFieldType.NUMERIC);
			dbf9.setDbFieldSize(9);
			dbf9.setDbFieldScale(0);
			dbf9.setIsNull(true);
			dbf9.setLabel_code("master_repo.cover_class");
			dbf9.setCode_list_user_code("SDI_COVER_CLASS");
			dbf9.setGui_metadata(getDefaultSDIMetadata().toString());

			dbt.setDbTableFields(new DbDataField[10]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf4;
			dbt.getDbTableFields()[4] = dbf4_1;
			dbt.getDbTableFields()[5] = dbf5;
			dbt.getDbTableFields()[6] = dbf6;
			dbt.getDbTableFields()[7] = dbf7;
			dbt.getDbTableFields()[8] = dbf8;
			dbt.getDbTableFields()[9] = dbf9;

			return dbt;
		}
	}

	private static DbDataTable getMasterSDIDescriptor() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_sdi_descriptor");

			dbt.setDbRepoName("{REPO_TABLE_NAME}_sdi");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SDI_DESCRIPTOR);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sdi_descriptor");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(true);
			dbt.setConfigColumnName("SDI_TYPE");

			// f1 PKID
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2 SDI_TYPE
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("SDI_TYPE");
			dbf2.setDbFieldType(DbFieldType.NUMERIC);
			dbf2.setDbFieldSize(18);
			dbf2.setDbFieldScale(0);
			dbf2.setIsUnique(true);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.sdi_desc_type");

			// f2_1 SDI_SUB_TYPE
			DbDataField dbf2_1 = new DbDataField();
			dbf2_1.setDbFieldName("SDI_SUB_TYPE");
			dbf2_1.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2_1.setDbFieldSize(20);
			dbf2_1.setIsNull(true);
			dbf2_1.setLabel_code("master_repo.sdi_desc_sub_type");

			// f3 PARENT_TYPE
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("PARENT_TYPE");
			dbf3.setDbFieldType(DbFieldType.NUMERIC);
			dbf3.setDbFieldSize(18);
			dbf3.setDbFieldScale(0);
			dbf3.setIsNull(true);
			dbf3.setLabel_code("master_repo.sdi_desc_parent_type");

			// 3_1 CHILDREN_TYPE?

			// f4 PATH (URL)
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("PATH");
			dbf4.setDbFieldType(DbFieldType.NVARCHAR);
			dbf4.setDbFieldSize(200);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.sdi_desc_path");

			// f5 STYLE_CLASS
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("STYLE_FIELD");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(20);
			dbf5.setIsNull(true);
			dbf5.setLabel_code("master_repo.sdi_desc_style_class");

			// f6 STYLE (Style object, should at least contain weight, fill,
			// fill_color, opacity, fill_opacity as JSON string)
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("STYLE");
			dbf6.setDbFieldType(DbFieldType.TEXT);
			// dbf6.setDbFieldSize(5000); // can we expand this to 20000 ?
			dbf6.setIsNull(true);
			dbf6.setLabel_code("master_repo.sdi_desc_style");

			// f7 RENDER_PARAMS (Render object, should at least contain
			// render_min and render_max as JSON string)
			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("RENDER_PARAMS");
			dbf7.setDbFieldType(DbFieldType.NVARCHAR);
			dbf7.setDbFieldSize(500);
			dbf7.setIsNull(true); // setIsNull(false) perhaps?
			dbf7.setLabel_code("master_repo.sdi_desc_render_params");

			// f8 LABEL_PARAMS (label configuration object => show_label,
			// permanent, className, label_min, label_max as base options)
			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("LABEL_PARAMS");
			dbf8.setDbFieldType(DbFieldType.NVARCHAR);
			dbf8.setDbFieldSize(500);
			dbf8.setIsNull(true);
			dbf8.setLabel_code("master_repo.sdi_desc_label_params");

			// f9 DATA_PARAMS (which columns should be attached to geom object
			// returned in service as metadata)
			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("DATA_PARAMS");
			dbf9.setDbFieldType(DbFieldType.NVARCHAR);
			dbf9.setDbFieldSize(500);
			dbf9.setIsNull(true);
			dbf9.setLabel_code("master_repo.sdi_desc_data_params");

			// f10 CAN_SNAP
			DbDataField dbf10 = new DbDataField();
			dbf10.setDbFieldName("CAN_SNAP");
			dbf10.setDbFieldType(DbFieldType.BOOLEAN);
			dbf10.setIsNull(true);
			dbf10.setLabel_code("master_repo.can_snap");

			dbt.setDbTableFields(new DbDataField[11]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf2_1;
			dbt.getDbTableFields()[3] = dbf3;
			dbt.getDbTableFields()[4] = dbf4;
			dbt.getDbTableFields()[5] = dbf5;
			dbt.getDbTableFields()[6] = dbf6;
			dbt.getDbTableFields()[7] = dbf7;
			dbt.getDbTableFields()[8] = dbf8;
			dbt.getDbTableFields()[9] = dbf9;
			dbt.getDbTableFields()[10] = dbf10;

			return dbt;
		}
	}

	private static DbDataTable getMasterSDIService() {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName("{REPO_TABLE_NAME}" + "_sdi_service");

			dbt.setDbRepoName("{REPO_TABLE_NAME}_sdi");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setObjectId(svCONST.OBJECT_TYPE_SDI_SERVICE);
			dbt.setIsRepoTable(false);
			dbt.setLabel_code("master_repo.sdi_service");
			dbt.setUse_cache(true);
			dbt.setIsConfigTable(true);
			dbt.setConfigColumnName("NAME");

			// f1 PKID
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setLabel_code("master_repo.table_meta_pkid");

			// f2 SERVICE_NAME
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("SERVICE_NAME");
			dbf2.setDbFieldType(DbFieldType.NVARCHAR);
			dbf2.setDbFieldSize(100);
			dbf2.setIsUnique(true);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.sdi_service_name");
			dbf2.setCode_list_user_code("SDI_SERVICE_NAME");

			// f3 PATH (URL)
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("PATH");
			dbf3.setDbFieldType(DbFieldType.NVARCHAR);
			dbf3.setDbFieldSize(200);
			dbf3.setIsNull(false);
			dbf3.setLabel_code("master_repo.sdi_service_path");

			// f4 SERVICE_TYPE
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("SERVICE_TYPE");
			dbf4.setDbFieldType(DbFieldType.NVARCHAR);
			dbf4.setDbFieldSize(20);
			dbf4.setIsNull(false);
			dbf4.setLabel_code("master_repo.sdi_service_type");

			// f5 DATA_REF (LAYER NAME OR OTHER REF)
			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("DATA_REF");
			dbf5.setDbFieldType(DbFieldType.NVARCHAR);
			dbf5.setDbFieldSize(100);
			dbf5.setIsNull(false);
			dbf5.setLabel_code("master_repo.sdi_service_data_ref");

			// f6 AUTHORISATION
			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("AUTHORISATION");
			dbf6.setDbFieldType(DbFieldType.NVARCHAR);
			dbf6.setDbFieldSize(200);
			dbf6.setIsNull(true);
			dbf6.setLabel_code("master_repo.sdi_service_autorisation");

			// f7 DATA_PARAMS
			DbDataField dbf7 = new DbDataField();
			dbf7.setDbFieldName("DATA_PARAMS");
			dbf7.setDbFieldType(DbFieldType.TEXT);
			// dbf7.setDbFieldSize(2000);
			dbf7.setIsNull(true);
			dbf7.setLabel_code("master_repo.sdi_service_data_params");

			// f8 RENDER_PARAMS
			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("RENDER_PARAMS");
			dbf8.setDbFieldType(DbFieldType.TEXT);
			// dbf8.setDbFieldSize(2000);
			dbf8.setIsNull(true);
			dbf8.setLabel_code("master_repo.sdi_service_render_params");

			dbt.setDbTableFields(new DbDataField[8]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf2;
			dbt.getDbTableFields()[2] = dbf3;
			dbt.getDbTableFields()[3] = dbf4;
			dbt.getDbTableFields()[4] = dbf5;
			dbt.getDbTableFields()[5] = dbf6;
			dbt.getDbTableFields()[6] = dbf7;
			dbt.getDbTableFields()[7] = dbf8;

			return dbt;
		}
	}

	private static DbDataTable getRepoDescriptor(String repoTableName, Long repoObjectId) {
		{
			DbDataTable dbt = new DbDataTable();
			dbt.setDbTableName(repoTableName);
			dbt.setDbRepoName("{MASTER_REPO}");
			dbt.setDbSchema("{DEFAULT_SCHEMA}");
			dbt.setIsSystemTable(true);
			dbt.setIsRepoTable(true);
			if (repoObjectId != null)
				dbt.setObjectId(repoObjectId);
			dbt.setLabel_code("master_repo.repo_object");
			dbt.setIsConfigTable(false);
			dbt.setUse_cache(true);

			// f1
			DbDataField dbf1 = new DbDataField();
			dbf1.setDbFieldName("PKID");
			dbf1.setIsPrimaryKey(true);
			dbf1.setDbFieldType(DbFieldType.NUMERIC);
			dbf1.setDbFieldSize(18);
			dbf1.setDbFieldScale(0);
			dbf1.setIsNull(false);
			dbf1.setDbSequenceName("{REPO_TABLE_NAME}_pkid");
			dbf1.setLabel_code("master_repo.table_pkid");
			dbf1.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

			// f1.5
			DbDataField dbf9 = new DbDataField();
			dbf9.setDbFieldName("META_PKID");
			dbf9.setDbFieldType(DbFieldType.NUMERIC);
			dbf9.setDbFieldSize(18);
			dbf9.setDbFieldScale(0);
			dbf9.setIsNull(false);
			dbf9.setIndexName("meta_pkid_idx");
			dbf9.setLabel_code("master_repo.table_meta_pkid");
			dbf9.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

			// f4
			DbDataField dbf4 = new DbDataField();
			dbf4.setDbFieldName("OBJECT_ID");
			dbf4.setIsUnique(true);
			dbf4.setDbFieldType(DbFieldType.NUMERIC);
			dbf4.setDbFieldSize(18);
			dbf4.setDbFieldScale(0);
			dbf4.setIsNull(false);
			dbf4.setDbSequenceName("{REPO_TABLE_NAME}_oid");
			dbf4.setLabel_code("master_repo.object_id");
			dbf4.setGui_metadata(getUiWidth(getDefaultUiMeta(true, true, false, false), 60).toString());

			// f2
			DbDataField dbf2 = new DbDataField();
			dbf2.setDbFieldName("DT_INSERT");
			dbf2.setDbFieldType(DbFieldType.TIMESTAMP);
			dbf2.setDbFieldSize(3);
			dbf2.setIsNull(false);
			dbf2.setLabel_code("master_repo.insert_date");
			dbf2.setGui_metadata(getUiWidth(getDefaultUiMeta(true, true, false, false), 110).toString());

			// f3
			DbDataField dbf3 = new DbDataField();
			dbf3.setDbFieldName("DT_DELETE");
			dbf3.setDbFieldType(DbFieldType.TIMESTAMP);
			dbf3.setIsUnique(true);
			dbf3.setDbFieldSize(3);
			dbf3.setIsNull(false);
			dbf3.setIndexName("parent_id_dt_delete");
			dbf3.setLabel_code("master_repo.delete_date");
			dbf3.setGui_metadata(getDefaultUiMeta(true, true, false, false).toString());

			DbDataField dbf5 = new DbDataField();
			dbf5.setDbFieldName("PARENT_ID");
			dbf5.setDbFieldType(DbFieldType.NUMERIC);
			dbf5.setDbFieldSize(18);
			dbf5.setDbFieldScale(0);
			dbf5.setIndexName("parent_id_dt_delete");
			dbf5.setLabel_code("master_repo.parent_id");
			dbf5.setGui_metadata(getDefaultUiMeta(true, true, true, false).toString());

			// dbf5.setIsNull(false);

			DbDataField dbf6 = new DbDataField();
			dbf6.setDbFieldName("OBJECT_TYPE");
			dbf6.setDbFieldType(DbFieldType.NUMERIC);
			dbf6.setDbFieldSize(18);
			dbf6.setDbFieldScale(0);
			dbf6.setIsNull(false);
			dbf6.setLabel_code("master_repo.object_type");
			dbf6.setGui_metadata(getDefaultUiMeta(true, true, true, false).toString());

			DbDataField dbf8 = new DbDataField();
			dbf8.setDbFieldName("STATUS");
			dbf8.setDbFieldType(DbFieldType.NVARCHAR);
			dbf8.setDbFieldSize(10);
			dbf8.setIsNull(false);
			dbf8.setLabel_code("master_repo.object_status");
			dbf8.setCode_list_user_code("OBJ_STATUS");
			dbf8.setGui_metadata(
					"{'editoptions':{'readonly':true},'width':72,'react':{'filterable':true,'width':120,'visible':true,'resizable':true,'editable':false,'uischema':{'ui:readonly':true}}}");

			DbDataField dbf10 = new DbDataField();
			dbf10.setDbFieldName("USER_ID");
			dbf10.setDbFieldType(DbFieldType.NUMERIC);
			dbf10.setDbFieldSize(18);
			dbf10.setIsNull(false);
			dbf10.setLabel_code("master_repo.created_by_user");
			dbf10.setGui_metadata(getUiWidth(getDefaultUiMeta(true, true, false, false), 60).toString());

			dbt.setDbTableFields(new DbDataField[9]);
			dbt.getDbTableFields()[0] = dbf1;
			dbt.getDbTableFields()[1] = dbf9;
			dbt.getDbTableFields()[2] = dbf4;
			dbt.getDbTableFields()[3] = dbf2;
			dbt.getDbTableFields()[4] = dbf3;
			dbt.getDbTableFields()[5] = dbf5;
			dbt.getDbTableFields()[6] = dbf6;
			dbt.getDbTableFields()[7] = dbf8;
			dbt.getDbTableFields()[8] = dbf10;

			return dbt;
		}

	}

	private static DbDataTable getSDIMasterRepoObject() {
		return getRepoDescriptor("{REPO_TABLE_NAME}_sdi", null);
	}

	private static DbDataTable getMasterRepoObject() {
		return getRepoDescriptor("{REPO_TABLE_NAME}", svCONST.OBJECT_TYPE_REPO);
	}

	private static DbDataTable getRuleEngineMasterRepoObject() {
		return getRepoDescriptor("{REPO_TABLE_NAME}_re", null);
	}

	/**
	 * Method for adding sort order to existing DbDataTables
	 * 
	 * @param dbtt
	 *            A DbDataTable descriptor to which sort order should be added
	 * @return A DbDataTable descriptor with sort order in the fields array
	 */
	private static DbDataTable addSortOrder(DbDataTable dbtt) {
		Integer order = 100;
		if (dbtt.getDbTableFields() != null)
			for (DbDataField dbf : dbtt.getDbTableFields()) {
				if (dbf != null && dbf.getSort_order() == null) {
					dbf.setSort_order(order);
					order = order + 100;
				}
			}
		return dbtt;
	}

	/**
	 * List root objects which must be separated in order to embed them as
	 * resource in the svarorg release jar so we will not need a config folder
	 * for deployments.
	 * 
	 * @return A DbDataTable descriptor array
	 */
	static ArrayList<DbDataTable> getMasterRoot() {
		DbDataTable dbtt = null;
		ArrayList<DbDataTable> dbtList = new ArrayList<DbDataTable>();
		dbtt = getLocales();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getLabels();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterRepoObject();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterTable();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterFields();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterLinkType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createMasterFormType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterCodes();
		dbtList.add(addSortOrder(dbtt));

		return dbtList;
	}

	/**
	 * Method to remove duplicate table names from a list of objects. This is
	 * supposed to ensure smooth install in case someone tries to register
	 * duplicate tables.
	 * 
	 * @param dbtList
	 *            The arrayList of DbDataTable objects
	 * @return List containing object types unique by schema.table_name
	 */
	static ArrayList<DbDataTable> getDedupTables(ArrayList<DbDataTable> dbtList) {
		HashMap<String, DbDataTable> dedupDbt = new HashMap<String, DbDataTable>();
		for (DbDataTable dbt : dbtList) {
			dedupDbt.put(dbt.getDbSchema() + "." + dbt.getDbTableName(), dbt);
		}
		return new ArrayList<DbDataTable>(dedupDbt.values());

	}

	/**
	 * Wrapup method to gather all core objects in a single array
	 * 
	 * @return A DbDataTable descriptor array
	 */
	private static ArrayList<DbDataTable> getMasterObjectsImpl() {
		DbDataTable dbtt = null;
		ArrayList<DbDataTable> dbtList = new ArrayList<DbDataTable>();
		dbtList.addAll(getMasterRoot());
		dbtt = getMasterCodes();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getLocales();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getLabels();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterFiles();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterLink();
		dbtList.add(addSortOrder(dbtt));

		dbtt = getMasterUsers();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterWorkflow();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterACL();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterGroups();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterSIDACLs();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterSequence();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterSecurityLog();
		dbtList.add(addSortOrder(dbtt));

		dbtt = getMasterOU();
		dbtList.add(addSortOrder(dbtt));

		dbtt = createMasterForm();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createFormFieldType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createFormField();
		dbtList.add(addSortOrder(dbtt));

		// RULE ENGINE
		dbtt = getRuleEngineMasterRepoObject();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterRules();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterActions();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterExecutions();
		dbtList.add(addSortOrder(dbtt));
		dbtt = getMasterResults();
		dbtList.add(addSortOrder(dbtt));

		// BATCH EXECUTION ENGINE
		dbtt = createJobType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createTaskType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createJobTask();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createJob();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createTask();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createTaskDetail();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createJobObject();
		dbtList.add(addSortOrder(dbtt));
		// Add param config
		dbtt = createParamType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createParam();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createParamValue();
		dbtList.add(addSortOrder(dbtt));
		// Add UI structure
		dbtt = createRenderEngine();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createUIStructureType();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createUIStructureSource();
		dbtList.add(addSortOrder(dbtt));
		// Add contacat_data
		dbtt = createContactData();
		dbtList.add(addSortOrder(dbtt));

		// Add event/notification structure
		dbtt = createEvent();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createNotification();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createConversation();
		dbtList.add(addSortOrder(dbtt));
		dbtt = createMessage();
		dbtList.add(addSortOrder(dbtt));

		dbtt = getMasterExecutors();
		dbtList.add(addSortOrder(dbtt));

		// Add SDI structure
		if (SvConf.isSdiEnabled()) {
			dbtt = getSDIMasterRepoObject();
			dbtList.add(addSortOrder(dbtt));
			dbtt = getMasterSDIUnits();
			dbtList.add(addSortOrder(dbtt));
			dbtt = getMasterSDIBounds();
			dbtList.add(addSortOrder(dbtt));
			dbtt = getMasterSDICover();
			dbtList.add(addSortOrder(dbtt));
			dbtt = getMasterSDIUse();
			dbtList.add(addSortOrder(dbtt));
			dbtt = getMasterSDIDescriptor();
			dbtList.add(addSortOrder(dbtt));
			dbtt = getMasterSDIService();
			dbtList.add(addSortOrder(dbtt));
		} else
			log4j.info("Spatial Data is disabled. User paramater sys.gis.enable_spatial to set it");
		dbtt = getMasterNotes();
		dbtList.add(addSortOrder(dbtt));

		for (DbDataTable dbt : dbtList) {
			if (dbt.getObjectId() == null) {
				dbt.setObjectId(0L);
				// System.out.println("Object " + dbt.getDbTableName() +
				// "doesn't have ID, FIX IT FIX IT !!!");
				// return null;
			}

		}

		return dbtList;
	}

	/**
	 * Method to get all class instances implementing the IDbInit interface
	 * 
	 * @param subDir
	 *            Directory in which the method should scan the jar files for
	 *            IDbInit
	 * @return List of instances found
	 */
	@SuppressWarnings("unchecked")
	static ArrayList<IDbInit> getCustomDbInit(String subDir) {
		return (ArrayList<IDbInit>) loadClass(subDir, IDbInit.class);
	}

	static ArrayList<?> loadClass(String subDir, Class<?> clazz) {
		File customFolder = new File(subDir);
		ArrayList<Object> dbiResult = new ArrayList<>();
		if (!customFolder.exists())
			return dbiResult;
		File[] customJars = customFolder.listFiles();
		if (customJars != null) {
			Arrays.sort(customJars);
			for (int i = 0; i < customJars.length; i++) {
				if (customJars[i].getName().endsWith(".jar")) {
					ArrayList<Object> dbi = DbInit.loadClassFromJar(customJars[i].getAbsolutePath(), clazz);
					dbiResult.addAll((Collection<?>) dbi);
				}
			}
		}
		return dbiResult;
	}

	public static String createJsonMasterRepo() {

		ArrayList<DbDataTable> dbtList = getDedupTables(getMasterObjectsImpl());
		ArrayList<IDbInit> dbiResult = new ArrayList<IDbInit>();
		// load all dbinit instances from the legacy custom folder
		dbiResult.addAll(getCustomDbInit("custom/"));
		dbiResult.addAll(getCustomDbInit(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY)));

		for (IDbInit idb : dbiResult) {
			dbtList.addAll(idb.getCustomObjectTypes());
		}
		String fullRetval = "";
		for (int i = 0; i < dbtList.size(); i++) {
			DbDataTable dbt = dbtList.get(i);

			String retval = saveMasterJson(
					SvConf.getConfPath() + SvarogInstall.masterDbtPath
							+ dbt.getDbTableName().replace("{REPO_TABLE_NAME}", "master").toLowerCase() + "_repo.json",
					dbt, false);

			if (!retval.equals("")) {
				fullRetval += fullRetval + "; " + SvConf.getConfPath() + SvarogInstall.masterDbtPath
						+ dbt.getDbTableName().replace("{REPO_TABLE_NAME}", "master") + "_repo.json";
			}

		}
		return fullRetval;

	}

	private static void updateFileLists() {
		String fileList = "";

		File textFolder;
		try {
			textFolder = new File(SvConf.getConfPath() + SvarogInstall.masterDbtPath);
			File[] texFiles = textFolder.listFiles();
			for (int i = 0; i < texFiles.length; i++) {
				if (texFiles[i].getName().endsWith(".json"))
					fileList += texFiles[i].getName() + "\n";
			}
			SvUtil.saveStringToFile(SvConf.getConfPath() + SvarogInstall.masterDbtPath + SvarogInstall.fileListName,
					fileList);
			fileList = "";
			textFolder = new File(SvConf.getConfPath() + SvarogInstall.masterRecordsPath);
			texFiles = textFolder.listFiles();
			Arrays.sort(texFiles);
			for (int i = 0; i < texFiles.length; i++) {
				if (texFiles[i].getName().endsWith(".json"))
					fileList += texFiles[i].getName() + "\n";
			}
			SvUtil.saveStringToFile(SvConf.getConfPath() + SvarogInstall.masterRecordsPath + SvarogInstall.fileListName,
					fileList);

		} catch (Exception e) {
			System.out.println("Error Generating file list");
			e.printStackTrace();
		}
	}

	static String saveMasterJson(String jsonFilePath, Object dbt, Boolean isPretty) {
		try {
			JsonObject obj = ((Jsonable) dbt).toJson();
			Gson gson = (new GsonBuilder().setPrettyPrinting().create());

			String json = gson.toJson(obj);
			SvUtil.saveStringToFile(jsonFilePath, json);
		} catch (Exception e) {
			e.printStackTrace();
			return ("Failed streaming a DbDataTable to JSON file");
		}
		return "";

	}

	/**
	 * Method for loading all labels from external custom Jar
	 * 
	 * @param jarPath
	 *            The path to the JAR file
	 * @param mLabels
	 *            The map holding the list of labels
	 * @param locale
	 *            The local for which the labels should be loaded
	 */

	private static void loadLabelsFromCustom(String jarPath, HashMap<String, DbDataObject> mLabels, String locale) {
		InputStream iStr = null;
		Properties prop = new Properties();
		String labelsPath = "labels/" + locale + "Labels.properties";
		// load strings
		try {
			iStr = DbInit.loadCustomResources(jarPath, labelsPath);
			if (iStr != null) {
				prop.load(new InputStreamReader(iStr, "UTF-8"));
				log4j.info("Loaded labels file '" + labelsPath + "' from: " + jarPath);

				Iterator<Entry<Object, Object>> pit = prop.entrySet().iterator();
				if (prop.size() > 0)
					while (pit.hasNext()) {
						Entry<Object, Object> pair = pit.next();
						if (((String) pair.getKey()).endsWith("_l"))
							continue;

						DbDataObject dbo = new DbDataObject();
						dbo.setStatus(svCONST.STATUS_VALID);
						dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
						dbo.setDtDelete(SvConf.MAX_DATE);
						dbo.setVal("label_code", (String) pair.getKey());
						dbo.setVal("label_text", (String) pair.getValue());
						dbo.setObjectType(svCONST.OBJECT_TYPE_LABEL);

						try {
							dbo.setVal("label_descr", prop.getProperty((String) pair.getKey() + "_l"));
						} catch (Exception e) {
						}
						;
						dbo.setVal("locale_id", locale);
						mLabels.put((String) pair.getKey(), dbo);

					}
			} else
				log4j.trace("Labels file '" + labelsPath + "' not found in: " + jarPath);
		} catch (Exception e1) {
			log4j.error("Error loading labels from custom jar:" + jarPath, e1);
		} finally {
			try {
				if (iStr != null)
					iStr.close();
			} catch (IOException e) {
				log4j.error("Can not close input stream from custom jar:" + jarPath, e);
			}
		}

	}

	/**
	 * Method for loading the content of external JSON file and converting it to
	 * JsonObject
	 * 
	 * @param jarPath
	 *            The path to the JAR file from which the resource should be
	 *            loaded. If there is no jar file (jarPath==null) this method
	 *            will try to load the Json from the local file system according
	 *            to the filepath
	 * @param filePath
	 *            The path to the file which contains the JSON. If jarPath is
	 *            used this should be relative path within the jar.
	 * @return Null if there was error loading the file, otherwise a valid
	 *         JsonObject
	 */
	public static JsonElement loadJsonResource(String jarPath, String filePath) {
		Gson gson = new Gson();
		JsonElement json = null;
		InputStream istr = null;
		try {
			if (jarPath != null)
				istr = DbInit.loadCustomResources(jarPath, filePath);
			else
				istr = new FileInputStream(new File(filePath));
			String aclStr = IOUtils.toString(istr);
			aclStr = aclStr.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			aclStr = aclStr.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			json = gson.fromJson(aclStr, JsonElement.class);
		} catch (Exception ex) {
			log4j.trace(
					"Warning, no ACLs found in:" + jarPath + ", path:" + filePath + ". Exception:" + ex.getMessage());
			// ex.printStackTrace();
		} finally {
			if (istr != null)
				try {
					istr.close();
				} catch (IOException e) {
					log4j.warn("Can't close stream", e);
				}
		}
		return json;
	}

	public static DbDataObject createAclFromDbt(DbDataObject dbt, SvAccess accessLevel) {
		DbDataObject dbo = new DbDataObject();
		dbo.setStatus(svCONST.STATUS_VALID);
		dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
		dbo.setDtDelete(SvConf.MAX_DATE);
		dbo.setVal("ACCESS_TYPE", accessLevel.toString());
		dbo.setVal("acl_object_id", (String) dbt.getVal("TABLE_NAME"));
		dbo.setVal("acl_object_type", "TABLES");
		dbo.setVal("acl_config_unq", null);
		dbo.setVal("label_code", (String) dbt.getVal("TABLE_NAME") + "." + accessLevel.toString());
		dbo.setObjectType(svCONST.OBJECT_TYPE_ACL);
		return dbo;

	}

	public static void prepareSystemACLs(DbDataArray acls) {
		DbDataObject dbo = new DbDataObject();
		dbo.setStatus(svCONST.STATUS_VALID);
		dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
		dbo.setDtDelete(SvConf.MAX_DATE);
		dbo.setVal("ACCESS_TYPE", SvAccess.EXECUTE);
		dbo.setVal("acl_object_id", 0L);
		dbo.setVal("acl_object_type", "TABLES");
		dbo.setVal("acl_config_unq", svCONST.SUDO_ACL);
		dbo.setVal("label_code", svCONST.SUDO_ACL);
		dbo.setObjectType(svCONST.OBJECT_TYPE_ACL);
		acls.addDataItem(dbo);

		dbo = new DbDataObject();
		dbo.setStatus(svCONST.STATUS_VALID);
		dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
		dbo.setDtDelete(SvConf.MAX_DATE);
		dbo.setVal("ACCESS_TYPE", SvAccess.EXECUTE);
		dbo.setVal("acl_object_id", 0L);
		dbo.setVal("acl_object_type", "TABLES");
		dbo.setVal("acl_config_unq", svCONST.INSECURE_SQL_ACL);
		dbo.setVal("label_code", svCONST.INSECURE_SQL_ACL);
		dbo.setObjectType(svCONST.OBJECT_TYPE_ACL);
		acls.addDataItem(dbo);

		dbo = new DbDataObject();
		dbo.setStatus(svCONST.STATUS_VALID);
		dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
		dbo.setDtDelete(SvConf.MAX_DATE);
		dbo.setVal("ACCESS_TYPE", SvAccess.EXECUTE);
		dbo.setVal("acl_object_id", 0L);
		dbo.setVal("acl_object_type", "TABLES");
		dbo.setVal("acl_config_unq", svCONST.NULL_GEOMETRY_ACL);
		dbo.setVal("label_code", svCONST.NULL_GEOMETRY_ACL);
		dbo.setObjectType(svCONST.OBJECT_TYPE_ACL);
		acls.addDataItem(dbo);

	}

	public static void prepareDefaultACLs(DbDataArray acls, DbDataArray defaultDbts) {
		DbDataArray dbts = SvarogInstall.extractType(defaultDbts, svCONST.OBJECT_TYPE_TABLE, false);
		for (DbDataObject dbt : dbts.getItems()) {
			acls.addDataItem(createAclFromDbt(dbt, SvAccess.NONE));
			acls.addDataItem(createAclFromDbt(dbt, SvAccess.READ));
			acls.addDataItem(createAclFromDbt(dbt, SvAccess.WRITE));
			acls.addDataItem(createAclFromDbt(dbt, SvAccess.MODIFY));
			acls.addDataItem(createAclFromDbt(dbt, SvAccess.EXECUTE));
			acls.addDataItem(createAclFromDbt(dbt, SvAccess.FULL));
		}
	}

	/**
	 * Method that prepares the JSON files with ACS from the custom plugins and
	 * bundles
	 * 
	 * @return Description of the error if any
	 */
	public static String prepareACLs(DbDataArray defaultDbts, DbDataArray customDbts) {
		String testRetval = "";

		try {
			ArrayList<JsonElement> mACLs = new ArrayList<JsonElement>();
			ArrayList<JsonElement> mACLSIDs = new ArrayList<JsonElement>();
			DbDataArray arrAcl = new DbDataArray();
			prepareSystemACLs(arrAcl);
			prepareDefaultACLs(arrAcl, defaultDbts);
			prepareDefaultACLs(arrAcl, customDbts);

			DbDataArray arrAclSid = new DbDataArray();
			try {

				// load all custom JARs and try to load ACLs and ACL/SIDs from
				// those
				String aclFilePath = SvarogInstall.masterSecurityPath + SvarogInstall.aclFile;
				String aclSidFilePath = SvarogInstall.masterSecurityPath + SvarogInstall.aclSidFile;

				File customFolder = new File("custom/");
				File[] customJars = customFolder.exists() ? customFolder.listFiles() : null;
				if (customJars != null) {
					for (int i = 0; i < customJars.length; i++) {
						if (customJars[i].getName().endsWith(".jar")) {
							JsonElement jsonAcl = loadJsonResource(customJars[i].getAbsolutePath(), aclFilePath);
							if (jsonAcl != null) {
								mACLs.add(jsonAcl);
								System.out.println("Loaded ACLs from " + aclFilePath);
							}
							JsonElement jsonAclSID = loadJsonResource(customJars[i].getAbsolutePath(), aclSidFilePath);
							if (jsonAclSID != null) {
								mACLSIDs.add(jsonAclSID);
								System.out.println("Loaded ACL/SIDs from " + aclSidFilePath);
							}

						}
					}
				}
				// load ACSfrom the OSGI bundles dir too
				customFolder = new File(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY));
				customJars = customFolder.listFiles();
				if (customJars != null) {
					for (int i = 0; i < customJars.length; i++) {
						if (customJars[i].getName().endsWith(".jar")) {
							JsonElement jsonAcl = loadJsonResource(customJars[i].getAbsolutePath(), aclFilePath);
							if (jsonAcl != null) {
								mACLs.add(jsonAcl);
								System.out.println("Loaded ACLs from " + aclFilePath);
							}
							JsonElement jsonAclSID = loadJsonResource(customJars[i].getAbsolutePath(), aclSidFilePath);
							if (jsonAclSID != null) {
								mACLSIDs.add(jsonAclSID);
								System.out.println("Loaded ACL/SIDs from " + aclSidFilePath);
							}

						}
					}
				}
				aclFilePath = SvConf.getConfPath() + aclFilePath;
				aclSidFilePath = SvConf.getConfPath() + aclSidFilePath;

				JsonElement jsonAcl = loadJsonResource(null, aclFilePath);
				if (jsonAcl != null) {
					mACLs.add(jsonAcl);
					System.out.println("Loaded ACLs from " + aclFilePath);
				}
				JsonElement jsonAclSID = loadJsonResource(null, aclSidFilePath);
				if (jsonAclSID != null) {
					mACLSIDs.add(jsonAclSID);
					System.out.println("Loaded ACL/SIDs from " + aclSidFilePath);
				}

				if (mACLs.size() > 0) {
					// load strings
					Iterator<JsonElement> pit = mACLs.iterator();
					while (pit.hasNext()) {
						JsonElement acl = pit.next();
						if (acl.isJsonArray()) {
							JsonArray arr = acl.getAsJsonArray();
							for (int i = 0; i < arr.size(); i++) {
								JsonObject aclItem = arr.get(i).getAsJsonObject();
								DbDataObject dbo = new DbDataObject();
								dbo.setStatus(svCONST.STATUS_VALID);
								dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
								dbo.setDtDelete(SvConf.MAX_DATE);
								dbo.setVal("ACCESS_TYPE", aclItem.get("ACCESS_TYPE") != null
										? aclItem.get("ACCESS_TYPE").getAsString() : null);
								dbo.setVal("acl_object_id", aclItem.get("acl_object_id") != null
										? aclItem.get("acl_object_id").getAsString() : null);
								dbo.setVal("acl_object_type", aclItem.get("acl_object_type") != null
										? aclItem.get("acl_object_type").getAsString() : null);
								dbo.setVal("acl_config_unq", aclItem.get("acl_config_unq") != null
										? aclItem.get("acl_config_unq").getAsString() : null);
								dbo.setVal("label_code", aclItem.get("label_code") != null
										? aclItem.get("label_code").getAsString() : null);
								dbo.setObjectType(svCONST.OBJECT_TYPE_ACL);
								arrAcl.addDataItem(dbo);
							}
						} else
							System.out.println("Json object isn't array: " + acl.toString());
					}

				}

				if (mACLSIDs.size() > 0) {
					// load strings
					Iterator<JsonElement> pit = mACLSIDs.iterator();
					while (pit.hasNext()) {
						JsonElement acl = pit.next();
						if (acl.isJsonArray()) {
							JsonArray arr = acl.getAsJsonArray();
							for (int i = 0; i < arr.size(); i++) {
								JsonObject aclItem = arr.get(i).getAsJsonObject();
								DbDataObject dbo = new DbDataObject();
								dbo.setStatus(svCONST.STATUS_VALID);
								dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
								dbo.setDtDelete(SvConf.MAX_DATE);
								dbo.setVal("sid_object_id", aclItem.get("sid_object_id") != null
										? aclItem.get("sid_object_id").getAsString() : null);
								if (aclItem.get("status") != null)
									dbo.setStatus(aclItem.get("status").getAsString());

								dbo.setVal("acl_label_code", aclItem.get("acl_label_code") != null
										? aclItem.get("acl_label_code").getAsString() : null);
								dbo.setVal("group_name", aclItem.get("group_name") != null
										? aclItem.get("group_name").getAsString() : null);
								dbo.setObjectType(svCONST.OBJECT_TYPE_SID_ACL);
								arrAclSid.addDataItem(dbo);
							}
						} else
							System.out.println("Json object isn't array: " + acl.toString());
					}
				}
			} catch (Exception e) {
				System.out.println("Exception while parsing ACLs");
				e.printStackTrace();

			}

			testRetval += saveMasterJson(
					SvConf.getConfPath() + SvarogInstall.masterRecordsPath + "90. " + SvarogInstall.aclFile, arrAcl,
					true);
			testRetval += saveMasterJson(
					SvConf.getConfPath() + SvarogInstall.masterRecordsPath + "91. " + SvarogInstall.aclSidFile,
					arrAclSid, true);

		} catch (Exception e) {
			testRetval = "Error generating labels!";
			e.printStackTrace();
		}
		updateFileLists();
		return testRetval;
	}

	static void loadInternalLabels(String localeId, HashMap<String, DbDataObject> mLabels) {
		String labelFile = SvarogInstall.masterCodesPath + localeId + "Labels.properties";

		InputStream iStr = DbInit.class.getResourceAsStream("/" + labelFile);
		if (iStr == null)
			iStr = ClassLoader.getSystemClassLoader().getResourceAsStream(labelFile);

		Properties rb = null;

		if (iStr != null) {
			// load strings
			try {
				rb = new Properties();
				rb.load(new InputStreamReader(iStr, "UTF-8"));
				log4j.info("Loaded labels from: " + labelFile);
			} catch (IOException e) {
				log4j.trace("Error reading svarog root labels", e);
			} finally {
				try {
					if (iStr != null)
						iStr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if (rb != null) {
			// load strings
			Iterator<Object> pit = rb.keySet().iterator();
			while (pit.hasNext()) {
				String key = (String) pit.next();
				if (key.endsWith("_l"))
					continue;

				DbDataObject dbo = new DbDataObject();
				dbo.setStatus(svCONST.STATUS_VALID);
				dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
				dbo.setDtDelete(SvConf.MAX_DATE);
				dbo.setVal("label_code", key);
				dbo.setVal("label_text", rb.getProperty(key));
				dbo.setObjectType(svCONST.OBJECT_TYPE_LABEL);

				try {
					dbo.setVal("label_descr", rb.getProperty(key + "_l"));
				} catch (Exception e) {
				}
				;
				dbo.setVal("locale_id", localeId);
				mLabels.put(key, dbo);

			}
		}
	}

	/**
	 * Method that prepares the labels JSON files from the properties files for
	 * all languages
	 * 
	 * @return Description of the error if any
	 */
	public static String prepareLabels() {
		String testRetval = "";

		try {
			// HashMap<String, String> lblMap = new HashMap<String, String>();
			HashMap<String, DbDataObject> mLabels = new HashMap<String, DbDataObject>();
			DbDataArray arr = new DbDataArray();

			DbDataArray locales = SvarogInstall.getLocaleList();
			if (locales == null)
				return "Error loading system locales";

			// saveStringToFile(SvConf.getConfPath()+svCONST.masterRecordsPath+"10.
			// master_locales.json", locales.toJson().toString());
			for (DbDataObject entry : locales.getItems()) {
				try {

					loadInternalLabels((String) entry.getVal("locale_id"), mLabels);
					// load labers from the custom folder
					File customFolder = new File("custom/");
					if (customFolder != null) {
						File[] customJars = customFolder.listFiles();
						if (customJars != null) {
							for (int i = 0; i < customJars.length; i++) {
								log4j.debug("Trying to load labels from: " + customJars[i].getAbsolutePath());
								if (customJars[i].getName().endsWith(".jar"))
									loadLabelsFromCustom(customJars[i].getAbsolutePath(), mLabels,
											(String) entry.getVal("locale_id"));
							}
						}
					}
					// load labels from the svarog OSG bundles dir
					customFolder = new File(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY));
					if (customFolder != null) {
						File[] customJars = customFolder.listFiles();
						if (customJars != null) {
							for (int i = 0; i < customJars.length; i++) {
								if (customJars[i].getName().endsWith(".jar")) {
									loadLabelsFromCustom(customJars[i].getAbsolutePath(), mLabels,
											(String) entry.getVal("locale_id"));
								}
							}
						}
					}

				} catch (Exception e) {
					log4j.debug("Error reading svarog root labels", e);
				}

				if (mLabels != null && mLabels.size() > 0) {
					arr.setItems(new ArrayList<DbDataObject>(mLabels.values()));
					String strRetval = "";
					strRetval = saveMasterJson(SvConf.getConfPath() + SvarogInstall.masterRecordsPath
							+ SvarogInstall.labelsFilePrefix + entry.getVal("locale_id") + ".json", arr, true);
					if (!strRetval.equals("")) {
						testRetval += testRetval + strRetval + " json/records/20. master_labels_"
								+ entry.getVal("locale_id") + ".json; ";
					}
				}
				arr.getItems().clear();
				mLabels.clear();
				// save the array to disk
			}

		} catch (Exception e) {
			testRetval = "Error generating labels!";
			e.printStackTrace();
		}
		updateFileLists();
		// if (!testRetval.equals(""))
		// fail(testRetval);
		return testRetval;
	}

	/**
	 * Method for generating the base svarog configuration
	 * 
	 * @return String holding any error messages which might have occured
	 */
	public static String createJsonMasterTableRecords() {
		return createJsonMasterTableRecords(getDedupTables(getMasterObjectsImpl()));
	}

	/**
	 * Method to prepare the default code list
	 * 
	 * @param defaultCodes
	 *            Array hold the based default codes
	 * @return The object Id index at which the code list ends.
	 */
	public static Long prepDefaultCodeList(DbDataArray defaultCodes) {
		Long svObjectId = new Long(svCONST.CODES_STATUS);
		String[] retStr = new String[1];
		svObjectId = prepareCodes(svObjectId, retStr) + 1;
		String json;
		json = retStr[0];// IOUtils.toString(fis, "UTF-8");
		json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
		json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
		json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());
		Gson gson = new Gson();
		JsonObject jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
		defaultCodes.fromJson(jobj);
		return svObjectId;

	}

	/**
	 * Method to convert a legacy DbDataTable object to DbDataArray. FFS please
	 * remove DbDataTable in Svarog v3!!!
	 * 
	 * @param dbtList
	 *            The list of DbDataTable objects to convert
	 * @param dbarrOut
	 *            The output DbDataArray with DbDataObjects
	 * @param defaultCodes
	 *            The default set of codes
	 * @param svObjectId
	 *            The object ID index
	 * @param errMsg
	 *            Error string describing the errors
	 * @return The object id which should be used after generating the dbo style
	 *         objects
	 */
	public static Long dbTables2DbDataArray(ArrayList<DbDataTable> dbtList, DbDataArray dbarrOut,
			DbDataArray defaultCodes, Long svObjectId, StringBuilder errMsg) {

		for (int i = 0; i < dbtList.size(); i++) {
			DbDataTable dbt = dbtList.get(i);
			DbDataObject dbo = new DbDataObject();
			dbo.setObjectType(svCONST.OBJECT_TYPE_TABLE);

			dbo.setObjectId(dbt.getObjectId());
			dbo.setStatus(svCONST.STATUS_VALID);
			dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
			dbo.setDtDelete(SvConf.MAX_DATE);
			dbo.setVal("system_table", dbt.getIsSystemTable());
			dbo.setVal("repo_table", dbt.getIsRepoTable());
			dbo.setVal("table_name", dbt.getDbTableName().toUpperCase());
			dbo.setVal("schema", dbt.getDbSchema());
			dbo.setVal("repo_name", dbt.getDbRepoName().toUpperCase());
			dbo.setVal("label_code", dbt.getLabel_code());
			dbo.setVal("use_cache", true);
			dbo.setVal("is_config_table", dbt.getIsConfigTable());
			dbo.setVal("config_unq_id", dbt.getConfigColumnName());
			// reference to the original config type
			dbo.setVal("config_type_id", dbt.getConfigTypeName());
			dbo.setVal("config_relation_type", dbt.getConfigRelationType());
			dbo.setVal("config_relation_id", dbt.getConfigRelatedTypeName());

			if (dbt.getIsConfigTable()) {
				if (dbt.getConfigColumnName() == null || dbt.getConfigColumnName().length() < 1) {
					errMsg.append("Table:" + dbt.getDbTableName()
							+ " is set as config table but config_unq_id is null or empty");
					return svObjectId;
				}
			}
			dbo.setVal("parent_name", dbt.getParentName());

			if (dbt.getCacheType() == null || !dbt.getCacheType().equals("PERM")) {
				dbo.setVal("cache_type", dbt.getCacheType() == null ? "LRU_TTL" : dbt.getCacheType());
				dbo.setVal("cache_size", dbt.getCacheSize() == 0 ? 5000 : dbt.getCacheSize());
				dbo.setVal("cache_expiry", dbt.getCacheTTL() == 0 ? 30 : dbt.getCacheTTL());
			} else
				dbo.setVal("cache_type", dbt.getCacheType());

			dbarrOut.getItems().add(dbo);

			for (int j = 0; j < dbt.getDbTableFields().length; j++) {

				DbDataField dbf = dbt.getDbTableFields()[j];
				if (dbf == null)
					continue;

				// if this field is the config identifier, check if it has
				// unique constraint
				if (dbf.getDbFieldName().equalsIgnoreCase(dbt.getConfigColumnName())) {
					if (!dbf.getIsUnique()
							|| !(dbf.getUnique_level().equals("TABLE") || dbf.getUnique_level().equals("PARENT"))) {
						errMsg.append("Table:" + dbt.getDbTableName() + ",Field:" + dbf.getDbFieldName()
								+ " is set as config field but not unique or unique level is other than TABLE/PARENT");
						return svObjectId;
					}
				}

				dbo = new DbDataObject();
				dbo.setObjectType(svCONST.OBJECT_TYPE_FIELD);
				dbo.setParentId(dbt.getObjectId());

				dbo.setObjectId(svObjectId);
				svObjectId++;
				dbo.setStatus(svCONST.STATUS_VALID);
				dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
				dbo.setDtDelete(SvConf.MAX_DATE);

				dbo.setVal("field_name", dbf.getDbFieldName());
				dbo.setVal("field_type", dbf.getDbFieldType());
				dbo.setVal("field_size", dbf.getDbFieldSize());
				dbo.setVal("field_scale", dbf.getDbFieldScale());
				dbo.setVal("sequence_name", dbf.getDbSequenceName());
				dbo.setVal("is_null", dbf.getIsNull());
				dbo.setVal("is_unique", dbf.getIsUnique());
				dbo.setVal("unq_level", dbf.getIsUnique() && dbf.getUnique_level().equals("NOT_UNIQUE") ? "TABLE"
						: dbf.getUnique_level());
				dbo.setVal("unq_constraint_name", dbf.getUnique_constraint_name());
				dbo.setVal("is_primary_key", dbf.getIsPrimaryKey());
				dbo.setVal("index_name", dbf.getIndexName());
				dbo.setVal("label_code", dbf.getLabel_code());
				dbo.setVal("parent_name", dbt.getDbTableName());

				if (dbf.getDbFieldName().toUpperCase().equals("PKID"))
					dbo.setVal("gui_metadata", getDefaultUiMeta(true, true, true, false).toString());
				else
					dbo.setVal("gui_metadata", dbf.getGui_metadata());

				for (DbDataObject dbl : defaultCodes.getItems()) {
					if (dbl.getVal("CODE_VALUE").equals(dbf.getCode_user_code()))
						dbo.setVal("code_list_id", dbl.getObjectId());

				}
				dbo.setVal("code_list_mnemonic", dbf.getCode_user_code());
				dbo.setVal("is_updateable", dbf.getIs_updateable());
				dbo.setVal("sort_order", dbf.getSort_order());

				dbarrOut.getItems().add(dbo);

			}
		}
		return svObjectId;
	}

	/**
	 * Method to return an array of the basic core svarog objects needed to
	 * bootstrap
	 * 
	 * @param defaultCodes
	 *            The array with default codes
	 * @param defaultObjests
	 *            The array with default sys objects
	 */

	public static void initCoreRecords(DbDataArray defaultCodes, DbDataArray defaultObjests) {
		Long svObjectId = svCONST.MAX_SYS_OBJECT_ID;
		StringBuilder errMsg = new StringBuilder();
		svObjectId = dbTables2DbDataArray(getMasterRoot(), defaultObjests, defaultCodes, svObjectId, errMsg);

		String json = defaultObjests.toJson().toString();
		json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
		json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
		json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());
		Gson gson = new Gson();
		JsonObject jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
		defaultObjests.fromJson(jobj);

		for (DbDataObject dbo : defaultObjests.getItems()) {
			if (dbo.getObjectType().equals(svCONST.OBJECT_TYPE_TABLE)) {
				dbo.setVal("TABLE_NAME", ((String) dbo.getVal("TABLE_NAME")).toUpperCase());
				dbo.setVal("SCHEMA", ((String) dbo.getVal("SCHEMA")).toUpperCase());
			}
			dbo.setIsDirty(false);
		}
		if (defaultCodes.getItems().size() > 0) {
			json = defaultCodes.toJson().toString();
			json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());
			jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
			defaultCodes.fromJson(jobj);
			for (DbDataObject dbo : defaultCodes.getItems())
				dbo.setIsDirty(false);
		}
	}

	/**
	 * Method to generate the default Svarog link types
	 * 
	 * @param defaultObjests
	 */
	public static void addDefaultLinkTypes(DbDataArray defaultObjests) {

		DbDataObject dbl = new DbDataObject();
		dbl.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dbl.setStatus(svCONST.STATUS_VALID);
		dbl.setVal("LINK_TYPE", "USER_DEFAULT_GROUP");
		dbl.setVal("LINK_TYPE_DESCRIPTION", "User group default membership");
		dbl.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_USER);
		dbl.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_GROUP);
		defaultObjests.addDataItem(dbl);

		DbDataObject dbGroup = new DbDataObject();
		dbGroup.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dbGroup.setStatus(svCONST.STATUS_VALID);
		dbGroup.setVal("LINK_TYPE", "USER_GROUP");
		dbGroup.setVal("LINK_TYPE_DESCRIPTION", "User group additional membership");
		dbGroup.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_USER);
		dbGroup.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_GROUP);
		defaultObjests.addDataItem(dbGroup);

		DbDataObject dblFormParent = new DbDataObject();
		dblFormParent.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblFormParent.setStatus(svCONST.STATUS_VALID);
		dblFormParent.setVal("LINK_TYPE", "FORM_TYPE_PARENT");
		dblFormParent.setVal("link_type_description",
				"Link from form type to svarog object types, to signify which objects can have a form attached");
		dblFormParent.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_FORM_TYPE);
		dblFormParent.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_TABLE);
		defaultObjests.addDataItem(dblFormParent);

		DbDataObject dblFFieldParent = new DbDataObject();
		dblFFieldParent.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblFFieldParent.setStatus(svCONST.STATUS_VALID);
		dblFFieldParent.setVal("link_type_description",
				"Link between form field and form type to signify which fields should be shown on a form");
		dblFFieldParent.setVal("LINK_TYPE", "FORM_FIELD_LINK");
		dblFFieldParent.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_FORM_TYPE);
		dblFFieldParent.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
		defaultObjests.addDataItem(dblFFieldParent);

		// BATCH EXECUTION ENGINE LINKS

		DbDataObject dbl_bee_1 = new DbDataObject();
		dbl_bee_1.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dbl_bee_1.setStatus(svCONST.STATUS_VALID);
		dbl_bee_1.setVal("LINK_TYPE", "LINK_JOB_TASK");
		dbl_bee_1.setVal("LINK_TYPE_DESCRIPTION", "Link between job and task");
		dbl_bee_1.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_JOB_TYPE);
		dbl_bee_1.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_TASK_TYPE);

		defaultObjests.addDataItem(dbl_bee_1);

		DbDataObject dbl_bee_2 = new DbDataObject();
		dbl_bee_2.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dbl_bee_2.setStatus(svCONST.STATUS_VALID);
		dbl_bee_2.setVal("LINK_TYPE", "LINK_FILE");
		dbl_bee_2.setVal("LINK_TYPE_DESCRIPTION", "Link between job and file");
		dbl_bee_2.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_JOB_TYPE);
		dbl_bee_2.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_FILE);

		defaultObjests.addDataItem(dbl_bee_2);

		DbDataObject dbl_bee_3 = new DbDataObject();
		dbl_bee_3.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dbl_bee_3.setStatus(svCONST.STATUS_VALID);
		dbl_bee_3.setVal("LINK_TYPE", "LINK_JOB_OBJECT_WITH_TASK");
		dbl_bee_3.setVal("LINK_TYPE_DESCRIPTION", "Link between job_object and task");
		dbl_bee_3.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_JOB_OBJECT);
		dbl_bee_3.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_TASK);

		defaultObjests.addDataItem(dbl_bee_3);

		// RULE ENGINE LINK
		// DbDataArray arrActionFileTypes = new DbDataArray();

		DbDataObject dbaf1 = new DbDataObject();
		dbaf1.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dbaf1.setStatus(svCONST.STATUS_VALID);
		dbaf1.setVal("LINK_TYPE", "LINK_FILE");
		dbaf1.setVal("LINK_TYPE_DESCRIPTION", "file link to table");
		dbaf1.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_ACTION);
		dbaf1.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_FILE);

		defaultObjests.addDataItem(dbaf1);

		DbDataObject dblAdminHQ = new DbDataObject();
		dblAdminHQ.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblAdminHQ.setStatus(svCONST.STATUS_VALID);
		dblAdminHQ.setVal("LINK_TYPE", "POA");
		dblAdminHQ.setVal("LINK_TYPE_DESCRIPTION", "Power of attorney link for user on behalf of a OU");
		dblAdminHQ.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_USER);
		dblAdminHQ.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_ORG_UNITS);
		defaultObjests.addDataItem(dblAdminHQ);

		// Parameters link
		DbDataObject dblinkParam = new DbDataObject();
		dblinkParam.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblinkParam.setStatus(svCONST.STATUS_VALID);
		dblinkParam.setVal("LINK_TYPE", "LINK_CONFOBJ_WITH_PARAM_TYPE");
		dblinkParam.setVal("LINK_TYPE_DESCRIPTION", "link job type with param type");
		dblinkParam.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_JOB_TYPE);
		dblinkParam.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_PARAM_TYPE);
		defaultObjests.addDataItem(dblinkParam);

		// Notification link
		DbDataObject dblNotificationUser = new DbDataObject();
		dblNotificationUser.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblNotificationUser.setStatus(svCONST.STATUS_VALID);
		dblNotificationUser.setVal("LINK_TYPE", "LINK_NOTIFICATION_USER");
		dblNotificationUser.setVal("LINK_TYPE_DESCRIPTION", "link notification and user");
		dblNotificationUser.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_NOTIFICATION);
		dblNotificationUser.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_USER);
		defaultObjests.addDataItem(dblNotificationUser);

		// Notification link 2
		DbDataObject dblNotificationUserGroup = new DbDataObject();
		dblNotificationUserGroup.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblNotificationUserGroup.setStatus(svCONST.STATUS_VALID);
		dblNotificationUserGroup.setVal("LINK_TYPE", "LINK_NOTIFICATION_GROUP");
		dblNotificationUserGroup.setVal("LINK_TYPE_DESCRIPTION", "link notification and user group");
		dblNotificationUserGroup.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_NOTIFICATION);
		dblNotificationUserGroup.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_GROUP);
		defaultObjests.addDataItem(dblNotificationUserGroup);

		// BATCH LINK
		DbDataObject dblPrint = new DbDataObject();
		dblPrint.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dblPrint.setStatus(svCONST.STATUS_VALID);
		dblPrint.setVal("LINK_TYPE", "LINK_JOB_UI_STRUCT");
		dblPrint.setVal("LINK_TYPE_DESCRIPTION", "job_type link to ui_struct");
		dblPrint.setVal("LINK_OBJ_TYPE_1", svCONST.OBJECT_TYPE_JOB_TYPE);
		dblPrint.setVal("LINK_OBJ_TYPE_2", svCONST.OBJECT_TYPE_UI_STRUCTURE_SOURCE);
		defaultObjests.addDataItem(dblPrint);
		/*
		 * // link conversation and user DbDataObject dblConversationUser = new
		 * DbDataObject();
		 * dblNotificationUser.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		 * dblNotificationUser.setStatus(svCONST.STATUS_VALID);
		 * dblNotificationUser.setVal("LINK_TYPE",
		 * "LINK_CONVERSATION_ATTACHMENT");
		 * dblNotificationUser.setVal("LINK_TYPE_DESCRIPTION", "link user");
		 * dblNotificationUser.setVal("LINK_OBJ_TYPE_1",
		 * svCONST.OBJECT_TYPE_CONVERSATION);
		 * dblNotificationUser.setVal("LINK_OBJ_TYPE_2",
		 * svCONST.OBJECT_TYPE_USER);
		 * defaultObjests.addDataItem(dblConversationUser);
		 * 
		 * // link conversation and org unit DbDataObject dblConversationOrgUnit
		 * = new DbDataObject();
		 * dblConversationOrgUnit.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		 * dblConversationOrgUnit.setStatus(svCONST.STATUS_VALID);
		 * dblConversationOrgUnit.setVal("LINK_TYPE",
		 * "LINK_CONVERSATION_ATTACHMENT");
		 * dblConversationOrgUnit.setVal("LINK_TYPE_DESCRIPTION",
		 * "link org unit"); dblConversationOrgUnit.setVal("LINK_OBJ_TYPE_1",
		 * svCONST.OBJECT_TYPE_CONVERSATION);
		 * dblConversationOrgUnit.setVal("LINK_OBJ_TYPE_2",
		 * svCONST.OBJECT_TYPE_ORG_UNITS);
		 * defaultObjests.addDataItem(dblConversationOrgUnit);
		 */
	}

	public static Long addDefaultUnitsUsers(DbDataArray arrWF, Long svObjectId) {

		// HQ plus default link
		DbDataObject dboHQ = new DbDataObject();
		dboHQ.setObjectType(svCONST.OBJECT_TYPE_ORG_UNITS);
		dboHQ.setObjectId(svCONST.OBJECT_ID_HEADQUARTER);
		dboHQ.setVal("ORG_UNIT_TYPE", "HEADQUARTER");
		dboHQ.setVal("NAME", "HEADQUARTER");
		arrWF.addDataItem(dboHQ);

		// default user groups
		DbDataObject dboAdminGroup = new DbDataObject();
		dboAdminGroup.setObjectType(svCONST.OBJECT_TYPE_GROUP);
		dboAdminGroup.setObjectId(svCONST.SID_ADMINISTRATORS);
		dboAdminGroup.setVal("GROUP_TYPE", "ADMINISTRATORS");
		dboAdminGroup.setVal("GROUP_UID", svCONST.SID_ADMINISTRATORS_UID);
		dboAdminGroup.setVal("GROUP_NAME", "ADMINISTRATORS");
		dboAdminGroup.setVal("E_MAIL", "admin@admin.com");
		dboAdminGroup.setVal("GROUP_SECURITY_TYPE", "FULL");

		DbDataObject dboUserGroup = new DbDataObject();
		dboUserGroup.setObjectType(svCONST.OBJECT_TYPE_GROUP);
		dboUserGroup.setObjectId(svCONST.SID_USERS);
		dboUserGroup.setVal("GROUP_TYPE", "USERS");
		dboUserGroup.setVal("GROUP_UID", svCONST.SID_USERS_UID);
		dboUserGroup.setVal("GROUP_NAME", "USERS");
		dboUserGroup.setVal("E_MAIL", "user@user.com");
		dboUserGroup.setVal("GROUP_SECURITY_TYPE", "POA");

		arrWF.addDataItem(dboAdminGroup);
		arrWF.addDataItem(dboUserGroup);

		DbDataObject dboAdminUser = new DbDataObject();
		// dbo.setRepo_name(dbt.getDbRepoName());
		// dbo.setTable_name(dbt.getDbRepoName() + "_tables");
		// dbo.setSchema(dbt.getDbSchema());
		dboAdminUser.setObjectType(svCONST.OBJECT_TYPE_USER);
		dboAdminUser.setObjectId(svObjectId);
		svObjectId++;
		dboAdminUser.setVal("USER_TYPE", "INTERNAL");
		dboAdminUser.setVal("USER_UID", UUID.randomUUID().toString());
		dboAdminUser.setVal("USER_NAME", "ADMIN");
		dboAdminUser.setVal("FIRST_NAME", "ADMIN");
		dboAdminUser.setVal("LAST_NAME", "ADMIN");
		dboAdminUser.setVal("PIN", "67831213");
		dboAdminUser.setVal("E_MAIL", "admin@admin.com");
		dboAdminUser.setVal("PASSWORD_HASH", SvUtil.getMD5(SvUtil.getMD5("welcome").toUpperCase()));
		arrWF.addDataItem(dboAdminUser);

		return svObjectId;
	}

	/**
	 * Method to process a subdir in the Svarog working directory in order to
	 * find all potential IDbInit instances which provide custom objects to the
	 * configurator
	 * 
	 * @param subDir
	 *            The sub directory to be traversed
	 * @param svObjectId
	 *            The current max object id
	 * @param defaultCodes
	 *            the list of default codes used for decoding
	 * @return The updated max object id as result of the custom DbInit
	 *         processing
	 */
	static Long saveCustomToJson(String subDir, Long svObjectId, DbDataArray defaultCodes,
			DbDataArray customObjestsAll) {
		// DbDataArray defaultObjests = new DbDataArray();
		DbDataArray customObjests = new DbDataArray();
		StringBuilder errMsg = new StringBuilder();

		File customFolder = new File(subDir);
		File[] customJars = customFolder.listFiles();
		if (customJars != null) {
			Arrays.sort(customJars);
			for (int i = 0; i < customJars.length; i++) {
				if (customJars[i].getName().endsWith(".jar")) {
					log4j.info("Trying to load IDbInit from jar: " + customJars[i].getName());
					ArrayList<Object> dbi = DbInit.loadClassFromJar(customJars[i].getAbsolutePath(), IDbInit.class);
					if (dbi.size() > 0)
						log4j.info("Found IDbInit instance in jar: " + customJars[i].getName());
					customObjests.getItems().clear();
					for (Object idb : (ArrayList<Object>) dbi) {
						svObjectId = dbTables2DbDataArray(((IDbInit) idb).getCustomObjectTypes(), customObjests,
								defaultCodes, svObjectId, errMsg);
						if (!errMsg.toString().equals("")) {
							log4j.error("Error creating DbDataArray from custom IDbInit:" + customJars[i].getName()
									+ "." + errMsg.toString());
							return svObjectId;
						}

						for (DbDataObject dboCustom : ((IDbInit) idb).getCustomObjectInstances()) {
							for (DbDataObject dbl : defaultCodes.getItems()) {

								if (dboCustom.getObjectType().equals(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE)) {
									String codeVal = (String) dboCustom.getVal("code_list_id");
									if (dbl.getVal("CODE_VALUE").equals(codeVal))
										dboCustom.setVal("code_list_id", dbl.getObjectId());
								}

							}
							customObjests.addDataItem(dboCustom);
						}

					}
					String errStr = "";
					if (customObjests.size() > 0) {
						errStr = saveMasterJson(SvConf.getConfPath() + SvarogInstall.masterRecordsPath + "4" + i + ". "
								+ customJars[i].getName().replace(".jar", ".json"), customObjests, true);
						customObjestsAll.getItems().addAll(customObjests.getItems());
					}
					if (!errStr.equals("")) {
						log4j.error("Error saving DbDataArray to file from custom IDbInit:" + customJars[i].getName()
								+ "." + errStr.toString());
						return svObjectId;
					}

				}
			}
		}
		return svObjectId;
	}

	/**
	 * Method that creates all master records for the tables according to the
	 * array created by getMasterObjects();
	 * 
	 * @param dbtList
	 *            ArraList holding all DbDataTable objects from the master
	 *            records should be created
	 * @return Description of the error if any
	 */
	public static String createJsonMasterTableRecords(ArrayList<DbDataTable> dbtList) {

		StringBuilder errMsg = new StringBuilder();
		DbDataArray defaultCodes = new DbDataArray();
		DbDataArray defaultObjests = new DbDataArray();
		DbDataArray customObjests = new DbDataArray();
		DbDataArray customObjestsAll = new DbDataArray();
		Long svObjectId = prepDefaultCodeList(defaultCodes);
		String retval = "";

		svObjectId = dbTables2DbDataArray(dbtList, defaultObjests, defaultCodes, svObjectId, errMsg);

		if (!errMsg.toString().equals(""))
			return errMsg.toString();

		// load custom objects as well as from the OSGI bundles dir
		svObjectId = saveCustomToJson("custom/", svObjectId, defaultCodes, customObjests);
		customObjestsAll.getItems().addAll(customObjests.getItems());

		svObjectId = saveCustomToJson(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY), svObjectId, defaultCodes,
				customObjests);
		customObjestsAll.getItems().addAll(customObjests.getItems());

		DbDataArray arrWF = new DbDataArray();
		svObjectId = addDefaultUnitsUsers(arrWF, svObjectId);

		addDefaultLinkTypes(defaultObjests);

		retval += saveMasterJson(SvConf.getConfPath() + SvarogInstall.masterRecordsPath + "40. master_records.json",
				defaultObjests, true);

		retval += saveMasterJson(SvConf.getConfPath() + SvarogInstall.masterRecordsPath + SvarogInstall.usersFile,
				arrWF, true);

		prepareACLs(defaultObjests, customObjestsAll);

		updateFileLists();
		return retval;
	}

	static private void parseDefaultCodes(JsonArray obj, ArrayList<DbDataObject> items, Long startingObjId,
			JsonElement parentCodeValue) {
		Long parent_id = 0L;
		if (items.size() > 0)
			parent_id = items.get(items.size() - 1).getObjectId();

		// the starting object id
		Long object_id = startingObjId;

		int sort = 0;
		for (int i = 0; i < obj.size(); i++) {
			if (items.size() > 0)
				object_id = startingObjId + items.size();
			// String key = keys.nextElement();
			DbDataObject dbo = new DbDataObject();
			// dbo.setRepo_name("{MASTER_REPO}");
			// dbo.setTable_name("{MASTER_REPO}" + "_codes");
			// dbo.setSchema("{DEFAULT_SCHEMA}");
			dbo.setStatus(svCONST.STATUS_VALID);

			dbo.setObjectId(object_id);
			dbo.setObjectType(svCONST.OBJECT_TYPE_CODE);

			dbo.setParentId(parent_id);
			dbo.setDtInsert(new DateTime("2000-01-01T00:00:00"));
			dbo.setDtDelete(SvConf.MAX_DATE);
			JsonObject inObj = obj.get(i).getAsJsonObject();

			if (inObj.get("user_code").getAsString().equals("UNQ_LEVEL"))
				dbo.setObjectId(svCONST.CODES_UNIQUE_LEVEL);
			else if (inObj.get("user_code").getAsString().equals("FIELD_TYPES"))
				dbo.setObjectId(svCONST.CODES_FIELD_DATATYPES);
			else if (inObj.get("user_code").getAsString().equals("FILE_TYPES"))
				dbo.setObjectId(svCONST.CODES_FILE_TYPES);

			dbo.setVal("code_value", inObj.get("user_code").getAsString());
			dbo.setVal("label_code", inObj.get("label_code").getAsString());
			dbo.setVal("sort_order", sort);

			dbo.setVal("PARENT_CODE_VALUE", parentCodeValue != null ? parentCodeValue.getAsString() : null);

			// System.out.println(inObj.get("user_code").getAsString() + " objid
			// :" + object_id + " parid : " + parent_id);
			JsonElement children = inObj.get("children");
			items.add(dbo);
			if (children != null) {
				parseDefaultCodes(children.getAsJsonArray(), items, startingObjId, inObj.get("user_code"));
			}
			sort++;

		}
		return;
	}

	private static void mergeChildrenCodes(JsonObject jobj, JsonObject customJobj) {
		JsonElement customChildren = customJobj.get("children");
		JsonElement coreChildren = jobj.get("children");

		if (customChildren != null && customChildren.isJsonArray()) {
			JsonArray custChildArr = customChildren.getAsJsonArray();

			if (coreChildren == null) {
				jobj.add("children", custChildArr);
				return;
			} else {
				JsonArray coreChildArr = coreChildren.getAsJsonArray();
				for (int i = 0; i < custChildArr.size(); i++) {
					JsonObject childObj = custChildArr.get(i).getAsJsonObject();
					JsonObject coreChild = null;
					String userCode = childObj.get("user_code").getAsString();
					for (int j = 0; j < coreChildArr.size(); j++) {
						coreChild = coreChildArr.get(j).getAsJsonObject();
						if (coreChild.get("user_code").getAsString().equals(userCode))
							break;
						else
							coreChild = null;

					}

					if (coreChild == null) {
						coreChildArr.add(childObj);
					} else {
						mergeChildrenCodes(coreChild, childObj);
					}

				}
			}
		}
	}

	/**
	 * Method to load codes from either 'custom/' directory or OSGI bundles
	 * AUTODEPLOY DIR
	 * 
	 * @param jarPath
	 *            The path of the jar file containining
	 *            "labels/codes.properties"
	 * @param jCodes
	 *            The
	 * @throws IOException
	 */
	private static void loadCodesFromCustom(String jarPath, JsonObject jCodes) throws IOException {
		InputStream customIs = null;
		Gson gson = (new GsonBuilder().setPrettyPrinting().create());
		try {
			customIs = DbInit.loadCustomResources(jarPath, "labels/codes.properties");
			if (customIs != null) {
				String jsonCustom = IOUtils.toString(customIs, "UTF-8");
				JsonObject customJobj = gson.fromJson(jsonCustom, JsonElement.class).getAsJsonObject();
				mergeChildrenCodes(jCodes, customJobj);
				log4j.info("Loading 'labels/codes.properties' from custom jar:" + jarPath);
			}
		} catch (Exception e1) {
			log4j.error("Error loading codes from custom jar:" + jarPath);
			e1.printStackTrace();
			return;
		} finally {
			try {
				if (customIs != null)
					customIs.close();
			} catch (IOException e) {
				log4j.error("Can not close input stream from custom jar:" + jarPath, e);
			}
		}

	}

	/**
	 * Method to create a new link type
	 * 
	 * @param linkType
	 *            The link type mnemonic
	 * @param linkDesc
	 *            The description of the link type
	 * @param objectTypeId1
	 *            The left hand side object type id
	 * @param objectTypeId2
	 *            The right hand side object type id
	 * @param deferSecurity
	 *            Flag to defer the security checks
	 * @param svw
	 *            The SvWriter instance to be used for saving the link
	 * @return A DbDataObject link descriptor
	 * @throws SvException
	 *             Pass-thru any underlying exception
	 */
	public static DbDataObject createLinkType(String linkType, String linkDesc, Long objectTypeId1, Long objectTypeId2,
			Boolean deferSecurity, SvWriter svw) throws SvException {
		DbDataObject dboLinkType = new DbDataObject();
		dboLinkType.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
		dboLinkType.setVal("link_type", linkType);
		dboLinkType.setVal("link_type_description", linkDesc);
		dboLinkType.setVal("link_obj_type_1", objectTypeId1);
		dboLinkType.setVal("link_obj_type_2", objectTypeId2);
		dboLinkType.setVal("defer_security", deferSecurity);
		svw.saveObject(dboLinkType);
		svw.dbCommit();
		SvCore.initSvCore(true);
		return dboLinkType;
	}

	private static Long prepareCodes(Long startingObjId, String[] codesStr) {
		DbDataArray arr = new DbDataArray();

		try {

			String codesPath = SvarogInstall.masterCodesPath + "codes.properties";
			// File baseCodes = new File();

			InputStream fis = DbInit.class.getResourceAsStream("/" + codesPath);
			if (fis == null)
				fis = ClassLoader.getSystemClassLoader().getResourceAsStream(codesPath);

			String json = IOUtils.toString(fis, "UTF-8");

			Gson gson = (new GsonBuilder().setPrettyPrinting().create());
			JsonObject jCodes = gson.fromJson(json, JsonElement.class).getAsJsonObject();

			// load codes from the custom dir
			File customFolder = new File("custom/");
			File[] customJars = customFolder.listFiles();
			if (customJars != null) {
				for (int i = 0; i < customJars.length; i++) {
					if (customJars[i].getName().endsWith(".jar"))
						loadCodesFromCustom(customJars[i].getAbsolutePath(), jCodes);
				}
			}

			// load codes from the OSGI bundles dir too
			customFolder = new File(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY));
			customJars = customFolder.listFiles();
			if (customJars != null) {
				for (int i = 0; i < customJars.length; i++) {
					if (customJars[i].getName().endsWith(".jar"))
						loadCodesFromCustom(customJars[i].getAbsolutePath(), jCodes);
				}
			}

			// custom codes loading finished
			ArrayList<DbDataObject> items = new ArrayList<DbDataObject>();

			JsonElement children = jCodes.get("children");
			if (children != null) {
				parseDefaultCodes(children.getAsJsonArray(), items, startingObjId, null);
			}
			if (items.size() > 0)
				startingObjId = items.get(items.size() - 1).getObjectId();
			arr.setItems(items);

			// System.out.println("labels/"+entry.getValues().get("locale_id")+"Labels");
			;
			JsonObject obj = ((Jsonable) arr).toJson();
			String jsonCodes = gson.toJson(obj);
			// save the array to disk
			SvUtil.saveStringToFile(SvConf.getConfPath() + SvarogInstall.masterRecordsPath + "30. master_codes.json",
					jsonCodes);
			codesStr[0] = jsonCodes;
		} catch (Exception e) {
			log4j.error("Error processing codes: ", e);
		}
		updateFileLists();
		return startingObjId;

	}

	static String generateGrid() {
		String retval = "";
		System.out.println("Generating Tiles for system boundary");

		String geoJSONBounds = null;
		InputStream is = null;
		try {
			is = DbInit.class
					.getResourceAsStream(SvConf.getConfPath() + SvarogInstall.masterSDIPath + "/boundary.json");
			if (is == null) {
				String path = "./" + SvConf.getConfPath() + SvarogInstall.masterSDIPath + "/boundary.json";
				is = new FileInputStream(path);
			}
			if (is != null) {
				geoJSONBounds = IOUtils.toString(is);
			}
		} catch (IOException e) {
			retval = "ERROR System bounds can not be configured";
			e.printStackTrace();
			return retval;
		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

		if (geoJSONBounds == null) {
			retval = "Can't read system boundary polygon";
		}

		GeoJsonWriter jtsWriter = new GeoJsonWriter();
		GeoJsonReader jtsReader = new GeoJsonReader();

		int row = 0;
		int col = 0;
		try {
			Geometry geo = jtsReader.read(geoJSONBounds);
			Envelope env = geo.getEnvelopeInternal();
			env.expandBy(10);

			Envelope envGrid = new Envelope(java.lang.Math.round(env.getMinX()), java.lang.Math.round(env.getMaxX()),
					java.lang.Math.round(env.getMinY()), java.lang.Math.round(env.getMaxY()));

			ArrayList<Geometry> gridList = new ArrayList<Geometry>();
			Envelope currentGridItem = null;

			Geometry polygon = null;
			double currentMinY = envGrid.getMinY();
			double currentMaxX = envGrid.getMinX();

			String jtsJson = null;

			Boolean isFinished = false;

			while (!isFinished) {

				currentGridItem = new Envelope(currentMaxX, currentMaxX + SvConf.getSdiGridSize() * 1000, currentMinY,
						currentMinY + SvConf.getSdiGridSize() * 1000);
				currentMinY = currentGridItem.getMinY();
				currentMaxX = currentGridItem.getMaxX();

				polygon = SvUtil.sdiFactory.toGeometry(currentGridItem);
				if (!polygon.disjoint(geo)) {
					// polygon = polygon.intersection(geo);
					if (polygon.getArea() > 1) {
						polygon.setUserData(row + ":" + col + "-" + (!polygon.within(geo)));
						gridList.add(polygon);
						// System.out.println("INSERT INTO
						// public.\"administrativni granici komplet\"
						// (name,geom) "
						// + "VALUES
						// ('"+row+":"+col+"',st_setSrid(st_multi(st_geomfromgeojson('"+jtsWriter.write(polygon)+"')),6316));");

					}
				}
				col++;
				if (currentMaxX > envGrid.getMaxX()) {
					currentMinY = currentGridItem.getMaxY();
					currentMaxX = envGrid.getMinX();
					row++;
					col = 0;
					if (currentMinY > envGrid.getMaxY())
						isFinished = true;
				}

			}
			Geometry[] garr = new Geometry[gridList.size()];
			gridList.toArray(garr);
			GeometryCollection gcl = SvUtil.sdiFactory.createGeometryCollection(garr);
			jtsWriter.setUseFeatureType(true);
			jtsJson = jtsWriter.write(gcl);
			SvUtil.saveStringToFile(SvConf.getConfPath() + SvarogInstall.masterSDIPath + SvarogInstall.sdiGridFile,
					jtsJson);
			System.out.println("Number of tiles written:" + gridList.size());
			System.out.println("Generating Tiles finished successfully (" + SvConf.getConfPath()
					+ SvarogInstall.masterSDIPath + SvarogInstall.sdiGridFile + ")");

		} catch (com.vividsolutions.jts.io.ParseException e) {
			retval = "Error generating grid";
			e.printStackTrace();
		}
		return retval;

	}

	/**
	 * Method for loading all custom DbInit instances from external jars
	 * 
	 * @param pathToJar
	 *            The path to the location of the JAR
	 * @return ArrayList holding all classes implementing IDbInit in the
	 *         external jar
	 */
	public static ArrayList<Object> loadClassFromJar(String pathToJar, Class<?> clazz) {
		ArrayList<Object> dbi = new ArrayList<>();

		JarFile jarFile;
		if (pathToJar != null && !pathToJar.equals("")) {
			try {
				jarFile = new JarFile(pathToJar);
				@SuppressWarnings("rawtypes")
				Enumeration e = jarFile.entries();

				URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
				URLClassLoader cl = URLClassLoader.newInstance(urls, DbInit.class.getClassLoader());

				while (e.hasMoreElements()) {
					JarEntry je = (JarEntry) e.nextElement();
					if (je.isDirectory() || !je.getName().endsWith(".class")) {
						continue;
					}
					// -6 because of .class
					String className = je.getName().substring(0, je.getName().length() - 6);
					className = className.replace('/', '.');
					try {
						Class<?> c = cl.loadClass(className);
						if (clazz.isAssignableFrom(c)) {
							dbi.add(c.newInstance());

						}
					} catch (java.lang.NoClassDefFoundError | java.lang.IllegalAccessError | java.lang.VerifyError ex) {
						if (log4j.isDebugEnabled())
							log4j.trace("Error loading class", e);
					}

				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return dbi;
	}

	public static ISvarogExecutable loadCustomExecutor(String pathToJar) {
		ISvarogExecutable dbi = null;

		JarFile jarFile;
		if (pathToJar != null && !pathToJar.equals("")) {
			try {
				jarFile = new JarFile(pathToJar);
				@SuppressWarnings("rawtypes")
				Enumeration e = jarFile.entries();

				URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
				URLClassLoader cl = URLClassLoader.newInstance(urls);

				while (e.hasMoreElements()) {
					JarEntry je = (JarEntry) e.nextElement();
					if (je.isDirectory() || !je.getName().endsWith(".class")) {
						continue;
					}
					// -6 because of .class
					String className = je.getName().substring(0, je.getName().length() - 6);
					className = className.replace('/', '.');
					Class<?> c = cl.loadClass(className);
					if (ISvarogExecutable.class.isAssignableFrom(c)) {
						dbi = ((ISvarogExecutable) c.newInstance());
						break;
					}

				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return dbi;
	}

	public static InputStream loadCustomResources(String pathToJar, String resourceName) {

		JarFile jarFile;
		InputStream is = null;
		if (pathToJar != null && !pathToJar.equals("")) {
			try {
				jarFile = new JarFile(pathToJar);
				@SuppressWarnings("rawtypes")
				Enumeration e = jarFile.entries();

				// URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
				// URLClassLoader cl = URLClassLoader.newInstance(urls);
				while (e.hasMoreElements()) {
					JarEntry je = (JarEntry) e.nextElement();
					if (je.getName().equals(resourceName)) {
						is = jarFile.getInputStream(je);
						break;
					}

				}

			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return is;
	}

}