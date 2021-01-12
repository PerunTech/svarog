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

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.felix.main.AutoProcessor;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.prtech.svarog.SvConf.SvDbType;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataField;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbDataTable;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.DboFactory;
import com.prtech.svarog_common.SvCharId;
import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvConfiguration.UpdateType;
import com.prtech.svarog_common.DbDataField.DbFieldType;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;

/**
 * Main class for managing Svarog installation and upgrades
 *
 * @author PR01
 */
public class SvarogInstall {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvarogInstall.class);

	static final String JDBC_ERR = "Can't close JDBC connection";
	static final String TABLE_NAME = "table_name";
	static final String UNEXPECTED_EX = "Unexpected exception:";
	/**
	 * Path of the JSON file containing list of system Locales
	 */
	static final String LOCALE_PATH = "/com/prtech/svarog/json/src/master_locales.bjson";
	/**
	 * The map of fields in the repo table
	 */
	static private LinkedHashMap<String, DbDataField> repoFieldListDb = new LinkedHashMap<String, DbDataField>();

	/**
	 * Local variable holding all system locales
	 */
	static private DbDataArray sysLocales = null;

	/**
	 * Flag to mark if there is valid configuration in the database
	 */
	private static Boolean mIsAlreadyInstalled = null;

	/**
	 * String containing the type of operation install or upgrade
	 */
	static String operation = null;

	/**
	 * Flag to mark if any existing upgrades should be overwritten
	 */
	static boolean forceUpgrade = false;

	/**
	 * Flag to mark if the installation process should include running a
	 * configuration test
	 */
	static boolean runConfigurationTest = false;

	static ArrayList<Option> sysCoreOpts = new ArrayList<Option>();

	/**
	 * List of SvConfigurations available in the system
	 */
	static ArrayList<ISvConfiguration> iSvCfgs = null;

	public static final String labelsFilePrefix = "20. master_labels_";
	public static final String codesFile = "30. master_codes.json";
	public static final String usersFile = "999. default_users.json";
	public static final String masterRecordsPath = "records/";
	public static final String masterDbtPath = "config/";
	public static final String masterSDIPath = "sdi/";
	public static final String masterLabelsPath = "labels/";
	public static final String masterSecurityPath = "security/";
	public static final String masterCodesPath = "com/prtech/svarog/codes/";
	public static final String fileListName = "file_list.txt";
	public static final String sdiGridFile = "grid.json";

	public static final String aclFile = "acl.json";
	public static final String aclSidFile = "acl_sid.json";

	static void writeToScreen(String message) {
		Console console = System.console();
		if (console != null) {
			console.writer().println(message);
		} else {
			log4j.info(message);
		}
	}

	/**
	 * Main entry point of the svarog install Supported operations: JSON - will
	 * generate the svarog JSON structutre MIGRATE_FILESTORE - provide means to
	 * migrate file store data from DB to disk structure INSTALL - Install a brand
	 * new svarog DB INSTALL_DROP - Use only for fast recreation of tables when
	 * developing on Postgres UPGRADE - Upgrade the currently configured svarog
	 * database
	 * 
	 * @param args Command line list of arguments
	 */
	public static int main(String[] args) {
		CommandLineParser parser = new DefaultParser();
		Options options = getOptions();
		int returnStatus = 0;
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			if (line.hasOption("ct"))
				runConfigurationTest = true;
			// if the flag -d or --daemon has been set, simply return the status
			if (line.hasOption("dm")) {
				return SvarogDaemon.getDaemonStatus();
			} else // if the help option is set, just print help and do nothing
			if (line.hasOption("h")) {
				// print the value of block-size
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Svarog", options);
			} else {
				if (validateCommandLine(line)) {
					if (line.hasOption("j"))
						returnStatus = generateJsonCfg();
					else if (line.hasOption("gd"))
						returnStatus = generateGrid();
					else if (line.hasOption("i")) {
						returnStatus = validateInstall();
						if (returnStatus == 0 && line.hasOption("d"))
							returnStatus = SvarogInstall.cleanDb() == true ? 0 : -1;
						if (returnStatus == 0) {
							if (line.hasOption("a"))
								returnStatus = generateJsonCfg();
							if (returnStatus == 0)
								returnStatus = upgradeSvarog(false);
						}
					} else if (line.hasOption("u")) {
						returnStatus = validateInstall();
						if (returnStatus == 0 && line.hasOption("a"))
							returnStatus = generateJsonCfg();
						if (returnStatus == 0)
							returnStatus = runUpgrade(line);
					} else if (line.hasOption("m")) {
						returnStatus = manageSecurity(line);
					} else if (line.hasOption("o")) {
						HashMap<DbDataObject, DbDataObject> misconfigured = repoStat();
						if (line.hasOption("migrate-objects")) {
							returnStatus = repoSync(misconfigured);
						}
						if (line.hasOption("rebuild-index")) {
							refreshIndexes(line.getOptionValue("rebuild-index"), null);
						}

					}
				} else
					returnStatus = -2;
			}
		} catch (Exception exp) {
			log4j.error("Main thread raised unhandled exception.", exp);
			returnStatus = -2;
		}
		if (!SvCore.svDaemonRunning.get())
			System.exit(returnStatus);

		return returnStatus;

	}

	/**
	 * Method to test if svarog can connect to the database.
	 * 
	 * @return 0 if the connection is successful.
	 */
	private static int canConnectToDb() {
		int errStatus = -1;
		Connection conn = null;
		log4j.info("Validating connection to " + SvConf.getConnectionString());
		try {
			conn = SvConf.getDBConnection();
			if (conn != null)
				errStatus = 0;
		} catch (Exception e) {
			log4j.info("Can't connect to target database. Check svarog.properties configuration.");
			log4j.debug("Connection exception:", e);

		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					log4j.debug("Connection wont close:", e);
				}
		}
		return errStatus;
	}

	/**
	 * Method to validate if the prerequisites for the install are satisfied.
	 * 
	 * @return
	 */
	private static int validateInstall() {
		int errStatus = canConnectToDb();
		if (SvConf.isSdiEnabled()) {
			GeometryCollection c = SvGrid.loadGridFromJson("conf/sdi/grid.json");
			if (c == null)
				errStatus = -3;
		}
		return errStatus;
	}

	/**
	 * Method to parse the system boundary in json format and generate the grid of
	 * tiles for svarog SDI usage.
	 * 
	 * @return -1 in case of system error.
	 */
	private static int generateGrid() {
		Geometry boundary = DbInit.getSysBoundaryFromJson();
		GeometryCollection grid = SvGrid.generateGrid(boundary, SvConf.getSdiGridSize());
		boolean result = SvGrid.saveGridToMasterFile(grid);
		if (!result) {
			log4j.error("Error generating system tile grid.");
			return -1;
		}
		return 0;
	}

	private static String getOptionFromCmd(String optionName) {
		String retVal = null;
		Option opt = getOptions().getOption(optionName);
		if (opt != null) {
			retVal = getEntryFromCmd(optionName.toUpperCase(), opt.getDescription());
		} else
			writeToScreen("Error: Option " + optionName + " doesn't exist!");
		return retVal;
	}

	private static String getEntryFromCmd(String valueName, String valueDescription) {
		String retVal = null;
		Scanner scanner = null;
		try {
			scanner = new Scanner(System.in);
			writeToScreen("Please enter " + valueName + ", to " + valueDescription);
			writeToScreen(valueName + ":");
			retVal = scanner.nextLine();
		} finally {
			if (scanner != null)
				scanner.close();
		}
		retVal = retVal.trim();
		if (retVal.length() < 1)
			retVal = "";
		return retVal;
	}

	private static boolean getYNFromCmd(String valueDescription) {
		String retVal = "N/A";
		while (!(retVal.equalsIgnoreCase("N") || retVal.equalsIgnoreCase("Y"))) {
			retVal = getEntryFromCmd("Yes/No (Y/N)", valueDescription);
		}
		return retVal.equalsIgnoreCase("Y");
	}

	private static ArrayList<String> getUserPassFromCmd(CommandLine line) {
		ArrayList<String> values = new ArrayList<>();
		String userName = line.getOptionValue("user");
		if (userName == null || userName.isEmpty())
			userName = getOptionFromCmd("user");

		String password = line.getOptionValue("password");
		if (password == null || password.isEmpty())
			password = getOptionFromCmd("password");

		if (password != null && !password.isEmpty()) {
			String passwordConfirm;
			writeToScreen("Please confirm the entered password.");
			passwordConfirm = getOptionFromCmd("password");
			if (!password.equals(passwordConfirm)) {
				writeToScreen("Passwords dont' match!");
			} else {
				values.add(userName);
				values.add(password);
			}

		}
		return values;
	}

	private static int updatePassword(CommandLine line) {
		String screenText = "Password update. Username:";
		ArrayList<String> userNPass = getUserPassFromCmd(line);
		if (userNPass.isEmpty())
			return -3;

		String userName = userNPass.get(0);
		String password = userNPass.get(1);

		try (SvWriter svw = new SvWriter(); SvSecurity svs = new SvSecurity(svw);) {

			DbDataObject user = svs.getUser(userName);
			if (line.hasOption("p"))
				password = SvUtil.getMD5(password).toUpperCase();

			svs.createUser(userName, password, (String) user.getVal(Sv.FIRST_NAME), (String) user.getVal("last_name"),
					(String) user.getVal("e_mail"), (String) user.getVal("pin"), (String) user.getVal("tax_id"),
					(String) user.getVal("user_type"), user.getStatus(), true);
			writeToScreen(screenText + userName);
		} catch (SvException ex) {
			writeToScreen(Sv.ERROR + screenText + userName);
			writeToScreen(((SvException) ex).getFormattedMessage());
		} catch (Exception ex) {
			writeToScreen(Sv.ERROR + screenText + userName);
			writeToScreen(UNEXPECTED_EX);
			log4j.error(Sv.ERROR + screenText + userName, ex);

		}
		return 0;
	}

	private static int updateUser(CommandLine line) {
		int returnStatus = 0;
		String userName = line.getOptionValue("user");
		if (userName == null || userName == "")
			userName = getOptionFromCmd("user");

		String firstName = line.getOptionValue("name");
		if (firstName == null || firstName == "")
			firstName = getOptionFromCmd("name");

		String surName = line.getOptionValue("surname");
		if (surName == null || surName == "")
			surName = getOptionFromCmd("surname");

		String userPin = line.getOptionValue("pin");
		if (userPin == null || userPin == "")
			userPin = getOptionFromCmd("pin");

		String email = line.getOptionValue("email");
		if (email == null || email == "")
			email = getOptionFromCmd("email");

		String sidType = line.getOptionValue("sid_type");
		if (sidType == null || sidType == "")
			sidType = getOptionFromCmd("sid_type");

		String status = line.getOptionValue("status");
		if (status == null || status == "")
			status = getOptionFromCmd("status");

		if (status == "")
			status = null;

		writeToScreen("If you don't want to change the password, just hit enter");
		String password = line.getOptionValue("password");
		if (password == null || password == "")
			password = getOptionFromCmd("password");

		if (password != null && !password.isEmpty()) {
			String passwordConfirm;
			writeToScreen("Please confirm the entered password.");
			passwordConfirm = getOptionFromCmd("password");
			if (!password.equals(passwordConfirm)) {
				writeToScreen("Passwords dont' match!");
				return -3;
			}

		} else
			return -3;
		SvWriter svw = null;
		SvSecurity svs = null;
		String screenText = "User registration/update. Username:";
		try {
			svw = new SvWriter();
			svw.switchUser(Sv.ADMIN);
			svs = new SvSecurity(svw);
			if (line.hasOption("p"))
				password = SvUtil.getMD5(password).toUpperCase();

			svs.createUser(userName, password, firstName, surName, email, userPin, "", sidType, status, true);
			writeToScreen(screenText + userName + " successfully finished");
			returnStatus = 0;
		} catch (Exception ex) {
			writeToScreen(Sv.ERROR + screenText + userName);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error(Sv.ERROR + screenText + userName, ex);
			}

			returnStatus = -1;
		} finally {
			if (svw != null)
				svw.release();
			if (svs != null)
				svs.release();
		}
		return returnStatus;
	}

	private static int updateGroup(CommandLine line) {
		String screenText = "Group registration/update. Group name:";

		int returnStatus = 0;
		String groupName = line.getOptionValue("group");
		if (groupName == null || groupName == "")
			groupName = getOptionFromCmd("group");

		String securityType = line.getOptionValue("group_security");
		if (securityType == null || securityType == "")
			securityType = getOptionFromCmd("group_security");

		String email = line.getOptionValue("email");
		if (email == null || email == "")
			email = getOptionFromCmd("email");

		String sidType = line.getOptionValue("sid_type");
		if (sidType == null || sidType == "")
			sidType = getOptionFromCmd("sid_type");

		String status = line.getOptionValue("status");
		if (status == null || status == "")
			status = getOptionFromCmd("status");

		if (status == "")
			status = null;

		try (SvWriter svw = new SvWriter(); SvSecurity svs = new SvSecurity(svw);) {

			svw.switchUser(Sv.ADMIN);
			DbDataObject groupDbo = null;
			try {
				groupDbo = svs.getSid(groupName, svCONST.OBJECT_TYPE_GROUP);
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals("system.error.no_user_found"))
					throw (ex);
				else
					groupDbo = null;
			}
			if (groupDbo == null) {
				groupDbo = new DbDataObject(svCONST.OBJECT_TYPE_GROUP);
				groupDbo.setVal("group_uid", SvUtil.getUUID());
			}
			groupDbo.setStatus(status);
			groupDbo.setVal("group_type", sidType);
			groupDbo.setVal("group_name", groupName);
			groupDbo.setVal("group_name", groupName);
			groupDbo.setVal("group_security_type", securityType);
			groupDbo.setVal("e_mail", email);
			svw.saveObject(groupDbo);
			writeToScreen(screenText + groupName + " successfully finished");
			returnStatus = 0;
		} catch (Exception ex) {
			writeToScreen(Sv.ERROR + screenText + groupName);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error(Sv.ERROR + screenText + groupName, ex);
			}
			returnStatus = -1;
		}
		return returnStatus;
	}

	private static int exportGroupCfg(CommandLine line) {
		String group = line.getOptionValue("group");
		if (group == null || group == "")
			group = getOptionFromCmd("group");

		String exportFile = line.getOptionValue("export-group-cfg");
		if (exportFile == null || exportFile == "")
			exportFile = getOptionFromCmd("export-group-cfg");

		writeToScreen("Exporting group " + group + " config to " + exportFile);

		int returnStatus = 0;
		SvWriter svw = null;
		SvSecurity svs = null;
		try {

			svw = new SvWriter();
			svw.switchUser(Sv.ADMIN);
			svs = new SvSecurity(svw);
			DbDataObject groupDbo = null;
			groupDbo = svs.getSid(group, svCONST.OBJECT_TYPE_GROUP);
			DbDataArray acls = svw.getPermissions(groupDbo, svw);
			groupDbo.setVal(Sv.ACLS, acls);
			SvUtil.saveStringToFile(exportFile, groupDbo.toJson().toString());
			returnStatus = 0;
			writeToScreen("Successfully exported " + group + " config to " + exportFile);

		} catch (Exception ex) {
			writeToScreen("Error, can not get user groups for user " + group);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error("Error, can not get user groups for user " + group, ex);
			}
			returnStatus = -1;
		} finally {
			if (svw != null)
				svw.release();
			if (svs != null)
				svs.release();
		}
		return returnStatus;

	}

	private static int upgradeACLS(DbDataObject dboGroup, DbDataArray aclList, SvWriter svw) throws SvException {
		SvReader svr = null;
		int result = -1;
		try {
			DbDataArray newAcls = new DbDataArray();
			svr = new SvReader(svw);
			DbDataObject oldAcl = null;
			for (DbDataObject acl : aclList.getItems()) {
				oldAcl = svr.getObjectByUnqConfId((String) acl.getVal(Sv.LABEL_CODE),
						SvCore.getDbt(acl.getObjectType()));
				if (oldAcl == null) {
					acl.setObjectId(0L);
					acl.setPkid(0L);
					newAcls.addDataItem(acl);
				} else {
					if (shouldUpgradeConfig(oldAcl, acl, SvCore.getFields(acl.getObjectType()))) {
						acl.setObjectId(oldAcl.getObjectId());
						acl.setPkid(oldAcl.getObjectId());
						newAcls.addDataItem(acl);
					}
				}
			}
			svw.saveObject(newAcls);

			DbDataArray aclSids = svr.getSecurityObjects(dboGroup, svr, SvCore.getDbt(svCONST.OBJECT_TYPE_SID_ACL));
			aclSids.rebuildIndex(Sv.ACL_OBJECT_ID, true);

			DbDataArray newAclSids = new DbDataArray();
			for (DbDataObject acl : aclList.getItems()) {
				DbDataObject aclSid = aclSids.getItemByIdx(acl.getObjectId().toString());
				if (aclSid == null) {
					aclSid = new DbDataObject(svCONST.OBJECT_TYPE_SID_ACL);
					aclSid.setVal(Sv.SID_OBJECT_ID, dboGroup.getObjectId());
					aclSid.setVal(Sv.SID_TYPE_ID, dboGroup.getObjectType());
					aclSid.setVal(Sv.ACL_OBJECT_ID, acl.getObjectId());
					newAclSids.addDataItem(aclSid);
				}
			}
			svw.saveObject(newAclSids);
		} finally {
			if (svr != null)
				svr.release();
		}
		return result;
	}

	private static int importGroupCfg(CommandLine line) {
		String importFile = line.getOptionValue("import-group-cfg");
		if (importFile == null || importFile == "")
			importFile = getOptionFromCmd("import-group-cfg");

		JsonObject jObj = SvUtil.readJsonFromFile(importFile, false);
		if (jObj == null)
			return -1;

		int returnStatus = 0;
		String group = "UNKNOWN";
		try (SvWriter svw = new SvWriter(); SvSecurity svs = new SvSecurity(svw)) {
			DbDataObject dbo = new DbDataObject();
			DbDataObject groupDbo = null;
			dbo.fromJson(jObj);
			DbDataArray acls = (DbDataArray) dbo.getVal(Sv.ACLS);
			group = (String) dbo.getVal(Sv.GROUP_NAME);
			writeToScreen("Importing group " + group + " config from " + importFile);

			svw.switchUser(Sv.ADMIN);
			svs.switchUser(Sv.ADMIN);
			svw.setAutoCommit(false);
			try {
				groupDbo = svs.getSid(group, svCONST.OBJECT_TYPE_GROUP);
			} catch (SvException sv) {
				if (!sv.getLabelCode().equals("system.error.no_user_found"))
					throw (sv);
				groupDbo = null;
			}

			if (groupDbo != null) {
				if (shouldUpgradeConfig(groupDbo, dbo, SvCore.getFields(groupDbo.getObjectType()))) {
					if (!getYNFromCmd("confirm upgrade of the existing group " + group))
						return -1;
					else {
						dbo.setObjectId(groupDbo.getObjectId());
						dbo.setPkid(groupDbo.getPkid());
					}
				} else {
					log4j.info("Group already exists and its up to date");
					dbo = groupDbo;
				}
			} else {
				dbo.setObjectId(0L);
				dbo.setPkid(0L);
			}
			if (dbo.getIsDirty())
				svw.saveObject(dbo);

			log4j.info("Updating group ACLs");
			upgradeACLS(dbo, acls, svw);
			svw.dbCommit();
			returnStatus = 0;
			writeToScreen("Successfully imported " + group + " config from " + importFile);

		} catch (Exception ex) {
			writeToScreen("Error, can not get user groups for user " + group);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error("Error, can not get user groups for user " + group, ex);
			}
			returnStatus = -1;
		}
		return returnStatus;

	}

	private static int printGroupMembership(CommandLine line) {
		String user = line.getOptionValue("user");
		if (user == null || user == "")
			user = getOptionFromCmd("user");
		int returnStatus = 0;
		SvWriter svw = null;
		SvSecurity svs = null;
		try {

			svw = new SvWriter();
			svw.switchUser(Sv.ADMIN);
			svs = new SvSecurity(svw);
			DbDataObject userDbo = svs.getUser(user);
			DbDataArray defaultGroup = svw.getAllUserGroups(userDbo, true);
			String printDefault = " has NO default group configured!";
			if (defaultGroup != null && defaultGroup.size() > 0) {
				printDefault = " has configured a default group:" + defaultGroup.get(0).getVal("GROUP_NAME");

			}
			writeToScreen("User " + user + printDefault);

			defaultGroup = svw.getAllUserGroups(userDbo, false);
			writeToScreen("User " + user + " is member of the following groups:");
			writeToScreen("----------");
			for (DbDataObject dbg : defaultGroup.getItems()) {
				writeToScreen((String) dbg.getVal("GROUP_NAME"));
			}
			writeToScreen("Total of " + defaultGroup.size() + " groups associated to the user");

			returnStatus = 0;
		} catch (Exception ex) {
			writeToScreen("Error, can not get user groups for user " + user);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error("Error, can not get user groups for user " + user, ex);
			}
			returnStatus = -1;
		} finally {
			if (svw != null)
				svw.release();
			if (svs != null)
				svs.release();
		}
		return returnStatus;
	}

	private static int userGroupMembership(CommandLine line) {
		String user = line.getOptionValue("user");
		if (user == null || user == "")
			user = getOptionFromCmd("user");
		String group = line.getOptionValue("group");
		if (group == null || group == "")
			group = getOptionFromCmd("group");

		boolean isDefaultGroup = line.hasOption("default-group");
		boolean removeFromGroup = line.hasOption("remove-group");
		String groupOperation = removeFromGroup ? "removed from" : "added to";
		int returnStatus = 0;
		SvWriter svw = null;
		SvSecurity svs = null;
		try {

			svw = new SvWriter();
			svw.switchUser(Sv.ADMIN);
			svs = new SvSecurity(svw);
			DbDataObject userDbo = svs.getUser(user);
			DbDataObject groupDbo = svs.getSid(group, svCONST.OBJECT_TYPE_GROUP);
			if (removeFromGroup)
				svs.removeUserFromGroup(userDbo, groupDbo);
			else
				svs.addUserToGroup(userDbo, groupDbo, isDefaultGroup);
			writeToScreen("User " + user + " successfully " + groupOperation + " group: " + group);
			returnStatus = 0;
		} catch (Exception ex) {
			writeToScreen(Sv.ERROR + Sv.USER_NAME_LABEL + user + Sv.CANNOT + groupOperation + " group:" + group);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error(Sv.ERROR + Sv.USER_NAME_LABEL + user + Sv.CANNOT + groupOperation + " group:" + group, ex);
			}
			returnStatus = -1;
		} finally {
			if (svw != null)
				svw.release();
			if (svs != null)
				svs.release();
		}
		return returnStatus;
	}

	private static int updatePermissions(CommandLine line) {
		if (!line.hasOption("permissions")) {
			writeToScreen("Error: Missing --permissions parameter. " + "In order to grant or revoke permissions, "
					+ "the --permissions parameter must be set");
			return -4;
		}
		String user = line.getOptionValue("user");
		String group = line.getOptionValue("group");
		Boolean isGroup = false;
		if (group != null) {
			writeToScreen("Setting permissions for group:" + group);
			isGroup = true;
		} else if (user != null)
			writeToScreen("Setting permissions for user:" + user);

		String permissionMask = line.getOptionValue("permissions");
		boolean isGrant = line.hasOption("g");
		String permissionOperation = isGrant ? " is granted " : " is revoked ";
		int returnStatus = 0;
		SvReader svr = null;
		SvSecurity svs = null;
		try {
			svr = new SvReader();
			svr.switchUser(Sv.ADMIN);
			svs = new SvSecurity(svr);
			svs.setAutoCommit(false);
			DbDataArray permissions = svs.getPermissions(permissionMask);

			DbDataObject sid = null;
			if (isGroup)
				sid = svs.getSid(group, svCONST.OBJECT_TYPE_GROUP);
			else
				sid = svs.getUser(user);

			if (sid != null) {
				for (DbDataObject perm : permissions.getItems()) {
					if (isGrant)
						svs.grantPermission(sid, (String) perm.getVal(Sv.LABEL_CODE));
					else
						svs.revokePermission(sid, (String) perm.getVal(Sv.LABEL_CODE));
					writeToScreen("User " + user + " successfully " + permissionOperation + " permission: "
							+ (String) perm.getVal(Sv.LABEL_CODE));
				}
				svs.dbCommit();
			} else {
				writeToScreen("Error, sid " + (isGroup ? group : user) + Sv.CANNOT + " found");
				returnStatus = -5;
			}

		} catch (Exception ex) {
			writeToScreen(
					Sv.ERROR + Sv.USER_NAME_LABEL + user + Sv.CANNOT + permissionOperation + " permission:" + group);
			if (ex instanceof SvException)
				writeToScreen(((SvException) ex).getFormattedMessage());
			else {
				writeToScreen(UNEXPECTED_EX);
				log4j.error(
						Sv.ERROR + Sv.USER_NAME_LABEL + user + Sv.CANNOT + permissionOperation + " permission:" + group,
						ex);
			}
			returnStatus = -1;
		} finally {
			if (svr != null)
				svr.release();
			if (svs != null)
				svs.release();
		}
		return returnStatus;
	}

	private static int manageSecurity(CommandLine line) {

		int returnStatus = 0;
		if (line.hasOption("user") && line.hasOption("print-groups")) {
			returnStatus = printGroupMembership(line);
			return returnStatus;
		}
		if (line.hasOption("group") && (line.hasOption("export-group-cfg") || line.hasOption("import-group-cfg"))) {
			if (line.hasOption("export-group-cfg"))
				returnStatus = exportGroupCfg(line);
			else
				returnStatus = importGroupCfg(line);
			return returnStatus;
		}

		if (line.hasOption("user") && line.hasOption("group")) {
			// deal with user group membership
			returnStatus = userGroupMembership(line);
			return returnStatus;
		}
		if (line.hasOption("user") && !line.hasOption("group")) {
			if (line.hasOption("g") || line.hasOption("r")) {
				returnStatus = updatePermissions(line);
				// grant or revoke privileges
			} else {
				if (line.hasOption("password"))
					returnStatus = updatePassword(line);
				else
					returnStatus = updateUser(line);
				// deal with user creation modification
			}
			return returnStatus;
		}

		if (!line.hasOption("user") && line.hasOption("group")) {
			if (line.hasOption("g") || line.hasOption("r")) {
				returnStatus = updatePermissions(line);
				// grant or revoke privileges
			} else {
				returnStatus = updateGroup(line);
				// deal with user creation modification
			}
			return returnStatus;
		}

		return returnStatus;
	}

	static int runUpgrade(CommandLine line) {
		boolean labelsOnlyUpgrade = false;
		int returnStatus = 0;
		if (line.hasOption("f"))
			forceUpgrade = true;
		if (line.hasOption("s"))
			returnStatus = createMasterRepo() == true ? 0 : -1;
		else {
			if (line.hasOption("l"))
				labelsOnlyUpgrade = true;
			returnStatus = upgradeSvarog(labelsOnlyUpgrade);
		}
		return returnStatus;

	}

	static ArrayList<Option> getCoreOpts(CommandLine line) {
		ArrayList<Option> coreOpts = new ArrayList<Option>();
		for (Option opt : line.getOptions()) {
			if (sysCoreOpts.indexOf(opt) >= 0)
				coreOpts.add(opt);
		}
		return coreOpts;
	}

	/**
	 * Method to validate the command line and ensure that compatible options are
	 * set
	 * 
	 * @param line The command line which is subject of parsing.
	 * @return True if the command line is valid
	 */
	private static boolean validateCommandLine(CommandLine line) {
		ArrayList<Option> opts = getCoreOpts(line);
		if (opts.size() == 0)
			writeToScreen("No core options set. You must set at ONLY one of the core options:");
		else if (opts.size() > 1)
			writeToScreen("More than one core option set. You must set ONLY one of the core options:");
		if (opts.size() != 1) {
			HelpFormatter formatter = new HelpFormatter();
			Options options = new Options();
			for (Option opt : sysCoreOpts)
				options.addOption(opt);
			formatter.printHelp("Svarog Core Options", options);

			formatter.printHelp("Svarog ", getOptions());

			return false;
		}
		return true;

	}

	/**
	 * Method to configure the options for the command line parser
	 * 
	 * @return Options object containing all options for configuring the parser
	 */
	static Options getOptions() {
		// create the Options
		Options options = new Options();

		OptionGroup coreGroup = new OptionGroup();
		coreGroup.setRequired(true);

		Option opt = new Option("j", "json", false,
				"re-create all json configuration files, including the custom config");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		opt = new Option("gd", "grid", false, "re-create system grid from the sdi boundary in /conf/sdi/boundary.json");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		opt = new Option("i", "install", false, "perform Svarog installation against the configured database");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		opt = new Option("u", "upgrade", false, "upgrade the current active Svarog installation");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		opt = new Option("m", "manage", false,
				"update existing or create a new security identifier in Svarog (User or Group). Applicable only with --user or --group");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		opt = new Option("dm", "daemon", false, "start Svarog in daemon mode");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		coreGroup.addOption(opt);
		opt = new Option("h", "help", false, "print this message");
		coreGroup.addOption(opt);

		opt = new Option("o", "object-stat", false,
				"provides information about objects distribution between different repository tables in the system");
		coreGroup.addOption(opt);
		sysCoreOpts.add(opt);

		options.addOptionGroup(coreGroup);

		OptionGroup installGroup = new OptionGroup();
		opt = new Option("d", "drop", false,
				"drop the current database schema before performing installation." + "Applicable only with --install");
		installGroup.addOption(opt);

		options.addOptionGroup(installGroup);

		OptionGroup autoGroup = new OptionGroup();
		opt = new Option("a", "auto", false,
				"when using the auto option with install/upgrade option it generates the JSON config on the fly and install/upgrade based on the configuration");
		autoGroup.addOption(opt);
		options.addOptionGroup(autoGroup);

		OptionGroup repoGroup = new OptionGroup();
		opt = Option.builder().longOpt("migrate-objects")
				.desc("migrates all misconfigured objects to the correctly configured repo").build();
		repoGroup.addOption(opt);

		opt = Option.builder().longOpt("rebuild-index").desc("refresh the indexes of the specific repo").hasArg()
				.argName("TABLE").build();
		repoGroup.addOption(opt);

		options.addOptionGroup(repoGroup);

		OptionGroup upgradeGroup = new OptionGroup();

		opt = new Option("f", "force", false, "force the upgrade execution even if the flag for running upgrade is set."
				+ "Applicable only with --upgrade");
		options.addOption(opt);

		opt = new Option("s", "schema", false,
				"when upgrading, only upgrade the database schema, without the Svarog configuration."
						+ "Applicable only with --upgrade");
		upgradeGroup.addOption(opt);

		opt = new Option("l", "labels", false,
				"when upgrading, only upgrade the labels and code lists. Applicable only with --upgrade");
		upgradeGroup.addOption(opt);

		opt = new Option("ct", "confiuration-test", false,
				"when upgrading, run internal configuration test and report the calls in console");
		upgradeGroup.addOption(opt);
		options.addOptionGroup(upgradeGroup);

		OptionGroup manageGroup = new OptionGroup();

		manageGroup.addOption(opt);

		opt = new Option("p", "pass-double-hash", false,
				"when storing the password use double hash to simulate client side JS hashing."
						+ "Applicable only with --password");
		options.addOption(opt);

		OptionGroup grantGroup = new OptionGroup();
		opt = new Option("g", "grant", false, "grant a permission set to the user/group specified by --user or --group"
				+ ".Applicable only with --user or --group. Using grant with both --user and --group, will grant access to the user over the group");
		grantGroup.addOption(opt);
		opt = new Option("r", "revoke", false,
				"revoke a permission set to the user/group specified by --user or --group"
						+ ".Applicable only with --user or --group. Using revoke with both --user and --group, will revoke access from the user over the group");
		grantGroup.addOption(opt);
		options.addOptionGroup(grantGroup);

		opt = Option.builder().longOpt("permissions").desc(
				"identify a permission set for granting and revoking. Use % as wildcard. Example SVAROG%.READ would grant read permission to SVAROV tables")
				.hasArg().argName("MASK").build();

		options.addOption(opt);

		opt = Option.builder().longOpt("user").desc("create/update a user with user name 'USER'").hasArg()
				.argName("USER").build();
		options.addOption(opt);

		opt = Option.builder().longOpt("default-group").desc("register the group 'GROUP' as default group for 'USER'")
				.build();
		options.addOption(opt);

		opt = Option.builder().longOpt("print-groups").desc("print the list of group for 'USER'").build();
		options.addOption(opt);

		opt = Option.builder().longOpt("export-group-cfg")
				.desc("export assigned ACLs for group identified by --group to file").hasArg().argName("FILE_NAME")
				.build();

		options.addOption(opt);

		opt = Option.builder().longOpt("import-group-cfg")
				.desc("import assigned ACLs for group identified by --group from file").hasArg().argName("FILE_NAME")
				.build();
		options.addOption(opt);

		opt = Option.builder().longOpt("remove-group").desc("remove 'USER' from the group 'GROUP'").build();

		options.addOption(opt);

		opt = Option.builder().longOpt("pin").desc("assign personal identification number 'PIN' to the user").hasArg()
				.argName("PIN").build();

		options.addOption(opt);

		opt = Option.builder().longOpt("group").desc("create/update a with user name 'GROUP'").hasArg().argName("GROUP")
				.build();
		options.addOption(opt);

		opt = Option.builder().longOpt("password").desc("assign password PASSWORD to the user").hasArg()
				.argName("PASSWORD").build();
		options.addOption(opt);

		opt = Option.builder().longOpt("status").desc("assign the status STATUS to the user/group").hasArg()
				.argName("STATUS").build();
		options.addOption(opt);

		opt = Option.builder().longOpt("name").desc("assign first name NAME to the user").hasArg().argName("NAME")
				.build();
		options.addOption(opt);

		opt = Option.builder().longOpt("surname").desc("assign surname SURNAME to the user").hasArg().argName("SURNAME")
				.build();

		options.addOption(opt);

		opt = Option.builder().longOpt("email").desc("assign e-mail E-MAIL to the user/group").hasArg().argName("TYPE")
				.build();

		options.addOption(opt);

		opt = Option.builder().longOpt("sid_type").desc(
				"assign user type or group type to the user/group. Values must be INTERNAL or EXTERNAL for user and USERS or ADMINISTRATORS for groups")
				.hasArg().argName("E-MAIL").build();

		options.addOption(opt);

		opt = Option.builder().longOpt("group_security")
				.desc("assign group security type SECURITY_TYPE to the group. Values must be FULL or POA").hasArg()
				.argName("SECURITY_TYPE").build();

		options.addOption(opt);
		options.addOptionGroup(manageGroup);
		return options;

	}

	/**
	 * Method to generate the core svarog JSON structure
	 * 
	 * @return 0 if success, -1 if failed
	 */
	private static int generateJsonCfg() {
		// we should not connect to database at all and check if svarog is
		// installed therefore we fix the mIsAlreadyInstalled to false
		mIsAlreadyInstalled = false;

		try {
			File confDir = new File(SvConf.getConfPath());
			if (confDir.exists()) {
				for (File currFile : confDir.listFiles()) {
					if (!currFile.getName().equals("sdi"))
						FileUtils.deleteDirectory(currFile);
				}
			}

		} catch (IOException e) {
			log4j.error("Error deleting conf resources ", e);
		}
		String errorMessage = DbInit.createJsonMasterRepo();
		if (!errorMessage.equals("")) {
			log4j.error("Error building svarog master repo. " + errorMessage);
			return -1;
		}
		errorMessage = DbInit.createJsonMasterTableRecords();
		if (!errorMessage.equals("")) {
			log4j.error("Error building svarog master records. " + errorMessage);
			System.exit(-1);
			return -1;
		}
		errorMessage = DbInit.prepareLabels();
		if (!errorMessage.equals("")) {

			log4j.error("Error preparing labels. " + errorMessage);
			return -1;
		}
		log4j.info("JSON config files generated successfully");
		return 0;
	}

	/**
	 * Method to validate if there is existing repo and svarog.properties is
	 * configured accordingly
	 * 
	 * @return True if the installation is valid, false if it isn't
	 */
	private static boolean isSvarogInstalled() {
		if (mIsAlreadyInstalled == null) {
			mIsAlreadyInstalled = new Boolean(true);
			Connection conn = null;
			try {
				conn = SvConf.getDBConnection();
				if (!dbObjectExists(SvConf.getMasterRepo(), conn)) {
					mIsAlreadyInstalled = false;
				}
			} catch (Exception e) {
				log4j.error(
						"Can't check the database tables. Check svarog.properties for errors in connection strings!",
						e);
				mIsAlreadyInstalled = false;
			} finally {
				if (conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						log4j.error(JDBC_ERR, e);
					}
			}

		}
		return mIsAlreadyInstalled;
	}

	/**
	 * Method to sort an array of ISvConfiguration objects by different update types
	 * 
	 * @param cfgs       The array of ISvConfiguration objects
	 * @param updateType The specific update type to be used for sorting
	 * @return The sorted ArrayList
	 */
	static ArrayList<ISvConfiguration> getSortedCfgs(ArrayList<ISvConfiguration> cfgs,
			final ISvConfiguration.UpdateType updateType) {
		Collections.sort(cfgs, new Comparator<ISvConfiguration>() {
			public int compare(ISvConfiguration o1, ISvConfiguration o2) {
				return o1.executionOrder(updateType) - o2.executionOrder(updateType);
			}
		});
		return cfgs;
	}

	static void prepareConfig() {
		if (iSvCfgs == null) {
			iSvCfgs = new ArrayList<ISvConfiguration>();
			// add the system implementation
			iSvCfgs.add(new SvConfigurationImpl());

			ArrayList<Object> cfgs = DbInit.loadClass(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY),
					ISvConfiguration.class);
			if (cfgs != null)
				for (Object o : cfgs)
					if (o instanceof ISvConfiguration)
						iSvCfgs.add((ISvConfiguration) o);
		}
	}

	static void executeConfiguration(ISvConfiguration.UpdateType updateType) throws Exception {
		Connection conn = null;
		String schema = null;
		String msg = "";
		ISvCore svc = null;
		prepareConfig();

		// pre-install db handler call
		try {
			conn = SvConf.getDBConnection();
			schema = SvConf.getDefaultSchema();
			if (!updateType.equals(UpdateType.SCHEMA) || isSvarogInstalled())
				svc = new SvReader();
			for (ISvConfiguration conf : getSortedCfgs(iSvCfgs, updateType)) {
				switch (updateType) {
				case SCHEMA:
					msg = conf.beforeSchemaUpdate(conn, svc, schema);
					break;
				case LABELS:
					msg = conf.beforeLabelsUpdate(conn, svc, schema);
					break;
				case CODES:
					msg = conf.beforeCodesUpdate(conn, svc, schema);
					break;
				case TYPES:
					msg = conf.beforeTypesUpdate(conn, svc, schema);
					break;
				case LINKTYPES:
					msg = conf.beforeLinkTypesUpdate(conn, svc, schema);
					break;
				case ACL:
					msg = conf.beforeAclUpdate(conn, svc, schema);
					break;
				case SIDACL:
					msg = conf.beforeSidAclUpdate(conn, svc, schema);
					break;
				case FINAL:
					msg = conf.afterUpdate(conn, svc, schema);
					break;
				default:
					break;
				}
				log4j.info(msg);
			}
		} finally {
			if (svc != null)
				svc.release();
			if (conn != null)
				conn.close();
		}
	}

	/**
	 * Method for install/upgrade svarog, to the latest config.
	 * 
	 * @return Status of the upgrade. 0 for success.
	 */
	public static int upgradeSvarog(boolean labelsOnly) {
		// forse reset of the flag.
		mIsAlreadyInstalled = null;
		operation = (isSvarogInstalled() ? "install" : "upgrade");

		/*
		 * (if (!(firstInstall ^ isSvarogInstalled())) { log4j.error("Svarog " +
		 * operation + " failed! In-database config is " + (isSvarogInstalled() ?
		 * "valid" : "invalid") + "!"); return -2; }
		 */
		if (!isSvarogInstalled() && labelsOnly) {
			log4j.error("Svarog " + operation + " failed! In-database config is "
					+ (isSvarogInstalled() ? "valid" : "invalid") + "! Can't run LABELS_ONLY upgrade!");
			return -2;
		}
		SvParameter svp = null;
		try {

			// pre-install db handler call
			Connection conn = null;
			try {
				conn = SvConf.getDBConnection();
				String before = SvConf.getDbHandler().beforeInstall(conn, SvConf.getDefaultSchema());
				log4j.info("Svarog DbHandler pre-upgrade:" + before);
			} finally {
				if (conn != null)
					conn.close();
			}
			// if this isn't first time install then we can't set the upgrade
			// running param
			if (isSvarogInstalled()) {
				svp = new SvParameter();
				String isRunning = svp.getParamString("svarog.upgrade.running");
				if (isRunning != null && isRunning.equals("true") && !forceUpgrade) {
					log4j.error("Svarog " + operation + " is already running");
					return -2;
				}
				svp.setParamString("svarog.upgrade.running", "true");
				isRunning = svp.getParamString("svarog.upgrade.running");
				svp.release();
			}
			if (runConfigurationTest)
				prepareConfigurationTest();
			// pre-install db handler call
			executeConfiguration(ISvConfiguration.UpdateType.SCHEMA);

			// do the actual table creation, unless we run labels only upgrade
			if (!labelsOnly)
				if (!SvarogInstall.createMasterRepo()) {
					log4j.error("Svarog tables " + operation + " failed");
					return -2;
				}
			// do the actual records configuration
			if (!SvarogInstall.upgradeObjects(labelsOnly)) {
				log4j.error("Svarog master records " + operation + " failed");
				return -2;
			}
			// finally create the file store if it is initial install
			if (!isSvarogInstalled())
				if (!initFileStore()) {
					log4j.error("Initialising the svarog file store failed");
					return -2;
				}

			executeConfiguration(ISvConfiguration.UpdateType.FINAL);

			if (isSvarogInstalled())
				try {
					svp.setParamString("svarog.upgrade.running", "false");
				} catch (SvException e) {
					log4j.error("Can't set upgrade running flag to false", e);
				}

			try {
				conn = SvConf.getDBConnection();
				String after = SvConf.getDbHandler().afterInstall(conn, SvConf.getDefaultSchema());
				log4j.info("Svarog DbHandler post-upgrade:" + after);
			} finally {
				if (conn != null)
					conn.close();
			}
			log4j.info("Svarog " + operation + " finished");
		} catch (Exception e) {
			log4j.error("Svarog install/upgrade failed", e);
			return -2;
		} finally {
			if (svp != null) {
				svp.release();
			}
		}

		return 0;
	}

	private static void prepareConfigurationTest() {
		iSvCfgs = new ArrayList<ISvConfiguration>();
		iSvCfgs.add(new SvConfigurationDryRun());
	}

	/**
	 * Method to provide file migration from one filestore to another
	 */
	public static void migrateDbFileStore() {
		Connection conn = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try (SvFileStore fs = new SvFileStore();) {
			conn = SvConf.getDBConnection();
			st = conn.prepareStatement(
					"select * from " + SvConf.getParam("filestore.table") + " where dbms_lob.getlength(data)>1");
			rs = st.executeQuery();

			byte[] data = null;
			Long fileId = 0L;
			while (rs.next()) {
				fileId = rs.getLong(1);
				data = rs.getBytes(2);
				fs.fileSystemSaveByte(fileId, data);
			}
		} catch (Exception e) {
			log4j.error("Error migrating filestore", e);
		} finally {

			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				log4j.error(JDBC_ERR, e);
			}
			try {
				if (st != null)
					st.close();
			} catch (SQLException e) {
				log4j.error(JDBC_ERR, e);
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log4j.error(JDBC_ERR, e);
			}

		}

	}

	/**
	 * The method installObject is used to install the default system configuration
	 * from the JSON configuration to the target DB. It uses the predefined
	 * object_id values from the JSON config files
	 * 
	 * @param jsonObject The JSON object which should be installed.
	 * @param dbu        SvWriter instance to be used for installing objects
	 * @throws SvException If the saveObject method raised an exception, just
	 *                     forward it
	 */
	static void installObject(JsonObject jsonObject, SvWriter dbu) throws SvException {

		JsonElement jEl = jsonObject.get(DbDataArray.class.getCanonicalName());
		if (jEl != null) {
			DbDataArray dba = new DbDataArray();
			dba.fromJson(jsonObject);
			Boolean isBatch = false;
			if (dba.getItems().size() > 0 && dba.getItems().get(0).getObjectId() != 0L)
				isBatch = false;
			// todo fix link upgrade !!!
			dbu.saveObject(dba, isBatch, false);

		} else {

			DbDataObject dbo = new DbDataObject();
			dbo.getMembersJson().setMembersFromJson("", dbo, jsonObject);
			dbu.saveObject(dbo, false);
		}

		// return result;
	}

	/**
	 * Method to get the list of fields in the repo table
	 * 
	 * @param conn The JDBC connection to be used for query execution
	 * @return A map containing the DbDataFields of the repo table
	 */
	static LinkedHashMap<String, DbDataField> getRepoFieldListFromDb(Connection conn) {
		if (repoFieldListDb == null)
			repoFieldListDb = new LinkedHashMap<String, DbDataField>();

		if (repoFieldListDb.size() == 0) {
			repoFieldListDb = getFieldListFromDb(conn, SvConf.getMasterRepo(), SvConf.getDefaultSchema());
		}
		return repoFieldListDb;
	}

	static void rebuildIndexes(String tableName) {
		log4j.info("Rebuilding indexes for table:" + tableName);

		SvReader svr = null;
		try {
			svr = new SvReader();
			Connection conn = svr.dbGetConn();
			ArrayList<DbDataTable> dbts = readTablesFromConf();
			DbDataTable tableDescriptor = null;
			for (DbDataTable dbt : dbts) {
				if (dbt.getDbTableName().equals(tableName)) {
					tableDescriptor = dbt;
					break;
				}
			}
			if (createTable(tableDescriptor, conn))
				log4j.info("Indexes for table:" + tableName + " rebuilt with success");
			else
				log4j.error("Indexes for table:" + tableName + " were not rebuilt due to error");
		} catch (Exception e) {
			log4j.error("Can't rebuild indexes for table:" + tableName, e);

		} finally {
			if (svr != null)
				svr.release();
		}

	}

	static boolean refreshIndexes(String tableName, String schemaName) {
		String schema = schemaName != null ? schemaName : SvConf.getDefaultSchema();

		if (dropIndexes(tableName, schema))
			rebuildIndexes(tableName);
		else
			return false;
		return true;
	}

	static void dropIndex(String schemaName, String tableName, String constrName, String indexName, Connection conn)
			throws SQLException {

		StringBuilder sqlDropConstr = new StringBuilder();
		sqlDropConstr.append("ALTER TABLE ").append(schemaName).append(".");
		sqlDropConstr.append(tableName).append(" DROP CONSTRAINT ").append(constrName);

		StringBuilder sqlDropIndex = new StringBuilder().append("DROP INDEX ");
		sqlDropIndex.append(schemaName).append(".").append(indexName);

		PreparedStatement st = null;
		PreparedStatement st1 = null;

		try {
			st = conn.prepareStatement(sqlDropConstr.toString());
			st.execute();
			st1 = conn.prepareStatement(sqlDropIndex.toString());
			st1.execute();

		} finally {
			if (st != null)
				try {
					st.close();
				} catch (SQLException e) {
					log4j.error("Can't release drop constraint statement:" + tableName, e);
				}
			if (st1 != null)
				try {
					st1.close();
				} catch (SQLException e) {
					log4j.error("Can't release drop index statement:" + tableName, e);
				}

		}

	}

	static boolean dropIndexes(String tableName, String schemaName) {
		log4j.info("Dropping indexes for table:" + tableName);
		SvReader svr = null;

		try {
			svr = new SvReader();
			Connection conn = svr.dbGetConn();
			HashMap<String, String> indexNames = getIndexListFromDb(conn, tableName, schemaName);

			for (Entry<String, String> index : indexNames.entrySet()) {

				if (index.getValue() != null)
					dropIndex(schemaName, tableName, index.getValue(), index.getKey(), conn);
			}
			log4j.info("Indexes for table:" + tableName + " dropped with success");
			return true;
		} catch (Exception e) {
			log4j.error("Can't drop indexes for table:" + tableName, e);
			return false;
		} finally {

			if (svr != null)
				svr.release();
		}
	}

	/**
	 * Method to fetch the list of indexes which exist for a specific database table
	 * 
	 * @param conn       JDBC Connection to be used
	 * @param tableName  The table for which the fields should be fetched
	 * @param schemaName The database schema in which the table resides
	 * @return A map containing field name as key and DbDataField object describing
	 *         the field it self
	 */
	static HashMap<String, String> getIndexListFromDb(Connection conn, String tableName, String schemaName) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("OBJECT_NAME", tableName);
		params.put("SCHEMA_NAME", schemaName);

		HashMap<String, String> indexList = new HashMap<String, String>();
		ResultSet[] rs = new ResultSet[1];
		PreparedStatement[] ps = new PreparedStatement[1];
		if (executeDbScript("index_list.sql", params, conn, true, rs, ps)) {
			try {
				while (rs[0].next()) {

					indexList.put(rs[0].getString(1), rs[0].getString(2));
				}
			} catch (SQLException e) {
				log4j.error("Can't get list of indexes", e);
			} finally {
				try {
					if (rs[0] != null)
						rs[0].close();
					if (ps[0] != null)
						ps[0].close();
				} catch (SQLException e) {
					log4j.error("Can't close result set", e);
				}

			}
		}
		return indexList;
	}

	/**
	 * Method to fetch the fields/columns which exist in the database table
	 * 
	 * @param conn       JDBC Connection to be used
	 * @param tableName  The table for which the fields should be fetched
	 * @param schemaName The database schema in which the table resides
	 * @return A map containing field name as key and DbDataField object describing
	 *         the field it self
	 */
	static LinkedHashMap<String, DbDataField> getFieldListFromDb(Connection conn, String tableName, String schemaName) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("TABLE_NAME", tableName);
		params.put("SCHEMA_NAME", schemaName);

		LinkedHashMap<String, DbDataField> fieldsInDb = new LinkedHashMap<String, DbDataField>();
		ResultSet[] rs = new ResultSet[1];
		PreparedStatement[] ps = new PreparedStatement[1];
		if (executeDbScript("table_column_list.sql", params, conn, true, rs, ps)) {
			try {
				while (rs[0].next()) {
					DbDataField dft = new DbDataField();
					dft.setDbFieldName(rs[0].getString(1));
					dft.setDbFieldType(rs[0].getString(2));
					dft.setIsNull(rs[0].getString(3).equals("Y") ? true : false);
					dft.setDbFieldSize(rs[0].getInt(4));
					dft.setDbFieldScale(rs[0].getInt(5));
					fieldsInDb.put(dft.getDbFieldName(), dft);
				}
			} catch (SQLException e) {
				log4j.error("Can't check if object exists", e);
			} finally {
				try {
					if (rs[0] != null)
						rs[0].close();
					if (ps[0] != null)

						ps[0].close();
				} catch (SQLException e) {
					log4j.error("Can't close result set", e);
				}

			}
		}
		return fieldsInDb;
	}

	/**
	 * The method updateTableBase, updates the table structure in the target
	 * Database according to the configuration object DbDataTable
	 * 
	 * @param dbDataTable The DbDataTable object based on which you want to create
	 *                    the table in the DB
	 * @param fieldsInDb  A map containing the fiels indexed by name for the
	 *                    specific table
	 * @param conn        The JDBC connection which should be used for query
	 *                    execution
	 * @return True/False if the table has been created with success
	 */
	synchronized static Boolean updateTableBase(DbDataTable dbDataTable, HashMap<String, DbDataField> fieldsInDb,
			Connection conn) {
		Boolean retval = true;
		try {

			HashMap<String, String> params = new HashMap<String, String>();
			params.put("DB_TYPE", SvConf.getDbType().toString());
			params.put("DB_USER", SvConf.getUserName());
			params.put("TABLE_NAME", dbDataTable.getDbTableName());
			params.put("TABLE_SCHEMA",
					(dbDataTable.getDbSchema() != null ? dbDataTable.getDbSchema() : SvConf.getDefaultSchema()));
			params.put("SCHEMA_NAME", params.get("TABLE_SCHEMA"));

			for (DbDataField dft : dbDataTable.getDbTableFields()) {
				if (fieldsInDb.get(dft.getDbFieldName()) == null) {
					params.put("COLUMN_DEFINITION", dft.getSQLString());
					retval = executeDbScript("alter_table_add_column.sql", params, conn);
					log4j.info("Upgraded table " + dbDataTable.getDbTableName() + " with column def:"
							+ params.get("COLUMN_DEFINITION"));
					if (!retval)
						break;
				}

			}

		} catch (Exception e) {
			log4j.error("Error creating table:" + dbDataTable.getDbTableName(), e);
			retval = false;
		}
		return retval;
	}

	/**
	 * WARNING: This method drops and recreates the default schema! Use only for
	 * local development purposes.
	 * 
	 * @return True if the db was cleaned with success.
	 */
	public static Boolean cleanDb() {
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			if (SvConf.getDbType().equals(SvDbType.POSTGRES)) {
				conn = SvConf.getDBConnection();
				st = conn.createStatement();
				log4j.info("Dropping DB schema:" + SvConf.getDefaultSchema());
				try {
					st.execute("DROP SCHEMA " + SvConf.getDefaultSchema() + " CASCADE");
				} catch (Exception e) {
					log4j.error("Dropping DB schema failed", e);
				}

				log4j.info("Creating DB schema:" + SvConf.getDefaultSchema());
				st.execute("CREATE SCHEMA " + SvConf.getDefaultSchema());
				rs = st.executeQuery("    select * from pg_extension where upper(extname) = 'POSTGIS'");
				if (!rs.next()) {
					log4j.info("Create postgis extension:" + SvConf.getDefaultSchema());
					st.execute("CREATE EXTENSION postgis");
				} else
					// + "with schema " + SvConf.getDefaultSchema());
					log4j.info("Postgis exists in :" + SvConf.getDefaultSchema());
			} else if (SvConf.getDbType().equals(SvDbType.ORACLE)) {
				conn = SvConf.getDBConnection();
				st = conn.createStatement();
				log4j.info("Dropping tables in DB schema:" + SvConf.getDefaultSchema());
				try {

					String dropStr = "begin "
							+ "for c1 in ( select * from ALL_OBJECTS WHERE  upper(OWNER)=upper('{SCHEMA}') and "
							+ "object_type<>'INDEX' and object_type<>'LOB' and object_type<>'TYPE' and object_type<>'PROCEDURE' and secondary='N') "
							+ "loop " + "execute immediate 'DROP '||c1.object_type||' '||c1.owner||'.'||c1.object_name;"
							+ "end loop;" + "end;";
					st.execute(dropStr.replace("{SCHEMA}", SvConf.getDefaultSchema()));

				} catch (Exception e) {
					log4j.error("Dropping tables in DB schema failed", e);
				}

			}

			DbCache.clean();

			// SvCore.initCfgObjectsBase();
			return true;
		} catch (Exception e) {
			log4j.error("Can't clean the DB schema", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				log4j.error("Connection can't be released!", e);
			}
			try {
				if (st != null)
					st.close();
			} catch (Exception e) {
				log4j.error("Connection can't be released!", e);
			}
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				log4j.error("Connection can't be released!", e);
			}

		}

		return false;
	}

	/**
	 * Method to read all data table configurations from the JSON files in the
	 * config folder.
	 * 
	 * @return
	 */
	static ArrayList<DbDataTable> readTablesFromConf() {
		String confPath = SvConf.getConfPath() + masterDbtPath;
		InputStream flst = null;
		ArrayList<DbDataTable> dbTables = new ArrayList<>();
		try {
			// init the table configs
			flst = new FileInputStream(new File(confPath + fileListName));
			String[] texFiles = IOUtils.toString(flst, "UTF-8").split("\n");

			for (int i = 0; i < texFiles.length; i++) {
				if (texFiles[i].endsWith(".json")) {
					DbDataTable dbt = getDbtFromJson(confPath + texFiles[i], SvConf.getMasterRepo());
					if (dbt != null)
						dbTables.add(dbt);
				}
			}
		} catch (Exception e2) {
			log4j.error("Error reading config from folder " + confPath
					+ ", check if all objects are present and properly set", e2);

		} finally {
			if (flst != null) {
				try {
					flst.close();
				} catch (IOException e) {
					log4j.error("Can't close file stream", e);
				}
			}

		}
		return dbTables;
	}

	/**
	 * A method which creates all master configuration tables for Svarog. It
	 * initializes the DB structure, to be further used by the framework.
	 * 
	 * @return If the system has been installed properly or not.
	 */
	synchronized static Boolean createMasterRepo() {
		// File textFolder;
		Boolean retval = true;
		Connection conn = null;

		try {
			ArrayList<DbDataTable> dbTables = readTablesFromConf();
			conn = SvConf.getDBConnection();
			log4j.info("Svarog core " + operation + " started. Total objects to " + operation + ":"
					+ (dbTables != null ? dbTables.size() : 0));
			int upgradedObjects = 0;
			// first create all repo tables
			for (DbDataTable dbt : dbTables) {
				if (dbt.getIsRepoTable()) {
					upgradedObjects++;
					log4j.info("Repo " + operation + " " + upgradedObjects + " of " + dbTables.size() + " on "
							+ dbt.getDbTableName());
					if (!createTableFromJson(dbt, "", conn)) {
						retval = false;
						break;
					}
				}
			}
			// Create all other tables
			if (retval)
				for (DbDataTable dbt : dbTables) {
					if (!dbt.getIsRepoTable()) {
						upgradedObjects++;
						log4j.info("Table " + operation + " " + +upgradedObjects + " of " + dbTables.size() + " on "
								+ dbt.getDbTableName());

						if (!createTableFromJson(dbt, "", conn)) {
							retval = false;
							break;
						}
					}
				}
			/**
			 * Add MSSQL specific types
			 */
			if (retval && SvConf.getDbType().equals(SvDbType.MSSQL)) {
				HashMap<String, String> params = new HashMap<String, String>();
				params.put("DB_TYPE", SvConf.getDbType().toString());
				params.put("DB_USER", SvConf.getUserName());
				params.put("MASTER_REPO", SvConf.getMasterRepo());
				params.put("DEFAULT_SCHEMA", SvConf.getDefaultSchema());
				executeDbScript("post_create_prep.sql", params, conn);
			}
			log4j.info("Svarog core " + operation + " finished.");

		} catch (Exception e2) {
			log4j.error("Error creating master repo, check if all objects are present and properly set", e2);
			return false;
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (Exception e) {
					log4j.error("Connection can't be released!", e);
				}
			;

		}
		return retval;

	}

	/**
	 * Method to refresh the view for the specific DataTable descriptor
	 * 
	 * @param dbt  The DbDataTable descriptor object
	 * @param conn The JDBC connection to used for refreshing the view
	 * @return True if the view has been refreshed successfully. Otherwise false.
	 */
	static Boolean refreshView(DbDataTable dbt, Connection conn) {
		// no view refresh for the repo!
		if (dbt.getIsRepoTable())
			return true;

		StringBuilder sbt = new StringBuilder();
		sbt.append("SELECT ");
		LinkedHashMap<String, DbDataField> repoFields = getRepoFieldListFromDb(conn);

		for (DbDataField rdft : repoFields.values()) {
			if (rdft.getDbFieldName().equals("PKID")) {
				sbt.append("REP0." + SvConf.getSqlkw().getString("OBJECT_QUALIFIER_LEFT") + rdft.getDbFieldName()
						+ SvConf.getSqlkw().getString("OBJECT_QUALIFIER_RIGHT") + ",");
			} else
				sbt.append(SvConf.getSqlkw().getString("OBJECT_QUALIFIER_LEFT") + rdft.getDbFieldName()
						+ SvConf.getSqlkw().getString("OBJECT_QUALIFIER_RIGHT") + ",");
		}

		for (DbDataField rdft : dbt.getDbTableFields()) {
			if (!rdft.getDbFieldName().equals("PKID"))
				sbt.append(SvConf.getSqlkw().getString("OBJECT_QUALIFIER_LEFT") + rdft.getDbFieldName()
						+ SvConf.getSqlkw().getString("OBJECT_QUALIFIER_RIGHT") + ",");
		}
		sbt.setLength(sbt.length() - 1);
		sbt.append(" FROM " + dbt.getDbSchema() + "." + dbt.getDbRepoName() + " REP0 JOIN " + dbt.getDbSchema() + "."
				+ dbt.getDbTableName() + " TBL0 ON REP0.META_PKID = TBL0.PKID ");

		return createView("V" + dbt.getDbTableName(), sbt.toString(), conn);
	}

	/**
	 * The method orchestrates creation of database tables with their appropriate
	 * indices, sequences and other constraints
	 * 
	 * @param dbt  The data table object which is used to describe the table
	 * @param conn The JDBC connection to be used for executing queries against the
	 *             DB
	 * @return True, if the creation of the table was successful, otherwhise false.
	 * @throws Exception Raise any exception which occured
	 */
	public static Boolean createTable(DbDataTable dbt, Connection conn) throws Exception {
		Boolean retval = false;
		Boolean tableExists = dbObjectExists(dbt.getDbTableName(), conn);
		HashMap<String, DbDataField> fieldsInDb = getFieldListFromDb(conn, dbt.getDbTableName(),
				(dbt.getDbSchema() != null ? dbt.getDbSchema() : SvConf.getDefaultSchema()));

		if (tableExists) {
			log4j.debug("Table object " + dbt.getDbTableName() + " exists");
			retval = updateTableBase(dbt, fieldsInDb, conn);
		} else
			retval = createTableBase(dbt, conn);

		if (retval) {
			if (dbt.getDbTableFields() != null)
				for (int i = 0; i < dbt.getDbTableFields().length; i++) {
					String sName = dbt.getDbTableFields()[i].getDbSequenceName();
					if (sName != null && !sName.equals("")) {
						if (!dbObjectExists(sName, conn)) {
							retval = createSequence(sName, 1, svCONST.MAX_SYS_OBJECT_ID, conn);
							if (!retval)
								break;
						}
					}
				}
			if (dbt.getDbTableFields() != null) {
				// normal indices
				for (Entry<String, String> entry : dbt.getSQLTableIndices().entrySet()) {

					String idxName = entry.getKey();
					String columns = entry.getValue();
					String idxDbName = dbt.getDbTableName() + idxName;
					if (idxDbName.length() > 30)
						idxDbName = dbt.getDbTableName().substring(0, 30 - idxName.length()) + idxName;

					if (idxDbName != null && !idxDbName.equals("")) {
						if (!dbObjectExists(idxDbName, conn)) {
							retval = createIndex(idxDbName, columns, dbt.getDbTableName(), conn);
							if (!retval)
								break;
						}
					}
				}
				// spatial indices

				for (Entry<String, String> entry : dbt.getSQLSpatialIndices().entrySet()) {
					String idxName = entry.getKey();
					String columns = entry.getValue();
					String idxDbName = dbt.getDbTableName() + idxName;
					if (idxDbName.length() > 30)
						idxDbName = dbt.getDbTableName().substring(0, 30 - idxName.length()) + idxName;
					if (idxDbName != null && !idxDbName.equals("")) {
						if (!dbObjectExists(idxDbName, conn)) {
							// for spatial indexes MSSQL needs the system
							// bounding box
							retval = createSpatialIndex(idxDbName, columns,
									SvGeometry.getTileEnvelope(0L, "DUMMY", null), dbt.getDbTableName(), conn);
							if (!retval)
								break;
						}
					}
				}

				// spatial indices
				// for (Entry<String, String> entry :
				// dbt.getSQLSpatialIndices().entrySet()) {
				// String idxName = entry.getKey();
				// String columns = entry.getValue();
				// String idxDbName = dbt.getDbTableName() + idxName;
				// if (idxDbName.length() > 30)
				// idxDbName = dbt.getDbTableName().substring(0, 30 -
				// idxName.length()) + idxName;
				//
				// if (idxDbName != null && !idxDbName.equals("")) {
				// if (!dbObjectExists(idxDbName, conn)) {
				// retval = createIndex(idxDbName,
				// columns,sqlKw.getString("SPATIAL_INDEX_OPTS"),
				// dbt.getDbTableName(), conn);
				// if (!retval)
				// break;
				// }
				// }
				// }
			}
		}
		if (retval)
			return refreshView(dbt, conn);
		else
			return retval;

	}

	/**
	 * Method for loading a DbDataTable from a JSON configuration file
	 * 
	 * @param pathToJson The path to the JSON configuration table
	 * @param repoTable  Name of the repo table
	 * @return The DbDataTable instance if the JSON is valid, otherwise null
	 */
	static DbDataTable getDbtFromJson(String pathToJson, String repoTable) {
		InputStream fis = null;
		DbDataTable dbt = null;
		try {
			fis = new FileInputStream(new File(pathToJson));
			String json = IOUtils.toString(fis, "UTF-8");
			json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			if (repoTable != null && !repoTable.equals(""))
				json = json.replace("{REPO_TABLE_NAME}", repoTable);

			Gson gson = new Gson();
			JsonElement jelem = gson.fromJson(json, JsonElement.class);
			JsonObject jobj = jelem.getAsJsonObject();
			dbt = new DbDataTable();
			DbDataTable.setRbConf(SvConf.getSqlkw());
			DbDataField.setSqlKWResource(SvConf.getSqlkw());

			dbt.fromJson(jobj);
		} catch (Exception e) {
			log4j.error("Can't get DbDataTable for" + pathToJson, e);
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					log4j.error("Can't close input stream for:" + pathToJson, e);
				}
		}
		return dbt;
	}

	/**
	 * Method which creates a DB table based on a JSON file, containing a
	 * DbDataTable object
	 * 
	 * @param dbt          The table descriptor to be used as configuration
	 * @param newTableName The new table name (if renaming of the object is needed)
	 * @param conn         The JDBC connection to be used for executing the queries
	 * @return If the table has been created successfully it returns true.
	 */
	static Boolean createTableFromJson(DbDataTable dbt, String newTableName, Connection conn) {

		try {

			Boolean retval = true;
			if (newTableName != null && !newTableName.equals(""))
				dbt.setDbTableName(newTableName);
			retval = createTable(dbt, conn);
			return retval;
		} catch (Exception e) {
			log4j.error("Can't load master repo json config", e);
			return false;
		}

	}

	/**
	 * The method createTableBase, creates a basic table structure in the target
	 * Database based on the configuration object DbDataTable
	 * 
	 * @param dbDataTable The DbDataTable object based on which you want to create
	 *                    the table in the DB
	 * @param conn        The JDBC connection to be used for executing the queries
	 * @return True/False if the table has been created with success
	 */

	static synchronized Boolean createTableBase(DbDataTable dbDataTable, Connection conn) {
		try {
			HashMap<String, String> params = new HashMap<String, String>();
			params.put("DB_TYPE", SvConf.getDbType().toString());
			params.put("DB_USER", SvConf.getUserName());
			params.put("TABLE_NAME", dbDataTable.getDbTableName());
			params.put("TABLE_SCHEMA",
					(dbDataTable.getDbSchema() != null ? dbDataTable.getDbSchema() : SvConf.getDefaultSchema()));

			params.put("TABLE_ELEMENT_LIST", dbDataTable.getSQLTableElements());
			params.put("TABLE_CONSTRAINT_LIST", dbDataTable.getSQLTableConstraints());

			return executeDbScript("create_table.sql", params, conn);
		} catch (Exception e) {
			log4j.error("Error creating table:" + dbDataTable.getDbTableName(), e);
		}

		return false;
	}

	/**
	 * Method which creates a new sequence in the DB
	 * 
	 * @param seqName    The name of the sequence
	 * @param increment  The incrementing value when the sequence is incremented
	 * @param startsFrom The starting value from the sequence starts
	 * @param conn       The JDBC connection to be used for executing the queries
	 * @return True or False if the creation was successful
	 */
	static synchronized Boolean createSequence(String seqName, Integer increment, Long startsFrom, Connection conn) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("SEQUENCE_NAME", seqName);
		params.put("SCHEMA", SvConf.getDefaultSchema());
		params.put("INCREMENT", increment.toString());
		params.put("START_FROM", startsFrom.toString());
		return executeDbScript("create_sequence.sql", params, conn);

		// return true;
	}

	/**
	 * A method to check if the object specified by objectName exists in the default
	 * schema
	 * 
	 * @param objectName The name of the object to be checked for existence
	 * @param conn       The JDBC connection to be used for executing the queries
	 * @return True/false if the object exists in the database
	 */
	static synchronized Boolean dbObjectExists(String objectName, Connection conn) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("OBJECT_NAME", objectName);
		params.put("SCHEMA_NAME", SvConf.getDefaultSchema());
		ResultSet[] rs = new ResultSet[1];
		PreparedStatement[] ps = new PreparedStatement[1];
		Integer objCnt = 0;
		if (executeDbScript("db_object_exists.sql", params, conn, true, rs, ps)) {
			try {
				if (rs[0].next())
					objCnt = rs[0].getInt(1);
			} catch (SQLException e) {
				log4j.error("Can't check if object exists", e);
			} finally {
				try {
					if (rs[0] != null)
						rs[0].close();
					if (ps[0] != null)

						ps[0].close();
				} catch (SQLException e) {
					log4j.error("Can't close result set", e);
				}

			}
		}
		if (objCnt > 0)
			return true;
		else
			return false;
	}

	/**
	 * A method to check if the columnName parameter is already used as a column be
	 * the table tableName
	 * 
	 * @param columnName The name of the column
	 * @param tableName  The name of the table
	 * @param conn       The JDBC Connection to be used for executing queries.
	 * @return True/false if the column exists in the specified table
	 */
	static synchronized Boolean tableColumnExists(String columnName, String tableName, Connection conn,
			String schemaName) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("TABLE_NAME", tableName);
		params.put("COLUMN_NAME", columnName);
		params.put("SCHEMA_NAME", schemaName);
		ResultSet[] rs = new ResultSet[1];
		PreparedStatement[] ps = new PreparedStatement[1];
		Integer objCnt = 0;
		if (executeDbScript("table_column_exists.sql", params, conn, true, rs, ps)) {
			try {
				if (rs[0].next())
					objCnt = rs[0].getInt(1);
			} catch (SQLException e) {
				log4j.error("Can't check if object exists", e);
			} finally {
				try {
					if (rs[0] != null)
						rs[0].close();
					if (ps[0] != null)

						ps[0].close();
				} catch (SQLException e) {
					log4j.error("Can't close result set", e);
				}

			}
		}
		if (objCnt > 0)
			return true;
		else
			return false;
	}

	/**
	 * Method that creates a new index object in the target DB
	 * 
	 * @param idxName    Name of the index
	 * @param columnList List of column names split by a comma
	 * @param tableName  Name of the table on which the index is created
	 * @param conn       The JDBC connection to be used for executing the queries
	 * @return True/false if the index creation was successful
	 */
	static synchronized Boolean createIndex(String idxName, String columnList, String tableName, Connection conn) {

		return createIndex(idxName, columnList, "", tableName, conn);
		// return true;
	}

	/**
	 * Method that creates a new index object in the target DB
	 * 
	 * @param idxName         Name of the index
	 * @param columnList      List of column names split by a comma
	 * @param tableName       Name of the table on which the index is created
	 * @param extendedOptions Additional parameter used in the index creation (see
	 *                        the create_index.sql script for more info)
	 * @param conn            the JDBC connection to be used for query execution
	 * @return True/false if the index creation was successful
	 */
	static synchronized Boolean createIndex(String idxName, String columnList, String extendedOptions, String tableName,
			Connection conn) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("INDEX_NAME", idxName);
		params.put("SCHEMA", SvConf.getDefaultSchema());
		params.put("TABLE_NAME", tableName);
		params.put("COLUMN_LIST", columnList);
		params.put("EXTENDED_OPTIONS", extendedOptions);
		return executeDbScript("create_index.sql", params, conn);

	}

	/**
	 * Method that creates a new index object in the target DB
	 * 
	 * @param idxName     Name of the index
	 * @param columnName  The column on which the index should be created
	 * @param sysEnvelope The system boundary Envelope
	 * 
	 * @param tableName   Name of the table on which the index is created
	 * @param conn        the JDBC connection to be used for query execution
	 * @return True/false if the creation was successful
	 */
	static synchronized Boolean createSpatialIndex(String idxName, String columnName, Envelope sysEnvelope,
			String tableName, Connection conn) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());
		params.put("INDEX_NAME", idxName);
		params.put("SCHEMA", SvConf.getDefaultSchema());
		params.put("TABLE_NAME", tableName);
		params.put("COLUMN_NAME", columnName);
		params.put("MINX", Double.toString(sysEnvelope.getMinX()));
		params.put("MINY", Double.toString(sysEnvelope.getMinY()));
		params.put("MAXX", Double.toString(sysEnvelope.getMaxX()));
		params.put("MAXY", Double.toString(sysEnvelope.getMaxY()));
		params.put("SRID", SvConf.getParam("sys.gis.default_srid"));
		return executeDbScript("create_spatial_index.sql", params, conn);

		// return true;
	}

	static synchronized Boolean createView(String viewName, String selectQuery, Connection conn) {
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("DB_TYPE", SvConf.getDbType().toString());
		params.put("DB_USER", SvConf.getUserName());

		params.put("VIEW_NAME", viewName);
		params.put("VIEW_SCHEMA", SvConf.getDefaultSchema());
		params.put("SELECT_QUERY", selectQuery);
		if (SvConf.getDbType().equals(SvDbType.POSTGRES)) {
			if (dbObjectExists(viewName, conn))
				executeDbScript("drop_view.sql", params, conn);
		}

		return executeDbScript("create_view.sql", params, conn);

		// return true;
	}

	/**
	 * A method which executes SQL statements from a file. It expects that the
	 * statements are separated by a colon (;) in the script. The script can be
	 * parameterized by using parameters specified by a single word text between
	 * curved brackets, like {PARAM}. The parameteres are initialized from the
	 * "params" HashMap. This method allows a script to return a result set in the
	 * resultSet array. If one needs to get the result set from a script, then it
	 * needs also to pass an instance of empty prepared statement array which will
	 * return also the prepared statement object which was used to obtain the result
	 * set
	 * 
	 * @param scriptName       The name of the file containing the script. It is
	 *                         expected that it resides in the "sql" sub-folder.
	 * @param params           A HashMap containing the parameters which should be
	 *                         substituted in the script.
	 * @param returnsResultSet Flag to mark if the script execution should return a
	 *                         result set
	 * @param resultSet        array of ResultSet objects which were generated by
	 *                         the script execution
	 * @param prepStatement    array of PreparedStatement objects which were
	 *                         generated by the script execution
	 * @param conn             the JDBC connection to be used for query execution
	 * @return Returns true if the script was executed with success.
	 */
	static Boolean executeDbScript(String scriptName, HashMap<String, String> params, Connection conn,
			Boolean returnsResultSet, ResultSet[] resultSet, PreparedStatement[] prepStatement) {
		Boolean retval = false;
		if (returnsResultSet && (resultSet == null || prepStatement == null)) {
			log4j.error("Can't store resultsets in null object!");
			return false;
		}

		log4j.debug("executeDbScript() for " + scriptName + " started.");

		PreparedStatement ps = null;
		try {

			Set<String> paramKeys = (Set<String>) params.keySet();
			String script = SvConf.getDbHandler().getSQLScript(scriptName);
			String[] items = null;
			items = script.split(SvConf.getDbHandler().getDbScriptDelimiter());
			if (returnsResultSet && resultSet != null
					&& (resultSet.length < items.length || prepStatement.length < items.length)) {
				log4j.info("Can't execute " + Integer.toString(items.length)
						+ " statements and store in result set array with length "
						+ Integer.toString(resultSet.length));
				return false;
			}
			for (int i = 0; i < items.length; i++) {
				String stmt = items[i];
				if (stmt.trim().length() <= 0)
					continue;
				Iterator<String> it = paramKeys.iterator();
				while (it.hasNext()) {
					String currentKey = it.next();
					if (params.get(currentKey) != null)
						stmt = stmt.replace("{" + currentKey + "}", params.get(currentKey));
				}

				log4j.debug(stmt.toUpperCase());

				if (returnsResultSet) {
					prepStatement[i] = conn.prepareStatement(stmt.toUpperCase());
					resultSet[i] = prepStatement[i].executeQuery();
				} else {
					ps = conn.prepareStatement(stmt.toUpperCase());
					ps.execute();
				}

			}
			retval = true;
		} catch (Exception ex) {

			if (SvConf.getDbType().equals(SvDbType.ORACLE) && ((SQLException) ex).getErrorCode() == 1408) {
				retval = true;
				log4j.warn("Duplicate ORACLE index. Warning executing " + scriptName + ". " + ex.getMessage()
						+ ". Parameters maps:" + params.toString());
			} else {
				log4j.fatal("Error executing " + scriptName + ". " + ex.getMessage() + ". Parameters maps:"
						+ params.toString());
				retval = false;
			}

		} finally {
			if (ps != null)
				try {
					ps.close();
				} catch (Exception e) {
					log4j.error("PreparedStatement can't be released!", e);
				}
			;

		}
		log4j.debug("executeDbScript() for " + scriptName + " finished.");
		return retval;
	}

	/**
	 * Overloaded version of executeDbScript targeted at running DDL or other
	 * scripts which don't return value or result set
	 * 
	 * @param scriptName The name of the file containing the script. It is expected
	 *                   that it resides in the "sql/DATABASE_NAME/" sub-folder.
	 * @param params     A HashMap containing the parameters which should be
	 *                   substituted in the script.
	 * @param conn       The SQL Connection to be used for execution of the script
	 * @return True/false depending if the script execution was a success or not
	 */
	static Boolean executeDbScript(String scriptName, HashMap<String, String> params, Connection conn) {
		return executeDbScript(scriptName, params, conn, false, null, null);
	}

	/**
	 * Overloaded method to check if a config object should be upgraded.
	 * 
	 * @param oldDbo                The old version of the config object
	 * @param newDbo                The new version of the config object
	 * @param dboFields             The list of fields applicable for the object
	 *                              type of the two objects subject to comparison
	 * @param avoidParentComparison If true, the method will not compare the
	 *                              parent_id's of objects
	 * @return True if upgrade is needed else false
	 */
	public static boolean shouldUpgradeConfig(DbDataObject oldDbo, DbDataObject newDbo, DbDataArray dboFields,
			Boolean avoidParentComparison) {
		if (!avoidParentComparison)
			if (!oldDbo.getParentId().equals(newDbo.getParentId()))
				return true;
		if (!(oldDbo.getDtDelete().equals(SvConf.MAX_DATE) && newDbo.getDtDelete() == null))
			if (!oldDbo.getDtDelete().equals(newDbo.getDtDelete()))
				return true;
		if (!oldDbo.getObjectType().equals(newDbo.getObjectType()))
			return true;
		if (!oldDbo.getStatus().equals(newDbo.getStatus()))
			return true;

		if (dboFields.getItemByIdx("PKID", oldDbo.getObjectType()) == null)
			dboFields.rebuildIndex(Sv.FIELD_NAME.toString());
		// if(oldDbo.getValues().size()!=newDbo.getValues().size())
		// return true;
		Boolean cmpVal = false;
		for (Entry<SvCharId, Object> ent : oldDbo.getValuesMap().entrySet()) {
			if (dboFields.getItemByIdx(ent.getKey().toString(), oldDbo.getObjectType()) == null)
				continue;
			// never upgrade because of changes in metadata or extended params
			if (ent.getKey().equals(SvCharId.toSvCharId(Sv.GUI_METADATA))
					|| ent.getKey().equals(SvCharId.toSvCharId(Sv.EXTENDED_PARAMS)))
				continue;

			Object oldVal = ent.getValue();
			Object newVal = newDbo.getVal(ent.getKey());
			if (oldVal == null && newVal == null)
				continue;
			if (oldVal != null) {
				if (!oldVal.equals(newVal)) {
					cmpVal = true;
					break;
				}

			} else {
				cmpVal = true;
				break;
			}

		}

		for (Entry<SvCharId, Object> ent : newDbo.getValuesMap().entrySet()) {
			if (dboFields.getItemByIdx(ent.getKey().toString(), oldDbo.getObjectType()) == null)
				continue;
			// never upgrade because of changes in metadata or extended params
			// never upgrade because of changes in metadata or extended params
			if (ent.getKey().equals(SvCharId.toSvCharId(Sv.GUI_METADATA))
					|| ent.getKey().equals(SvCharId.toSvCharId(Sv.EXTENDED_PARAMS)))
				continue;

			Object oldVal = oldDbo.getVal(ent.getKey());
			Object newVal = ent.getValue();
			if (oldVal == null && newVal == null)
				continue;
			if (oldVal != null) {
				if (!oldVal.equals(newVal)) {
					cmpVal = true;
					break;
				}

			} else {
				cmpVal = true;
				break;
			}

		}
		return cmpVal;
	}

	/**
	 * Overloaded method to check if a config object should be upgraded. This
	 * version strictly compares parent config object change
	 * 
	 * @param oldDbo    The old version of the config object
	 * @param newDbo    The new version of the config object
	 * @param dboFields The list of fields applicable for the object type of the two
	 *                  objects subject to comparison
	 * @return True if upgrade is needed else false
	 */
	static boolean shouldUpgradeConfig(DbDataObject oldDbo, DbDataObject newDbo, DbDataArray dboFields) {
		return shouldUpgradeConfig(oldDbo, newDbo, dboFields, false);
	}

	/**
	 * Method to perform upgrade of the link types in the system
	 * 
	 * @param dba Array list of link types to be upgraded
	 * @return If successful, true else false
	 * @throws SvException Re-raised SvException
	 */
	static Boolean upgradeLinkTypes(DbDataArray dba) throws SvException {

		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr)) {

			svw.isInternal = true;
			DbDataArray toSave = new DbDataArray();
			DbDataArray existingTypes = null;
			if (isSvarogInstalled())
				existingTypes = svr.getObjects(null, svCONST.OBJECT_TYPE_LINK_TYPE, null, 0, 0);
			for (DbDataObject dbo : dba.getItems()) {
				if (dbo.getObjectType().equals(svCONST.OBJECT_TYPE_LINK_TYPE)) {
					Long objType1 = null;
					Long objType2 = null;
					if (dbo.getVal("link_obj_type_1") instanceof String) {
						objType1 = SvCore.getTypeIdByName((String) dbo.getVal("link_obj_type_1"));
						dbo.setVal("link_obj_type_1", objType1);
					} else
						objType1 = (Long) dbo.getVal("link_obj_type_1");
					if (dbo.getVal("link_obj_type_2") instanceof String) {
						objType2 = SvCore.getTypeIdByName((String) dbo.getVal("link_obj_type_2"));
						dbo.setVal("link_obj_type_2", objType2);
					} else
						objType2 = (Long) dbo.getVal("link_obj_type_2");

					boolean shouldSaveType = true;
					if (existingTypes != null && existingTypes.size() > 0) {
						DbDataArray linkFields = SvCore.getFields(svCONST.OBJECT_TYPE_LINK_TYPE);
						linkFields.rebuildIndex("FIELD_NAME");
						for (DbDataObject oldLink : existingTypes.getItems()) {
							if (dbo.getVal("link_obj_type_1").equals(oldLink.getVal("link_obj_type_1"))
									&& dbo.getVal("link_obj_type_2").equals(oldLink.getVal("link_obj_type_2"))
									&& dbo.getVal("link_type").equals(oldLink.getVal("link_type"))) {
								if (shouldUpgradeConfig(oldLink, dbo, linkFields)) {
									dbo.setObjectId(oldLink.getObjectId());
									dbo.setPkid(oldLink.getPkid());
								} else
									shouldSaveType = false;
								break;
							}
						}
					}

					if (shouldSaveType)
						toSave.addDataItem(dbo);

				} else
					log4j.warn("Non link type dbo detected: " + dbo.toSimpleJson().toString());

			}
			svw.saveObject(toSave);
		}
		return true;
	}

	/**
	 * Method for upgrading Link configurations
	 * 
	 * @param upgradeFile     The file to be used for the upgrade
	 * @param allNonProcessed List of all non processed objects to which we add
	 *                        whatever object we didn't process
	 * @throws SvException Any underlying exception
	 */
	static void upgradeLinkCfg(String upgradeFile, DbDataArray allNonProcessed) throws SvException {
		DbDataArray allObjects = getDbArrayFromFile(upgradeFile);
		DbDataArray dbLinksUpgrade = extractType(allObjects, svCONST.OBJECT_TYPE_LINK_TYPE, true);
		if (dbLinksUpgrade != null && dbLinksUpgrade.size() > 0) {
			log4j.info("Processing link type " + operation + ": " + upgradeFile + ". Total number of objects:"
					+ dbLinksUpgrade.size());
			upgradeLinkTypes(dbLinksUpgrade);
		}
	}

	/**
	 * Method for upgrading Object configurations
	 * 
	 * @param upgradeFile     The file to be used for the upgrade
	 * @param allNonProcessed List of all non processed objects to which we add
	 *                        whatever object we didn't process
	 * @throws SvException Any underlying exception
	 */
	static void upgradeObjectCfg(String upgradeFile, DbDataArray allNonProcessed) throws SvException {
		DbDataArray allObjects = getDbArrayFromFile(upgradeFile);

		int upgradeSize = allObjects.size();
		log4j.info("Processing base table upgrades: " + upgradeFile + ". :" + upgradeSize);

		DbDataArray dbFieldsUpgrade = extractType(allObjects, svCONST.OBJECT_TYPE_FIELD, true);
		DbDataArray dbTablesUpgrade = extractType(allObjects, svCONST.OBJECT_TYPE_TABLE, true);
		upgradeObjectCfg(dbTablesUpgrade, dbFieldsUpgrade);

		log4j.info("Finished " + operation + " using records file: " + upgradeFile
				+ ". Total number of upgraded objects:" + (upgradeSize - allObjects.size()));
		if (allObjects.size() > 0) {
			allNonProcessed.getItems().addAll(allObjects.getItems());
			log4j.debug("Non-processed objects: " + allObjects.toSimpleJson().toString());
		}

	}

	/**
	 * Method for upgrading ACL configurations
	 * 
	 * @param upgradeFile     The file to be used for the upgrade
	 * @param allNonProcessed List of all non processed objects to which we add
	 *                        whatever object we didn't process
	 * @throws SvException Any underlying exception
	 */
	static void upgradeAclCfg(String upgradeFile, DbDataArray allNonProcessed) throws SvException {
		DbDataArray allObjects = getDbArrayFromFile(upgradeFile);
		int upgradeSize = allObjects.size();
		log4j.info("Processing base table upgrades: " + upgradeFile + ". Total number of objects:" + upgradeSize);

		DbDataArray dbACLUpgrade = extractType(allObjects, svCONST.OBJECT_TYPE_ACL, true);
		upgradeAcl(dbACLUpgrade);

		log4j.info("Finished " + operation + " using records file: " + upgradeFile
				+ ". Total number of upgraded objects:" + (upgradeSize - allObjects.size()));

		if (allObjects.size() > 0)
			log4j.info("Non-processed objects: " + allObjects.toSimpleJson().toString());

	}

	/**
	 * Method for upgrading SID to ACL configurations
	 * 
	 * @param upgradeFile     The file to be used for the upgrade
	 * @param allNonProcessed List of all non processed objects to which we add
	 *                        whatever object we didn't process
	 * @throws SvException Any underlying exception
	 */
	static void upgradeSidAclCfg(String upgradeFile, DbDataArray allNonProcessed) throws SvException {
		DbDataArray allObjects = getDbArrayFromFile(upgradeFile);
		int upgradeSize = allObjects.size();
		log4j.info("Processing base table upgrades: " + upgradeFile + ". Total number of objects:" + upgradeSize);

		DbDataArray dbACLUpgrade = extractType(allObjects, svCONST.OBJECT_TYPE_SID_ACL, true);
		upgradeSidAcl(dbACLUpgrade);

		log4j.info("Finished " + operation + " using records file: " + upgradeFile
				+ ". Total number of upgraded objects:" + (upgradeSize - allObjects.size()));

		if (allObjects.size() > 0)
			log4j.info("Non-processed objects: " + allObjects.toSimpleJson().toString());
	}

	/**
	 * Wrapper method which encapsulated the separate types of upgrades
	 * 
	 * 
	 * @return If successful, true else false
	 */
	static Boolean upgradeObjects(boolean labelsOnly) {
		try {
			// if not first install, then init the core
			// else boot strap the local installation
			if (isSvarogInstalled()) {
				sysLocales = null;
				SvCore.initSvCore(true);
			} else
				installLocales();

			String confPath = SvConf.getConfPath() + masterRecordsPath;
			File confFolder = new File(confPath);
			File[] confFiles = confFolder.listFiles();
			DbDataArray allNonProcessed = new DbDataArray();

			executeConfiguration(ISvConfiguration.UpdateType.LABELS);

			Arrays.sort(confFiles);
			// iterate over all files containing Labels and run upgrade
			for (int i = 0; i < confFiles.length; i++) {
				if (confFiles[i].getName().startsWith(labelsFilePrefix)) {
					upgradeLabels(confPath + confFiles[i].getName());
				}
			}
			// if not first install, perform re-initialisation to refresh the
			// labels
			if (isSvarogInstalled())
				SvCore.initSvCoreImpl(true);

			executeConfiguration(ISvConfiguration.UpdateType.CODES);
			// run the upgrade of the codes
			upgradeCodes(SvConf.getConfPath() + masterRecordsPath + codesFile);

			// if we wanted to run labels only upgrade, stop here
			if (labelsOnly)
				return true;

			executeConfiguration(ISvConfiguration.UpdateType.TYPES);
			// for each config file run the upgrade against the database
			for (int i = 0; i < confFiles.length; i++) {
				if (confFiles[i].getName().startsWith("4")) {
					if (isSvarogInstalled())
						SvCore.initSvCore(true);
					String upgradeFile = confPath + confFiles[i].getName();
					upgradeObjectCfg(upgradeFile, allNonProcessed);
				}
			}
			executeConfiguration(ISvConfiguration.UpdateType.LINKTYPES);
			for (int i = 0; i < confFiles.length; i++) {
				if (confFiles[i].getName().startsWith("4")) {
					String upgradeFile = confPath + confFiles[i].getName();
					upgradeLinkCfg(upgradeFile, allNonProcessed);
				}
			}

			SvCore.initSvCore(true);
			executeConfiguration(ISvConfiguration.UpdateType.ACL);
			for (int i = 0; i < confFiles.length; i++) {
				if (confFiles[i].getName().startsWith("90")) {
					String upgradeFile = confPath + confFiles[i].getName();
					upgradeAclCfg(upgradeFile, allNonProcessed);
				}
			}
			if (!isSvarogInstalled()) {
				SvCore.initSvCore(true);
				installLocalData(allNonProcessed);
			}

			executeConfiguration(ISvConfiguration.UpdateType.SIDACL);
			for (int i = 0; i < confFiles.length; i++) {
				if (confFiles[i].getName().startsWith("91")) {
					String upgradeFile = confPath + confFiles[i].getName();
					upgradeSidAclCfg(upgradeFile, allNonProcessed);
				}
			}

		} catch (Exception e) {
			log4j.error("Upgrade failed!", e);
			if (e instanceof SvException)
				log4j.error(((SvException) e).getFormattedMessage());
			return false;
		}
		return true;
	}

	/**
	 * Method to perform initial Local install such users, groups and the initial
	 * cluster record
	 * 
	 * @return True if there was no error
	 * @throws SvException Raised exception from SvWriter
	 */
	static Boolean installLocalData(DbDataArray nonProcessed) throws SvException {
		log4j.info("Install of system locales started");

		DbDataArray localUsers = getDbArrayFromFile(SvConf.getConfPath() + masterRecordsPath + usersFile);
		try (SvWriter svw = new SvWriter(); SvLink svl = new SvLink(svw);) {

			svw.isInternal = true;
			svw.saveObject(localUsers, false);
			DbDataArray users = extractType(localUsers, svCONST.OBJECT_TYPE_USER, false);
			DbDataObject defaultUserGroupLink = SvCore.getLinkType("USER_DEFAULT_GROUP", svCONST.OBJECT_TYPE_USER,
					svCONST.OBJECT_TYPE_GROUP);
			for (DbDataObject user : users.getItems()) {
				svl.linkObjects(user.getObjectId(), svCONST.SID_ADMINISTRATORS, defaultUserGroupLink.getObjectId(),
						"Default link");
			}

			DbDataArray otherUsers = extractType(nonProcessed, svCONST.OBJECT_TYPE_USER, false);
			if (otherUsers.size() > 0)
				svw.saveObject(otherUsers);

			DbDataArray otherGroups = extractType(nonProcessed, svCONST.OBJECT_TYPE_GROUP, false);
			if (otherGroups.size() > 0)
				svw.saveObject(otherGroups);

			// this is to initialise the cluster record.
			DbDataArray cluster = extractType(nonProcessed, svCONST.OBJECT_TYPE_CLUSTER, false);
			if (cluster.size() > 0)
				svw.saveObject(cluster);

			svw.dbCommit();
		}
		return true;
	}

	/**
	 * Method to perform initial Locale install
	 * 
	 * @return True if there was no error
	 * @throws SvException Raised exception from SvWriter
	 */
	static Boolean installLocales() throws SvException {
		log4j.info("Install of system locales started");

		DbDataArray locales = getLocaleList();
		try (SvWriter svw = new SvWriter()) {
			svw.saveObject(locales);
			svw.dbCommit();
		}
		return true;
	}

	/**
	 * Method to upgrade the svarog label list according to the latest upgrade
	 * 
	 * @param filePath The path to the labels upgrade file
	 * @return False if the upgrade failed, otherwise true
	 * @throws SvException Any exception raised during the upgrade of the labels
	 */
	static Boolean upgradeLabels(String filePath) throws SvException {

		log4j.info("Labels " + operation + " started:" + filePath);
		log4j.info("Labels locale:" + filePath);
		boolean retVal = false;
		String locale = null;
		if (filePath.indexOf(labelsFilePrefix) > 0) {
			locale = filePath.substring(filePath.indexOf(labelsFilePrefix) + labelsFilePrefix.length());
			locale = locale.replace(".json", "");
		}
		DbDataArray locales = getLocaleList();
		if (locales == null) {
			log4j.info("System locales can't be loaded. Operation failed");
			return false;
		}
		if (locale != null && locales.getItemByIdx(locale) != null) {
			DbDataArray labelUpgrade = getDbArrayFromFile(filePath);
			if (labelUpgrade != null && labelUpgrade.size() > 0) {
				DbDataArray labels = extractType(labelUpgrade, svCONST.OBJECT_TYPE_LABEL, false);
				log4j.info("Number of labels in " + operation + "  file:" + labels.size());

				DbDataObject dboLocale = locales.getItemByIdx(locale);

				DbDataArray labelsFieldList = SvCore.getFields(svCONST.OBJECT_TYPE_LABEL);
				labelsFieldList.rebuildIndex("FIELD_NAME");
				try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {

					DbSearchCriterion dbs = new DbSearchCriterion("LOCALE_ID", DbCompareOperand.EQUAL, locale);

					DbDataArray existingLabels = svr.getObjects(dbs, svCONST.OBJECT_TYPE_LABEL, null, 0, 0);
					if (existingLabels != null)
						existingLabels.rebuildIndex(Sv.LABEL_CODE.toString(), true);

					svw.setAutoCommit(false);
					if (isSvarogInstalled() && existingLabels.size() > 0) {
						for (DbDataObject dboLabel : labels.getItems()) {
							if (!dboLabel.getParentId().equals(dboLocale.getObjectId()))
								dboLabel.setParentId(dboLocale.getObjectId());

							String labelCode = (String) dboLabel.getVal(Sv.LABEL_CODE);
							DbDataObject existingDbo = null;
							if (existingLabels != null)
								existingDbo = existingLabels.getItemByIdx(labelCode);
							if (existingDbo != null) // upgrade
							{
								if (shouldUpgradeConfig(existingDbo, dboLabel, labelsFieldList)) {
									dboLabel.setPkid(existingDbo.getPkid());
									dboLabel.setObjectId(existingDbo.getObjectId());
									// dboLabel.setParentId(existingDbo.getParentId());
									svw.saveObject(dboLabel);
								}
							} else // save new label
							{
								svw.saveObject(dboLabel);
							}
						}
					} else {
						for (DbDataObject dboLabel : labels.getItems()) {
							if (!dboLabel.getParentId().equals(dboLocale.getObjectId()))
								dboLabel.setParentId(dboLocale.getObjectId());
						}
						svw.saveObject(labels, true);
					}

					svw.dbCommit();
				}
			} else
				log4j.warn("No labels to install/upgrade found in:" + filePath);

		} else {
			log4j.info("Labels upgrade file doesn't follow naming specs!");
			retVal = false;
		}

		return retVal;
	}

	private static void getRepoData(String tableName, String sqlQuery, Connection conn, DbDataArray dbTables,
			HashMap<DbDataObject, DbDataObject> misconfiguredDbt, DbDataObject repo) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			ps = conn.prepareStatement(sqlQuery);
			rs = ps.executeQuery();
			while (rs.next()) {
				Long objCount = rs.getLong(1);
				Long tableId = rs.getLong(2);
				for (DbDataObject dbt : dbTables.getItems()) {
					if (tableId.equals(dbt.getObjectId())) {
						log4j.info("Table '" + dbt.getVal(TABLE_NAME) + "', has " + objCount + " objects in repo:"
								+ tableName);
						if (!dbt.getVal("repo_name").equals(tableName)) {
							log4j.info("Configured table repo is '" + dbt.getVal("repo_name")
									+ "'. The data shall be migrated");
							misconfiguredDbt.put(dbt, repo);
						}

					}
				}
			}
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				log4j.error("Closing prepared statement failed with exception", e);
			}
			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
				log4j.error("Closing prepared statement failed with exception", e);
			}
		}

	}

	public static HashMap<DbDataObject, DbDataObject> repoStat() {
		log4j.info("Starting repo analysis");
		HashMap<DbDataObject, DbDataObject> misconfiguredDbt = new HashMap<DbDataObject, DbDataObject>();
		DbDataArray dbTables = null;
		DbDataArray dbRepos = new DbDataArray();
		SvReader svr = null;

		try {
			svr = new SvReader();
			dbTables = svr.getObjects(null, svCONST.OBJECT_TYPE_TABLE, null, 0, 0);
			Iterator<DbDataObject> it = dbTables.getItems().iterator();
			while (it.hasNext()) {
				DbDataObject dbt = it.next();
				if ((Boolean) dbt.getVal("repo_table")) {
					dbRepos.addDataItem(dbt);
					it.remove();
				}
			}

			int count = 1;
			Connection conn = svr.dbGetConn();

			for (DbDataObject repo : dbRepos.getItems()) {

				String tableName = (String) repo.getVal(TABLE_NAME);
				log4j.info("Analysing repo " + tableName + ", repo " + count + " of " + dbRepos.getItems().size());
				String sqlQuery = "SELECT count(*), object_type from " + (String) repo.getVal("schema") + "."
						+ tableName + " GROUP BY object_type";

				getRepoData(tableName, sqlQuery, conn, dbTables, misconfiguredDbt, repo);
				count++;
			}

		} catch (Exception e) {
			log4j.error("Sync of the repo tables failed with exception", e);
		} finally {

			if (svr != null)
				svr.release();
		}
		log4j.info("Number of misconfigured objects:" + misconfiguredDbt.size());
		if (misconfiguredDbt.size() > 0)
			log4j.info("Run svarog install with --migrate-objects to finalise the repo update");
		return misconfiguredDbt;

	}

	public static int repoSync(HashMap<DbDataObject, DbDataObject> tables2Sync) {
		log4j.info("Executing migration of objects between the updated repo tables. This will take a while.");
		PreparedStatement ps1 = null;
		PreparedStatement ps2 = null;
		int retval = 0;
		if (tables2Sync.size() < 1) {
			log4j.error("The instance doesn't contain misconfigured objects to migrate!");
		} else {

			SvReader svr = null;
			try {
				svr = new SvReader();
				DbDataArray repoTables = svr.getObjects(
						new DbSearchCriterion("repo_table", DbCompareOperand.EQUAL, true), svCONST.OBJECT_TYPE_TABLE,
						null, 0, 0);
				repoTables.rebuildIndex(TABLE_NAME, true);
				Connection conn = svr.dbGetConn();
				conn.setAutoCommit(false);

				StringBuilder fields = new StringBuilder();

				for (DbDataObject field : SvCore.getRepoDbtFields().getItems()) {
					fields.append(field.getVal("field_name") + ",");
				}
				fields.setLength(fields.length() - 1);
				for (Entry<DbDataObject, DbDataObject> entry : tables2Sync.entrySet()) {
					try {
						DbDataObject table = entry.getKey();
						DbDataObject oldRepo = entry.getValue();
						DbDataObject newRepo = repoTables.getItemByIdx((String) table.getVal("repo_name"));

						log4j.info("Migrating data for '" + table.getVal(TABLE_NAME) + "' from repo '"
								+ oldRepo.getVal(TABLE_NAME) + "' to repo '" + newRepo.getVal(TABLE_NAME) + "'");

						StringBuilder sqlMigrate = new StringBuilder();
						sqlMigrate.append("INSERT INTO " + newRepo.getVal("schema") + "." + newRepo.getVal(TABLE_NAME)
								+ " (" + fields + ") ");
						sqlMigrate.append("SELECT " + fields + " FROM " + oldRepo.getVal("schema") + "."
								+ oldRepo.getVal(TABLE_NAME) + " r1 WHERE ");
						sqlMigrate.append("EXISTS (SELECT 1 FROM " + table.getVal("schema") + "."
								+ table.getVal(TABLE_NAME) + " t1 WHERE t1.pkid=r1.meta_pkid)");
						log4j.info("Executing " + sqlMigrate.toString());
						try {
							ps1 = conn.prepareStatement(sqlMigrate.toString());
							ps1.execute();
						} finally {
							try {
								if (ps1 != null)
									ps1.close();
							} catch (SQLException e) {
								log4j.error("Closing of prepared statements failed with exception", e);
							}
						}
						sqlMigrate = new StringBuilder();
						if (dropIndexes((String) oldRepo.getVal(TABLE_NAME), (String) oldRepo.getVal("schema"))) {

							sqlMigrate.append("DELETE FROM " + oldRepo.getVal("schema") + "."
									+ oldRepo.getVal(TABLE_NAME) + " r1 WHERE ");
							sqlMigrate.append("r1.meta_pkid in (SELECT t1.pkid FROM " + table.getVal("schema") + "."
									+ table.getVal(TABLE_NAME) + " t1)");
							log4j.info("Executing " + sqlMigrate.toString());
							try {
								ps2 = conn.prepareStatement(sqlMigrate.toString());
								ps2.execute();
							} finally {

								try {
									if (ps2 != null)
										ps2.close();
								} catch (SQLException e) {
									log4j.error("Closing of prepared statements failed with exception", e);
								}
							}
							log4j.info("Repo data successfully migrated, commiting changes");
							conn.commit();
							rebuildIndexes((String) oldRepo.getVal(TABLE_NAME));
						} else
							conn.rollback();
					} catch (Exception e) {
						log4j.error("Exception occured, rolling back the previous statements", e);
						conn.rollback();
					}

				}
			} catch (Exception e) {
				log4j.error("Sync of the repo tables failed with exception", e);
				retval = -3;
			} finally {

				if (svr != null)
					svr.release();
			}
		}
		return retval;
	}

	/**
	 * Recursively called method to upgrade all codes with their respective children
	 * 
	 * @param newCodes    The new set of codes which should be available after
	 *                    upgrade
	 * @param oldCodes    The old set of codes which was available prior upgrade
	 * @param newParentId The parent Id of the code for which the upgrade is run
	 * @param oldParentId If the parent id is different in the new set, this would
	 *                    include the new parent id
	 * @param dbu         DbUtil instance to be used for supporting funcationalities
	 * @param dboFields   The list of fields of the codes object
	 * @throws SvException Any SvExceptin raised during the method execution is
	 *                     reaised up
	 */
	static void upgradeCode(DbDataArray newCodes, DbDataArray oldCodes, Long newParentId, Long oldParentId,
			SvWriter dbu, DbDataArray dboFields) throws SvException {
		dbu.isInternal = true;
		Boolean isExisting = false;
		for (DbDataObject codeToUpgrade : newCodes.getItems()) {
			// run upgrade for root Codes only
			Long parentId = codeToUpgrade.getParentId();
			if (parentId != null && parentId.equals(newParentId)) {

				Boolean updateRequired = true;
				DbDataObject existingCode = null;
				if (oldParentId != null)
					existingCode = oldCodes.getItemByIdx((String) codeToUpgrade.getVal("CODE_VALUE"), oldParentId);

				Long nextLoopNewParentId = codeToUpgrade.getObjectId();
				Long nextLoopOldParentId = existingCode != null ? existingCode.getObjectId() : null;

				updateRequired = true;
				isExisting = true;
				if (existingCode != null) {
					Long existingPkid = existingCode.getPkid();
					Long existingOID = existingCode.getObjectId();
					updateRequired = shouldUpgradeConfig(existingCode, codeToUpgrade, dboFields, true);
					if (updateRequired) {
						codeToUpgrade.setPkid(existingPkid);
						codeToUpgrade.setObjectId(existingOID);
						codeToUpgrade.setParentId(oldParentId);
					}
				} else {
					isExisting = false;
					codeToUpgrade.setObjectId(0L);
					codeToUpgrade.setParentId(oldParentId != null ? oldParentId : newParentId);
				}
				if (updateRequired) {

					dbu.saveObject(codeToUpgrade, false);
					if (!isExisting)
						nextLoopOldParentId = codeToUpgrade.getObjectId();
				}

				upgradeCode(newCodes, oldCodes, nextLoopNewParentId, nextLoopOldParentId, dbu, dboFields);

			}
		}

	}

	/**
	 * Method to upgrade the svarog system codes to the latest version available in
	 * the master file containing Codes
	 * 
	 * @param upgradeFile the path to the file subject of upgrade
	 * @throws SvException Any raised exception is re-raised as SvException
	 */
	static void upgradeCodes(String upgradeFile) throws SvException {
		try (SvWriter dbu = new SvWriter();) {

			dbu.dbSetAutoCommit(false);
			log4j.info("Processing code " + operation + ": " + upgradeFile);

			InputStream fis = new FileInputStream(new File(upgradeFile));
			String json = IOUtils.toString(fis, "UTF-8");
			json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());

			Gson gson = new Gson();
			JsonObject jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
			DbDataArray newCodes = new DbDataArray();
			newCodes.fromJson(jobj);

			DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
					SvCore.getDbt(svCONST.OBJECT_TYPE_CODE), SvCore.getFields(svCONST.OBJECT_TYPE_CODE), null, null,
					null);
			DbDataArray oldCodes = dbu.getObjects(query, 0, 0);

			oldCodes.rebuildIndex("CODE_VALUE");
			newCodes.rebuildIndex("CODE_VALUE");

			DbDataArray codeFields = SvCore.getFields(svCONST.OBJECT_TYPE_CODE);
			codeFields.rebuildIndex("FIELD_NAME");

			upgradeCode(newCodes, oldCodes, 0L, 0L, dbu, codeFields);

			dbu.dbCommit();

		} catch (IOException e) {
			throw (new SvException("system.error.upgrade_no_records_conf", svCONST.systemUser, null, upgradeFile, e));
		}
		log4j.info("Finished " + operation + " using records file: " + upgradeFile);

	}

	/**
	 * A method to upgrade the svarog link types according to a JSON config file
	 * 
	 * @param linkTypes DbDataArray holding the link types
	 * @throws SvException static void upgradeTypeCfg(DbDataArray linkTypes) throws
	 *                     SvException { // default upgrade file "conf/records/40.
	 *                     master_records.json" SvWriter dbu = null; SvReader svr =
	 *                     null; try { // init the table configs
	 * 
	 *                     log4j.info("Processing base table upgrades: " +
	 *                     upgradeFile); dbu = new SvWriter(); dbu.initSvCore(true);
	 *                     dbu.isInternal = true; dbu.dbSetAutoCommit(false); svr =
	 *                     new SvReader(dbu);
	 * 
	 * 
	 *                     DbQueryObject query = new DbQueryObject(SvCore.repoDbt,
	 *                     SvCore.repoDbtFields,
	 *                     SvCore.getDbt(svCONST.OBJECT_TYPE_LINK_TYPE),
	 *                     SvCore.getFields(svCONST.OBJECT_TYPE_LINK_TYPE), null,
	 *                     null, null);
	 * 
	 *                     DbDataArray dbLinkTypes = dbu.getObjects(query, 0, 0);
	 * 
	 *                     Boolean updateRequired = true; Long existingPkid = 0L;
	 *                     Long existingOID = 0L;
	 * 
	 *                     // dbTablesUpgrade.rebuildIndex("OBJECT_ID"); for
	 *                     (DbDataObject dboUpgrade : dbTablesUpgrade.getItems()) {
	 *                     DbDataObject existingTbl = dbTables.getItemByIdx((String)
	 *                     dboUpgrade.getVal("TABLE_NAME"), 0L); updateRequired =
	 *                     true;
	 * 
	 *                     if (existingTbl != null) { existingPkid =
	 *                     existingTbl.getPkid(); existingOID =
	 *                     existingTbl.getObjectId(); updateRequired =
	 *                     shouldUpgradeConfig(existingTbl, dboUpgrade,
	 *                     tableFields); dboUpgrade.setPkid(existingPkid);
	 *                     dboUpgrade.setObjectId(existingOID); //
	 *                     dboUpgrade.setPkid(existingTbl.getPkid()); //
	 *                     dboUpgrade.setObjectId(existingTbl.getObjectId()); } if
	 *                     (updateRequired) { dbu.saveObject(dboUpgrade, false);
	 *                     log4j.info( "Successful upgrade of object: " +
	 *                     dboUpgrade.toJson()); } }
	 * 
	 *                     dbu.dbCommit(); } catch (IOException e) { throw (new
	 *                     SvException("system.error.upgrade_no_records_conf",
	 *                     dbu.instanceUser, null, linkTypes, e)); } finally { if
	 *                     (svr != null) svr.release(); if (dbu != null)
	 *                     dbu.release(); } log4j.info( "Upgrade finished using
	 *                     records file: " +linkTypes); }
	 */

	/**
	 * Method to delete all svarog fields which aren't found in the upgraded table
	 * structure
	 * 
	 * @param dbu           the SvWriter instance to be used for saving to the DB
	 * @param tblUpgrade    The descriptor of the table which is upgraded
	 * @param fieldsUpgrade List of fields which are upgraded (as property of the
	 *                      table)
	 * @param dbTbl         The descriptor of the existing table in the system
	 * @param dbFields      List of field descriptors already existing in the system
	 * @throws SvException If any exception occurs, raise it.
	 */
	static void deleteRedundantFields(SvWriter dbu, DbDataObject tblUpgrade, DbDataArray fieldsUpgrade,
			DbDataObject dbTbl, DbDataArray dbFields) throws SvException {
		DbDataArray tblFields = new DbDataArray();
		DbDataArray tblDbFields = new DbDataArray();
		for (DbDataObject dbField : fieldsUpgrade.getItems())
			if ((dbField.getObjectId() != 0L && dbField.getParentId().equals(tblUpgrade.getObjectId()))
					|| dbField.getVal("PARENT_NAME").equals(tblUpgrade.getVal("TABLE_NAME")))
				tblFields.addDataItem(dbField);

		for (DbDataObject dbField : dbFields.getItems())
			if (dbField.getObjectId() != 0L && dbField.getParentId().equals(dbTbl.getObjectId()))
				tblDbFields.addDataItem(dbField);
		tblFields.rebuildIndex("FIELD_NAME", true);

		for (DbDataObject currentDbField : tblDbFields.getItems()) {
			DbDataObject newField = tblFields.getItemByIdx((String) currentDbField.getVal("FIELD_NAME"));
			if (newField == null)
				dbu.deleteObject(currentDbField);
		}
	}

	/**
	 * Method to load get DbDataArray with all system locales
	 * 
	 * @return The object containing the locales
	 */
	static DbDataArray getLocaleList() {
		if (sysLocales == null) {
			if (!isSvarogInstalled()) {
				DbDataArray locales = new DbDataArray();
				InputStream fis = SvCore.class.getResourceAsStream(LOCALE_PATH);
				String json;
				try {
					json = IOUtils.toString(fis, "UTF-8");
					json = json.replace("{OBJECT_TYPE_LOCALE}", Long.toString(svCONST.OBJECT_TYPE_LOCALE));
					Gson gson = new GsonBuilder().create();
					JsonObject jobj = gson.fromJson(json, JsonObject.class);
					locales.fromJson(jobj);
					locales.rebuildIndex("LOCALE_ID", true);

					sysLocales = locales;
				} catch (IOException e) {
					log4j.error("Error loading locale list", e);
					sysLocales = null;
				}
			} else {
				SvReader svr = null;
				try {
					svr = new SvReader();
					DbSearchCriterion dbs = new DbSearchCriterion("OBJECT_TYPE", DbCompareOperand.EQUAL,
							svCONST.OBJECT_TYPE_LOCALE);
					DbDataArray locales = svr.getObjects(dbs, svCONST.OBJECT_TYPE_LOCALE, null, 0, 0);
					locales.rebuildIndex("LOCALE_ID", true);
					// ensure that the local list is read-only
					for (DbDataObject locale : locales.getItems())
						DboFactory.makeDboReadOnly(locale);
					sysLocales = locales;

				} catch (SvException e) {
					log4j.error("Repo seems valid, but system locales aren't loaded", e);
				} finally {
					if (svr != null)
						svr.release();
				}
			}
		}
		return sysLocales;
	}

	/**
	 * Method to prepare a JSON configuration file for use and load it into a
	 * DbDataArray
	 * 
	 * @param filePath The path to the configuration file
	 * @return DbDataArray holding all configuration objects loaded from the file
	 * @throws SvException if exception was raised during parsing the JSON file it
	 *                     is re-raised as SvException
	 */
	static DbDataArray getDbArrayFromFile(String filePath) throws SvException {
		DbDataArray retArr = null;
		InputStream fis = null;
		try {
			fis = new FileInputStream(new File(filePath));

			String json = IOUtils.toString(fis, "UTF-8");
			json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());

			Gson gson = new Gson();
			JsonObject jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();

			JsonElement jEl = jobj.get(DbDataArray.class.getCanonicalName());
			if (jEl != null) {
				retArr = new DbDataArray();
				retArr.fromJson(jobj);
			}
		} catch (Exception e) {
			log4j.error("Exception reading config records from: " + filePath, e);
			retArr = null;
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					log4j.error("Can't close upgrade file stream", e);
				}

		}
		if (retArr == null)
			throw (new SvException("system.error.upgrade_no_records_conf", svCONST.systemUser, null, filePath));
		else
			return retArr;

	}

	/**
	 * Method to extract only objects of certain type from the input array
	 * 
	 * @param sourceArray      The source array from which we should extract objects
	 * @param objectType       The id of the object type which should be extracted
	 * @param removeFromSource Flag to mark if the extracted object should be
	 *                         removed from the source array
	 * @return DbDataArray containing the extracted objects
	 */
	static DbDataArray extractType(DbDataArray sourceArray, Long objectType, boolean removeFromSource) {
		DbDataArray extractedObjects = new DbDataArray();
		Iterator<DbDataObject> iter = sourceArray.getItems().iterator();
		while (iter.hasNext()) {
			DbDataObject dbo = iter.next();
			if (dbo.getObjectType().equals(objectType)) {
				extractedObjects.addDataItem(dbo);
				if (removeFromSource)
					iter.remove();
			}

		}
		return extractedObjects;
	}

	/**
	 * Method to upgrade a single DBT configuration
	 * 
	 * @param svw             The SvWriter instance to be used for db operations
	 * @param dbtUpgrade      The DBT object which is upgraded
	 * @param dbTablesUpgrade The list of new DBT objects (needed for dependency
	 *                        search
	 * @param dbTables        The list of DBTs already installed in the DB
	 * @param tableFields     The fields of the object type subject of upgrade
	 * @param dbFieldsUpgrade The new fields for the dbtUpgrade
	 * @param dbFields        The old fields for of dbtOld
	 * @param listOfUpgraded  The list of Tables for which the upgrade was already
	 *                        executed
	 * 
	 * @throws SvException Re-throw any underlying exception
	 * 
	 */
	static void upgradeDbt(SvWriter svw, DbDataObject dbtUpgrade, DbDataArray dbTablesUpgrade, DbDataArray dbTables,
			DbDataArray tableFields, DbDataArray dbFieldsUpgrade, DbDataArray dbFields,
			ArrayList<String> listOfUpgraded) throws SvException {
		String tableName = (String) dbtUpgrade.getVal("TABLE_NAME");
		String configRelationName = null;
		DbDataObject dbtOld = dbTables.getItemByIdx(tableName);
		if (listOfUpgraded.contains(tableName))
			return;

		Boolean updateRequired = true;

		derefenceDependency(svw, dbtUpgrade, dbTablesUpgrade, dbTables, tableFields, dbFieldsUpgrade, dbFields,
				listOfUpgraded, true, "PARENT_NAME");
		derefenceDependency(svw, dbtUpgrade, dbTablesUpgrade, dbTables, tableFields, dbFieldsUpgrade, dbFields,
				listOfUpgraded, false, "CONFIG_TYPE_ID");

		if (dbtUpgrade.getVal("config_relation_id") != null
				&& dbtUpgrade.getVal("config_relation_id") instanceof String) {
			configRelationName = (String) dbtUpgrade.getVal("config_relation_id");
			dbtUpgrade.setVal("config_relation_id", nameToId(dbtUpgrade, (Long) dbtUpgrade.getVal("CONFIG_TYPE_ID"),
					svCONST.OBJECT_TYPE_FIELD, configRelationName, svw));
		}

		updateRequired = true;

		if (dbtOld != null) {
			Long existingPkid = dbtOld.getPkid();
			Long existingOID = dbtOld.getObjectId();
			updateRequired = shouldUpgradeConfig(dbtOld, dbtUpgrade, tableFields);
			dbtUpgrade.setPkid(existingPkid);
			dbtUpgrade.setObjectId(existingOID);
			// dboUpgrade.setPkid(existingTbl.getPkid());
			// dboUpgrade.setObjectId(existingTbl.getObjectId());
		}
		if (updateRequired) {
			svw.saveObject(dbtUpgrade, false);
			listOfUpgraded.add(tableName);
		}
		if (dbtOld != null)
			deleteRedundantFields(svw, dbtUpgrade, dbFieldsUpgrade, dbtOld, dbFields);

		upgradeObjectFields(tableName, dbTablesUpgrade, dbFieldsUpgrade, dbTables, dbFields, svw);

		// try to resolve the id of the related field
		if (configRelationName != null) {
			dbtUpgrade.setVal("config_relation_id", nameToId(dbtUpgrade, (Long) dbtUpgrade.getVal("CONFIG_TYPE_ID"),
					svCONST.OBJECT_TYPE_FIELD, configRelationName, svw));
			if ((dbtOld != null && shouldUpgradeConfig(dbtOld, dbtUpgrade, tableFields)) || dbtOld == null)
				svw.saveObject(dbtUpgrade, false);
		}

		svw.dbCommit();
	}

	public static Long nameToId(DbDataObject upgradeObject, long foreignObjectId, long typeDescriptor, String name,
			SvCore core) {
		Long retval = 0L;
		boolean found = false;
		SvReader svr = null;
		DbDataArray fields = null;
		if (typeDescriptor == svCONST.OBJECT_TYPE_FIELD) {
			try {
				svr = new SvReader(core);
				// load field types from the DB
				DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
						SvCore.getDbt(svCONST.OBJECT_TYPE_FIELD), SvCore.getFields(svCONST.OBJECT_TYPE_FIELD), null,
						null, null);

				DbSearchCriterion dbs = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL,
						upgradeObject.getObjectId());
				query.setSearch(dbs);
				fields = svr.getObjects(query, null, null);
			} catch (Exception e) {
				log4j.error("Can't get fields for table:" + upgradeObject.toJson());
			} finally {
				if (svr != null)
					svr.release();
			}

			if (fields != null && fields.size() > 0) {
				fields.rebuildIndex("FIELD_NAME", true);
				DbDataObject field = fields.getItemByIdx(name);
				if (field != null) {
					found = true;
					retval = field.getObjectId();
				}
			}
			if (!found)
				log4j.warn("Can't derefence field name:" + name + " in table:" + upgradeObject.toJson());
		} else if (typeDescriptor == svCONST.OBJECT_TYPE_LINK_TYPE) {

		}
		return retval;
	}

	/**
	 * Method to ensure the table's parent is upgraded in proper order.
	 * 
	 * @param svw             The SvWriter instance to be used for db operations
	 * @param dbtUpgrade      The DBT object which is upgraded
	 * @param dbTablesUpgrade The list of new DBT objects (needed for dependency
	 *                        search
	 * @param dbTables        The list of DBTs already installed in the DB
	 * @param tableFields     The fields of the object type subject of upgrade
	 * @param dbFieldsUpgrade The new fields for the dbtUpgrade
	 * @param dbFields        The old fields for of dbtOld
	 * @param listOfUpgraded  The list of Tables for which the upgrade was already
	 *                        executed
	 * @param isParent        If the dependency if of type parent or denormalised if
	 *                        false
	 * @param fieldName       the field name used for dereferencing the relation
	 * @throws SvException Re-throw any underlying exception
	 */
	static void derefenceDependency(SvWriter svw, DbDataObject dbtUpgrade, DbDataArray dbTablesUpgrade,
			DbDataArray dbTables, DbDataArray tableFields, DbDataArray dbFieldsUpgrade, DbDataArray dbFields,
			ArrayList<String> listOfUpgraded, boolean isParent, String fieldName) throws SvException {
		String dependentTable = (String) dbtUpgrade.getVal(fieldName);
		if (dependentTable != null && !dependentTable.equals("")) {
			DbDataObject dbot = SvCore.getDbtByName(dependentTable);
			if (dbot != null) {
				if (isParent)
					dbtUpgrade.setParentId(dbot.getObjectId());
				else
					dbtUpgrade.setVal(fieldName, dbot.getObjectId());
			} else {
				DbDataObject dependentDbt = dbTablesUpgrade.getItemByIdx(dependentTable);
				if (dependentDbt == null)
					dependentDbt = dbTablesUpgrade.getItemByIdx(SvConf.getMasterRepo() + "_" + dependentTable);
				if (dependentDbt != null) {

					Long dependentId = dependentDbt.getObjectId();
					if (!dependentId.equals(dbtUpgrade.getObjectId())) { // if
																			// parent
																			// is
																			// not
																			// self.
																			// upgrade
						upgradeDbt(svw, dependentDbt, dbTablesUpgrade, dbTables, tableFields, dbFieldsUpgrade, dbFields,
								listOfUpgraded);

					} else {// if parent is self, just assign
						dependentId = dbtUpgrade.getObjectId();
					}
					if (isParent)
						dbtUpgrade.setParentId(dependentId);
					else
						dbtUpgrade.setVal(fieldName, dependentId);
				} else
					log4j.warn("Parent not found! Table:" + dbtUpgrade.getVal("TABLE_NAME") + ", parent name:"
							+ dependentTable);
				// parentNotFound = true;
			}
		}
	}

	/**
	 * Method to upgrade the base svarog configuration of Tables/Fields
	 * 
	 * @param dbTablesUpgrade The array of table descriptors subject of upgrade
	 * @param dbFieldsUpgrade The array of associated field descriptors subject of
	 *                        upgrade
	 * @throws SvException Re-raised SvException from the process
	 */
	@SuppressWarnings("unused")
	static void upgradeObjectCfg(DbDataArray dbTablesUpgrade, DbDataArray dbFieldsUpgrade) throws SvException {
		// TODO this sausage must be refactored!!!
		Boolean updateRequired = true;
		Long existingPkid = 0L;
		Long existingOID = 0L;

		try (SvWriter dbu = new SvWriter(); SvReader svr = new SvReader(dbu)) {
			// init the table configs

			dbu.isInternal = true;
			dbu.dbSetAutoCommit(false);

			DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
					SvCore.getDbt(svCONST.OBJECT_TYPE_TABLE), SvCore.getFields(svCONST.OBJECT_TYPE_TABLE), null, null,
					null);

			DbDataArray dbTables = dbu.getObjects(query, 0, 0);

			query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields, SvCore.getDbt(svCONST.OBJECT_TYPE_FIELD),
					SvCore.getFields(svCONST.OBJECT_TYPE_FIELD), null, null, null);

			DbDataArray dbFields = dbu.getObjects(query, 0, 0);

			// dbTablesUpgrade.rebuildIndex("OBJECT_ID");
			dbTables.rebuildIndex("TABLE_NAME", true);
			dbFields.rebuildIndex("FIELD_NAME");
			dbTablesUpgrade.rebuildIndex("TABLE_NAME", true);

			DbDataArray tableFields = SvCore.getFields(svCONST.OBJECT_TYPE_TABLE);
			tableFields.rebuildIndex("FIELD_NAME");

			// for each of the new DBT configurations in the JSON run the
			// upgrade process
			Iterator<DbDataObject> dbtIter = dbTablesUpgrade.getItems().iterator();
			ArrayList<String> listOfUpgraded = new ArrayList<String>();
			while (dbtIter.hasNext()) {
				DbDataObject dbt2Upg = dbtIter.next();
				upgradeDbt(dbu, dbt2Upg, dbTablesUpgrade, dbTables, tableFields, dbFieldsUpgrade, dbFields,
						listOfUpgraded);
			}

			// ensure that the field with lowest sort order is always named
			// PKID!
			ArrayList<DbDataObject> sortedFields = dbFieldsUpgrade.getSortedItems("SORT_ORDER");
			if (sortedFields.size() > 0) {
				boolean pkidListInProgress = true;
				int pkidFieldCount = 0;
				for (DbDataObject dbo : sortedFields) {
					String fieldName = (String) dbo.getVal("FIELD_NAME");
					if (pkidListInProgress) {
						if (!fieldName.equals("PKID"))
							pkidListInProgress = false;
						else
							pkidFieldCount++;
					}

					if (!pkidListInProgress && pkidFieldCount != dbTablesUpgrade.size())
						throw (new SvException("system.error.field_pkid_not_first", dbu.instanceUser, null, dbo));

				}

			}
			dbu.dbCommit();
		}
	}

	/**
	 * Method to perform upgrade of the fields in the svarog platform
	 * 
	 * @param dbTablesUpgrade The configuration of the tables which coming from the
	 *                        JSON and should be applied to the DB
	 * @param dbFieldsUpgrade The configuration of the field which coming from the
	 *                        JSON and should be applied to the DB
	 * @param dbTables        The configuration of the tables which exists in the DB
	 * @param dbFields        The configuration of the fields which exists in the DB
	 * @throws SvException
	 */
	static void upgradeObjectFields(String parentTableName, DbDataArray dbTablesUpgrade, DbDataArray dbFieldsUpgrade,
			DbDataArray dbTables, DbDataArray dbFields, SvWriter dbu) throws SvException {
		Boolean updateRequired = true;
		Long existingPkid = 0L;
		Long existingOID = 0L;
		SvReader svr = null;

		DbDataArray fieldFields = SvCore.getFields(svCONST.OBJECT_TYPE_FIELD);
		fieldFields.rebuildIndex("FIELD_NAME");

		for (DbDataObject dboUpgrade : dbFieldsUpgrade.getItems()) {
			if (!dboUpgrade.getVal("PARENT_NAME").equals(parentTableName))
				continue;

			updateRequired = true;
			DbDataObject fieldUpgradeParent = null;
			for (DbDataObject dbt : dbTablesUpgrade.getItems())
				if ((dbt.getObjectId() != 0L && dboUpgrade.getParentId().equals(dbt.getObjectId()))
						|| dboUpgrade.getVal("PARENT_NAME").equals(dbt.getVal("TABLE_NAME")))
					fieldUpgradeParent = dbt;

			DbDataObject fieldDbParent = null;
			try {
				fieldDbParent = dbTables.getItemByIdx((String) fieldUpgradeParent.getVal("TABLE_NAME"), 0L);
				if (fieldDbParent == null)
					fieldDbParent = dbTablesUpgrade.getItemByIdx((String) fieldUpgradeParent.getVal("TABLE_NAME"));
				dboUpgrade.setParentId(fieldDbParent.getObjectId());
			} catch (Exception e) {
				log4j.error("Failed upgrade of:" + fieldUpgradeParent.toJson().toString());
				return;
			}
			DbDataObject existingField = null;

			existingField = dbFields.getItemByIdx((String) dboUpgrade.getVal("FIELD_NAME"),
					fieldDbParent.getObjectId());

			if (dboUpgrade.getVal("CODE_LIST_MNEMONIC") != null) {
				// sync ids of the code lists in the db
				DbSearchCriterion critCodeVal = new DbSearchCriterion("CODE_VALUE", DbCompareOperand.EQUAL,
						dboUpgrade.getVal("CODE_LIST_MNEMONIC"));
				DbSearchCriterion critCodeParent = new DbSearchCriterion("PARENT_ID", DbCompareOperand.EQUAL, 0L);
				DbSearchExpression searchCode = new DbSearchExpression();
				searchCode.addDbSearchItem(critCodeVal);
				searchCode.addDbSearchItem(critCodeParent);

				log4j.trace("Decoding CODE_LIST_MNEMONIC:" + dboUpgrade.getVal("CODE_LIST_MNEMONIC"));
				DbDataArray codes = null;
				try {
					svr = new SvReader(dbu);
					codes = svr.getObjects(searchCode, svCONST.OBJECT_TYPE_CODE, null, 0, 0);
				} catch (Exception ex) {
					log4j.error("Failed decoding mnemonic", ex);
				} finally {
					if (svr != null)
						svr.release();
				}
				if (codes != null && codes.getItems().size() > 0) {
					dboUpgrade.setVal("CODE_LIST_ID", codes.getItems().get(0).getObjectId());
				} else {
					throw (new SvException("system.error.upgrade_no_codelist", dbu.instanceUser, dboUpgrade,
							searchCode));

				}
			}

			if (existingField != null) {
				existingPkid = existingField.getPkid();
				existingOID = existingField.getObjectId();
				updateRequired = shouldUpgradeConfig(existingField, dboUpgrade, fieldFields, true);
				if (updateRequired) {
					dboUpgrade.setPkid(existingPkid);
					dboUpgrade.setObjectId(existingOID);
				}
			} else {
				dboUpgrade.setObjectId(0L);
			}
			if (updateRequired) {
				dbu.saveObject(dboUpgrade, false);
				if ((dboUpgrade.getParentId().equals(svCONST.OBJECT_TYPE_FIELD)
						|| dboUpgrade.getParentId().equals(svCONST.OBJECT_TYPE_TABLE)
						|| dboUpgrade.getParentId().equals(svCONST.OBJECT_TYPE_REPO))
						&& SvarogInstall.isSvarogInstalled()) {
					dbu.dbCommit();
					SvCore.initSvCore(true);
				}
				if (log4j.isDebugEnabled())
					log4j.debug("Successful upgrade of object: " + dboUpgrade.toJson());
			}
		}
	}

	static void upgradeSidAcl(DbDataArray dbAclSidUpgrade) throws SvException {

		DbDataArray aclSidsToSave = new DbDataArray();
		try (SvWriter dbu = new SvWriter(); SvReader svr = new SvReader(dbu)) {
			// init the table configs
			dbu.isInternal = true;
			dbu.setAutoCommit(false);
			svr.isInternal = true;
			svr.setAutoCommit(false);

			DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
					SvCore.getDbt(svCONST.OBJECT_TYPE_SID_ACL), SvCore.getFields(svCONST.OBJECT_TYPE_SID_ACL), null,
					null, null);

			DbDataArray oldSidACLs = dbu.getObjects(query, 0, 0);
			DbDataArray sidAclFields = SvCore.getFields(svCONST.OBJECT_TYPE_SID_ACL);
			sidAclFields.rebuildIndex(Sv.FIELD_NAME.toString());
			Iterator<DbDataObject> iteratorAclUpgrade = dbAclSidUpgrade.getItems().iterator();
			while (iteratorAclUpgrade.hasNext()) {
				DbDataObject acl = iteratorAclUpgrade.next();
				boolean shouldIgnore = true;
				DbDataObject dbt = null;
				if (acl.getVal(Sv.SID_TYPE_ID) != null) {
					if (acl.getVal(Sv.SID_TYPE_ID) instanceof String) {
						String strTypeId = (String) acl.getVal(Sv.SID_OBJECT_ID);
						dbt = SvCore.getDbtByName(strTypeId);
					}
				} else
					dbt = SvCore.getDbt(svCONST.OBJECT_TYPE_GROUP);

				acl.setVal(Sv.SID_TYPE_ID, dbt != null ? dbt.getObjectId() : dbt);
				if (acl.getVal(Sv.SID_OBJECT_ID) instanceof String) {
					DbDataObject dbf = null;
					dbf = svr.getObjectByUnqConfId((String) acl.getVal(Sv.SID_OBJECT_ID), dbt);
					if (dbf != null) {
						acl.setVal(Sv.SID_OBJECT_ID, dbf.getObjectId());
						shouldIgnore = false;
					}
				}
				if (acl.getVal(Sv.ACL_LABEL_CODE) instanceof String) {
					DbDataObject dbf = null;
					dbf = svr.getObjectByUnqConfId((String) acl.getVal(Sv.ACL_LABEL_CODE),
							SvCore.getDbt(svCONST.OBJECT_TYPE_ACL));
					if (dbf != null) {
						acl.setVal(Sv.ACL_OBJECT_ID, dbf.getObjectId());
						shouldIgnore = false;
					}
				}

				if (shouldIgnore) {
					log4j.warn("ACL object not assigned to any SID, because ACL, Object or Type is not found!"
							+ acl.toSimpleJson().toString());
					iteratorAclUpgrade.remove();
				} else {
					boolean shouldUpgrade = true;
					for (DbDataObject sidAcl : oldSidACLs.getItems()) {
						if (sidAcl.getVal(Sv.SID_OBJECT_ID).equals(acl.getVal(Sv.SID_OBJECT_ID))
								&& sidAcl.getVal(Sv.ACL_OBJECT_ID).equals(acl.getVal(Sv.ACL_OBJECT_ID))
								&& sidAcl.getVal(Sv.SID_TYPE_ID).equals(acl.getVal(Sv.SID_TYPE_ID))) {
							if (shouldUpgradeConfig(sidAcl, acl, sidAclFields)) {
								acl.setObjectId(sidAcl.getObjectId());
								acl.setPkid(sidAcl.getPkid());
							} else
								shouldUpgrade = false;
						}
					}
					if (shouldUpgrade)
						aclSidsToSave.addDataItem(acl);
				}
			}

			dbu.saveObject(aclSidsToSave);
			dbu.dbCommit();
		}
	}

	/**
	 * Method to upgrade the system ACLs according to the array holding new and
	 * updated ACLs
	 * 
	 * @param dbAclUpgrade The array of new and updated ACLs
	 * @throws SvException Forward any raised exception
	 */
	static void upgradeAcl(DbDataArray dbAclUpgrade) throws SvException {
		try (SvWriter dbu = new SvWriter(); SvReader svr = new SvReader(dbu)) {
			// init the table configs
			dbu.isInternal = true;
			dbu.setAutoCommit(false);
			svr.isInternal = true;
			svr.setAutoCommit(false);

			DbDataArray aclsToSave = new DbDataArray();

			DbQueryObject query = new DbQueryObject(SvCore.repoDbt, SvCore.repoDbtFields,
					SvCore.getDbt(svCONST.OBJECT_TYPE_ACL), SvCore.getFields(svCONST.OBJECT_TYPE_ACL), null, null,
					null);

			dbAclUpgrade.rebuildIndex(Sv.LABEL_CODE.toString(), true);
			DbDataArray dbAcls = dbu.getObjects(query, 0, 0);
			dbAcls.rebuildIndex(Sv.LABEL_CODE.toString(), true);

			// Iterator<DbDataObject> iteratorAcl =
			// dbAcls.getItems().iterator();
			Iterator<DbDataObject> iteratorAclUpgrade = dbAclUpgrade.getItems().iterator();
			while (iteratorAclUpgrade.hasNext()) {
				DbDataObject acl = iteratorAclUpgrade.next();
				boolean shouldIgnore = false;
				if (acl.getVal("acl_object_type") instanceof String) {
					String strTypeId = (String) acl.getVal("acl_object_type");
					Long dbtId = SvCore.getTypeIdByName(strTypeId);
					if (dbtId.equals(0L)) {
						shouldIgnore = true;
					} else {
						acl.setVal("acl_object_type", dbtId);
						if (acl.getVal(Sv.ACL_OBJECT_ID) instanceof String) {
							DbDataObject dbf = null;
							if (dbtId.equals(svCONST.OBJECT_TYPE_FIELD))
								dbf = SvCore.getFieldByName(strTypeId, (String) acl.getVal(Sv.ACL_OBJECT_ID));
							else
								dbf = svr.getObjectByUnqConfId((String) acl.getVal(Sv.ACL_OBJECT_ID), strTypeId);
							if (dbf == null) {
								shouldIgnore = true;
							} else
								acl.setVal(Sv.ACL_OBJECT_ID, dbf.getObjectId());
						}

					}
				}
				if (shouldIgnore) {
					log4j.warn("ACL object not upgraded, because object or type is not found!"
							+ acl.toSimpleJson().toString());
					iteratorAclUpgrade.remove();
				}

			}
			iteratorAclUpgrade = dbAclUpgrade.getItems().iterator();
			DbDataArray aclFields = SvCore.getFields(svCONST.OBJECT_TYPE_ACL);
			aclFields.rebuildIndex("FIELD_NAME");
			while (iteratorAclUpgrade.hasNext()) {
				DbDataObject acl = iteratorAclUpgrade.next();
				DbDataObject oldAcl = dbAcls.getItemByIdx((String) acl.getVal(Sv.LABEL_CODE));
				if (oldAcl != null) {
					acl.setObjectId(oldAcl.getObjectId());
					acl.setPkid(oldAcl.getPkid());
					if (shouldUpgradeConfig(oldAcl, acl, aclFields))
						aclsToSave.addDataItem(acl);
				} else
					aclsToSave.addDataItem(acl);
			}
			// else
			// aclsToSave = dbAclUpgrade;
			dbu.saveObject(aclsToSave);
			dbu.dbCommit();
		}

	}

	/**
	 * A method that converts a InputStream into a String.
	 * 
	 * @param is InputStream. The InputStream we want converted into String.
	 * @return The converted String
	 * 
	 * @throws IOException IO exception if the file hasn't been found
	 */
	public static String convertStreamToString(InputStream is) throws IOException {
		/*
		 * To convert the InputStream to String we use the Reader.read(char[] buffer)
		 * method. We iterate until the Reader return -1 which means there's no more
		 * data to read. We use the StringWriter class to produce the string.
		 */
		if (is != null) {
			Writer writer = new StringWriter();

			char[] buffer = new char[1024];
			try {
				Reader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
				int n;
				while ((n = reader.read(buffer)) != -1) {
					writer.write(buffer, 0, n);
				}
			} finally {
				is.close();
			}
			return writer.toString();
		} else {
			return "";
		}
	}

	/**
	 * Method which reads a file and returns the content as byte array
	 * 
	 * @param file The File object which should be read
	 * @return The content of the file (byte[])
	 */
	public static byte[] getBytesFromFile(File file) {
		try (InputStream is = new FileInputStream(file)) {

			// Get the size of the file
			long length = file.length();

			if (length > Integer.MAX_VALUE) {
				// File is too large
			}

			// Create the byte array to hold the data
			byte[] bytes = new byte[(int) length];

			// Read in the bytes
			int offset = 0;
			int numRead = 0;
			while (offset < bytes.length && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
				offset += numRead;
			}

			// Ensure all the bytes have been read in
			if (offset < bytes.length) {
				is.close();
				throw new IOException("Could not completely read file " + file.getName());
			}

			// Close the input stream and return bytes
			return bytes;
		} catch (Exception ex) {
			log4j.error("Error reading file:" + file.getAbsolutePath(), ex);
		}
		return null;
	}

	/**
	 * Method which reads a file and returns the content as char array
	 * 
	 * @param file The File object which should be read
	 * @return The content of the file (char[])
	 */
	public static char[] getStringFromFile(File file) {
		try (FileReader is = new FileReader(file)) {

			// Get the size of the file
			long length = file.length();

			if (length > Integer.MAX_VALUE) {
				// File is too large
			}

			// Create the byte array to hold the data
			char[] bytes = new char[(int) length];

			// Read in the bytes
			int offset = 0;
			int numRead = 0;
			while (offset < bytes.length && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
				offset += numRead;
			}

			// Ensure all the bytes have been read in
			if (offset < bytes.length) {
				is.close();
				throw new IOException("Could not completely read file " + file.getName());
			}

			// Close the input stream and return bytes

			return bytes;
		} catch (Exception ex) {
			log4j.error("Error reading file.", ex);
		}
		return null;
	}

	/**
	 * Method to split a string into ArrayList according to separator
	 * 
	 * @param stringArray The string to be split into list
	 * @param separator   The separator to be used for splitting
	 * @return The list of strings resulting from the split
	 */
	public static List<String> stringToList(String stringArray, String separator) {
		List<String> separatedList = new ArrayList<String>();
		String[] temp = null;
		temp = stringArray.split(separator);
		int size = temp.length;
		for (int i = 0; i < size; i++) {
			separatedList.add(temp[i]);
		}
		if (separatedList.isEmpty())
			return null;
		else
			return separatedList;
	}

	/**
	 * Method to get the existing configuration object from the Database based on
	 * JSON Object
	 * 
	 * @param dbo The DbDataObject coming from JSON config
	 * @return The Configuration DbDataObject coming from the Database
	 */
	@SuppressWarnings("unused")
	private DbDataObject getConfigObjectFromDb(DbDataObject dbo) {
		// TODO to be used for configuration upgrade
		/*
		 * if (isCfgInDb) { DbDataObject dbt = getDbt(dbo.getObjectType()); //
		 * if(dbt.getVal(key)) }
		 */
		return null;
	}

	/**
	 * Check if an object descriptor is a config object or not
	 * 
	 * @param dbo The object descriptor to be checked
	 * @return True if the object is of configuration type
	 */
	Boolean isConfigObject(DbDataObject dbo) {
		// TODO to be used for configuration upgrade
		// return false;
		Boolean isConfig = false;
		/*
		 * long[] errorCode = new long[1]; DbDataObject dbt =
		 * getObjectById(dbo.getObjectType(), svCONST.OBJECT_TYPE_TABLE, null,
		 * errorCode); if (errorCode[0] == svCONST.SUCCESS && dbt != null) { if
		 * ((Boolean) dbt.getVal("is_config_table")) isConfig = true; }
		 */

		return isConfig;
	}

	static void registerFormFieldType(String fieldType, String fieldLabelCode, Boolean fieldIsNull,
			DbDataObject parentForm, SvCore parentCore) throws SvException {
		DbDataObject dboFieldType = new DbDataObject(svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
		dboFieldType.setVal("FIELD_TYPE", fieldType);
		dboFieldType.setVal(Sv.LABEL_CODE, fieldLabelCode);
		dboFieldType.setVal("IS_NULL", fieldIsNull);
		registerFormFieldType(dboFieldType, parentForm, parentCore);
		//
	}

	static void registerFormFieldType(DbDataObject fieldTypeDbo, DbDataObject parentForm, SvCore parentCore)
			throws SvException {

		DbDataObject dboFieldType = null;
		boolean linkExists = false;
		String fieldLabelCode = (String) fieldTypeDbo.getVal(Sv.LABEL_CODE);
		try (SvReader svr = new SvReader(parentCore); SvWriter svw = new SvWriter(svr); SvLink svl = new SvLink(svr)) {

			svw.setAutoCommit(false);
			svr.setAutoCommit(false);
			svl.setAutoCommit(false);

			DbDataObject linkType = SvCore.getLinkType("FORM_FIELD_LINK", svCONST.OBJECT_TYPE_FORM_TYPE,
					svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
			DbDataArray formFields = svr.getObjectsByLinkedId(parentForm.getObjectId(), linkType, null, 0, 0);

			// if no forms fields exists, then please create it
			if (formFields != null) {
				formFields.rebuildIndex(Sv.LABEL_CODE.toString(), true);
				dboFieldType = formFields.getItemByIdx(fieldLabelCode);
			}

			if (dboFieldType == null) {
				DbDataArray flds = svr.getObjects(
						new DbSearchCriterion(Sv.LABEL_CODE.toString(), DbCompareOperand.EQUAL, fieldLabelCode),
						svCONST.OBJECT_TYPE_FORM_FIELD_TYPE, null, 0, 0);

				if (flds.size() < 1) {
					dboFieldType = fieldTypeDbo;
					svw.saveObject(dboFieldType);
				} else
					dboFieldType = flds.get(0);
			} else
				linkExists = true;

			if (!linkExists) {
				DbDataObject dbl = SvCore.getLinkType("FORM_FIELD_LINK", svCONST.OBJECT_TYPE_FORM_TYPE,
						svCONST.OBJECT_TYPE_FORM_FIELD_TYPE);
				svl.linkObjects(parentForm.getObjectId(), dboFieldType.getObjectId(), dbl.getObjectId(), "");
			}
		}

	}

	static DbDataObject registerLinkType(String linkType, Long objType1, Long objType2, String linkDescription)
			throws SvException {
		DbDataObject dblink = null;
		try (SvWriter svw = new SvWriter()) {

			dblink = SvCore.getLinkType(linkType, objType1, objType2);
			if (dblink == null) {
				dblink = new DbDataObject();
				dblink.setObjectType(svCONST.OBJECT_TYPE_LINK_TYPE);
				dblink.setVal("link_type", linkType);
				dblink.setVal("link_obj_type_1", objType1);
				dblink.setVal("link_obj_type_2", objType2);
				dblink.setVal("LINK_TYPE_DESCRIPTION", linkDescription);
				svw.saveObject(dblink, true);
				// we must re-init the core to catch the link type change
				SvCore.initSvCore(true);
			}
		}
		return dblink;
	}

	static DbDataObject getFormType(String formLabel, String formCategory, boolean multiEntry, boolean autoInstance,
			boolean mandatoryValue, boolean createIfNotExists) throws SvException {
		DbDataObject dboFormType = null;
		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {

			// try to load the form config
			DbDataArray res = svr.getObjects(
					new DbSearchCriterion(Sv.LABEL_CODE.toString(), DbCompareOperand.EQUAL, formLabel),
					svCONST.OBJECT_TYPE_FORM_TYPE, null, 0, 0);

			// if the config exists, load it
			if (res.getItems().size() > 0)
				dboFormType = res.getItems().get(0);
			else {
				if (createIfNotExists) {
					// else create our own form type
					dboFormType = new DbDataObject();
					dboFormType.setObjectType(svCONST.OBJECT_TYPE_FORM_TYPE);
					dboFormType.setVal("FORM_CATEGORY", formCategory);
					dboFormType.setVal("MULTI_ENTRY", multiEntry);
					dboFormType.setVal("AUTOINSTANCE_SINGLE", autoInstance);
					dboFormType.setVal("MANDATORY_BASE_VALUE", mandatoryValue);
					dboFormType.setVal(Sv.LABEL_CODE, formLabel);
					svw.saveObject(dboFormType);
					SvCore.initSvCore(true);
				}
			}
		}
		return dboFormType;

	}

	/**
	 * Method to initialise the FileStore table structure
	 * 
	 * @return true if initialised correctly
	 */
	static boolean initFileStore() {

		DbDataTable dbt = new DbDataTable(SvConf.getSqlkw());
		dbt.setDbTableName(SvConf.getParam("filestore.table"));
		dbt.setDbRepoName(SvConf.getMasterRepo());
		dbt.setDbSchema(SvConf.getDefaultSchema());

		// f1
		DbDataField dbf1 = new DbDataField();
		dbf1.setDbFieldName("PKID");
		dbf1.setIsPrimaryKey(true);
		dbf1.setDbFieldType(DbFieldType.NUMERIC);
		dbf1.setDbFieldSize(18);
		dbf1.setDbFieldScale(0);
		dbf1.setIsNull(false);
		dbf1.setDbSequenceName(dbt.getDbTableName() + "_pkid");

		// f2
		DbDataField dbf2 = new DbDataField();
		dbf2.setDbFieldName("DATA");
		dbf2.setDbFieldType(DbFieldType.BLOB);
		dbf2.setIsNull(false);
		dbf2.setLabel_code("master_repo.file_type");

		dbt.setDbTableFields(new DbDataField[2]);
		dbt.getDbTableFields()[0] = dbf1;
		dbt.getDbTableFields()[1] = dbf2;
		Boolean retval = false;
		DbDataTable.setRbConf(SvConf.getSqlkw());
		DbDataField.setSqlKWResource(SvConf.getSqlkw());

		Connection conn = null;
		try {

			conn = SvConf.getDBConnection();
			retval = SvarogInstall.createTable(dbt, conn);
		} catch (Exception e) {
			log4j.error("Failed initialising file store!", e);
			retval = false;
		} finally

		{
			if (conn != null)
				try {
					conn.close();
				} catch (Exception e) {
					log4j.error("Connection can't be released!", e);
				}
			;

		}
		return retval;

	}
}
