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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.prtech.svarog_common.DbDataField;
import com.prtech.svarog_common.DbDataTable;
import com.prtech.svarog_common.DboFactory;
import com.prtech.svarog_interfaces.ISvDatabaseIO;

/**
 * Main svarog configuration class. This class provides all base configurations
 * as well as plain database connections. The main two functionalities of this
 * class are the
 * 
 * @author PR01
 *
 */
public class SvConf {
	/**
	 * Static block to give priority to the log4j2.xml file which resides in the
	 * working directory. If you want to use the standard log4j configuration, then
	 * just remove the log4j2.xml file from the working dir
	 */
	static {
		String path = "./log4j2.xml";
		File pFile = new File(path);
		if (pFile.exists()) {
			System.out.println("Using log4j2.xml from:" + pFile.getAbsolutePath());
			System.setProperty("log4j.configurationFile", pFile.getAbsolutePath());
		}
	}

	/**
	 * Enum listing the supported database types
	 * 
	 * @author XPS13
	 *
	 */
	public enum SvDbType {
		POSTGRES, ORACLE, MSSQL
	};

	/**
	 * Enum holding the both methods for connection to the DB
	 * 
	 * @author XPS13
	 *
	 */
	enum SvDbConnType {
		JDBC, JNDI
	};

	/**
	 * Log4j object to log issues
	 */
	private static final Logger log4j = LogManager.getLogger(SvConf.class.getName());

	/**
	 * Variable holding the svarog global limit for sending JDBC batches
	 */
	private static Integer jdbcBatchSize = null;
	/**
	 * Variable holding the svarog global limit for sending JDBC batches
	 */
	private static long maxLockTimeout = 60000;

	/**
	 * Maximum number of locks managed by SvLock
	 */
	private static int maxLockCount;

	/**
	 * Maximum time before the node will perform cluster maintenance.
	 */
	private static int clusterMaintenanceInterval;

	/**
	 * TCP/IP port on which the server will listen for heartbeat.
	 */
	private static int heartBeatPort;

	/**
	 * The heart beat interval between two heart beats for configuring the cluster
	 * client
	 */
	private static int heartBeatInterval;

	/**
	 * Filed to register the IP of the VM Bridge. Svarog will not register this IP
	 * as valid ip for heartbeat connections
	 */
	private static String vmBridgeIPAddress;
	/**
	 * The maximum timeout which shall we wait before declaring the peer as dead
	 */
	private static int heartBeatTimeOut;

	private static String admUnitClass;

	private static boolean intersectSysBoundary;

	/**
	 * Flag to mark if SDI is enabled
	 */
	private static boolean sdiEnabled = false;

	/**
	 * Property holding the system spatial SRID
	 */
	private static String sdiSrid = null;

	/**
	 * Property holding the default Spatial Precision
	 */
	private static Double sdiPrecision = null;

	/**
	 * Property holding the configuration folder containing all svarog JSON config
	 */
	private static String configurationFolder = null;

	/**
	 * Variable to set the cleanup interval for rogue cores
	 */
	private static Integer coreIdleTimeout = null;

	/**
	 * Variable storing the name of the default object repository
	 */
	private static String repoName;
	/**
	 * Variable storing the name of the default DB Schema
	 */
	private static String defaultSchema;

	/**
	 * The internal static geometry handler instance
	 */
	private static volatile ISvDatabaseIO dbHandler = null;

	/**
	 * The connection type JDBC or JNDI
	 */
	private static SvDbConnType svDbConnType;

	/**
	 * The enum hold database type
	 */
	private static SvDbType svDbType;

	/**
	 * The Datasource reference
	 */
	private static volatile DataSource sysDataSource = null;

	/**
	 * Set the default multi select separator. It is used to separate selected
	 * values when stored in the d
	 */
	private static String multiSelectSeparator = ";";

	/**
	 * Size of the generated grid of the territory covered by the spatial modules
	 */
	static int sdiGridSize;
	/**
	 * Flag to enable svarog to recalculate geometry area/perimeter
	 */
	static boolean sdiOverrideGeomCalc;

	private static boolean clusterEnabled = true;
	/**
	 * Fields storing application information
	 */
	static final String appName = "Svarog Business Platform";
	/**
	 * Fields storing Application version
	 */
	static final String appVersion = loadInfo("git.build.version");
	/**
	 * Fields storing Application build number
	 */
	static final String appBuild = loadInfo("git.build.time");

	/**
	 * Array holding the list of service classes allowed to switch to the Service
	 * user without being authenticated
	 */
	static ArrayList<String> serviceClasses = new ArrayList<String>();

	/**
	 * Array holding the list of service classes allowed to switch to the System
	 * user without being authenticated. Use only for importing
	 */
	static ArrayList<String> systemClasses = new ArrayList<String>();

	/**
	 * Properties object holding the svarog master configuration
	 */
	static final Properties config = initConfig();

	/**
	 * Static method to return a Logger from the svarog class loader to the Svarog
	 * OSGI modules
	 * 
	 * @param parentClass The class under which the log will be written
	 * @return The logger instance
	 */
	public static Logger getLogger(Class<?> parentClass) {
		return LogManager.getLogger(parentClass.getName());

	}

	/**
	 * Method to set the maximum record validity. With certain databases which
	 * understand timezones, it is necessary to tweak this date so the database can
	 * work on servers with different timezones by using the config param
	 * sys.force_timezone
	 * 
	 * @return the maximum date time for the specific time zone
	 */
	private static DateTime getMaxDate() {
		DateTime maxDate = new DateTime("9999-12-31T00:00:00+00");
		String timezone = SvConf.getParam("sys.force_timezone");

		if (timezone != null && !timezone.trim().isEmpty()) {
			try {
				// forID(timezone)
				DateTimeZone dtz = DateTimeZone.getDefault();

				DateTimeZone dtzForced = DateTimeZone.forID(timezone);
				int offsetForced = dtzForced.getOffsetFromLocal(maxDate.getMillis());

				int offset = dtz.getOffsetFromLocal(maxDate.getMillis()) - offsetForced;
				log4j.info("Offset for:" + timezone + " is: " + offset);

				maxDate = new DateTime(maxDate.getMillis() - offset);
			} catch (Exception e) {
				log4j.error("Can't find timezone:" + timezone
						+ "! All available timezones in the system can be found here: http://joda-time.sourceforge.net/timezones.html");
			}
		}
		DboFactory.setInitialMaxDate(maxDate);
		log4j.debug("Set MAX_DATE:" + maxDate.toString());

		return maxDate;
	}

	/**
	 * Method to set the maximum record validity. With certain databases which
	 * understand timezones, it is necessary to tweak this date so the database can
	 * work on servers with different timezones by using the config param
	 * sys.force_timezone
	 * 
	 * @return the maximum date time for the specific time zone
	 */
	private static Timestamp getMaxDateTS() {
		return new Timestamp(getMaxDate().getMillis());
	}

	/**
	 * Final field representing the Maximum date time value used in Svarog. Includes
	 * time zone
	 */
	public static final DateTime MAX_DATE = getMaxDate();

	/**
	 * SQL Timestamp version of the MAX DATE used for SQL queries
	 */
	static final Timestamp MAX_DATE_SQL = getMaxDateTS();

	/**
	 * The resource bundle containing all specific SQL Keywords for different RDBMS
	 * engines
	 */
	private static ResourceBundle sqlKw = null;

	/**
	 * flag to enable or disable overriding the DtInsert and dt Delete timestamps
	 */
	private static boolean overrideTimeStamps = true;

	private static boolean deleteCodesOnUpgrade = false;

	private static int maxRequestsPerMinute;

	/**
	 * Method to return the currently configured ISvDatabaseIO instance
	 * 
	 * @return ISvDatabaseIO instance
	 * @throws SvException
	 */
	public static ISvDatabaseIO getDbHandler() throws SvException {
		if (dbHandler == null) {
			synchronized (SvConf.class) {
				if (dbHandler == null) {
					dbHandlerInit();
				}
			}
		}
		return dbHandler;
	}

	/**
	 * Method to load the Database Handler implementing all methods needed for
	 * database handling via the ISvDatabaseIO interface
	 * 
	 * @throws SvException
	 */
	private static void dbHandlerInit() throws SvException {
		String dbHandlerClass = SvConf.getParam("conn.dbHandlerClass");
		if (dbHandlerClass == null || dbHandlerClass.equals("")) {
			dbHandlerClass = SvPostgresIO.class.getName();
			log4j.warn("Param conn.dbHandlerClass is not set. Defaulting to POSTGRES. Handler class:" + dbHandlerClass);

		}
		try {
			Class<?> c = Class.forName(dbHandlerClass);
			if (ISvDatabaseIO.class.isAssignableFrom(c)) {
				dbHandler = ((ISvDatabaseIO) c.newInstance());
				if (!dbHandler.getHandlerType().equals(SvConf.getDbType().toString())) {
					log4j.error("Database type is: " + SvConf.getDbType().toString() + ", while handler type is: "
							+ dbHandler.getHandlerType() + ".Handler class:" + dbHandlerClass
							+ " is incompatible with the database type");
					dbHandler = null;
				}
			}
		} catch (Exception e) {
			log4j.error("Can't find Database Handler named: " + SvConf.getParam("conn.dbHandlerClass"));
			// e.printStackTrace();
		}
		if (dbHandler == null) {
			log4j.error("Can't load Database Handler Handler named:" + SvConf.getParam("conn.dbHandlerClass"));
			throw (new SvException("system.error.misconfigured_dbhandlerclass", svCONST.systemUser));
		} else {
			sqlKw = dbHandler.getSQLKeyWordsBundle();
			if (getSDISrid() != null) {
				dbHandler.initSrid(getSDISrid());
				DbDataTable.initSrid(getSDISrid());
				DbDataField.initSrid(getSDISrid());
				// TODO fix srid
			}
		}
	}

	/**
	 * Method to do the initial reading of the svarog.properties file. It will try
	 * to load the config file from the system property names "svarog.properties" if
	 * it exists. If it doesn't it will try to load the file names
	 * 'svarog.properties' from the current directory
	 * 
	 * @return Properties object
	 */
	static Properties loadSvarogProperties() {

		Properties mainProperties = null;
		InputStream props = null;
		boolean hasErrors = true;
		try {
			String path = System.getProperties().getProperty(Sv.CONFIG_FILENAME, "./" + Sv.CONFIG_FILENAME);
			File pFile = new File(path);
			log4j.info("Loading configuration from:" + pFile.getCanonicalPath());
			props = new FileInputStream(pFile);
			log4j.info("Starting " + appName);
			log4j.info("Version:" + appVersion);
			log4j.info("Build:" + appBuild);

			log4j.debug("Commit:" + loadInfo("git.commit.id.full"));
			log4j.debug("Commit Desc:" + loadInfo("git.commit.id.describe"));
			log4j.debug("Host:" + loadInfo("git.build.host"));
			log4j.debug("Branch:" + loadInfo("git.branch"));
			log4j.debug("Repository:" + loadInfo("git.remote.origin.url"));

			// load all the properties from this file
			Properties temp = new Properties();
			temp.load(props);
			// all well
			mainProperties = temp;
			hasErrors = false;
		} catch (Exception e) {
			log4j.error("Svarog.properties config file can not be found. Svarog not initialised.", e);
		} finally {
			try {
				if (props != null)
					props.close();
			} catch (IOException e) {
				log4j.error("Svarog.properties config file not properly read. Svarog initialisation error.", e);
				hasErrors = true;
			}
		}
		if (hasErrors)
			mainProperties = null;
		return mainProperties;
	}

	/**
	 * Method to configure the database connection based on the main properties
	 * loaded from svarog.properties. It is separate from initConfig because
	 * {@link #initConfig()} might be invokes without need for database access, such
	 * as grid preparation or JSON scripts
	 * 
	 * @param mainProperties svarog main parameters
	 * @return DataSource instance representing JNDI/DBCP data source
	 */
	private static DataSource initDataSource(Properties mainProperties) {
		boolean hasErrors = true;
		DataSource dataSource = null;
		try {
			try {
				Class.forName(mainProperties.getProperty("driver.name").trim());
			} catch (java.lang.ClassNotFoundException e) {
				log4j.error(
						"Database driver class name can not be found on the class path. "
								+ "Please make sure the library containing "
								+ mainProperties.getProperty("driver.name").trim() + " is on the classpath of the JVM",
						e);

			}

			if (svDbConnType.equals(SvDbConnType.JNDI)) {
				String jndiDataSourceName = getProperty(mainProperties, "jndi.datasource", "");
				log4j.info("DB connection type is JNDI, datasource name:" + jndiDataSourceName);
				Context initialContext = new InitialContext();
				sysDataSource = (DataSource) initialContext.lookup(jndiDataSourceName);
			} else {
				log4j.info("DB connection type is JDBC, using DBCP2");
				configureDBCP(mainProperties);
			}
			hasErrors = false;
			if (!(svDbType.equals(SvDbType.ORACLE) || svDbType.equals(SvDbType.POSTGRES)
					|| svDbType.equals(SvDbType.MSSQL))) {
				log4j.error("Wrong database type! Must be one of: POSTGRES, MSSQL or ORACLE");
				hasErrors = true;
			}
		} catch (Exception e) {
			log4j.error("Svarog.properties config file not properly parsed. Svarog initialisation error.", e);
			hasErrors = true;

		}
		if (hasErrors)
			dataSource = null;

		return dataSource;
	}

	/**
	 * Method to configure the svarog system data source
	 * 
	 * @return DataSource instance
	 */
	static DataSource getDataSource() {

		// if Svarog is already initialised, simply return
		if (sysDataSource != null)
			return sysDataSource;
		else {
			synchronized (config) {
				if (sysDataSource == null)
					initDataSource(config);
				return sysDataSource;
			}
		}

	}

	static void privilegedClassesConf(Properties mainProperties) {
		// configure svarog service classes
		String svcClass = mainProperties.getProperty("sys.service_class");
		if (svcClass != null && svcClass.length() > 1) {
			String[] list = svcClass.trim().split(";");
			if (list.length > 0)
				serviceClasses.addAll(Arrays.asList(list));
		}
		// configure svarog service classes
		svcClass = mainProperties.getProperty("sys.system_class");
		if (svcClass != null && svcClass.length() > 1) {
			String[] list = svcClass.trim().split(";");
			if (list.length > 0)
				systemClasses.addAll(Arrays.asList(list));
		}
	}

	/**
	 * Method to locate and read the main svarog.properties config file
	 * 
	 * @return Properties instance
	 */
	private static Properties initConfig() {
		boolean hasErrors = true;

		Properties mainProperties = loadSvarogProperties();
		try {
			// make sure we set the oracle compliance level
			String oracleJdbcComliance = getProperty(mainProperties, "oracle.jdbc.J2EE13Compliant", "false");
			System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", oracleJdbcComliance);
			repoName = getProperty(mainProperties, "sys.masterRepo", "SVAROG").toUpperCase();
			defaultSchema = getProperty(mainProperties, "conn.defaultSchema", "SVAROG").toUpperCase();
			sdiGridSize = getProperty(mainProperties, "sys.gis.grid_size", 10);
			sdiOverrideGeomCalc = getProperty(mainProperties, "sys.gis.override_user_area_perim", false);

			maxLockTimeout = getProperty(mainProperties, "sys.lock.max_wait_time", 5) * 60L * 1000L;
			maxLockCount = getProperty(mainProperties, "sys.lock.max_count", 5000);
			multiSelectSeparator = getProperty(mainProperties, "sys.codes.multiselect_separator", "");
			sdiEnabled = getProperty(mainProperties, "sys.gis.enable_spatial", false);

			// cluster params
			clusterMaintenanceInterval = getProperty(mainProperties, "sys.cluster.max_maintenance", 60);
			heartBeatPort = getProperty(mainProperties, "sys.cluster.heartbeat_port", 6783);
			heartBeatInterval = getProperty(mainProperties, "sys.cluster.heartbeat_interval", 1000);
			heartBeatTimeOut = getProperty(mainProperties, "sys.cluster.heartbeat_timeout", 10000);
			vmBridgeIPAddress = getProperty(mainProperties, "sys.cluster.vmbridge_ip", "");
			clusterEnabled = getProperty(mainProperties, "sys.cluster.enabled", true);
			deleteCodesOnUpgrade = getProperty(mainProperties, "sys.codes.delete_upgrade", false);

			overrideTimeStamps = getProperty(mainProperties, "sys.core.override_timestamp", true);
			maxRequestsPerMinute = getProperty(mainProperties, "sys.max.requests_per_min", 60);

			admUnitClass = getProperty(mainProperties, "sys.gis.legal_sdi_unit_type", "0");
			intersectSysBoundary = getProperty(mainProperties, "sys.gis.allow_boundary_intersect", false);

			svDbType = SvDbType.valueOf(mainProperties.getProperty("conn.dbType").trim().toUpperCase());
			svDbConnType = SvDbConnType.valueOf(mainProperties.getProperty("conn.type").trim().toUpperCase());

			// make sure we configure the service classes as well as system
			// classes
			privilegedClassesConf(mainProperties);
			// init was successful
			hasErrors = false;
		} catch (Exception e) {
			log4j.error("Svarog.properties config file not properly parsed. Svarog initialisation error.", e);
			hasErrors = true;

		}

		// check if sdi shall be enabled
		if (hasErrors)
			mainProperties = null;
		return mainProperties;
	}

	/**
	 * Method to load a value from the version properties.
	 * 
	 * @param key The key generated by mvn
	 * @return The value which was generated from the pom file
	 */
	static String loadInfo(String key) {

		Properties info = null;
		InputStream props = null;
		String retval = null;
		try {

			props = SvConf.class.getClassLoader().getResourceAsStream("version.properties");
			info = new Properties();
			info.load(props);
			retval = info.getProperty(key);
		} catch (Exception e) {
			log4j.debug("Can't read svarog version info", e);
		} finally {
			if (props != null)
				try {
					props.close();
				} catch (IOException e) {
					log4j.error("Can't read svarog version info", e);
				}
		}
		return retval;

	}

	/**
	 * Method to try to parse a property to integer and set to default value if it
	 * fails
	 * 
	 * @param mainProperties the list of properties
	 * @param propName       the name of the property
	 * @param defaultValue   the default value to be set to if parsing fails
	 * @return the int value of the property
	 */
	static int getProperty(Properties mainProperties, String propName, int defaultValue) {
		int intProp = defaultValue;
		try {
			intProp = Integer.parseInt(mainProperties.getProperty(propName).trim());
		} catch (Exception ex) {
			log4j.debug(propName + " config property is unreadable, using default initial value = " + defaultValue);
		}
		return intProp;
	}

	/**
	 * Method to try to parse a property to boolean and set to default value if it
	 * fails
	 * 
	 * @param mainProperties the list of properties
	 * @param propName       the name of the property
	 * @param defaultValue   the default value to be set to if parsing fails
	 * @return the boolean value of the property
	 */
	static boolean getProperty(Properties mainProperties, String propName, boolean defaultValue) {
		boolean boolProp = defaultValue;
		try {
			boolProp = Boolean.parseBoolean(mainProperties.getProperty(propName).trim());
		} catch (Exception ex) {
			log4j.debug(propName + " config property is unreadable, using default initial value = " + defaultValue);
		}
		return boolProp;
	}

	/**
	 * Method to try to parse a property to boolean and set to default value if it
	 * fails
	 * 
	 * @param mainProperties the list of properties
	 * @param propName       the name of the property
	 * @param defaultValue   the default value to be set to if parsing fails
	 * @return the string value of the property
	 */
	static String getProperty(Properties mainProperties, String propName, String defaultValue) {
		String prop = defaultValue;
		try {
			prop = mainProperties.getProperty(propName).trim();
			if (prop.isEmpty()) {
				prop = defaultValue;
				throw (new Exception("empty.prop"));
			}
		} catch (Exception ex) {
			log4j.debug(propName + " config property is unreadable, using default initial value = " + defaultValue);
		}
		return prop;
	}

	/**
	 * Method to configure the DBCP from the main properties
	 * 
	 * @param mainProperties the svarog main properties
	 * @return configured DBCP data source
	 */
	static DataSource configureDBCP(Properties mainProperties) {

		sysDataSource = new BasicDataSource();
		((BasicDataSource) sysDataSource).setDriverClassName(mainProperties.getProperty("driver.name").trim());
		((BasicDataSource) sysDataSource).setUrl(mainProperties.getProperty("conn.string").trim());
		log4j.info("Configuring connection to: " + mainProperties.getProperty("conn.string").trim());
		log4j.info("Configuring database user name: " + mainProperties.getProperty("user.name").trim());
		log4j.info("Configuring database schema: " + defaultSchema);
		((BasicDataSource) sysDataSource).setUsername(mainProperties.getProperty("user.name").trim());
		((BasicDataSource) sysDataSource).setPassword(mainProperties.getProperty("user.password").trim());

		// Parameters for connection pooling
		((BasicDataSource) sysDataSource).setInitialSize(getProperty(mainProperties, "dbcp.init.size", 10));
		((BasicDataSource) sysDataSource).setMaxTotal(getProperty(mainProperties, "dbcp.max.total", 200));
		((BasicDataSource) sysDataSource).setTestOnBorrow(getProperty(mainProperties, "dbcp.test.borrow", true));
		((BasicDataSource) sysDataSource).setTestWhileIdle(getProperty(mainProperties, "dbcp.test.idle", true));
		String defaultValidationQuery = "SELECT 1" + (svDbType.equals(SvDbType.ORACLE) ? " FROM DUAL" : "");
		((BasicDataSource) sysDataSource)
				.setValidationQuery(getProperty(mainProperties, "dbcp.validation.query", defaultValidationQuery));
		((BasicDataSource) sysDataSource)
				.setValidationQueryTimeout(getProperty(mainProperties, "dbcp.validation.timoeut", 3000));
		((BasicDataSource) sysDataSource)
				.setAccessToUnderlyingConnectionAllowed(getProperty(mainProperties, "dbcp.access.conn", true));
		((BasicDataSource) sysDataSource)
				.setRemoveAbandonedOnBorrow(getProperty(mainProperties, "dbcp.remove.abandoned", true));
		((BasicDataSource) sysDataSource)
				.setRemoveAbandonedOnMaintenance(getProperty(mainProperties, "dbcp.remove.abandoned", true));
		((BasicDataSource) sysDataSource)
				.setRemoveAbandonedTimeout(getProperty(mainProperties, "dbcp.abandoned.timeout", 600));
		((BasicDataSource) sysDataSource)
				.setTimeBetweenEvictionRunsMillis(getProperty(mainProperties, "dbcp.eviction.time", 3000));
		((BasicDataSource) sysDataSource).setMaxIdle(getProperty(mainProperties, "dbcp.max.idle", 10));
		return sysDataSource;
	}

	/**
	 * Method to check if a class name is registered in the list of service classes
	 * in svarog.properties.
	 * 
	 * @param className The class name to be checked
	 * @return True if the class name is a registered service class
	 */
	static boolean isServiceClass(String className) {
		return serviceClasses.contains(className);
	}

	/**
	 * Method to check if a class name is registered in the list of system classes
	 * in svarog.properties.
	 * 
	 * @param className The class name to be checked
	 * @return True if the class name is a registered service class
	 */
	static boolean isSystemClass(String className) {
		return systemClasses.contains(className);
	}

	/**
	 * Method to get a new JDBC connection to the database
	 * 
	 * @return A JDBC connection object
	 * @throws SvException If connection can't be acquired from the datasource a
	 *                     system.error.db_conn_err is thrown
	 */
	static Connection getDBConnection() throws SvException {
		Connection result = null;
		try {
			result = getDataSource().getConnection();
		} catch (Exception ex) {
			throw (new SvException("system.error.db_conn_err", svCONST.systemUser, ex));
		}
		log4j.trace("New DB connection acquired");
		return result;
	}

	/**
	 * Method to get parameter values from the svarog config file.
	 * 
	 * @param paramName
	 * @return
	 */
	public static String getParam(String paramName) {

		String val = config.getProperty(paramName);
		if (val != null)
			return val.trim();
		else {
			log4j.trace("No configuration for parameter:" + paramName + ", exists in the configuration file");
			return null;
		}
	}

	/**
	 * Method to get the SDI srid configured in svarog properties
	 * 
	 * @return
	 */
	public static String getSDISrid() {
		if (sdiSrid == null) {
			try {
				String srid = config.getProperty("sys.gis.default_srid").trim();
				if (srid == null || srid.isEmpty() || srid.equals(Sv.SQL.NULL)) {
					switch (getDbType()) {
					case POSTGRES:
						srid = "0";
						break;
					case ORACLE:
						srid = Sv.SQL.NULL;
						break;
					default:
						srid = "0";
					}
				}

				if (!srid.equals(Sv.SQL.NULL))
					sdiSrid = Integer.toString(Integer.parseInt(srid));

				if (srid.equals(Sv.SQL.NULL)) {
					log4j.warn("SRID is set to NULL");
					sdiSrid = srid;
				}
			} catch (Exception ex) {
				log4j.warn("Exception parsing SRID", ex);
			}
			if (sdiSrid == null) {
				log4j.warn("Can't get SRID from config (parameter:sys.gis.default_srid). Defaulting to 0");
				sdiSrid = "0";
			}
		}
		return sdiSrid;
	}

	public static double getSDIPrecision() {
		if (sdiPrecision == null) {
			try {
				sdiPrecision = Double.parseDouble(config.getProperty("sys.gis.precision_scale").trim());
			} catch (Exception ex) {
				log4j.warn(
						"Can't get SDI Precision model from config (parameter:sys.gis.precision_scale. Defaulting to 1000");
				sdiPrecision = new Double(1000);
			}
		}
		return sdiPrecision;
	}

	/**
	 * Method to get the configuration about JDBC batch size. It seems oracle has a
	 * bug and doesn't handle more than 10 statements in the JDBC batch.
	 * 
	 * @return
	 */
	public static int getJDBCBatchSize() {

		if (jdbcBatchSize == null) {
			try {
				jdbcBatchSize = Integer.parseInt(config.getProperty("sys.jdbc.batch_size").trim());
			} catch (Exception ex) {
				log4j.warn("Can't get batch size from config (parameter:sys.jdbc.batch_size). Defaulting to 10");
				jdbcBatchSize = new Integer(10);
			}
		}
		return jdbcBatchSize;
	}

	/**
	 * Method to get the configuration about maximum SvCore Idle time. Default is 30
	 * minutes.
	 * 
	 * @return Max minutes of idle time an SvCore instance is allowed.
	 */
	public static int getCoreIdleTimeout() {

		if (coreIdleTimeout == null) {
			try {
				coreIdleTimeout = Integer.parseInt(config.getProperty("sys.core.cleanup_time").trim());
				coreIdleTimeout = coreIdleTimeout * 60000;
			} catch (Exception ex) {
				log4j.warn(
						"Can't get core timeout from config (parameter:sys.core.cleanup_time). Defaulting to 30 minutes");
				coreIdleTimeout = new Integer(30 * 60000);
			}
		}
		return coreIdleTimeout;
	}

	/**
	 * Method returning the configured debug status of the core
	 * 
	 * @return
	 */
	public static boolean getIsDebugEnabled() {
		String isDebug = config.getProperty("sys.core.is_debug");
		if (isDebug != null && isDebug.trim().toUpperCase().equals("TRUE"))
			return true;
		else
			return false;
	}

	/**
	 * Method to get the path of the conf directory holding all JSON config files
	 * used for install
	 * 
	 * @return
	 */
	public static String getConfPath() {
		if (configurationFolder == null) {
			String confPath = config.getProperty("sys.conf.path");
			if (confPath != null) {
				configurationFolder = confPath.trim();
				if (!configurationFolder.endsWith("/"))
					configurationFolder = configurationFolder + "/";
			} else {
				log4j.warn("Parameter sys.conf.path missing from config, defaulting to 'conf/'");
				configurationFolder = "conf/";
			}

		}
		return configurationFolder;
	}

	/**
	 * Method to set the core idle time out in Minutes
	 * 
	 * @param timeout The number of minutes, before the core is considered as idle
	 */
	public static void setCoreIdleTimeout(int timeout) {
		coreIdleTimeout = timeout * 60000;
	}

	/**
	 * Method to set the core idle time out in Minutes
	 * 
	 * @param timeout The number of minutes, before the core is considered as idle
	 */
	public static void setCoreIdleTimeoutMilis(int timeoutMilis) {
		coreIdleTimeout = timeoutMilis;
	}

	public static int getClusterMaintenanceInterval() {
		return clusterMaintenanceInterval;
	}

	public static void setClusterMaintenanceInterval(int clusterMaintenanceInterval) {
		SvConf.clusterMaintenanceInterval = clusterMaintenanceInterval;
	}

	public static String getMasterRepo() {
		return repoName;
	}

	public static String getCustomJar() {
		return config.getProperty("custom.jar").trim();
	}

	public static String getDefaultSchema() {
		return defaultSchema;
	}

	static String getDriverName() {
		return config.getProperty("driver.name").trim();
	}

	static String getConnectionString() {
		return config.getProperty("conn.string").trim();
	}

	static String getUserName() {
		return config.getProperty("user.name");
	}

	static String getPassword() {
		return config.getProperty("user.password");
	}

	public static SvDbType getDbType() {
		return svDbType;
	}

	public static String getDefaultLocale() {
		return config.getProperty("sys.defaultLocale").trim();
	}

	public static String getDefaultDateFormat() {
		return config.getProperty("sys.defaultDateFormat").trim();
	}

	public static String getDefaultTimeFormat() {
		return config.getProperty("sys.defaultTimeFormat").trim();
	}

	public static String getDefaultJSDateFormat() {
		return config.getProperty("sys.defaultJSDateFormat").trim();
	}

	public static String getDefaultJSTimeFormat() {
		return config.getProperty("sys.defaultJSTimeFormat").trim();
	}

	public static ResourceBundle getSqlkw() {
		return sqlKw;
	}

	public static int getSdiGridSize() {
		return sdiGridSize;
	}

	public static void setSdiGridSize(int sdiGridSize) {
		SvConf.sdiGridSize = sdiGridSize;
	}

	public static long getMaxLockTimeout() {
		return maxLockTimeout;
	}

	public static void setMaxLockTimeout(long maxLockTimeout) {
		SvConf.maxLockTimeout = maxLockTimeout;
	}

	public static String getMultiSelectSeparator() {
		return multiSelectSeparator;
	}

	public static void setMultiSelectSeparator(String multiSelectSeparator) {
		SvConf.multiSelectSeparator = multiSelectSeparator;
	}

	public static boolean isSdiEnabled() {
		return sdiEnabled;
	}

	public static void setSdiEnabled(boolean sdiEnabled) {
		SvConf.sdiEnabled = sdiEnabled;
	}

	public static int getMaxLockCount() {
		return maxLockCount;
	}

	public static void setMaxLockCount(int maxLockCount) {
		SvConf.maxLockCount = maxLockCount;
	}

	public static int getHeartBeatPort() {
		return heartBeatPort;
	}

	public static void setHeartBeatPort(int heartBeatPort) {
		SvConf.heartBeatPort = heartBeatPort;
	}

	public static int getHeartBeatInterval() {
		return heartBeatInterval;
	}

	public static void setHeartBeatInterval(int heartBeatInterval) {
		SvConf.heartBeatInterval = heartBeatInterval;
	}

	public static String getVmBridgeIPAddress() {
		return vmBridgeIPAddress;
	}

	public static void setVmBridgeIPAddress(String vmBridgeIPAddress) {
		SvConf.vmBridgeIPAddress = vmBridgeIPAddress;
	}

	public static boolean isClusterEnabled() {
		return clusterEnabled;
	}

	public static void setClusterEnabled(boolean enableCluster) {
		SvConf.clusterEnabled = enableCluster;
	}

	public static boolean isOverrideTimeStamps() {
		return overrideTimeStamps;
	}

	public static void setOverrideTimeStamps(boolean overrideTimeStamps) {
		SvConf.overrideTimeStamps = overrideTimeStamps;
	}

	public static int getHeartBeatTimeOut() {
		return heartBeatTimeOut;
	}

	public static void setHeartBeatTimeOut(int heartBeatTimeOut) {
		SvConf.heartBeatTimeOut = heartBeatTimeOut;
	}

	public static String getAdmUnitClass() {
		return admUnitClass;
	}

	public static void setAdmUnitClass(String admUnitClass) {
		SvConf.admUnitClass = admUnitClass;
	}

	public static boolean isIntersectSysBoundary() {
		return intersectSysBoundary;
	}

	public static void setIntersectSysBoundary(boolean intersectSysBoundary) {
		SvConf.intersectSysBoundary = intersectSysBoundary;
	}

	public static int getMaxRequestsPerMinute() {
		return maxRequestsPerMinute;
	}

	public static void setMaxRequestsPerMinute(int maxRequestsPerMinute) {
		SvConf.maxRequestsPerMinute = maxRequestsPerMinute;
	}

}
