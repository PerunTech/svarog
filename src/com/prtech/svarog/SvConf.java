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
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.prtech.svarog_interfaces.ISvDatabaseIO;

/**
 * Main svarog configuration class. This class provides all base config as well
 * as plain database connections.
 * 
 * @author PR01
 *
 */
public class SvConf {
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
	 * Flag to mark if SDI is enabled
	 */
	private static boolean sdiEnabled = false;

	/**
	 * Property holding the system spatial SRID
	 */
	private static Integer sdiSrid = null;

	/**
	 * Property holding the default Spatial Precision
	 */
	private static Double sdiPrecision = null;

	/**
	 * Property holding the configuration folder containing all svarog JSON
	 * config
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
	private static ISvDatabaseIO dbHandler = null;

	/**
	 * String variable holding old style db type
	 */
	@Deprecated
	private static String dbType;

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
	private static DataSource coreDataSource = null;

	/**
	 * Set the default multi select separator
	 */
	private static String multiSelectSeparator = ";";
	/**
	 * JDBC conn string, user and pass
	 */
	private static String connString = null;
	private static String connUser = null;
	private static String connPass = null;

	static int sdiGridSize;

	static ArrayList<String> serviceClasses = new ArrayList<String>();
	/**
	 * Properties object holding the svarog master configuration
	 */
	static final Properties config = initConfig();

	/**
	 * Static method to return a Logger from the svarog class loader to the
	 * Svarog OSGI modules
	 * 
	 * @param parentClass
	 *            The class under which the log will be written
	 * @return The logger instance
	 */
	public static Logger getLogger(Class<?> parentClass) {
		return LogManager.getLogger(parentClass.getName());

	}

	/**
	 * Method to set the maximum record validity. With certain databases which
	 * understand timezones, it is necessary to tweak this date so the database
	 * can work on servers with different timezones by using the config param
	 * sys.force_timezone
	 * 
	 * @return the maximum date time for the specific time zone
	 */
	public static DateTime getMaxDate() {
		DateTime maxDate = new DateTime("9999-12-31T00:00:00+00");
		String timezone = SvConf.getParam("sys.force_timezone");

		if (timezone != null) {
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
		log4j.info("Set MAX_DATE:" + maxDate.toString());

		return maxDate;
	}

	public static final DateTime MAX_DATE = getMaxDate();
	public static final Timestamp MAX_DATE_SQL = new Timestamp(MAX_DATE.getMillis());

	/**
	 * The resource bundle containing all specific SQL Keywords for different
	 * RDBMS engines
	 */
	private static ResourceBundle sqlKw = null;

	/**
	 * Method to return the currently configured ISvDatabaseIO instance
	 * 
	 * @return ISvDatabaseIO instance
	 */
	public static ISvDatabaseIO getDbHandler() {
		if (dbHandler != null)
			return dbHandler;
		String dbHandlerClass = SvConf.getParam("conn.dbHandlerClass");
		if (dbHandlerClass == null || dbHandlerClass.equals("")) {
			dbHandlerClass = SvPostgresIO.class.getName();
			log4j.warn("Param conn.dbHandlerClass is not set. Defaulting to POSTGRES. Handler class:" + dbHandlerClass);

		}
		try {
			Class<?> c = Class.forName(dbHandlerClass);
			if (ISvDatabaseIO.class.isAssignableFrom(c)) {
				dbHandler = ((ISvDatabaseIO) c.newInstance());
				String srid = config.getProperty("sys.gis.default_srid");
				if (srid != null)
					dbHandler.initSrid(srid.trim());
				if (!dbHandler.getHandlerType().equals(SvConf.getDbType().toString())) {
					log4j.error("Database type is: " + SvConf.getDbType().toString() + ", while handler type is: "
							+ dbHandler.getHandlerType() + ".Handler class:" + dbHandlerClass
							+ " is incompatible with the database type");
					dbHandler = null;
				}
			}
		} catch (Exception e) {
			log4j.error("Can't find Database Handler named: " + SvConf.getParam("conn.dbHandlerClass"));
			e.printStackTrace();
		}
		if (dbHandler == null)
			log4j.error("Can't load Database Handler Handler named:" + SvConf.getParam("conn.dbHandlerClass"));
		else
			sqlKw = dbHandler.getSQLKeyWordsBundle();

		return dbHandler;
	}

	/**
	 * Method to locate and read the main svarog.properties config file
	 * 
	 * @return Properties instance
	 */
	private static Properties initConfig() {
		Properties mainProperties = null;
		InputStream props = null;
		boolean hasErrors = true;
		try {

			URL propsUrl = SvConf.class.getClassLoader().getResource("svarog.properties");
			if (propsUrl != null) {

				log4j.info("Loading configuration from:" + propsUrl.getFile());
				props = SvConf.class.getClassLoader().getResourceAsStream("svarog.properties");
			}
			if (props == null) {
				String path = "./svarog.properties";
				File pFile = new File(path);
				if (pFile != null) {
					log4j.info("Loading configuration from:" + pFile.getCanonicalPath());
					props = SvConf.class.getClassLoader().getResourceAsStream("svarog.properties");
				}
				props = new FileInputStream(pFile);
			}
			log4j.info("Svarog Version:" + SvConf.class.getPackage().getImplementationVersion());

			// load all the properties from this file
			Properties temp = new Properties();
			temp.load(props);
			// all well
			mainProperties = temp;
			try {
				Class.forName(mainProperties.getProperty("driver.name").trim());
			} catch (java.lang.ClassNotFoundException e) {
				log4j.error(
						"Database driver class name can not be found on the class path. "
								+ "Please make sure the library containing "
								+ mainProperties.getProperty("driver.name").trim() + " is on the classpath of the JVM",
						e);

			}
			hasErrors = false;
		} catch (Exception e) {
			log4j.error("Svarog.properties config file can not be found. Svarog not initialised.", e);
		} finally {
			try {
				props.close();
			} catch (IOException e) {
				log4j.error("Svarog.properties config file not properly read. Svarog initialisation error.", e);
				hasErrors = true;
			}
		}
		try {
			// make sure we set the oracle compliance level
			String oracleJdbcComliance = mainProperties.getProperty("oracle.jdbc.J2EE13Compliant");
			if (oracleJdbcComliance != null && oracleJdbcComliance.equalsIgnoreCase("true"))
				oracleJdbcComliance = "true";
			else
				oracleJdbcComliance = "false";

			System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", oracleJdbcComliance);

			repoName = mainProperties.getProperty("sys.masterRepo").trim().toUpperCase();
			defaultSchema = mainProperties.getProperty("conn.defaultSchema").trim().toUpperCase();
			try {
				sdiGridSize = Integer.parseInt(mainProperties.getProperty("sys.gis.grid_size"));
			} catch (Exception ex) {
				log4j.warn("Svarog sys.gis.grid_size can't be read. Defaulting to 10km grid size.");
				sdiGridSize = 10;
			}

			try {
				maxLockTimeout = Integer.parseInt(mainProperties.getProperty("sys.lock.max_wait_time")) * 60 * 1000;
			} catch (Exception ex) {
				log4j.warn("Svarog sys.lock.max_wait_time can't be read. Defaulting to 5 minutes max wait time.");
				maxLockTimeout = 5 * 60 * 1000;
			}
			dbType = mainProperties.getProperty("conn.dbType").trim().toUpperCase();
			if (!(dbType.equals("ORACLE") || dbType.equals("POSTGRES") || dbType.equals("MSSQL"))) {
				log4j.error("Bad DB Type!!! Must be POSTGRES, MSSQL and ORACLE");
				hasErrors = true;
			}
			svDbType = SvDbType.valueOf(mainProperties.getProperty("conn.dbType").trim().toUpperCase());
			svDbConnType = SvDbConnType.valueOf(mainProperties.getProperty("conn.type").trim().toUpperCase());

			if (svDbConnType.equals(SvDbConnType.JNDI)) {
				Context initialContext = new InitialContext();
				coreDataSource = (DataSource) initialContext
						.lookup(mainProperties.getProperty("jndi.datasource").trim());
			} else {
				connString = mainProperties.getProperty("conn.string").trim();
				connUser = mainProperties.getProperty("user.name").trim();
				connPass = mainProperties.getProperty("user.password").trim();

				coreDataSource = new BasicDataSource();
				((BasicDataSource) coreDataSource).setDriverClassName(mainProperties.getProperty("driver.name").trim());
				((BasicDataSource) coreDataSource).setUrl(mainProperties.getProperty("conn.string").trim());
				((BasicDataSource) coreDataSource).setUsername(mainProperties.getProperty("user.name").trim());
				((BasicDataSource) coreDataSource).setPassword(mainProperties.getProperty("user.password").trim());

				// username="svarog" password="svarog" maxActive="200"
				// maxIdle="10"
				// maxWait="-1"

				boolean removeAbandoned = false;
				int removeAbandonedTimeout = 3000;

				int timeBetweenEvictionRunsMillis = 10000;
				int validationInterval = 30000;
				String validationQuery = "SELECT 1 FROM DUAL";
				boolean accessToUnderlyingConnectionAllowed = true;
				int poolInitialSize = 10;
				int poolMaxTotal = 100;
				int poolMaxIdle = 10;
				boolean testOnBorrow = true;
				boolean testWhileIdle = true;
				try {
					poolInitialSize = Integer.parseInt(mainProperties.getProperty("dbcp.init.size").trim());
					poolMaxTotal = Integer.parseInt(mainProperties.getProperty("dbcp.max.total").trim());
				} catch (Exception ex) {
					log4j.warn("DBCP config is unreadable, using default initial size = 10, max idle=10");

				}

				// Parameters for connection pooling
				((BasicDataSource) coreDataSource).setInitialSize(poolInitialSize);
				((BasicDataSource) coreDataSource).setMaxActive(poolMaxTotal);
				((BasicDataSource) coreDataSource).setTestOnBorrow(testOnBorrow);
				((BasicDataSource) coreDataSource).setTestWhileIdle(testWhileIdle);
				((BasicDataSource) coreDataSource).setValidationQuery(validationQuery);
				//((BasicDataSource) coreDataSource).setFastFailValidation(true);
				((BasicDataSource) coreDataSource).setValidationQueryTimeout(validationInterval);
				((BasicDataSource) coreDataSource).setMaxActive(-1);
				((BasicDataSource) coreDataSource)
						.setAccessToUnderlyingConnectionAllowed(accessToUnderlyingConnectionAllowed);
				((BasicDataSource) coreDataSource).setRemoveAbandoned(true);
				((BasicDataSource) coreDataSource).setRemoveAbandonedTimeout(removeAbandonedTimeout);
				((BasicDataSource) coreDataSource).setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
				((BasicDataSource) coreDataSource).setMaxIdle(poolMaxIdle);

			}

			if (mainProperties.containsKey("sys.codes.multiselect_separator"))
				multiSelectSeparator = mainProperties.getProperty("sys.codes.multiselect_separator");

		} catch (Exception e) {
			log4j.error("Svarog.properties config file not properly parsed. Svarog initialisation error.", e);
			hasErrors = true;
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}

		// check if sdi shall be enabled
		if (mainProperties.getProperty("sys.gis.enable_spatial") != null
				&& mainProperties.getProperty("sys.gis.enable_spatial").equals("true"))
			sdiEnabled = true;

		String svcClass = mainProperties.getProperty("sys.service_class");

		if (svcClass != null && svcClass.length() > 1) {
			String[] list = svcClass.trim().split(";");
			if (list.length > 0)
				serviceClasses.addAll(Arrays.asList(list));
		}
		if (hasErrors)
			mainProperties = null;
		return mainProperties;
	}

	/**
	 * Method to check if a class name is registered in the list of service
	 * classes in svarog.properties.
	 * 
	 * @param className
	 *            The class name to be checked
	 * @return True if the class name is a registered service class
	 */
	static boolean isServiceClass(String className) {
		return serviceClasses.contains(className);
	}

	/**
	 * Method to get a new JDBC connection to the database
	 * 
	 * @return A JDBC connection object
	 * @throws Exception
	 */
	static Connection getDBConnection() throws SvException {
		Connection conn = getDBConnectionImpl();
		log4j.trace("New DB connection acquired");
		return conn;
	}

	/**
	 * Method to return a connection for fetching and storing files in Database
	 * BLOBs.
	 * 
	 * @return
	 * @throws SvException
	 */
	/*
	 * static Connection getFSDBConnection() throws SvException { String
	 * fs_conn_type = "filestore."; if (config.getProperty(fs_conn_type +
	 * "conn.type").trim().equals("DEFAULT")) fs_conn_type = "";
	 * 
	 * Connection conn=getDBConnectionImpl(fs_conn_type); log4j.trace(
	 * "New FileStore DB connection acquired"); return conn; }
	 */

	/**
	 * The mediator method that decides if the connection should be established
	 * via JDBC or via JNDI data source.
	 * 
	 * @param connPrefix
	 *            The prefix name of the connection
	 * @return A JDBC Connection Object
	 * @throws SvException
	 */
	static private Connection getDBConnectionImpl() throws SvException {
		if (svDbConnType.equals(SvDbConnType.JDBC))
			return getJDBConnection();
		else
			return getJNDIConnection();
	}

	/**
	 * A method to acquire a connection from a JNDI data source
	 * 
	 * @param connPrefix
	 *            Connection prefix to be used for configuring the JNDI data
	 *            source
	 * @return A JDBC connection
	 * @throws SvException
	 */
	static private Connection getJNDIConnection() throws SvException {
		Connection result = null;
		try {
			result = coreDataSource.getConnection();
		} catch (Exception ex) {
			throw (new SvException("system.error.db_conn_err", svCONST.systemUser, ex));
		}
		return result;
	}

	/**
	 * A method to acquire a connection from a JDBC driver
	 * 
	 * @param connPrefix
	 *            Connection prefix to be used for configuring the JDBC driver
	 * @return A JDBC connection
	 * @throws SvException
	 */
	private static Connection getJDBConnection() throws SvException {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(connString, connUser, connPass);
			//conn = coreDataSource.getConnection();
		} catch (Exception ex) {
			throw (new SvException("system.error.db_conn_err", svCONST.systemUser, ex));
		}
		return conn;
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

	public static int getSDISrid() {
		if (sdiSrid == null) {
			try {
				String srid = config.getProperty("sys.gis.default_srid").trim();
				if (srid != null && !srid.isEmpty())
					sdiSrid = Integer.parseInt(srid);
			} catch (Exception ex) {
				log4j.warn("Exception parsing SRID", ex);
			}
			if (sdiSrid == null) {
				log4j.warn("Can't get SRID from config (parameter:sys.gis.default_srid). Defaulting to 0");
				sdiSrid = new Integer(0);
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
	 * Method to get the configuration about JDBC batch size. It seems oracle
	 * has a bug and doesn't handle more than 10 statements in the JDBC batch.
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
	 * Method to get the configuration about maximum SvCore Idle time. Default
	 * is 30 minutes.
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
	 * Method to get the path of the conf directory holding all JSON config
	 * files used for install
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
	 * @param timeout
	 *            The number of minutes, before the core is considered as idle
	 */
	public static void setCoreIdleTimeout(int timeout) {
		coreIdleTimeout = timeout * 60000;
	}

	/**
	 * Method to set the core idle time out in Minutes
	 * 
	 * @param timeout
	 *            The number of minutes, before the core is considered as idle
	 */
	public static void setCoreIdleTimeoutMilis(int timeoutMilis) {
		coreIdleTimeout = timeoutMilis;
	}

	public static String getMasterRepo() {
		return repoName;
		// return config.getProperty("sys.masterRepo").trim();
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

	@Deprecated
	public static String getDBType() {
		return dbType;
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

}
