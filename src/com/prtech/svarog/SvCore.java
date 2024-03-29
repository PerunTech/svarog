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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.prtech.svarog.SvConf.SvDbType;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataField;
import com.prtech.svarog_common.DbDataField.DbFieldType;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQuery;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.DboFactory;
import com.prtech.svarog_common.DboUnderground;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvDatabaseIO;
import com.prtech.svarog_common.ISvOnSave;
import com.prtech.svarog_common.ResponseHandler;
import com.prtech.svarog_common.SvCharId;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * 
 * SvCore is core abstract class of the Svarog Platform. It provides all the
 * basic functions to classes inheriting from it. It serves as basic
 * configuration management tool, managing the list of object types, the list of
 * fields, the constraint rules. It also handles all different methods related
 * to management of Forms (except for reading forms which is assigned to the
 * SvReader). It also ensures that proper SvCore chaining is validated and
 * allows sharing JDBC connections between different cores. It also performs all
 * Svarog related authorization processes which turn implement the security
 * enforcement via SVAROG ACLs. Internally it provides the basic reading of data
 * from the database structure and transforming it into set of DbDataObject
 * POJOs.
 * 
 */
public abstract class SvCore implements ISvCore, java.lang.AutoCloseable {
	/////////////////////////////////////////////////////////////
	// SvCore static variables and methods
	/////////////////////////////////////////////////////////////
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvCore.class);

	/**
	 * The property name used to specify whether the launcher should execute a
	 * svarog shutdown hook.
	 **/
	public static final String SVAROG_SHUTDOWN_HOOK_PROP = "svarog.shutdown.hook";
	public static final String SVAROG_STARTUP_HOOK_PROP = "svarog.startup.hook";

	/**
	 * Static block to set the shutdown hooks only once at boot
	 */
	static {
		setShutDownHook();
	}

	/**
	 * Enumeration holding flags about access over svarog objects.
	 * 
	 * @author XPS13
	 *
	 */
	public enum SvAccess {
		NONE(1 << 0), READ(1 << 1), WRITE(1 << 2), EXECUTE(1 << 3), MODIFY(1 << 4), FULL(1 << 5);

		private final long accessLevelValue;

		SvAccess(long accessLevelValue) {
			this.accessLevelValue = accessLevelValue;
		}

		public long getAccessLevelValue() {
			return accessLevelValue;
		}
	}

	/**
	 * Translates a numeric status code into a Set of SvAccess enums
	 * 
	 * @param accessValue bit shifted representation of the access flags
	 * 
	 * @return EnumSet representing a svarog access level status
	 */
	public static EnumSet<SvAccess> getAccessFlags(long accessValue) {
		EnumSet<SvAccess> statusFlags = EnumSet.noneOf(SvAccess.class);
		for (SvAccess flag : SvAccess.values()) {
			long flagValue = flag.getAccessLevelValue();
			if ((flagValue & accessValue) == flagValue) {
				statusFlags.add(flag);
			}
		}
		return statusFlags;
	}

	/**
	 * Translates a set of SvAccess enums into a numeric access code
	 * 
	 * @param flags Set of access flags
	 * 
	 * @return numeric representation of the access level code
	 */
	public static long getAccessValue(Set<SvAccess> flags) {
		long value = 0;
		for (SvAccess flag : flags) {
			value |= flag.getAccessLevelValue();
		}
		return value;
	}

	/**
	 * HashMap containing all table descriptors mapped to the table name
	 */
	static final HashMap<String, DbDataObject> dbtMap = new HashMap<String, DbDataObject>();
	/**
	 * Flag which enable core tracing to track potential connection leaks
	 */
	static Boolean isDebugEnabled = SvConf.getIsDebugEnabled();
	/**
	 * Limit the JDBC batch size to address an Oracle bug
	 */
	static int batchSize = SvConf.getJDBCBatchSize();
	/**
	 * Static variable to hold the default repo Object(Table) descriptor
	 * 
	 */
	protected static DbDataObject repoDbt;
	/**
	 * Static variable to hold the list of fields in default repo Object(Table)
	 * descriptor
	 * 
	 */
	protected static DbDataArray repoDbtFields;
	/**
	 * Flag used to prevent executing a DROP SCHEMA statement. It can be only
	 * changed via JUnit
	 */
	static Boolean enableCleanDb = false;
	/**
	 * Information about the source of the loaded Svarog config (DB or file)
	 */
	static Boolean isCfgInDb = false;

	/**
	 * Minimum time between two session refresh notifications are published in the
	 * cluster The purpose is to prevent throttling of the refresh notification in
	 * the cluster.
	 */
	static long sessionDebounceInterval = 5000;

	/**
	 * The POA types defined in Svarog, we need them for security checks.
	 */
	protected static DbDataArray poaDbLinkTypes = new DbDataArray();

	/**
	 * The link types defined in Svarog
	 */
	protected static DbDataArray dbLinkTypes = new DbDataArray();
	/**
	 * List of currently configured geometry types in Svarog. If and Object Type Id
	 * is in this list SvWriter will throw an exception when saveObject is attempted
	 * on such Object Type. The Object Type Ids in this list must be handled by
	 * SvGeometry.saveObject and compatible methods.
	 */
	private static ArrayList<Long> geometryTypes = new ArrayList<Long>();
	/**
	 * Map of constraints per object type.
	 */
	protected static HashMap<Long, SvObjectConstraints> uniqueConstraints = new HashMap<Long, SvObjectConstraints>();

	/**
	 * Map containing all repo objects
	 */
	private static HashMap<String, DbDataObject> repoDbtMap = new HashMap<String, DbDataObject>(1, 1);

	/**
	 * Map containing all key maps for the specified Object Type
	 */
	private static HashMap<Long, LinkedHashMap<SvCharId, Object>> dbtKeyMap = new HashMap<Long, LinkedHashMap<SvCharId, Object>>();

	/**
	 * Map containing all keys the specified Object Type, without the field
	 * descriptors
	 */
	private static HashMap<Long, LinkedHashMap<SvCharId, Object>> emptyKeyMap = new HashMap<Long, LinkedHashMap<SvCharId, Object>>();

	/**
	 * Flag with information about the successful Svarog initialization
	 */
	static AtomicBoolean isInitialized = new AtomicBoolean(false);

	/**
	 * Flag with information about the status of the Svarog initialization
	 */
	static AtomicBoolean isValid = new AtomicBoolean(false);

	/**
	 * Flag which tells if svarog daemon is running
	 */
	static AtomicBoolean svDaemonRunning = new AtomicBoolean(false);

	/**
	 * Timestamp which registers the SvCore last cleanup. A cleanup is performed
	 * every XX minutes as configured by sys.core.cleanup_time
	 */
	static long coreLastCleanup = DateTime.now().getMillis();
	/**
	 * The reference queue keeping all SvCore soft refs ready for cleaning
	 */
	static ReferenceQueue<SvCore> svcQueue = new ReferenceQueue<SvCore>();

	/**
	 * List of callbacks to be executed before and after save of objects
	 */
	protected static HashMap<Long, CopyOnWriteArrayList<ISvOnSave>> onSaveCallbacks = new HashMap<Long, CopyOnWriteArrayList<ISvOnSave>>();

	/**
	 * Local static var holding the default system locale
	 */
	private static volatile DbDataObject defaultSysLocale = null;

	/////////////////////////////////////////////////////////////
	// SvCore instance (non-static) variables and methods
	/////////////////////////////////////////////////////////////
	/**
	 * A weak reference to self. We need this for getting tracked JDBC connections
	 */
	protected final SoftReference<SvCore> weakThis = new SoftReference<SvCore>(this, svcQueue);
	/**
	 * A weak reference to the SvCore object with whom we would like to share a
	 * connection. We need this for getting tracked JDBC connections
	 */
	protected final SoftReference<SvCore> weakSrcCore;
	/**
	 * The user object under which this instance will run
	 */
	protected DbDataObject instanceUser;

	/**
	 * The user object under which this instance was running before a
	 * {@link #switchUser(String)} was executed.
	 */
	protected DbDataObject previousUser = null;

	/**
	 * The permissions associated with this instance
	 */
	private HashMap<SvAclKey, HashMap<String, DbDataObject>> instancePermissions = null;
	/**
	 * The permissions indexed by key (label_code)
	 */
	private HashMap<String, DbDataObject> permissionKeys = null;

	/**
	 * The user object under which objects saved by this instance will be registered
	 */
	protected DbDataObject saveAsUser = null;

	/**
	 * The current user default user group
	 */
	private DbDataObject instanceUserDefaultGroup;

	/**
	 * Variable to identify if install is in progress and loosen the OID/PKID
	 * validations
	 */
	Boolean isInternal = false;
	/**
	 * Variable to identify that a core has long running ops and it shouldn't be
	 * subject of cleanup
	 */
	Boolean isLongRunning = false;
	/**
	 * Timestamp which registers the SvCore construction time. It is used to
	 * identify rogue cores which are created but not released. Based on the time of
	 * inactivity
	 */
	long coreCreation = DateTime.now().getMillis();
	/**
	 * Timestamp which registers the SvCore last activity. It is used in combination
	 * with the coreCreation time to identify inactive cores, which are still
	 * lurking around.
	 */
	long coreLastActivity = coreCreation;
	/**
	 * Timestamp which registers the SvCore last activity. It is used in combination
	 * with the coreCreation time to identify inactive cores, which are still
	 * lurking around.
	 */
	String coreTraceInfo = null;
	/**
	 * The current session_id associated with the core
	 */
	String coreSessionId = null;
	/**
	 * Variable to identify if geometries should be returned by the getObjects base
	 * method validations
	 */
	Boolean includeGeometries = false;
	/**
	 * Writer object to serialize geometries
	 */
	WKBWriter wkbWriter = null;

	public Boolean getIncludeGeometries() {
		return includeGeometries;
	}

	public void setIncludeGeometries(Boolean includeGeometries) {
		this.includeGeometries = includeGeometries;
	}

	/**
	 * Writer object to serialize geometries
	 */
	WKBReader wkbReader = null;
	/**
	 * Flag to enable autoCommit on SvCore instance level.
	 */
	protected Boolean autoCommit = true;

	/**
	 * Method to set the information about the line number, method and class where
	 * the instance was created. This is usefull when looking for connection leaks.
	 */
	private void setDebugInfo() {
		StackTraceElement[] traces = Thread.currentThread().getStackTrace();
		StackTraceElement singleTrace = traces[0];
		for (StackTraceElement strace : traces) {
			if (strace.getMethodName().equals(Sv.INIT) || strace.getMethodName().equals(Sv.STACK_TRACE)
					|| strace.getMethodName().equals(Sv.DEBUG_INFO))
				continue;
			else {
				singleTrace = strace;
				break;
			}
		}

		this.coreTraceInfo = Sv.FILE + Sv.COLON + singleTrace.getFileName() + Sv.SEMICOLON + Sv.CLASS + Sv.COLON
				+ singleTrace.getClassName() + Sv.SEMICOLON + Sv.METHOD + Sv.COLON + singleTrace.getMethodName()
				+ Sv.SEMICOLON + Sv.LINE + Sv.COLON + singleTrace.getLineNumber();
	}

	/**
	 * Default SvCore constructor which is usually used internally in the package.
	 * 
	 * @param srcCore      The parent SvCore instance
	 * @param instanceUser The user descriptor as which the instance should execute
	 * @throws SvException If the new SvCore instance uses SvSecurity instance as
	 *                     parent an exception is thrown. SvSecurity runs as SYSTEM
	 *                     user, so creating new instances from SvSecurity will
	 *                     result in running as system user without authentication.
	 *                     Another type of exception is thrown if the system wasn't
	 *                     properly intialised
	 */
	protected SvCore(DbDataObject instanceUser, SvCore srcCore) throws SvException {
		if (isDebugEnabled)
			setDebugInfo();
		if (srcCore != null && srcCore instanceof SvSecurity)
			if (!srcCore.isService())
				throw (new SvException("system.error.cant_use_svsec_as_core", instanceUser));

		this.instanceUser = (srcCore != null ? srcCore.instanceUser : instanceUser);
		this.isInternal = (srcCore != null ? srcCore.isInternal : this.isInternal);

		if (this.instanceUser == null)
			throw (new SvException("system.error.instance_user_null", svCONST.systemUser));

		this.autoCommit = (srcCore != null ? srcCore.autoCommit : this.autoCommit);

		if (SvConf.isClusterEnabled() && (SvarogDaemon.osgiFramework != null && !SvCluster.getIsActive().get())) {
			if ((srcCore != null && !srcCore.isInternal))
				throw (new SvException(Sv.Exceptions.CLUSTER_INACTIVE, instanceUser));
		}
		// if the svarog core is not in valid state we should start the
		// initialisation
		if (!isValid.get())
			initSvCoreImpl(false);

		if (srcCore != null && srcCore != this) {
			weakSrcCore = srcCore.weakThis;
			this.coreSessionId = srcCore.getSessionId();
		} else
			weakSrcCore = null;
	}

	/**
	 * Constructor based on existing SvCore instance
	 * 
	 * @param srcCore The source SvCore instance based on which this instance should
	 *                get the JDBC connection
	 * @throws SvException Throws exception if the srcCore parameter is null
	 */
	protected SvCore(SvCore srcCore) throws SvException {
		this((DbDataObject) null, srcCore);
		if (srcCore == null)
			throw (new SvException("system.error.parent_core_isnull", instanceUser));
	}

	/**
	 * Constructor creating SvCore based on user session and existing SvCore
	 * instance which will be used for sharing the JDBC connection.
	 * 
	 * @param session_id The token used to identify the Svarog session
	 * @param srcCore    The source/parent SvCore instance which should be used for
	 *                   connection sharing
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public SvCore(String session_id, SvCore srcCore) throws SvException {
		this(getUserBySession(session_id), srcCore);
		if (this.coreSessionId == null)
			this.coreSessionId = session_id;
	}

	/**
	 * Constructor creating SvCore based on user session.
	 * 
	 * @param session_id The token used to identify the Svarog session
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public SvCore(String session_id) throws SvException {
		this(getUserBySession(session_id), null);
		this.coreSessionId = session_id;
	}

	/**
	 * Protected constructor to be used only inside the Svarog package
	 */
	protected SvCore() {
		this.instanceUser = svCONST.systemUser;
		weakSrcCore = null;
	}

	/**
	 * The CodeList object is used for decoding, so we want to avoid creating many
	 * instances of it, for each decoder.
	 */
	private CodeList instanceCodeList;

	/**
	 * Method to return the current CodeList object instance used for this SvCore
	 * object
	 * 
	 * @return Object instance of CodeList
	 */
	@Deprecated
	protected CodeList getCodeList() {
		if (instanceCodeList == null)
			try {
				instanceCodeList = new CodeList(this);
			} catch (Exception e) {
				log4j.error("Can't instantiate a CodeList object, something went wrong", e);
			}
		return instanceCodeList;
	}

	/**
	 * Method returning the constraints for the specified object type
	 * 
	 * @param objecType The id of the object type for which we need the constraints
	 * @return the SvObjectConstraints instance
	 */
	static SvObjectConstraints getObjectConstraints(Long objecType) {
		return uniqueConstraints.get(objecType);
	}

	/**
	 * Method to register an OnSave callback
	 * 
	 * @param callback Reference to a class implementing the {@link ISvOnSave}
	 *                 interface
	 */
	static synchronized void registerOnSaveCallback(ISvOnSave callback) {
		CopyOnWriteArrayList<ISvOnSave> global = onSaveCallbacks.get(0L);
		if (global == null) {
			global = new CopyOnWriteArrayList<ISvOnSave>();
			onSaveCallbacks.put(0L, global);
		}
		global.add(callback);
	}

	/**
	 * Method to register an OnSave callback for specific object type
	 * 
	 * @param callback Reference to a class implementing the {@link ISvOnSave}
	 *                 interface
	 * @param type     The id of the object type for which we want the call back to
	 *                 be executed
	 */
	public synchronized static void registerOnSaveCallback(ISvOnSave callback, Long type) {
		CopyOnWriteArrayList<ISvOnSave> local = onSaveCallbacks.get(type);
		if (local == null) {
			local = new CopyOnWriteArrayList<ISvOnSave>();
			onSaveCallbacks.put(type, local);
		}
		local.add(callback);
	}

	/**
	 * Method to unregister an OnSave callback
	 * 
	 * @param callback Reference to a class implementing the {@link ISvOnSave}
	 *                 interface
	 */
	public static void unregisterOnSaveCallback(ISvOnSave callback) {
		CopyOnWriteArrayList<ISvOnSave> global = onSaveCallbacks.get(0L);
		if (global != null) {
			if (global.contains(callback))
				global.remove(callback);
			else
				log4j.error("Can't remove callback, are you creating a memory leak darling?");
		}
	}

	/**
	 * Method to unregister an OnSave callback
	 * 
	 * @param callback Reference to a class implementing the {@link ISvOnSave}
	 *                 interface
	 * @param type     Id of the type of object for which the callback was
	 *                 registered
	 */
	public synchronized static void unregisterOnSaveCallback(ISvOnSave callback, Long type) {
		CopyOnWriteArrayList<ISvOnSave> local = onSaveCallbacks.get(type);
		if (local != null) {
			if (local.contains(callback))
				local.remove(callback);
			else
				log4j.error("Can't remove callback, are you creating a memory leak darling?");

		}
	}

	/**
	 * Method to access info about the default locale configurations
	 * 
	 * @return A {@link DbDataObject} reference holding the default locale
	 *         descriptor
	 */
	public static DbDataObject getDefaultLocale() {
		if (defaultSysLocale == null) {
			synchronized (SvCore.class) {
				DbDataObject dboLocale = SvarogInstall.getLocaleList().getItemByIdx(SvConf.getDefaultLocale());
				if (dboLocale == null)
					log4j.error("System locale is misconfigured, expect unexpected behavior");
				else
					DboFactory.makeDboReadOnly(dboLocale);
				defaultSysLocale = dboLocale;
			}
		}
		return defaultSysLocale;
	}

	/**
	 * Method returning the Repo Object Type descriptor aka DBT This will return the
	 * default repo. You should use the repo according to your specific object type
	 * 
	 * @return DbDataObject describing the repo object type
	 */
	@Deprecated
	public static DbDataObject getRepoDbt() {
		return repoDbt;
	}

	/**
	 * Method to verify if a specific object id is infact an Object Descriptor
	 * 
	 * @param objectId The Id to verify
	 * @return True if the object id is identifier for a DBT
	 */
	public static boolean isDbt(Long objectId) {
		DbDataObject dbt = DbCache.getObject(objectId, svCONST.OBJECT_TYPE_TABLE);
		String tableName = (String) dbt.getVal(Sv.TABLE_NAME);
		return dbtMap.containsKey(tableName);
	}

	/**
	 * Method to return the repo descriptor for a given object type
	 * 
	 * @param objectTypeId The id of the object type
	 * @return The repo object type descriptor
	 */
	public static DbDataObject getRepoDbt(Long objectTypeId) {
		DbDataObject dbt = DbCache.getObject(objectTypeId, svCONST.OBJECT_TYPE_TABLE);
		String repoName = (String) dbt.getVal(Sv.REPO_NAME);
		return repoDbtMap.get(repoName);

	}

	/**
	 * Method returning the default RepoFields. These should be constant accross all
	 * repo objects in the system
	 * 
	 * @return A {@link DbDataArray} reference holding all field descriptors for the
	 *         REPO object
	 */
	public static DbDataArray getRepoDbtFields() {
		return repoDbtFields;
	}

	/**
	 * Method for getting an Object Descriptor for a certain type. To get the fields
	 * for a specific Dbt see @getFields; To get the base repo see @getRepoDbt
	 * 
	 * @param objectTypeId The Id of the type
	 * @return DbDataObject describing the object table storage
	 * @throws SvException Exception with label "system.error.no_dbt_found" is
	 *                     raised if no object descriptor can be found for the
	 *                     DbDataObject instance
	 */
	public static DbDataObject getDbt(Long objectTypeId) throws SvException {
		DbDataObject dbt = null;
		dbt = DbCache.getObject(objectTypeId, svCONST.OBJECT_TYPE_TABLE);
		if (dbt == null) {
			String exceptionMessage = Sv.Exceptions.NO_DBT_FOUND;
			throw (new SvException(exceptionMessage, svCONST.systemUser, null, objectTypeId));
		}
		return dbt;
	}

	/**
	 * Method that returns the DbDataObject for the reference type of the object
	 * 
	 * @param dbo The object for which the reference type will be returned
	 * @return The @DbDataObject describing the reference type
	 * @throws SvException Exception with label "system.error.no_dbt_found" is
	 *                     raised if no object descriptor can be found for the
	 *                     DbDataObject instance
	 */
	public static DbDataObject getDbt(DbDataObject dbo) throws SvException {
		DbDataObject dbt = null;
		if (dbo.getObjectType() != null && dbo.getObjectType() > 0)
			dbt = DbCache.getObject(dbo.getObjectType(), svCONST.OBJECT_TYPE_TABLE);
		if (dbt == null) {
			String exceptionMessage = "system.error.no_dbt_found";
			throw (new SvException(exceptionMessage, svCONST.systemUser, dbo, null));
		}
		return dbt;
	}

	/**
	 * Method to get a list of fields defined for a specific Object Descriptor
	 * Typically object descriptors are fetched with getDbt() methods
	 * 
	 * @param objectTypeId The id of the type
	 * @return Array of Objects, each representing a metadata field in the object
	 */
	public static DbDataArray getFields(Long objectTypeId) {
		DbDataArray fields = null;
		fields = DbCache.getObjectsByParentId(objectTypeId, svCONST.OBJECT_TYPE_FIELD_SORT);
		return fields;
	}

	/**
	 * Method to get a list of fields defined for a specific Object Descriptor
	 * Typically object descriptors are fetched with getDbt() methods
	 * 
	 * @param tableName The name of the table in the Svarog System
	 * 
	 * @param fieldName The name of the field in the table
	 * @return Array of Objects, each representing a metadata field in the object
	 */
	public static DbDataObject getFieldByName(String tableName, String fieldName) {
		// get the unique field id from svarog_tables
		DbDataObject tableObj = getDbtByName(tableName);
		DbDataArray fields = null;
		DbDataObject retField = null;
		if (tableObj != null)
			fields = DbCache.getObjectsByParentId(tableObj.getObjectId(), svCONST.OBJECT_TYPE_FIELD_SORT);
		if (fields != null)
			for (DbDataObject dbf : fields.getItems()) {
				if (dbf.getVal(Sv.FIELD_NAME).equals(fieldName)) {
					retField = dbf;
					break;
				}
			}
		return retField;
	}
	/**
	 * Method to get the link types between two object regardless of the link code.
	 * 
	 * @param typeId1       The first type id
	 * @param typeId2       The second type id
	 * @param bidirectional If the bidirectional flag is set, the method will the
	 *                      link type regardless of the order first or second.
	 * @return List of the link types
	 */
	public static List<DbDataObject> getLinkTypes(Long typeId1, Long typeId2, boolean bidirectional) {
		ArrayList<DbDataObject> retval = new ArrayList<>();
		for (DbDataObject link : dbLinkTypes.getItems()) {
			if ((link.getVal(Sv.Link.LINK_OBJ_TYPE_1).equals(typeId1)
					&& link.getVal(Sv.Link.LINK_OBJ_TYPE_2).equals(typeId2))
					|| (bidirectional && link.getVal(Sv.Link.LINK_OBJ_TYPE_2).equals(typeId1)
							&& link.getVal(Sv.Link.LINK_OBJ_TYPE_1).equals(typeId2))

			) {
				retval.add(link);
			}
		}
		return retval;
	}

	/**
	 * Method to get a link type descriptor in DbDataObject format
	 * 
	 * @param linkTypeId The id of the type
	 * @return A link type descriptor object
	 */
	public static DbDataObject getLinkType(Long linkTypeId) {
		DbDataObject dblt = null;
		dblt = DbCache.getObject(linkTypeId, svCONST.OBJECT_TYPE_LINK_TYPE);
		return dblt;
	}

	/**
	 * Method to get a link type descriptor in DbDataObject format
	 * 
	 * @param linkType      The link code
	 * @param objectTypeId1 The object id of the first object type
	 * @param objectTypeId2 The object id of the second object type
	 * @return A link type descriptor object
	 */
	public static DbDataObject getLinkType(String linkType, Long objectTypeId1, Long objectTypeId2) {
		String uqVals = linkType + Sv.DOT + objectTypeId1.toString() + Sv.DOT + objectTypeId2.toString();
		return DbCache.getObject(uqVals, svCONST.OBJECT_TYPE_LINK_TYPE);
	}

	/**
	 * Method to get a link type descriptor in DbDataObject format
	 * 
	 * @param linkType The link code
	 * @param dbt1     The descriptor of the first object type
	 * @param dbt2     The descriptor of the first object type
	 * @return A link type descriptor object
	 */
	public static DbDataObject getLinkType(String linkType, DbDataObject dbt1, DbDataObject dbt2) {
		String uqVals = linkType + Sv.DOT + dbt1.getObjectId().toString() + Sv.DOT + dbt2.getObjectId().toString();
		return DbCache.getObject(uqVals, svCONST.OBJECT_TYPE_LINK_TYPE);
	}

	/**
	 * Method to verify if Object Type Id contains GEOMETRY objects.
	 * 
	 * @param objectTypeId The id of the type
	 * @return True if the object type descriptor has at least one GEOMETRY field
	 */
	public static boolean hasGeometries(Long objectTypeId) {
		return geometryTypes.contains(objectTypeId);
	}

	/**
	 * Method to get a form type descriptor in DbDataObject format
	 * 
	 * @param formTypeId The id of the type
	 * @return A form type descriptor object
	 */
	public static DbDataObject getFormType(Long formTypeId) {
		DbDataObject dblt = null;
		dblt = DbCache.getObject(formTypeId, svCONST.OBJECT_TYPE_FORM_TYPE);
		return dblt;
	}

	/**
	 * A method to return the type Id of an object according to the object name.
	 * This method doesn't work for system objects, which are prefixed with repo
	 * name. If you can't get an id by using this method you should look at
	 * {@link svCONST} and you will probably find a constant referencing the object
	 * you need.
	 * 
	 * @param objectName The name of the object type (table name)
	 * @return A object type descriptor
	 * @throws SvException If the object type identified by name ObjectName is not
	 *                     found "system.error.no_dbt_found" exception is thrown
	 */
	public static Long getTypeIdByName(String objectName) throws SvException {
		return getTypeIdByName(objectName, null);
	}

	/**
	 * Method to return the typeId from a objectName code
	 * 
	 * @param objectName   The physical table name of the object
	 * @param objectSchema The schema in which the table resides
	 * @return Object Id of the object type descriptor
	 * @throws SvException If the object type identified by name ObjectName is not
	 *                     found "system.error.no_dbt_found" exception is thrown
	 */
	public static Long getTypeIdByName(String objectName, String objectSchema) throws SvException {
		DbDataObject objType = null;
		if (objectName != null)
			objType = getDbtByName(objectName, objectSchema);

		if (objType == null) {
			String exceptionMessage = "system.error.no_dbt_found";
			throw (new SvException(exceptionMessage, svCONST.systemUser, null, objectName));
		} else
			return objType.getObjectId();
	}

	/**
	 * A method to return the type descriptor of an object according to the object
	 * name.
	 * 
	 * @param objectName Name of the object type (table name)
	 * @return A DbDataObject descriptor of the DBT
	 */
	public static DbDataObject getDbtByName(String objectName) {
		return getDbtByName(objectName, null);
	}

	/**
	 * Method to return the type descriptor from a objectName code
	 * 
	 * @param objectName   The physical table name of the object
	 * @param objectSchema The schema in which the table resides
	 * @return An instance of object type descriptor
	 */
	public static DbDataObject getDbtByName(String objectName, String objectSchema) {
		if (objectName == null)
			return null;
		String objName = objectName.toUpperCase();
		objectSchema = (objectSchema != null ? objectSchema : SvConf.getDefaultSchema());
		DbDataObject objType = dbtMap.get(objectSchema + Sv.DOT + objName);
		if (objType == null) {
			objName = objectSchema + Sv.DOT + SvConf.getMasterRepo() + "_" + objName;
			objType = dbtMap.get(objName);
		}
		return objType;
	}

	/**
	 * Method to return the currently configured ISvDatabaseIO instance
	 * 
	 * @return ISvDatabaseIO instance
	 * @throws SvException
	 */
	public static ISvDatabaseIO getDbHandler() throws SvException {
		return SvConf.getDbHandler();
	}

	/**
	 * Method to check if the master repo table exists as a basic data table in the
	 * DB
	 * 
	 * @param conn The JDBC connection to be used for executing the query
	 * @return True if the table exists
	 */
	private static boolean masterRepoExistsInDb(Connection conn) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement("select * from " + SvConf.getDefaultSchema() + "." + SvConf.getMasterRepo()
					+ " where object_id=" + svCONST.OBJECT_TYPE_REPO);
			ps.execute();
			rs = ps.getResultSet();
			if (rs.next())
				return true;
		} catch (Exception e) {
			log4j.error("Master repo table doesn't exist:" + SvConf.getDefaultSchema() + "." + SvConf.getMasterRepo()
					+ ". You should run install!");
		} finally {
			try {
				closeResource((AutoCloseable) rs, svCONST.systemUser);
			} catch (Exception e) {
				log4j.error("Recordset can't be released!", e);
			}
			try {
				closeResource((AutoCloseable) ps, svCONST.systemUser);
			} catch (Exception e) {
				log4j.error("Recordset can't be released!", e);
			}

		}
		return false;
	}

	/**
	 * Method that initializes the system from JSON config files. This method is
	 * first one that gets invoked when svarog is started. The method loads all json
	 * configuration records from: 1./json/records/40. master_records.json
	 * 2./json/records/30. master_codes.json
	 * 
	 * After the loading of the base JSON configuration files without exception,
	 * svarog is properly loaded and running.
	 * 
	 * @param coreObjects reference to the core objects array
	 * @param coreCodes   reference to the core codes array
	 * @return True if the system has been initialised properly.
	 */
	private static synchronized Boolean initSvCoreLocal(DbDataArray coreObjects, DbDataArray coreCodes) {
		// init the table configs
		isCfgInDb = false;
		InputStream fis = null;
		DbCache.clean();
		try {
			fis = SvConf.class.getClassLoader().getResourceAsStream("conf/records/40. master_records.json");
			if (fis == null) {
				String path = "./conf/records/40. master_records.json";
				fis = new FileInputStream(path);
			}
			String json = IOUtils.toString(fis, "UTF-8");
			json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());
			Gson gson = new Gson();
			JsonObject jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
			DbDataArray sysObjects = new DbDataArray();
			sysObjects.fromJson(jobj);
			for (DbDataObject dbo : sysObjects.getItems()) {
				if (dbo.getObjectType().equals(svCONST.OBJECT_TYPE_TABLE)) {
					dbo.setVal(Sv.TABLE_NAME, ((String) dbo.getVal(Sv.TABLE_NAME)).toUpperCase());
					dbo.setVal(Sv.SCHEMA, ((String) dbo.getVal(Sv.SCHEMA)).toUpperCase());
				}
				dbo.setIsDirty(false);
				coreObjects.addDataItem(dbo);
			}
			DbCache.addArrayWithParent(sysObjects);
			repoDbt = DbCache.getObject(svCONST.OBJECT_TYPE_REPO, svCONST.OBJECT_TYPE_TABLE);
			repoDbtFields = DbCache.getObjectsByParentId(svCONST.OBJECT_TYPE_REPO, svCONST.OBJECT_TYPE_FIELD_SORT);
		} catch (IOException e) {
			File currDir = new File(".");
			String currPath = "UNKNOWN PATH";
			try {
				currPath = currDir.getCanonicalPath();
			} catch (IOException e1) {
				log4j.error("Can't get current working directory", e);
			}
			log4j.error("Can't load master repo json config in folder:" + currPath, e);
			return false;
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					log4j.error("Can't close file stream for loading master repo", e);
				}
		}
		try {
			fis = SvCore.class.getClassLoader().getResourceAsStream("conf/records/30. master_codes.json");
			if (fis == null) {
				String path = "./conf/records/30. master_codes.json";
				fis = new FileInputStream(path);
			}
			String json = IOUtils.toString(fis, "UTF-8");
			json = json.replace("{MASTER_REPO}", SvConf.getMasterRepo());
			json = json.replace("{DEFAULT_SCHEMA}", SvConf.getDefaultSchema());
			json = json.replace("{REPO_TABLE_NAME}", SvConf.getMasterRepo());
			Gson gson = new Gson();
			JsonObject jobj = gson.fromJson(json, JsonElement.class).getAsJsonObject();
			DbDataArray sysCodes = new DbDataArray();
			sysCodes.fromJson(jobj);
			for (DbDataObject dbo : sysCodes.getItems()) {
				dbo.setIsDirty(false);
				coreCodes.addDataItem(dbo);
			}
			DbCache.addArrayWithParent(sysCodes);
		} catch (IOException e) {
			log4j.error("Can't load master codes", e);
			return false;
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					log4j.error("Can't close stream. Errgh something bad happened?", e);
				}
		}
		return true;
	}

	/**
	 * Performs basic initialisation of svarog by using the DbInit classes. This
	 * version doesn't use the json config files so it can run without config folder
	 * 
	 * @param coreObjects reference to the core objects array
	 * @param coreCodes   reference to the core codes array
	 * @return True if the system has been initialised properly.
	 */
	private static synchronized Boolean initSvCoreNoCfg(DbDataArray coreObjects, DbDataArray coreCodes) {
		// init the table configs
		isCfgInDb = false;
		// init the base records
		DbInit.initCoreRecords(coreCodes, coreObjects);
		DbCache.addArrayWithParent(coreObjects);
		repoDbt = DbCache.getObject(svCONST.OBJECT_TYPE_REPO, svCONST.OBJECT_TYPE_TABLE);
		repoDbtFields = DbCache.getObjectsByParentId(svCONST.OBJECT_TYPE_REPO, svCONST.OBJECT_TYPE_FIELD_SORT);
		DbCache.addArrayWithParent(coreCodes);
		return true;
	}

	/**
	 * Method to pre-process the field descriptors in the system
	 * 
	 * @param fields The fields array to be preprocessed
	 * @throws SvException
	 */
	static void prepareFields(DbDataArray fields) throws SvException {
		Gson gs = new Gson();

		for (DbDataObject dbo : fields.getItems()) {
			try {
				if (dbo.getVal(Sv.GUI_METADATA) != null) {
					JsonObject jo = gs.fromJson((String) dbo.getVal(Sv.GUI_METADATA), JsonObject.class);
					dbo.setVal(Sv.GUI_METADATA, jo);
					dbo.setIsDirty(false);
				}

				if (dbo.getVal(Sv.EXTENDED_PARAMS) != null)
					prepareExtOpts(dbo);
				// finally link the ext params and gui metadata to the parent
				// meta
				linkToParentDbt(dbo);
			} catch (Exception e) {
				log4j.warn("Field :" + dbo.getVal(Sv.FIELD_NAME) + " has non-JSON gui metadata");
				if (log4j.isDebugEnabled())
					log4j.warn(e);
			}

		}
	}

	/**
	 * Method to prepare the extended params field for each Field Descriptor
	 * 
	 * @param dbo The field Descriptor
	 */
	private static void prepareExtOpts(DbDataObject dbo) {
		Gson gs = new Gson();
		{
			try {
				JsonObject jo = gs.fromJson((String) dbo.getVal(Sv.EXTENDED_PARAMS), JsonObject.class);
				for (Entry<String, JsonElement> je : jo.entrySet())
					dbo.setVal(je.getKey(), getJsonValue(je));
				dbo.setIsDirty(false);

			} catch (Exception e) {
				log4j.warn("Field :" + dbo.getVal(Sv.FIELD_NAME) + " has non-JSON, EXTENDED_PARAMS value");
			}
		}
	}

	/**
	 * Method to translate json element into primitive
	 * 
	 * @param je The JSON element
	 * @return The resulting object, primitive or json object.
	 */
	private static Object getJsonValue(Entry<String, JsonElement> je) {
		Object returnValue = null;
		JsonElement jel = je.getValue();
		if (jel.isJsonPrimitive()) {
			if (jel.getAsJsonPrimitive().isBoolean())
				returnValue = jel.getAsBoolean();
			if (jel.getAsJsonPrimitive().isString())
				returnValue = jel.getAsString();
			if (jel.getAsJsonPrimitive().isNumber()) {
				returnValue = jel.getAsNumber();
			}
		} else if (jel.isJsonArray() || jel.isJsonObject())
			returnValue = jel.getAsJsonObject();
		return returnValue;
	}

	/**
	 * Method to link the meta data from the field descriptor to the parent table
	 * 
	 * @param dbo The field descriptor
	 * @throws SvException Any underlying exception that might be thrown
	 */
	private static void linkToParentDbt(DbDataObject dbo) throws SvException {
		DbDataObject dbt = null;
		try {
			dbt = SvCore.getDbt(dbo.getParentId());
		} catch (SvException ex) {
			if (ex.getLabelCode().equals("system.error.no_dbt_found"))
				return;
		}
		assert (dbt != null);

		Object gmeta = dbt.getVal(Sv.GUI_METADATA);
		JsonObject meta = null;
		if (gmeta != null && gmeta instanceof JsonObject) {
			meta = (JsonObject) gmeta;
		} else {
			try (SvReader svr = new SvReader()) {
				DboUnderground.revertReadOnly(dbt, svr);
				meta = new JsonObject();
				dbt.setVal(Sv.GUI_METADATA, meta);
				dbt.setIsDirty(false);
				DboFactory.makeDboReadOnly(dbt);
			}
		}

		JsonElement j = meta.get(Sv.FIELDS);
		if (j == null) {
			j = new JsonObject();
			meta.add(Sv.FIELDS, j);
		}

		JsonObject jo = j.getAsJsonObject();
		if (dbo.getVal(Sv.GUI_METADATA) != null && !jo.has((String) dbo.getVal(Sv.FIELD_NAME)))
			jo.add((String) dbo.getVal(Sv.FIELD_NAME), (JsonElement) dbo.getVal(Sv.GUI_METADATA));

		JsonObject params = (JsonObject) dbt.getVal(Sv.EXTENDED_PARAMS);
		if (params == null) {
			try (SvReader svr = new SvReader()) {
				DboUnderground.revertReadOnly(dbt, svr);
				params = new JsonObject();
				dbt.setVal(Sv.EXTENDED_PARAMS, params);
				dbt.setIsDirty(false);
				DboFactory.makeDboReadOnly(dbt);
			}
		}
		JsonElement jx = params.get(Sv.FIELDS);
		if (jx == null) {
			jx = new JsonObject();
			params.add(Sv.FIELDS, jx);
		}

		JsonObject jox = jx.getAsJsonObject();
		if (dbo.getVal(Sv.EXTENDED_PARAMS) != null && !jox.has((String) dbo.getVal(Sv.FIELD_NAME)))
			jox.add((String) dbo.getVal(Sv.FIELD_NAME), (JsonElement) dbo.getVal(Sv.EXTENDED_PARAMS));

	}

	/**
	 * Method to pre-process the field descriptors in the system
	 * 
	 * @param tables DbDataArray of all configured tables in Svarog
	 * 
	 * @param fields The fields array assigned to the tables objects
	 * 
	 * @param links  Array of links types configured in Svarog
	 */
	private static void prepareTables(DbDataArray tables, DbDataArray fields, DbDataArray links) {
		boolean found = false;
		Gson gs = new Gson();
		for (DbDataObject dbt : tables.getItems()) {
			found = false;
			if (dbt.getVal(Sv.GUI_METADATA) != null) {
				JsonObject jo = gs.fromJson((String) dbt.getVal(Sv.GUI_METADATA), JsonObject.class);
				dbt.setVal(Sv.GUI_METADATA, jo);
				dbt.setIsDirty(false);
			}

			if (dbt.getVal(Sv.EXTENDED_PARAMS) != null)
				prepareExtOpts(dbt);

			if (dbt.getVal(Sv.CONFIG_TYPE_ID) != null) {
				Object cfgTypeId = dbt.getVal(Sv.CONFIG_TYPE_ID);

				if (!(cfgTypeId instanceof Long))
					continue;

				for (DbDataObject searchDbt : tables.getItems()) {
					if (searchDbt.getObjectId().equals(cfgTypeId)) {
						dbt.setVal("config_type", searchDbt);
						DbDataArray searchList = null;
						searchList = dbt.getVal("config_relation_type").equals(Sv.FIELD) ? fields : searchList;
						searchList = dbt.getVal("config_relation_type").equals(Sv.Link.LINK) ? fields : searchList;
						if (searchList != null && dbt.getVal("config_relation_id") != null) {
							for (DbDataObject searchField : searchList.getItems()) {
								if (searchField.getObjectId().equals(dbt.getVal("config_relation_id"))) {
									dbt.setVal("config_relation", searchField);
									found = true;
									break;
								}
							}
						}
						break;
					}
				}
				if (!found)
					log4j.error(
							"Misconfigured table object. Inconsistent config_type_id, config_relation_type, config_relation_id. Descriptor:"
									+ dbt.toJson());
				dbt.setIsDirty(false);
			}
		}
	}

	/**
	 * Method to initialise the system objects from the 3 core arrays, object,
	 * fields and link types
	 * 
	 * @param objectTypes The list of object types with appropriate configuration
	 *                    and table names
	 * @param fieldTypes  The list of field types in the system
	 * @param linkTypes   The available link t
	 * @throws Exception
	 */
	private static void initSysObjects(DbDataArray objectTypes, DbDataArray fieldTypes, DbDataArray linkTypes)
			throws Exception {
		// perform initialisation of the basemaps
		if (objectTypes != null && fieldTypes != null) {
			repoDbtMap.clear();
			dbtMap.clear();
			prepareTables(objectTypes, fieldTypes, linkTypes);
			for (DbDataObject dbo : objectTypes.getItems()) {
				DboFactory.makeDboReadOnly(dbo);
				String key = ((String) dbo.getVal(Sv.SCHEMA)).toUpperCase() + Sv.DOT
						+ ((String) dbo.getVal(Sv.TABLE_NAME)).toUpperCase();
				DbCache.addObject(dbo, key, true);
				dbtMap.put(key, dbo);
				if ((Boolean) dbo.getVal(Sv.REPO_TABLE))
					repoDbtMap.put((String) dbo.getVal(Sv.TABLE_NAME), dbo);
			}
			prepareFields(fieldTypes);
			for (DbDataObject dbo : fieldTypes.getItems()) {
				DboFactory.makeDboReadOnly(dbo);
				if (dbo.getVal(Sv.FIELD_TYPE).equals(Sv.GEOMETRY) && !geometryTypes.contains(dbo.getParentId()))
					geometryTypes.add(dbo.getParentId());
				DbCache.addObject(dbo, null, true);
			}
		}

		ResponseHandler.setI18n(new I18n());
		ResponseHandler.setDefaultLocale(SvConf.getDefaultLocale().toUpperCase());
		SvException.i18n = new I18n();
		// verify minimal configuration exists
		validateSvarogConfig();
	}

	/**
	 * Method to validate the currently loaded svarog configuration contains the
	 * base tables.
	 * 
	 * @throws SvException In case of bad config exception
	 *                     "system.error.misconfigured_dbt" is thrown
	 */
	public static void validateSvarogConfig() throws SvException {

		DbDataArray coreObjects = new DbDataArray();
		DbDataArray coreCodes = new DbDataArray();

		DbInit.initCoreRecords(coreCodes, coreObjects);

		for (DbDataObject baseType : coreObjects.getItems()) {
			if (baseType.getObjectType().equals(svCONST.OBJECT_TYPE_TABLE)) {
				String key = ((String) baseType.getVal(Sv.SCHEMA)).toUpperCase() + Sv.DOT
						+ ((String) baseType.getVal(Sv.TABLE_NAME)).toUpperCase();
				if (!dbtMap.containsKey(key))
					throw (new SvException("Bad {REPO}_TABLES configuration. Check the database.".replace("{REPO}",
							SvConf.getMasterRepo()), baseType));
			}
		}
	}

	/**
	 * Default SvCore initialisation. It doesn't use JSON configuration.
	 * 
	 * @return True if the system has been initialised properly.
	 * @throws SvException Re-throw underlying exceptions
	 */
	public static boolean initSvCore() throws SvException {
		return initSvCoreImpl(false);
	}

	/**
	 * Default SvCore initialisation. It doesn't use JSON configuration.
	 * 
	 * @param forceInit flag to allow the user to force Svarog to re-read the core
	 *                  config from the database, if Svarog is already initialised
	 * @throws SvException Re-throw underlying exceptions
	 */
	public static void initSvCore(boolean forceInit) throws SvException {
		if (forceInit)
			isInitialized.compareAndSet(true, false);
		initSvCoreImpl(false);
	}

	/**
	 * Method to set a shutdown hook to the JVM in order to perform cleanup and
	 * shutdown the osgi framework
	 */
	static void setShutDownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread("Svarog Shutdown Hook") {
			public void run() {
				try {
					log4j.info("Shutting down svarog");
					// Svarog shut down executing list of executors
					if (SvConf.isClusterEnabled()) {
						log4j.info("Shutting down the cluster infrastructure");
						SvCluster.shutdown(false);
						SvMaintenance.shutdown();
						SvCluster.resignCoordinator();
					}
					log4j.info("Shutting down the OSGI Framework");
					if (SvarogDaemon.osgiFramework != null) {
						SvarogDaemon.osgiFramework.stop();
						SvarogDaemon.osgiFramework.waitForStop(0);
					}
					log4j.info("Svarog shut down successfully");
				} catch (Exception ex) {
					System.err.println("Error stopping Svarog: " + ex);
				}
			}
		});
	}

	static void loadCoreObjects(SvCore svc) throws Exception {
		Connection conn = null;
		conn = svc.dbGetConn();
		// Svarog base objects which are permanently in memory.
		// If a new object type is added here make sure you configure it
		// in the DbInit.initCoreRecords

		// load field types from the DB
		DbQueryObject query = new DbQueryObject(getDbt(svCONST.OBJECT_TYPE_TABLE), null, null, null);
		// new DbQueryObject(repoDbt, repoDbtFields, getDbt(svCONST.OBJECT_TYPE_TABLE),
		// getFields(svCONST.OBJECT_TYPE_TABLE), null, null, null);

		query.setDbtFields(getDbFields(conn, svCONST.OBJECT_TYPE_TABLE));
		DbDataArray objectTypes = svc.getObjects(query, null, null);

		// load field types from the DB
		query = new DbQueryObject(repoDbt, repoDbtFields, getDbt(svCONST.OBJECT_TYPE_FIELD),
				getFields(svCONST.OBJECT_TYPE_FIELD), null, null, null);
		query.setDbtFields(getDbFields(conn, svCONST.OBJECT_TYPE_FIELD));
		DbDataArray fieldTypes = svc.getObjects(query, null, null);

		// load link types from the DB
		query = new DbQueryObject(repoDbt, repoDbtFields, getDbt(svCONST.OBJECT_TYPE_LINK_TYPE),
				getFields(svCONST.OBJECT_TYPE_LINK_TYPE), null, null, null);
		DbDataArray linkTypes = svc.getObjects(query, null, null);
		dbLinkTypes.setItems(linkTypes.getItems());
		// load form types from the DB
		query = new DbQueryObject(repoDbt, repoDbtFields, getDbt(svCONST.OBJECT_TYPE_FORM_TYPE),
				getFields(svCONST.OBJECT_TYPE_FORM_TYPE), null, null, null);
		query.setDbtFields(getDbFields(conn, svCONST.OBJECT_TYPE_FORM_TYPE));
		DbDataArray formTypes = svc.getObjects(query, null, null);

		// before purging the cache check that we at least have loaded
		// the config from database
		if (objectTypes == null || objectTypes.isEmpty() || fieldTypes == null || fieldTypes.isEmpty())
			throw (new SvException(
					"Bad {REPO}_TABLES configuration. Check the database.".replace("{REPO}", SvConf.getMasterRepo()),
					svCONST.systemUser));

		// Purge the initial config from the cache
		DbCache.clean();

		// initialise the system objects from the Database configuration
		initSysObjects(objectTypes, fieldTypes, linkTypes);

		prepareCache(objectTypes, linkTypes, formTypes);
	}

	/**
	 * Method to ensure all system objects are read-only and the base cache is
	 * populated so we execute database queries
	 * 
	 * @param objectTypes The list of object types defined in the system
	 * @param linkTypes   The list of link types defined in the system
	 * @param formTypes   The list of form types defined in the system
	 */
	static void prepareCache(DbDataArray objectTypes, DbDataArray linkTypes, DbDataArray formTypes) {
		for (DbDataObject dbo : linkTypes.getItems()) {
			DboFactory.makeDboReadOnly(dbo);
			String type = (String) dbo.getVal(Sv.Link.LINK_TYPE);
			String uqVals = type + "." + dbo.getVal(Sv.Link.LINK_OBJ_TYPE_1).toString() + "."
					+ dbo.getVal(Sv.Link.LINK_OBJ_TYPE_2).toString();
			DbCache.addObject(dbo, uqVals, true);
			if (type != null && (type.equals(Sv.POA) || type.equals(Sv.POA_OU)))
				poaDbLinkTypes.addDataItem(dbo);
		}

		for (DbDataObject dbo : formTypes.getItems()) {
			DboFactory.makeDboReadOnly(dbo);
			DbCache.addObject(dbo, null, true);
		}
		// init the default DbLink type
		DbQueryExpression.setDblt(DbCache.getObject(svCONST.OBJECT_TYPE_LINK, svCONST.OBJECT_TYPE_TABLE));
		repoDbt = DbCache.getObject(svCONST.OBJECT_TYPE_REPO, svCONST.OBJECT_TYPE_TABLE);
		repoDbtFields = DbCache.getObjectsByParentId(svCONST.OBJECT_TYPE_REPO, svCONST.OBJECT_TYPE_FIELD_SORT);
		for (DbDataObject currentDbt : objectTypes.getItems()) {
			try {
				SvObjectConstraints constr = new SvObjectConstraints(currentDbt);
				if (constr.getConstraints().size() > 0)
					uniqueConstraints.put(currentDbt.getObjectId(), constr);
			} catch (SvException ex) {
				log4j.warn("Constraints disabled on object: " + (String) currentDbt.getVal(Sv.TABLE_NAME));
				log4j.warn(ex.getFormattedMessage());
			}
		}
		if (SvWriter.queryCache != null)
			SvWriter.queryCache.clear();
	}

	/**
	 * Default method for initialising the system. It first uses the JSON config
	 * files for temporary initialisation, then loads the configuration from the
	 * database.
	 * 
	 * @param useJsonCfg flag to note if the system should be initialized from JSON
	 *                   ( {@link #initSvCoreLocal(DbDataArray,DbDataArray)} or from
	 *                   DbInit {@link #initSvCoreNoCfg(DbDataArray,DbDataArray)}
	 * @return True if the system has been initialised properly.
	 */
	static synchronized Boolean initSvCoreImpl(Boolean useJsonCfg) {
		// if Svarog is already initialised, simply return
		if (!isInitialized.compareAndSet(false, true))
			return true;

		log4j.info("Svarog initialization in progress");
		DbCache.clean();
		Boolean baseInitialisation = false;
		Boolean svarogState = false;

		DbDataArray coreObjects = new DbDataArray();
		DbDataArray coreCodes = new DbDataArray();

		if (useJsonCfg)
			baseInitialisation = initSvCoreLocal(coreObjects, coreCodes);
		else
			baseInitialisation = initSvCoreNoCfg(coreObjects, coreCodes);

		if (baseInitialisation) {
			try (SvCore svc = new SvReader()) {
				repoDbtMap.clear();
				repoDbtMap.put(SvConf.getMasterRepo(), repoDbt);
				if (!masterRepoExistsInDb(svc.dbGetConn()))
					return true;
				// we have the master repo in the database so lets load the core
				loadCoreObjects(svc);

				// calculate the session debounce interval
				DbDataObject sessionDbt = getDbt(svCONST.OBJECT_TYPE_SECURITY_LOG);
				// the cache expiry is in minutes, convert to milis 60*1000
				Long ttlMilis = (Long) sessionDbt.getVal("cache_expiry") * 60 * 1000;
				// set the debounce interval at 1% of the cache expiry
				sessionDebounceInterval = (new Double(ttlMilis * 0.01)).intValue();

				// if the daemon is not running, start the maintenance thread
				if (!SvCore.svDaemonRunning.get()) {
					// Set the proxy to process the notifications it self
					SvClusterNotifierProxy.processNotification = true;
					// set the client to rejoin on failed beat
					SvClusterClient.rejoinOnFailedHeartBeat = true;
					// start the maintenance
					SvMaintenance.initMaintenance();
				}

				isCfgInDb = true;
				svarogState = true;
			} catch (SvException ex) {
				log4j.fatal("Can't load basic configurations! Svarog not loaded!" + ex.getFormattedMessage(), ex);
			} catch (Exception ex) {
				log4j.fatal("Can't load basic configurations! Svarog not loaded!");
				log4j.debug("System loading error", ex);
				svarogState = false;
			}
		} else {
			log4j.fatal("Can't load basic configurations! System not loaded!");
			svarogState = false;
		}
		if (svarogState)
			log4j.info("Svarog initialization finished successfully");
		else
			log4j.error("Svarog initialization failed!");

		isValid.compareAndSet(false, svarogState);
		isInitialized.compareAndSet(true, svarogState);
		return svarogState;

	}

	/**
	 * Get the list of fields per object type limited to the ones existing in the DB
	 * only. This method would return different list of fields only during upgrade
	 * 
	 * @param conn         The JDBC connection which should be used for fetching
	 *                     list of fields in the table
	 * @param objectTypeId The ID of the object descriptor
	 * @return DbDataArray containing the list of fields
	 * @throws SvException Throws underlying exception from {@link #getDbt(Long)}
	 */
	public static DbDataArray getDbFields(Connection conn, Long objectTypeId) throws SvException {
		DbDataArray cachedFields = getFields(objectTypeId);
		DbDataArray finalFields = new DbDataArray();
		DbDataObject dbt = getDbt(objectTypeId);
		LinkedHashMap<String, DbDataField> dbFields = SvarogInstall.getFieldListFromDb(conn,
				Sv.V + (String) dbt.getVal(Sv.TABLE_NAME), (String) dbt.getVal(Sv.SCHEMA));
		@SuppressWarnings("unchecked")
		ArrayList<DbDataObject> lstFields = (ArrayList<DbDataObject>) ((ArrayList<DbDataObject>) cachedFields
				.getItems()).clone();

		Iterator<DbDataObject> it = lstFields.iterator();
		while (it.hasNext()) {
			DbDataObject fld = it.next();
			String fieldName = (String) fld.getVal(Sv.FIELD_NAME);
			if (!dbFields.containsKey(fieldName))
				it.remove();
		}
		finalFields.setItems(lstFields);
		return finalFields;
	}

	/**
	 * Method to return a DbDataObject containing user data according to a svarog
	 * session
	 * 
	 * @param session_id The session id for which we want to get the user
	 * @return A DbDataObject describing the logged on user associated with the
	 *         session
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public static DbDataObject getUserBySession(String session_id) throws SvException {
		DbDataObject svToken = DbCache.getObject(session_id, svCONST.OBJECT_TYPE_SECURITY_LOG);
		if (!SvCluster.isCoordinator() && SvCluster.getIsActive().get()) {
			// we are not coordinator so lets communucate our session
			// management with the coordinator
			if (svToken != null) {
				// we found a local session, and send refresh to coordinator
				if (((DateTime) svToken.getVal(Sv.LAST_REFRESH)).withDurationAdded(sessionDebounceInterval, 1)
						.isBeforeNow()) {
					if (SvClusterClient.refreshToken(session_id)) {
						DboUnderground.revertReadOnly(svToken, new SvReader());
						svToken.setVal(Sv.LAST_REFRESH, DateTime.now());
						svToken.setIsDirty(false);
						DboFactory.makeDboReadOnly(svToken);
						DbCache.addObject(svToken, session_id);
					}
				}
			} else // no local session was found get one from coordinator
			{
				svToken = SvClusterClient.getToken(session_id);
				if (svToken != null) {
					svToken.setVal(Sv.LAST_REFRESH, DateTime.now());
					svToken.setIsDirty(false);
					DboFactory.makeDboReadOnly(svToken);
					DbCache.addObject(svToken, session_id);
				}

			}
		}

		if (svToken == null)
			throw (new SvException(Sv.INVALID_SESSION, svCONST.systemUser));

		DbDataObject dbu = null;
		// if the last refresh + the interval is in the past, send a refresh in
		// the cluster
		try (SvSecurity svs = new SvSecurity()) {
			dbu = svs.getSid((Long) svToken.getVal("user_object_id"), svCONST.OBJECT_TYPE_USER);
		}

		if (dbu == null)
			throw (new SvException("system.error.no_user_found", svCONST.systemUser));
		return dbu;
	}

	/**
	 * Method to return all user groups associated with a specific user.
	 * 
	 * @param user              The user object for which we want to get the groups
	 * @param returnOnlyDefault Flag if we want to get the default user group or all
	 *                          groups
	 * @return The DbDataArray containing all linked groups.
	 * @throws SvException Re-throws any underlying exception
	 */
	static DbDataArray getUserGroups(DbDataObject user, boolean returnOnlyDefault) throws SvException {
		try (SvReader svr = new SvReader()) {
			svr.isInternal = true;
			return getUserGroups(user, returnOnlyDefault, svr);
		}
	}

	/**
	 * Method to return all user groups associated with a specific user.
	 * 
	 * @param user              The user object for which we want to get the groups
	 * @param returnOnlyDefault Flag if we want to get the default user group or all
	 *                          groups
	 * @param svc               The core instance to be used for executing the query
	 * @return The DbDataArray containing all linked groups.
	 * @throws SvException Re-throws any underlying exception
	 */
	static DbDataArray getUserGroups(DbDataObject user, boolean returnOnlyDefault, SvCore svc) throws SvException {
		DbDataArray groups = null;
		try (SvReader svr = new SvReader(svc)) {
			String userGroupLinkType = Sv.USER_DEFAULT_GROUP;
			DbDataObject dbLink = getLinkType(userGroupLinkType, svCONST.OBJECT_TYPE_USER, svCONST.OBJECT_TYPE_GROUP);

			svr.isInternal = true;
			groups = svr.getObjectsByLinkedId(user.getObjectId(), dbLink, null, 0, 0);
			if (!returnOnlyDefault) {
				userGroupLinkType = Sv.USER_GROUP;
				dbLink = getLinkType(userGroupLinkType, svCONST.OBJECT_TYPE_USER, svCONST.OBJECT_TYPE_GROUP);
				if (dbLink != null) {
					DbDataArray otherGroups = svr.getObjectsByLinkedId(user.getObjectId(), dbLink, null, 0, 0);
					if (groups != null && groups.getItems().size() > 0)
						otherGroups.addDataItem(groups.getItems().get(0));
					groups = otherGroups;
				}
			}
		}

		return groups;
	}

	/**
	 * Method to close a JDBC resource, while wrapping the exception in SvException
	 * 
	 * @param ps           The resource to be closed
	 * @param instanceUser If there is instance user linked to the resource
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public static void closeResource(AutoCloseable ps, DbDataObject instanceUser) throws SvException {
		try {
			if (ps != null)
				ps.close();
		} catch (Exception e) {
			throw (new SvException("system.error.jdbc_cant_realease", instanceUser, null, null, e));
		}
	}

	/**
	 * Method to open a database connection and associate it with the current SvCore
	 * instance This method explicitly opens a connection to the DB. You should let
	 * svarog decide if it needs a connection at all or everything is cached.
	 * 
	 * @throws SvException Any underlying exception is re-thrown
	 * @return JDBC Connection object
	 */
	public Connection dbGetConn() throws SvException {
		coreLastActivity = DateTime.now().getMillis();
		return SvConnTracker.getTrackedConnection(weakThis, weakSrcCore);
	}

	/**
	 * Wrapper method for Connection.setAutoCommit(autoCommit). The wrapper is
	 * needed to get the tracked JDBC connection associated with this specific
	 * instance. If the connection is shared between multiple SvCore instances it
	 * will of course affect those
	 * 
	 * @param autoCommit Boolean flag to enable/disable auto commit
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public void dbSetAutoCommit(Boolean autoCommit) throws SvException {
		try {
			Connection conn = this.dbGetConn();
			conn.setAutoCommit(autoCommit);
		} catch (SQLException ex) {
			throw (new SvException("system.error.sql_err", this.instanceUser, null, null, ex));
		}
	}

	/**
	 * Wrapper method for Connection.commit(). The wrapper is needed to get the
	 * tracked JDBC connection associated with this specific instance. If the
	 * connection is shared between multiple SvCore instances it will of course
	 * affect those instances too.
	 * 
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public void dbCommit() throws SvException {
		try {
			Connection conn = this.dbGetConn();
			if (!conn.getAutoCommit())
				conn.commit();
		} catch (SQLException ex) {
			throw (new SvException("system.error.sql_err", this.instanceUser, null, null, ex));
		}
	}

	/**
	 * Wrapper method for Connection.rollback(). The wrapper is needed to get the
	 * tracked JDBC connection associated with this specific instance. If the
	 * connection is shared between multiple SvCore instances it will of course
	 * affect those instances too.
	 * 
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public void dbRollback() throws SvException {
		try {
			Connection conn = this.dbGetConn();
			if (!conn.getAutoCommit())
				conn.rollback();
		} catch (SQLException ex) {
			throw (new SvException("system.error.sql_err", this.instanceUser, null, null, ex));
		}
	}

	/**
	 * Method to close a database connection associated with this SvCore instance.
	 * If the connection is shared between multiple SvCore instances, this method
	 * will not perform a real JDBC close on the connection, but rather decrease the
	 * usage count. Invoking DbClose on the last active SvCore instance will perform
	 * the actual closing of the connection. Before closing the connection, a
	 * rollback will be performed by default. If there was no {@link #dbCommit()}
	 * executed before and the connection was in AutoCommit=false mode, all data
	 * will be lost.
	 * 
	 * @param isManual Flag to note if the release was done manually, or from an
	 *                 enqueued soft reference
	 */
	void dbReleaseConn(Boolean isManual) {
		dbReleaseConn(isManual, false);
	}

	/**
	 * Method to close a database connection associated with this SvCore instance.
	 * If the connection is shared between multiple SvCore instances, this method
	 * will not perform a real JDBC close on the connection, but rather decrease the
	 * usage count. Invoking DbClose on the last active SvCore instance will perform
	 * the actual closing of the connection. Before closing the connection, a
	 * rollback will be performed by default. If there was no {@link #dbCommit()}
	 * executed before and the connection was in AutoCommit=false mode, all data
	 * will be lost.
	 * 
	 * @param isManual    Flag to note if the release was done manually, or from an
	 *                    enqueued soft reference
	 * @param hardRelease enables releasing all connections up in the SvCore chain
	 */
	private void dbReleaseConn(Boolean isManual, Boolean hardRelease) {
		SvConnTracker.releaseTrackedConnection(weakThis, isManual, hardRelease);
	}

	/**
	 * Method to close a database connection associated with this SvCore instance.
	 * If the connection is shared between multiple SvCore instances, this method
	 * will not perform a real JDBC close on the connection, but rather decrease the
	 * usage count. Invoking DbClose on the last active SvCore instance will perform
	 * the actual closing of the connection. Before closing the connection, a
	 * rollback will be performed by default. If there was no {@link #dbCommit()}
	 * executed before and the connection was in AutoCommit=false mode, all data
	 * will be lost.
	 */
	public void release() {
		release(false);
	}

	/**
	 * Overriden method of the Autocloseable interface to allow Svarog to be used in
	 * try-with-resources
	 */
	@Override
	public void close() {
		release();

	}

	/**
	 * Wrapper method for better legibility same as release(true);
	 */
	public void hardRelease() {
		release(true);
	}

	/**
	 * Method to close a database connection associated with this SvCore instance.
	 * If the connection is shared between multiple SvCore instances, this method
	 * will not perform a real JDBC close on the connection, but rather decrease the
	 * usage count. Invoking DbClose on the last active SvCore instance will perform
	 * the actual closing of the connection. Before closing the connection, a
	 * rollback will be performed by default. If there was no {@link #dbCommit()}
	 * executed before and the connection was in AutoCommit=false mode, all data
	 * will be lost.
	 * 
	 * @param hardRelease enables releasing all connections up in the SvCore chain
	 */
	public void release(Boolean hardRelease) {
		if (SvConnTracker.hasTrackedConnection(weakThis))
			dbReleaseConn(true, hardRelease);
		if (instanceCodeList != null)
			instanceCodeList.release();
	}

	/**
	 * Method to return the SvCore object which was used for initiating this
	 * instance for the purpose of sharing a single DB connection.
	 * 
	 * @return SvCore reference
	 */
	public SvCore getParentSvCore() {
		if (weakSrcCore != null)
			return weakSrcCore.get();
		else
			return null;
	}

	/**
	 * Method to switch the current user under which the SvCore instance runs. In
	 * order to switch the user you must have SYSTEM.SUDO acl in your ACL list. To
	 * return the instance to the previous user, use resetUser
	 * 
	 * @param userName The user name of the user
	 * @throws SvException If the switch failed, an exception is thrown
	 */
	public void switchUser(String userName) throws SvException {
		try (SvSecurity svs = new SvSecurity(this)) {
			DbDataObject user = svs.getUser(userName);
			switchUser(user);
		}

	}

	/**
	 * Method to switch the current user under which the SvCore instance runs. In
	 * order to switch the user you must have SYSTEM.SUDO acl in your ACL list. To
	 * return the instance to the previous user, use resetUser
	 * 
	 * @param user The object descriptor of the user we want to switch to
	 * @throws SvException If the switch failed, an exception is thrown
	 */
	public void switchUser(DbDataObject user) throws SvException {
		DbDataObject currentUser = null;
		if (isSystem()) {
			currentUser = svCONST.systemUser;
		} else if (isService()) {
			currentUser = svCONST.serviceUser;
		} else {
			currentUser = getUserBySession(coreSessionId);
		}
		if (currentUser != null && user != null) {
			if (isSystem() || isService() || hasPermission(svCONST.SUDO_ACL)) {
				instanceUser = user;
				previousUser = currentUser;
				if (user.getObjectId().equals(currentUser.getObjectId()))
					throw (new SvException("system.error.can_not_switch_to_same_user", instanceUser));

				// reset all lazy loaded user specific config
				instancePermissions = null;
				permissionKeys = null;
				saveAsUser = null;
				instanceUserDefaultGroup = null;
			} else
				throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, instanceUser));

		} else
			throw (new SvException("system.error.target_user_isnull", instanceUser));
	}

	/**
	 * Method to reset the SvCore instance back to the previous user under which it
	 * was running before {@link #switchUser(String)} was executed. In the linux
	 * world, this would equal to sudo su then exit.
	 * 
	 * @throws SvException If there was no previous user, to reset to throw
	 *                     exception
	 */
	public void resetUser() throws SvException {
		if (previousUser != null) {
			instanceUser = previousUser;
			previousUser = null;
		} else
			throw (new SvException("system.error.illegal_reset_state", instanceUser));

	}

	/**
	 * Method to return the UserGroup configured as default associated with the user
	 * under which this SvCore instance is running.
	 * 
	 * @return DbDataObject containing the UserGroup
	 * @throws SvException Re-throws any underlying exception
	 */
	public DbDataObject getDefaultUserGroup() throws SvException {
		if (instanceUserDefaultGroup == null && instanceUser.getObjectId() != svCONST.OBJECT_USER_SYSTEM) {
			DbDataArray groups = getUserGroups(instanceUser, true);
			if (groups != null && groups.getItems().size() > 0)
				instanceUserDefaultGroup = groups.getItems().get(0);

		}
		return instanceUserDefaultGroup;
	}

	/**
	 * Method to return all user groups associated with a specific user.
	 * 
	 * @param user              The user object for which we want to get the groups
	 * @param returnOnlyDefault Flag if we want to get the default user group or all
	 *                          groups
	 * @return The DbDataArray containing all linked groups.
	 * @throws SvException re-throw underlying exception
	 */
	public DbDataArray getAllUserGroups(DbDataObject user, boolean returnOnlyDefault) throws SvException {
		return SvCore.getUserGroups(user, returnOnlyDefault, this);
	}

	/**
	 * Method to return all user groups associated with current user associated with
	 * the instance
	 * 
	 * @return The DbDataArray containing all linked groups.
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public DbDataArray getUserGroups() throws SvException {
		return SvCore.getUserGroups(this.instanceUser, false);
	}

	/**
	 * Method to modify the underlying query to include power of attorney
	 * crosschecks
	 * 
	 * @param query
	 * @param dbc
	 * @return
	 * @throws SvException
	 */
	DbQueryExpression updatedQuery(DbQuery query, DbDataObject dbc) throws SvException {
		DbDataObject dboUG = getDefaultUserGroup();
		DbDataObject dbt = ((DbQueryObject) query).getDbt();
		Long objectType = dbt.getObjectId();
		if (Sv.POA.equals(dboUG.getVal(Sv.GROUP_SECURITY_TYPE))
				&& ((Long) dbc.getVal(Sv.Link.LINK_OBJ_TYPE_1)).equals(svCONST.OBJECT_TYPE_USER)
				&& objectType.equals((Long) dbc.getVal(Sv.Link.LINK_OBJ_TYPE_2))) {
			DbDataObject usersDbt = getDbt(svCONST.OBJECT_TYPE_USER);
			DbQueryExpression dqe = new DbQueryExpression();
			DbQueryObject dqo = new DbQueryObject(usersDbt,
					new DbSearchCriterion(Sv.OBJECT_ID, DbCompareOperand.EQUAL, instanceUser.getObjectId()),
					DbJoinType.INNER, dbc, LinkType.DBLINK, null, null);
			dqe.addItem(dqo);
			dqe.addItem((DbQueryObject) query);
			((DbQueryObject) query).setIsReturnType(true);
			return dqe;
		} else if (Sv.POA_OU.equals(dboUG.getVal(Sv.GROUP_SECURITY_TYPE))
				&& ((Long) dbc.getVal(Sv.Link.LINK_OBJ_TYPE_1)).equals(svCONST.OBJECT_TYPE_ORG_UNITS)
				&& objectType.equals((Long) dbc.getVal(Sv.Link.LINK_OBJ_TYPE_2))) {

			DbSearchExpression dbs = new DbSearchExpression();
			try (SvReader svr = new SvReader(this)) {
				svr.isInternal = true;
				String uqVals = Sv.POA + "." + Long.toString(svCONST.OBJECT_TYPE_USER) + "."
						+ Long.toString(svCONST.OBJECT_TYPE_ORG_UNITS);
				DbDataObject linkUser2OU = DbCache.getObject(uqVals, svCONST.OBJECT_TYPE_LINK_TYPE);
				DbDataArray linkedOU = svr.getObjectsByLinkedId(instanceUser.getObjectId(), linkUser2OU, null, null,
						null);
				for (DbDataObject ou : linkedOU.getItems()) {
					dbs.addDbSearchItem(new DbSearchCriterion(Sv.OBJECT_ID, DbCompareOperand.EQUAL, ou.getObjectId(),
							DbSearch.DbLogicOperand.OR));
				}
			}

			DbDataObject usersDbt = getDbt(svCONST.OBJECT_TYPE_ORG_UNITS);
			DbQueryExpression dqe = new DbQueryExpression();
			DbQueryObject dqo = new DbQueryObject(usersDbt, dbs, DbJoinType.INNER, dbc, LinkType.DBLINK, null, null);
			dqe.addItem(dqo);
			dqe.addItem((DbQueryObject) query);
			((DbQueryObject) query).setIsReturnType(true);
			return dqe;

		}
		return null;
	}

	boolean isLinkTypeCompatible(DbDataObject userGroup, DbDataObject linkType, DbDataObject dbt) {

		String groupSecurityType = (String) userGroup.getVal(Sv.GROUP_SECURITY_TYPE);
		boolean isCompatible = false;
		isCompatible = ((Long) linkType.getVal(Sv.Link.LINK_OBJ_TYPE_1)).equals(svCONST.OBJECT_TYPE_USER)
				&& dbt.getObjectId().equals((Long) linkType.getVal(Sv.Link.LINK_OBJ_TYPE_2));

		if (Sv.POA.equals(groupSecurityType) && dbt.getObjectId().equals(svCONST.OBJECT_TYPE_ORG_UNITS))
			isCompatible = false;
		return isCompatible;
	}

	/**
	 * Method for checking if the current user has power of attorney over the
	 * dataset he is fetching. If the user group security type is Power of Attorney
	 * (POA) then the query is modified to contain the empowerment
	 * 
	 * @param query The original query to be executed subject of modification
	 * @return The new DbQuery object which is based on the original query including
	 *         the POA empowerment
	 * @throws SvException Any underlying exception is re-thrown
	 */
	private DbQuery addEmpoweredCriteria(DbQuery query) throws SvException {
		// if we are running the query internally from the svarog package itself
		// avoid the security checks
		if (isInternal)
			return query;
		DbDataObject dboUG = getDefaultUserGroup();
		DbQuery retVal = query;
		if (dboUG != null && (Sv.POA.equals(dboUG.getVal(Sv.GROUP_SECURITY_TYPE))
				|| Sv.POA_OU.equals(dboUG.getVal(Sv.GROUP_SECURITY_TYPE)))) {
			if (query instanceof DbQueryObject) {
				for (DbDataObject dblPoa : poaDbLinkTypes.getItems()) {
					DbQuery updated = updatedQuery(query, dblPoa);
					if (updated != null) {
						retVal = updated;
						break;
					}
				}
			} else if (query instanceof DbQueryExpression && !((DbQueryExpression) query).getIsReverseExpression()) {
				DbDataObject dbt = ((DbQueryExpression) query).getItems().get(0).getDbt();
				for (DbDataObject dbc : poaDbLinkTypes.getItems()) {
					if (isLinkTypeCompatible(dboUG, dbc, dbt)) {
						DbDataObject usersDbt = getDbt(svCONST.OBJECT_TYPE_USER);
						DbQueryExpression dqe = new DbQueryExpression();
						DbQueryObject dqo = new DbQueryObject(usersDbt,
								new DbSearchCriterion(Sv.OBJECT_ID, DbCompareOperand.EQUAL, instanceUser.getObjectId()),
								DbJoinType.INNER, dbc, LinkType.DBLINK, null, null);
						dqo.setIsReturnType(false);
						dqo.setSqlTablePrefix("POA_3129USERNAME");
						dqe.addItem(dqo);
						for (DbQueryObject dqotmp : ((DbQueryExpression) query).getItems()) {
							// dqotmp.setIsReturnType(true);
							dqe.addItem(dqotmp);
						}
						if (query.getReturnType() != null)
							dqe.setReturnType(query.getReturnType());
						retVal = dqe;
					}
				}

			}
		}
		return retVal;
	}

	void bindQueryVals(PreparedStatement ps, ArrayList<Object> bindVals) throws SQLException {
		Integer paramIdx = 1;
		for (Object obj : bindVals) {
			if (obj != null && obj.getClass().equals(DateTime.class))
				obj = new Timestamp(((DateTime) obj).getMillis());
			if (log4j.isDebugEnabled())
				log4j.trace("Bind Variable:" + paramIdx.toString() + ", value:"
						+ (obj != null ? obj.toString() : Sv.SQL.NULL));
			ps.setObject(paramIdx, obj);
			paramIdx++;
		}
	}

	/**
	 * A method that creates PreparedStatement and returns a ResultSet object for
	 * the specific search
	 * 
	 * @param query    The DbQuery object from which the SQL shall be generated and
	 *                 a prepared statement created
	 * @param conn     DB Connection which should be used
	 * @param rowLimit The maximum number of rows/objects to be returned by the
	 *                 query
	 * @param offset   The offset from which query will start returning objects.
	 * @return A ResultSet containing the results from the DB
	 * @throws SvException Any underlying exception is re-thrown
	 */
	private StringBuilder getSQLStatement(DbQuery query, Integer rowLimit, Integer offset) throws SvException {
		StringBuilder sqlQry;
		sqlQry = query.getSQLExpression(false, includeGeometries);
		if (rowLimit != null && offset != null && (rowLimit > 0 || offset > 0)) {
			String sRowLimit = SvConf.getSqlkw().getString(Sv.LIMIT_OFFSET).replace(Sv.OFFSET, offset.toString())
					.replace(Sv.LIMIT, rowLimit.toString());
			int orderPos = sqlQry.indexOf(Sv.ORDER_BY);
			if (SvConf.getDbType().equals(SvDbType.ORACLE)) {
				if (orderPos > 0)
					sqlQry.insert(orderPos, Sv.SPACE + Sv.AND + Sv.SPACE + sRowLimit + Sv.SPACE + Sv.AND + Sv.SPACE);
				else
					sqlQry.append(Sv.SPACE + Sv.AND + Sv.SPACE + sRowLimit);
			} else
				sqlQry.append(Sv.SPACE + sRowLimit);
		}
		if (log4j.isDebugEnabled())
			log4j.trace("Generating SQL: " + sqlQry);
		return sqlQry;
	}

	/**
	 * Method to check if the current user has assigned the permission/ACL uniquely
	 * identified by permissionKey
	 * 
	 * @param permissionKey The unique ID of the permission
	 * @return True if the user has the permission granted
	 */
	public boolean hasPermission(String permissionKey) {
		// if we are system user just cut the crap
		if (isSystem() || isService())
			return true;

		boolean result = false;
		try {
			// if the default user group is ADMINISTRATORS, then grant the
			// permission
			if (isAdmin()) {
				result = true;
			} else {
				if (getPermissionsByKey() != null && getPermissionsByKey().containsKey(permissionKey))
					result = true;
			}
		} catch (SvException e) {
			log4j.warn("Exception occured while fetching permissions", e);
		}
		return result;
	}

	/**
	 * Method to generate an ACL key from an ACL object type
	 * 
	 * @param dboAcl DbDataObject instance of ACL type
	 * @return SvAclKey object generated from the dbo
	 */
	private SvAclKey getAclKey(DbDataObject dboAcl) {
		if (dboAcl.getObjectType().equals(svCONST.OBJECT_TYPE_ACL))
			return new SvAclKey(dboAcl);
		else
			return null;
	}

	/**
	 * Method to return a map all access control list (ACL) objects by key based on
	 * the label_code of the ACL.
	 * 
	 * @return Key/Value map of ACLs mapped by LABEL_CODE
	 */
	public HashMap<String, DbDataObject> getPermissionsByKey() throws SvException {
		if (permissionKeys == null) {
			permissionKeys = new HashMap<String, DbDataObject>();
			for (HashMap<String, DbDataObject> pMap : getPermissions().values())
				for (DbDataObject dboAcl : pMap.values())
					permissionKeys.put((String) dboAcl.getVal(Sv.LABEL_CODE), dboAcl);
		}
		return permissionKeys;
	}

	/**
	 * Method to fetch all ACL or SID_ACL objects or linked to a specific SID
	 * 
	 * @param sid        The Security identifier for which we want to get the ACLs
	 *                   (User or User Group)
	 * @param core       The SvCore instance which should guarantee that we have
	 *                   privilege to read the permissions of the sid
	 * @param returnType DbDataObject descriptor of the type to be returned. The
	 *                   return types can be of type ACL or SID_ACL.
	 * @return A DbDataArray instance holding all permissions linked to the specific
	 *         SID
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public DbDataArray getSecurityObjects(DbDataObject sid, SvCore core, DbDataObject returnType) throws SvException {
		DbDataArray permissions = null;
		DbDataArray groups = null;
		try (SvReader svr = new SvReader(core)) {

			if (sid.getObjectType().equals(svCONST.OBJECT_TYPE_USER))
				groups = SvCore.getUserGroups(sid, false);

			DbQueryObject dqoAcl = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_ACL), null, null,
					DbJoinType.INNER);

			// START creation of the search expressions
			// create the SID search expression
			DbSearchExpression dbxSid = new DbSearchExpression()
					.addDbSearchItem(new DbSearchCriterion(Sv.SID_OBJECT_ID, DbCompareOperand.EQUAL, sid.getObjectId()))
					.addDbSearchItem(
							new DbSearchCriterion(Sv.SID_TYPE_ID, DbCompareOperand.EQUAL, sid.getObjectType()));
			dbxSid.setNextCritOperand(Sv.OR);

			DbSearchExpression dbx = new DbSearchExpression().addDbSearchItem(dbxSid);

			// if the SID type was user, than we will also have the user groups.
			if (groups != null) {
				// create the groups search expression
				DbSearchExpression dbxGroupsList = new DbSearchExpression();
				for (DbDataObject group : groups.getItems()) {
					dbxGroupsList.addDbSearchItem(new DbSearchCriterion(Sv.SID_OBJECT_ID, DbCompareOperand.EQUAL,
							group.getObjectId(), DbLogicOperand.OR));
				}

				DbSearchExpression dbxGroups = new DbSearchExpression().addDbSearchItem(
						new DbSearchCriterion(Sv.SID_TYPE_ID, DbCompareOperand.EQUAL, svCONST.OBJECT_TYPE_GROUP))
						.addDbSearchItem(dbxGroupsList);

				dbx.addDbSearchItem(dbxGroups);
			}
			// END creation of the search expressions

			DbQueryObject dqoSID = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_SID_ACL), dbx, null,
					DbJoinType.INNER);

			dqoSID.setSqlTablePrefix("SID");
			dqoAcl.setSqlTablePrefix("ACL");

			DbQueryExpression dqe = null;

			if (returnType != null) {
				if (returnType.getObjectId().equals(dqoSID.getDbt().getObjectId())) {
					dqoSID.setIsReturnType(true);
					dqoSID.addChild(dqoAcl);
					dqoAcl.setLinkToNextType(LinkType.DENORMALIZED_REVERSE);
					dqoSID.setDenormalizedFieldName(Sv.ACL_OBJECT_ID);
					dqe = new DbQueryExpression(dqoSID);
				} else {
					dqoSID.setLinkToNextType(LinkType.DENORMALIZED);
					dqoSID.setDenormalizedFieldName(Sv.ACL_OBJECT_ID);
					dqoAcl.setIsReturnType(true);
					dqoAcl.addChild(dqoSID);
					dqe = new DbQueryExpression(dqoAcl);
				}
			}

			permissions = svr.getObjects(dqe, null, null);
			for (DbDataObject dbop : permissions.getItems()) {
				SvAccess acType = SvAccess.NONE;
				String act = (String) dbop.getVal(Sv.ACCESS_TYPE);
				if (act != null)
					acType = SvAccess.valueOf(act);

				dbop.setVal(Sv.ACCESS_TYPE, acType);
				dbop.setIsDirty(false);
			}
		}
		return permissions;
	}

	/**
	 * Method to fetch all ACL objects linked to a specific SID
	 * 
	 * @param sid  The Security identifier for which we want to get the ACLs (User
	 *             or User Group)
	 * @param core The SvCore instance which should guarantee that we have privilege
	 *             to read the permissions of the sid
	 * @return A DbDataArray instance holding all permissions linked to the specific
	 *         SID
	 * @throws SvException Any underlying exception is re-thrown
	 */
	public DbDataArray getPermissions(DbDataObject sid, SvCore core) throws SvException {
		return getSecurityObjects(sid, core, SvCore.getDbt(svCONST.OBJECT_TYPE_ACL));
	}

	/**
	 * Method to get a {@link DbDataArray} holding all permissions (ACLs) for the
	 * current user
	 * 
	 * @return Reference to the {@link DbDataArray} holding the permissions. Null if
	 *         the user is system
	 * @throws SvException Any underlying exception is re-thrown
	 */
	@SuppressWarnings("unchecked")
	public HashMap<SvAclKey, HashMap<String, DbDataObject>> getPermissions() throws SvException {
		HashMap<SvAclKey, HashMap<String, DbDataObject>> tmpPermissions = getPermissionsImpl();
		HashMap<SvAclKey, HashMap<String, DbDataObject>> permissions = null;
		if (tmpPermissions != null) {
			permissions = new HashMap<SvAclKey, HashMap<String, DbDataObject>>(tmpPermissions.size());
			for (Entry<SvAclKey, HashMap<String, DbDataObject>> objPermission : tmpPermissions.entrySet()) {
				permissions.put(objPermission.getKey(),
						(HashMap<String, DbDataObject>) objPermission.getValue().clone());
			}
		}
		return permissions;
	}

	/**
	 * Method to get a {@link DbDataArray} holding all permissions (ACLs) for the
	 * current user
	 * 
	 * @return Reference to the {@link DbDataArray} holding the permissions. Null if
	 *         the user is system
	 * @throws SvException Any underlying exception is re-thrown
	 */
	HashMap<SvAclKey, HashMap<String, DbDataObject>> getPermissionsImpl() throws SvException {
		// system and service users have no permissions, they are gods
		if (isSystem() || isService())
			return null;

		String lockKey = (String) instanceUser.getVal(Sv.USER_NAME) + "-ACL";
		ReentrantLock lock = null;
		SvReader svr = null;
		// if the lazy loaded instance permissions is null, then load
		if (instancePermissions == null) {
			try {
				// instantiate a system level SvReader
				svr = new SvReader();
				// lock the user key
				lock = SvLock.getLock(lockKey, true, SvConf.getMaxLockTimeout());
				if (lock != null) {
					if (instancePermissions == null) {
						instancePermissions = new HashMap<SvAclKey, HashMap<String, DbDataObject>>();
						// make sure to check first if we have already cached a
						// list of permissions for this specific user in the
						// svarog system cache
						DbDataArray permissions = DbCache.getObjectsByParentId(instanceUser.getObjectId(),
								svCONST.OBJECT_TYPE_ACL);

						if (permissions == null) {
							// if there is nothing cached, make sure to get all
							// permissions from the db
							permissions = getPermissions(this.instanceUser, svr);
							// after fetching the objects from the DB, cache
							// them please
							DbCache.addArrayByParentId(permissions, svCONST.OBJECT_TYPE_ACL, instanceUser.getObjectId(),
									false);
						}
						// now process all access control objects for faster
						// indexing using aclkeys
						for (DbDataObject dboAcl : permissions.getItems()) {
							SvAclKey key = getAclKey(dboAcl);
							HashMap<String, DbDataObject> pMap = instancePermissions.get(key);
							if (pMap != null) {
								DbDataObject dbo = pMap.get((String) dboAcl.getVal(Sv.ACL_CONFIG_UNQ));
								if (dbo == null || ((SvAccess) dbo.getVal(Sv.ACCESS_TYPE))
										.compareTo((SvAccess) dboAcl.getVal(Sv.ACCESS_TYPE)) < 0)
									pMap.put((String) dboAcl.getVal(Sv.ACL_CONFIG_UNQ), dboAcl);
							} else {
								pMap = new HashMap<String, DbDataObject>();
								// make sure the ACL is read-only so we
								DboFactory.makeDboReadOnly(dboAcl);
								pMap.put((String) dboAcl.getVal(Sv.ACL_CONFIG_UNQ), dboAcl);
								instancePermissions.put(key, pMap);
							}
						}
					}

				}

			} finally {
				if (lock != null)
					SvLock.releaseLock(lockKey, lock);
				if (svr != null)
					svr.release();
			}
		}
		return instancePermissions;
	}

	/**
	 * Method returning the locale object for a specific svarog session
	 * 
	 * @param sessionToken the svarog session from which we will get the user object
	 * @return the locale object descriptor attached to the user object
	 * @throws SvException any underlying SvException
	 */
	public DbDataObject getSessionLocale(String sessionToken) throws SvException {
		DbDataObject user = getUserBySession(sessionToken);
		return getUserLocale(user);
	}

	/**
	 * Method returning the locale object for a specific user
	 * 
	 * @param userObject the user description for which we want to get the locale
	 * @return the locale object descriptor attached to the user object
	 * @throws SvException any underlying SvException
	 */
	public String getUserLocaleId(DbDataObject userObject) throws SvException {
		String locale = null;
		if (userObject != null) {
			try (SvParameter svp = new SvParameter()) {
				if (userObject.getVal(Sv.LOCALE) == null) {
					locale = svp.getParamString(userObject, Sv.LOCALE);
					if (locale == null)
						locale = SvConf.getDefaultLocale();

					userObject.setVal(Sv.LOCALE, locale);
					userObject.setIsDirty(false);
				} else
					locale = (String) userObject.getVal(Sv.LOCALE);

			}
		}
		return locale;
	}

	/**
	 * Method returning the locale object for a specific user
	 * 
	 * @param userObject the user description for which we want to get the locale
	 * @return the locale object descriptor attached to the user object
	 * @throws SvException any underlying SvException
	 */
	public DbDataObject getUserLocale(DbDataObject userObject) throws SvException {
		DbDataObject localeObj = null;
		localeObj = SvarogInstall.getLocaleList().getItemByIdx(getUserLocaleId(userObject));
		return localeObj;
	}

	/**
	 * Method returning the locale object for a specific user
	 * 
	 * @param userId the user name of the user for which we want to get the locale
	 * @return the locale object descriptor attached to the user object
	 * @throws SvException any underlying SvException
	 */
	public DbDataObject getUserLocale(String userId) throws SvException {
		DbDataObject localeObj = null;
		try (SvSecurity svc = new SvSecurity()) {
			DbDataObject user = svc.getUser(userId);
			localeObj = getUserLocale(user);
		}
		return localeObj;
	}

	/**
	 * Method to set a locale to a specific User in the system
	 * 
	 * @param userName The user name of the of the user
	 * @param locale   The local which exists in the list of system locales
	 * @throws SvException Any underlying exception
	 */
	public void setUserLocale(String userName, String locale) throws SvException {

		DbDataObject localeObj = null;
		try (SvParameter svp = new SvParameter(); SvSecurity svc = new SvSecurity()) {
			DbDataObject userObject = svc.getUser(userName);
			localeObj = SvarogInstall.getLocaleList().getItemByIdx(locale);

			if (localeObj != null) {
				svp.setParamImpl(userObject, Sv.LOCALE, locale, true, true);
			}
		}
	}

	/**
	 * Method to create empty DbDataObject with fields map according to the type
	 * descriptor
	 * 
	 * @param dbt The type descriptor of the object
	 * @return A DbDataObject instance configured according to the type descriptor
	 */
	public DbDataObject createDboByType(DbDataObject dbt) {
		DbDataObject dbo = null;
		LinkedHashMap<SvCharId, Object> emptyMap = emptyKeyMap.get(dbt.getObjectId());
		if (emptyMap == null) {
			DbDataArray dbfs = DbCache.getObjectsByParentId(dbt.getObjectId(), svCONST.OBJECT_TYPE_FIELD_SORT);
			emptyMap = new LinkedHashMap<SvCharId, Object>(dbfs.size(), 1);
			LinkedHashMap<SvCharId, Object> keyMap = new LinkedHashMap<SvCharId, Object>(dbfs.size(), 1);

			for (DbDataObject dbf : dbfs.getSortedItems(Sv.SORT_ORDER)) {
				emptyMap.put(new SvCharId((String) dbf.getVal(Sv.FIELD_NAME)), null);
				if (!Sv.PKID.equals((String) dbf.getVal(Sv.FIELD_NAME)))
					keyMap.put(new SvCharId((String) dbf.getVal(Sv.FIELD_NAME)), dbf);
				else
					keyMap.put(new SvCharId((String) dbf.getVal(Sv.FIELD_NAME)), null);
			}
			dbtKeyMap.put(dbt.getObjectId(), keyMap);
			emptyKeyMap.put(dbt.getObjectId(), emptyMap);
		}
		dbo = new DbDataObject(dbt.getObjectId(), emptyMap);
		return dbo;

	}

	void recordPostProcess(SvCharId fieldName, Object fieldVal, DbDataObject dbo, DbDataObject dbf) throws SvException {

		// deserialize multi
		if (dbf != null) {
			if (dbf.getVal(Sv.SV_MULTISELECT) != null && (Boolean) dbf.getVal(Sv.SV_MULTISELECT)) {
				String[] multivals = ((String) fieldVal).split(SvConf.getMultiSelectSeparator());
				fieldVal = new ArrayList<String>(Arrays.asList(multivals));
				dbo.setVal(fieldName, fieldVal);
			}
			if (dbf.getVal(Sv.SV_ISLABEL) != null && (Boolean) dbf.getVal(Sv.SV_ISLABEL)
					&& dbf.getVal(Sv.SV_LOADLABEL) != null && (Boolean) dbf.getVal(Sv.SV_LOADLABEL)) {

				fieldVal = I18n.getText(getUserLocaleId(instanceUser), (String) fieldVal);
				dbo.setVal(fieldName, fieldVal);
			}

		}
		//
	}

	/**
	 * A method for converting a row from the resultset into a DbDataObject
	 * 
	 * @param rs        A ResultSet object which contains the data
	 * @param tblPrefix String prefix of the metadata fields. MUST BE UPPERCASED!!!
	 * @param query     the Svarog DbQuery object which was used for execution
	 * @param rsmt      The metadata of the resultset
	 * 
	 * @return DbDataObject containing the data.
	 * @throws SQLException   Underlying exceptions from the JDBC structures
	 * @throws ParseException Exception from conversion of datatypes between DB/Java
	 * @throws SvException    Re-throws any underlying exception
	 */
	private DbDataObject getObjectFromRecord(ResultSet rs, String tblPrefix, DbQuery query, ResultSetMetaData rsmt)
			throws SQLException, ParseException, SvException {
		DbDataObject object = null;
		Long typeId = query.getReturnType() != null ? query.getReturnType().getObjectId() : 0L;
		boolean isExpression = query instanceof DbQueryExpression;
		boolean isReverse = isExpression ? ((DbQueryExpression) query).getIsReverseExpression() : false;
		int numberOfColumns = rs.getMetaData().getColumnCount();
		if (!typeId.equals(0L)) {
			object = createDboByType(query.getReturnType());
		} else
			object = new DbDataObject();

		String fieldName = "";
		if (query.getReturnType() != null && !isReverse) {
			tblPrefix = tblPrefix + query.getReturnTypeSequence();
		}
		// end of part which should be moved one up
		String fieldType = "";
		HashMap<SvCharId, Object> fields = dbtKeyMap.get(typeId);

		for (int colIndex = 1; colIndex <= numberOfColumns; colIndex++) {
			fieldName = rsmt.getColumnName(colIndex).toUpperCase();
			if (query.getReturnType() != null) {
				fieldName = fieldName.substring(tblPrefix.length() + 1);
				// if the column is not a repo field, process it
				if (svCONST.repoFieldNames.indexOf(fieldName) < 0) {
					SvCharId fieldId = new SvCharId(fieldName);
					DbDataObject dbf = (DbDataObject) fields.get(fieldId);
					fieldType = (dbf != null ? (String) dbf.getVal(Sv.FIELD_TYPE) : null);
					Object fieldVal = getObjectFromCol(rs, fieldType, colIndex, rsmt);
					object.setVal(fieldId, fieldVal);
					if (fieldVal instanceof Geometry) {
						((Geometry) fieldVal).setUserData(object);
					}
					recordPostProcess(fieldId, fieldVal, object, dbf);
				}
			} else {
				Object obj = getObjectFromCol(rs, null, colIndex, rsmt);
				fieldName = fieldName.replace(tblPrefix + "_", "").toUpperCase();
				object.setVal(fieldName, obj);
			}
		}
		if (query.getReturnType() != null && (!isExpression || (isExpression && query.getReturnTypes().size() == 1))) {
			setObjectRepoData(object, rs, tblPrefix);
			object.setObjectType(query.getReturnType().getObjectId());
			if (hasGeometries(query.getReturnType().getObjectId()))
				DboFactory.dboIsGeometryType(object);
			if (includeGeometries)
				DboFactory.dboHasGeometry(object);
		}
		object.setIsDirty(false);
		return object;
	}

	/**
	 * Method to populate basic object data from a resultset
	 * 
	 * @param object    The DbDataObject instance which will be initialised from the
	 *                  resultset
	 * @param rs        The JDBC resultset which will be used for initialising the
	 *                  object metadata
	 * @param colPrefix The column prefix used in the query
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	private void setObjectRepoData(DbDataObject object, ResultSet rs, String colPrefix) throws SQLException {
		object.setPkid(rs.getLong(colPrefix + "_PKID"));
		object.setObjectId(rs.getLong(colPrefix + "_OBJECT_ID"));
		object.setDtInsert(new DateTime(rs.getTimestamp(colPrefix + "_DT_INSERT")));
		object.setDtDelete(new DateTime(rs.getTimestamp(colPrefix + "_DT_DELETE")));
		object.setParentId(rs.getLong(colPrefix + "_PARENT_ID"));
		object.setObjectType(rs.getLong(colPrefix + "_OBJECT_TYPE"));
		object.setStatus(rs.getString(colPrefix + "_STATUS"));
		object.setUserId(rs.getLong(colPrefix + "_USER_ID"));
	}

	/**
	 * Fetch a value from a resultset col into a Java Object, thus maintaining basic
	 * svarog types
	 * 
	 * @param rs        The JDBC result set reference from which the data should be
	 *                  fetched
	 * @param fieldType The type of the field
	 * @param colIndex  The index of the field in the resultset
	 * @return A java object of the specific type (string, number, geometry,
	 *         boolean)
	 * @throws SQLException   Any of the vcalls to the ResultSet methods raised an
	 *                        exception
	 * @throws ParseException Is thrown if the WKB Reader failed to parse the WKB
	 *                        geometry format
	 * @throws SvException    If the getDbHandler thrown exception
	 */

	private Object getObjectFromCol(ResultSet rs, String fieldType, int colIndex, ResultSetMetaData rsmt)
			throws SQLException, ParseException, SvException {

		Object obj = null;

		if (fieldType != null) {
			switch (fieldType) {
			case Sv.TEXT:
				obj = rs.getString(colIndex);
				break;
			case Sv.GEOMETRY:
				if (includeGeometries) {
					byte[] geom = SvConf.getDbHandler().getGeometry(rs, colIndex);
					if (geom != null) {
						obj = getWKBReader().read(geom);
					}
				}
				break;
			case Sv.BOOLEAN:
				// if the db type is oracle of course deal with booleans.
				// now you might wonder why false is 0 and true is 1
				if (SvConf.getDbType().equals(SvDbType.ORACLE)) {
					obj = rs.getObject(colIndex);
					if (obj != null)
						obj = new Boolean(((String) obj).equals(Sv.ZERO) ? false : true);
				}
			}
		}
		switch (rsmt.getColumnType(colIndex)) {
		case java.sql.Types.TIMESTAMP:
			obj = rs.getTimestamp(colIndex);
			if (obj != null)
				obj = new DateTime(obj);
			break;
		case java.sql.Types.NUMERIC:
			obj = rs.getBigDecimal(colIndex);
			if (obj != null && rsmt.getScale(colIndex) == 0)
				obj = ((BigDecimal) obj).longValue();
			break;
		}

		if (obj == null)
			obj = rs.getObject(colIndex);

		return obj;
	}

	/**
	 * To be removed in Svarog v2.0. This is deprecated version of the root getter
	 * method. This method is responsible for rendering the DbQuery object to a
	 * plain SQL and then running the statement against the DB to get the record
	 * set. The record set is then translated into a DbDataArray object, containing
	 * the resulting data.
	 * 
	 * @param query     {@link DbQueryObject} to be executed against the underlying
	 *                  DB
	 * @param errorCode standard svCONST error code
	 * @param rowLimit  maximum number of objects to be returned
	 * @param offset    offset from which the objects should be returned
	 * @return A {@link DbDataArray} object containing all returned data in
	 *         DbDataObject format
	 */
	@Deprecated
	DbDataArray getObjects(DbQuery query, long[] errorCode, Integer rowLimit, Integer offset) {
		DbDataArray result = null;
		try {
			result = getObjects(query, rowLimit, offset);
			errorCode[0] = svCONST.SUCCESS;
		} catch (SvException ex) {
			log4j.error(ex.getFormattedMessage(), ex);
			errorCode[0] = svCONST.GENERAL_ERROR;
		}
		return result;
	}

	/**
	 * Method to check if the current SvCore instance has access rights over a
	 * specific object type descriptor.
	 * 
	 * @param dbt         The object type descriptor
	 * @param
	 * @param accessLevel The required access level
	 * @return True if the instance has the required permissions
	 * @throws SvException Throw any underlying exception
	 */
	protected boolean hasDbtAccess(DbDataObject dbt, String unqConfigId, SvAccess accessLevel) throws SvException {
		boolean hasAccess = false;
		if (!isSystem() && !isService()) {
			SvAclKey aclKey = new SvAclKey(dbt);
			HashMap<String, DbDataObject> aclMap = getPermissions().get(aclKey);
			if (aclMap != null) {
				DbDataObject acl = aclMap.get(unqConfigId);
				if (acl != null) {
					SvAccess accessType = (SvAccess) acl.getVal(Sv.ACCESS_TYPE);
					hasAccess = accessType.getAccessLevelValue() >= accessLevel.getAccessLevelValue();
				}
			}
		} else
			hasAccess = true;
		return hasAccess;
	}

	protected boolean authoriseDqoByConfig(DbQueryObject dqo, HashMap<String, DbDataObject> aclMap,
			SvAccess accessLevel) {
		boolean hasAccess = false;
		// create expression to hold the config criteria
		DbSearchExpression innerDbx = new DbSearchExpression();
		innerDbx.setNextCritOperand(Sv.AND);
		// get the config field name to filter by
		String cfgFieldName = (String) dqo.getDbt().getVal(Sv.CONFIG_UNQ_ID);

		for (Entry<String, DbDataObject> permItem : aclMap.entrySet()) {
			// if the key of the permission is null, than its the global table
			// key, so we ignore it
			if (permItem.getKey() != null && permItem.getValue() != null) {
				DbDataObject acl = permItem.getValue();
				SvAccess accessType = (SvAccess) acl.getVal(Sv.ACCESS_TYPE);
				if (accessType.getAccessLevelValue() >= accessLevel.getAccessLevelValue()) {
					hasAccess = true;
					try {
						DbSearchCriterion crit = new DbSearchCriterion(cfgFieldName, DbCompareOperand.EQUAL,
								permItem.getKey());
						crit.setNextCritOperand(Sv.OR);
						innerDbx.addDbSearchItem(crit);
					} catch (SvException e) {
					}
				}
			}
		}
		// if we granted access, make sure we modify the search criteria
		if (hasAccess) {
			// create on final criteria, based on the innerDbx populated few
			// rows up
			DbSearchExpression finalDbx = new DbSearchExpression().addDbSearchItem(innerDbx);
			if (dqo.getSearchExternal() != null) {
				finalDbx.addDbSearchItem(dqo.getSearchExternal());
			}
			dqo.setSearch(finalDbx);
		}
		return hasAccess;

	}

	protected boolean authoriseDqoByConfigType(DbQueryObject dqo, HashMap<String, DbDataObject> aclMap,
			SvAccess accessLevel) throws SvException {
		boolean hasAccess = false;
		DbDataObject dbt = dqo.getDbt();

		// create expression to hold the config criteria
		DbSearchExpression innerDbx = new DbSearchExpression();
		innerDbx.setNextCritOperand(Sv.AND);

		DbSearchExpression subCfgDbx = new DbSearchExpression();
		subCfgDbx.setNextCritOperand(Sv.AND);

		// get the config field name to filter by
		DbDataObject cfgDbt = (DbDataObject) dbt.getVal(Sv.CONFIG_TYPE);
		DbDataObject cfgObject = (DbDataObject) dbt.getVal(Sv.CONFIG_RELATION);

		// the cfg column from the configuration type table
		String cfgFieldName = (String) cfgDbt.getVal(Sv.CONFIG_UNQ_ID);
		for (Entry<String, DbDataObject> permItem : aclMap.entrySet()) {
			// if the key of the permission is null, than its the global table
			// key, so we ignore it
			if (permItem.getKey() != null && permItem.getValue() != null) {
				DbDataObject acl = permItem.getValue();
				SvAccess accessType = (SvAccess) acl.getVal(Sv.ACCESS_TYPE);
				if (accessType.getAccessLevelValue() >= accessLevel.getAccessLevelValue()) {
					hasAccess = true;
					try {
						subCfgDbx.addDbSearchItem(
								new DbSearchCriterion(cfgFieldName, DbCompareOperand.EQUAL, permItem.getKey()));
					} catch (SvException e) {
					}
				}
			}
		}
		if (dbt.getVal(Sv.CONFIG_RELATION_TYPE).equals(Sv.FIELD)) {
			DbQueryObject subDqo = new DbQueryObject(cfgDbt, subCfgDbx, null, null);
			subDqo.setCustomFieldsList(new ArrayList<String>(Arrays.asList(Sv.OBJECT_ID)));
			DbSearchCriterion finalCrit = new DbSearchCriterion((String) cfgObject.getVal(Sv.FIELD_NAME),
					DbCompareOperand.IN_SUBQUERY);
			finalCrit.setInSubQuery(subDqo);
			innerDbx.addDbSearchItem(finalCrit);
		}
		// TODO add a new query for relation by link ;)

		// if we granted access, make sure we modify the search criteria
		if (hasAccess) {
			// create on final criteria, based on the innerDbx populated few
			// rows up
			DbSearchExpression finalDbx = new DbSearchExpression().addDbSearchItem(innerDbx);
			if (dqo.getSearchExternal() != null) {
				finalDbx.addDbSearchItem(dqo.getSearchExternal());
			}
			dqo.setSearch(finalDbx);
		}
		return hasAccess;

	}

	protected boolean authoriseDqo(DbQueryObject dqo, SvAccess accessLevel) throws SvException {
		boolean hasAccess = false;
		if (!isSystem() && !isService() && dqo.getDbt() != null) {
			DbDataObject dbt = dqo.getDbt();
			SvAclKey aclKey = new SvAclKey(dbt);
			HashMap<String, DbDataObject> aclMap = this.getPermissionsImpl().get(aclKey);
			if (aclMap != null) {
				// check for full table access via null key
				DbDataObject acl = aclMap.get(null);
				if (acl != null) {
					SvAccess accessType = (SvAccess) acl.getVal(Sv.ACCESS_TYPE);
					hasAccess = accessType.getAccessLevelValue() >= accessLevel.getAccessLevelValue();
				}
				// if the full table access was not authorised, try to
				// authorise
				// based on config id. only if its a config table of course.
				if (!hasAccess && (boolean) dqo.getDbt().getVal(Sv.IS_CONFIG_TABLE)) {
					hasAccess = authoriseDqoByConfig(dqo, aclMap, accessLevel);
				}
				// Authorising implementation tables based on related config
				if (!hasAccess && dqo.getDbt().getVal(Sv.CONFIG_TYPE_ID) != null) {
					hasAccess = authoriseDqoByConfigType(dqo, aclMap, accessLevel);
				}
			}
			if (!hasAccess)
				throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, instanceUser, dbt, accessLevel.toString()));
		} else
			hasAccess = true;
		return hasAccess;
	}

	private boolean hasDQOTreeAccess(DbQueryObject dqo, SvAccess accessLevel) throws SvException {
		// DbDataArray perms = getPermissions();
		boolean hasAccess = authoriseDqo(dqo, SvAccess.READ);
		for (DbQueryObject dqoChild : dqo.getChildren()) {
			hasAccess = hasDQOTreeAccess(dqoChild, accessLevel);
		}
		return hasAccess;

	}

	public boolean isSystem() {
		return instanceUser.equals(svCONST.systemUser);
	}

	public boolean isService() {
		return instanceUser.equals(svCONST.serviceUser);
	}

	public boolean isAdmin() throws SvException {
		return isSystem() || isService() || (this.getDefaultUserGroup() != null
				&& this.getDefaultUserGroup().getObjectId().equals(svCONST.SID_ADMINISTRATORS));

	}

	protected void authoriseSelectQuery(DbQuery query) throws SvException {
		if (!isSystem() && !isService() && !isAdmin()) {
			DbDataObject currentDbt = null;
			boolean hasAccess = true;
			if (query instanceof DbQueryObject) {
				hasAccess = authoriseDqo((DbQueryObject) query, SvAccess.READ);
				// DbDataObject dbt = ((DbQueryObject) query).getDbt();
				// hasAccess = hasDbtAccess(dbt, SvAccess.READ);
			} else {
				DbQueryExpression dqe = ((DbQueryExpression) query);

				if (dqe.getIsReverseExpression())
					hasAccess = hasDQOTreeAccess(dqe.getRootQueryObject(), SvAccess.READ);
				else
					for (DbQueryObject dqo : dqe.getItems()) {
						hasAccess = authoriseDqo((DbQueryObject) dqo, SvAccess.READ);

						// currentDbt = dqo.getDbt();
						// hasAccess = hasDbtAccess(currentDbt, SvAccess.READ);
						if (!hasAccess)
							break;
					}

			}
			if (!hasAccess)
				throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, instanceUser, currentDbt,
						SvAccess.READ.toString()));
		}
	}

	/**
	 * This is the root getter method. This method is responsible for rendering the
	 * DbQuery object to a plain SQL and then running the statement against the DB
	 * to get the record set. The record set is then translated into a DbDataArray
	 * object, containing the resulting data.
	 * 
	 * @param query    {@link DbQueryObject} to be executed against the underlying
	 *                 DB
	 * @param rowLimit maximum number of objects to be returned
	 * @param offset   offset from which the objects should be returned
	 * @return A {@link DbDataArray} object containing all returned data in
	 *         DbDataObject format
	 * @throws SvException All underlying JDBC exceptions are wrapped in
	 *                     SvException, which contains more information about the
	 *                     query which is executed. Also a parse exception can be
	 *                     thrown in case of fetching a Geometry whose WKT format is
	 *                     bad.
	 */
	DbDataArray getObjects(DbQuery query, Integer rowLimit, Integer offset) throws SvException {
		// Check for read access to all query objects in the DbQuery
		authoriseSelectQuery(query);

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		DbQuery fullQuery = addEmpoweredCriteria(query);
		DbDataArray result = new DbDataArray();
		try {
			conn = this.dbGetConn();
			// execute the db query to fetch data for the requested item
			ps = conn.prepareStatement(getSQLStatement(fullQuery, rowLimit, offset).toString());
			// bind the parameters
			bindQueryVals(ps, fullQuery.getSQLParamVals());
			// System.out.println("Before exec "+new DateTime().toString());
			rs = ps.executeQuery();
			// System.out.println("After exec "+new DateTime().toString());
			String tblPrefix = Sv.TBL;
			if ((fullQuery instanceof DbQueryExpression) && ((DbQueryExpression) fullQuery).getIsReverseExpression()) {
				if (fullQuery.getReturnTypes().size() == 1) {
					tblPrefix = ((DbQueryExpression) fullQuery).getRootQueryObject().getSqlTablePrefix();
				}
			}
			ResultSetMetaData rsmt = rs.getMetaData();
			while (rs.next()) {
				// System.out.println("After rs next:"+new
				// DateTime().toString());
				// must use uppercase table prefix!!!
				DbDataObject obj = getObjectFromRecord(rs, tblPrefix, fullQuery, rsmt);
				// System.out.println("After object parsing:"+new
				// DateTime().toString());
				if (obj != null)
					result.addDataItem(obj);
			}
			// System.out.println("After full fetch"+new DateTime().toString());
			return result;
		} catch (SQLException ex) {
			try {
				log4j.error("Error in getObjects() with criteria:" + fullQuery.getSQLExpression() + ", query:"
						+ fullQuery.toJson().toString(), ex);
			} catch (Exception e) {
				log4j.error("Error getting DbSearch.getSQLExpression!");
			}
			throw (new SvException("system.error.sql_statement_err", instanceUser, null, query, ex.getCause()));
		} catch (ParseException ex) {
			try {
				log4j.error("Error in getObject() with criteria:" + fullQuery.getSQLExpression());
			} catch (Exception e) {
				log4j.error("Error getting DbSearch.getSQLExpression!");
			}
			throw (new SvException("system.error.wkb_parse_err", instanceUser, null, query, ex.getCause()));
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (Exception ex) {
					log4j.error("Error releasing result set", ex);
				}
			if (ps != null)
				try {
					ps.close();
				} catch (Exception ex) {
					log4j.error("Error releasing prepared statement", ex);
				}
		}
	}

	/**
	 * Method to get the object type for a certain object ID.
	 * 
	 * @param objectId   The object Id for which we need the type
	 * @param schemaName The schema in which the repo is stored
	 * @param repoName   The repo in which the object is stored
	 * @param conn       Connection to be used for the query
	 * @return The type id
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private Long getObjectTypeById(Long objectId, String schemaName, String repoName, Connection conn)
			throws SQLException {
		schemaName = (schemaName != null ? schemaName : SvConf.getDefaultSchema());
		if (repoName == null)
			repoName = SvConf.getMasterRepo();
		String dbtKey = schemaName + "." + repoName;
		DbDataObject repo = DbCache.getObject(dbtKey, svCONST.OBJECT_TYPE_TABLE);
		PreparedStatement ps = null;
		ResultSet rs = null;
		Long retval = svCONST.OBJ_ID_NOTEXIST;
		try {
			String sql = "SELECT distinct object_type FROM " + repo.getVal("SCHEMA") + "." + repo.getVal("TABLE_NAME")
					+ " WHERE object_id=?";
			log4j.debug("SELECT Query Executed:" + sql);
			ps = conn.prepareStatement(sql);
			ps.setLong(1, objectId);
			ps.execute();
			rs = ps.getResultSet();
			if (rs.next())
				retval = rs.getLong(1);
		} catch (Exception e) {
			log4j.error("Error getting Object Type for object id:" + objectId, e);
			return svCONST.GENERAL_ERROR;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return retval;
	}

	/**
	 * Method for binding object to prepared statement variables
	 * 
	 * @param ps             The used SQL prepared statement
	 * @param dbf            The descriptor of the field
	 * @param bindAtPosition The position at which the object value will be bind
	 * @param value          The value which should be bind
	 * @param lob            Reference to the LOB management class
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	protected void bindInsertQueryVars(PreparedStatement ps, DbDataObject dbf, int bindAtPosition, Object value,
			SvLob lob) throws SQLException, SvException {

		DbFieldType type = DbFieldType.valueOf((String) dbf.getVal(Sv.FIELD_TYPE));
		if (log4j.isDebugEnabled())
			log4j.trace("For field:+" + dbf.getVal(Sv.FIELD_NAME) + ", binding value "
					+ (value != null ? value.toString() : Sv.SQL.NULL) + " at position " + bindAtPosition
					+ " as data type " + type.toString());

		switch (type) {
		case BOOLEAN:
			bindBoolean(ps, bindAtPosition, value);
			break;
		case NUMERIC:
			bindNumeric(ps, dbf, bindAtPosition, value);
			break;
		case DATE:
		case TIME:
		case TIMESTAMP:
			bindDateTime(ps, bindAtPosition, value);
			break;
		case TEXT:
		case NVARCHAR:
			bindString(ps, dbf, bindAtPosition, value, lob, type);
			break;
		case GEOMETRY:
			if (value != null) {
				byte[] byteVal = getWKBWriter().write((Geometry) value);
				try {
					SvConf.getDbHandler().setGeometry(ps, bindAtPosition, byteVal);
				} catch (Exception e) {
					throw (new SvException("system.error.bind_geometry", this.instanceUser, dbf, value, e));
				}
			} else
				ps.setNull(bindAtPosition, java.sql.Types.STRUCT, SvGeometry.GEOM_STRUCT_TYPE);
			break;
		default:
			ps.setString(bindAtPosition, (String) value);
			break;
		}
	}

	/**
	 * Method for binding Booleam parameter to the prepare statement based on field
	 * configuration at specified position
	 * 
	 * @param ps             The used SQL prepared statement
	 * @param bindAtPosition The position at which the object value will be bind
	 * @param value          The value which should be bind
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	private void bindBoolean(PreparedStatement ps, int bindAtPosition, Object value) throws SQLException {

		if (value == null) {
			if (SvConf.getDbType().equals(SvDbType.ORACLE))
				ps.setNull(bindAtPosition, java.sql.Types.CHAR);
			else
				ps.setNull(bindAtPosition, java.sql.Types.BOOLEAN);
		} else
			ps.setBoolean(bindAtPosition, (Boolean) value);

	}

	/**
	 * Method for binding Numeric parameter to the prepare statement based on field
	 * configuration at specified position
	 * 
	 * @param ps             The used SQL prepared statement
	 * @param dbf            The descriptor of the field
	 * @param bindAtPosition The position at which the object value will be bind
	 * @param value          The value which should be bind
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	private void bindNumeric(PreparedStatement ps, DbDataObject dbf, int bindAtPosition, Object value)
			throws SQLException {
		if (value == null)
			ps.setNull(bindAtPosition, java.sql.Types.NUMERIC);
		else {
			Long fScale = (Long) dbf.getVal(Sv.FIELD_SCALE);
			if (fScale != null && fScale > 0) {
				BigDecimal bdcml;
				if (value instanceof BigDecimal) {
					bdcml = (BigDecimal) value;
					if (bdcml.scale() > fScale) {
						bdcml = bdcml.setScale(fScale.intValue(), BigDecimal.ROUND_HALF_UP);
					}
				} else {
					double dbl = ((Number) value).doubleValue();
					bdcml = BigDecimal.valueOf(dbl).setScale(fScale.intValue(), BigDecimal.ROUND_HALF_UP);
				}
				ps.setBigDecimal(bindAtPosition, bdcml);
			} else
				ps.setBigDecimal(bindAtPosition, new BigDecimal(((Number) value).longValue()));
		}
	}

	/**
	 * Method for binding DateTime parameter to the prepare statement based on field
	 * configuration at specified position
	 * 
	 * @param ps             The used SQL prepared statement
	 * @param bindAtPosition The position at which the object value will be bind
	 * @param value          The value which should be bind
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	private void bindDateTime(PreparedStatement ps, int bindAtPosition, Object value) throws SQLException {
		if (value == null)
			ps.setNull(bindAtPosition, java.sql.Types.TIMESTAMP);
		else if (value.getClass().equals(DateTime.class)) {
			ps.setTimestamp(bindAtPosition, new Timestamp(((DateTime) value).getMillis()));
		} else {
			DateTime dt = new DateTime(value.toString());
			ps.setTimestamp(bindAtPosition, new Timestamp(dt.getMillis()));
		}
	}

	/**
	 * Method for binding string parameter to the prepare statement based on field
	 * configuration at specified position
	 * 
	 * @param ps             The used SQL prepared statement
	 * @param dbf            The descriptor of the field
	 * @param bindAtPosition The position at which the object value will be bind
	 * @param value          The value which should be bind
	 * @param lob            Reference to the LOB management class
	 * @param type           The type of the value bound to the field
	 * @throws SQLException Any underlying exception is re-thrown
	 */
	@SuppressWarnings("unchecked")
	private void bindString(PreparedStatement ps, DbDataObject dbf, int bindAtPosition, Object value, SvLob lob,
			DbFieldType type) throws SQLException {
		if (value == null)
			ps.setString(bindAtPosition, null);
		else {
			Boolean sv_multi = (Boolean) dbf.getVal(Sv.SV_MULTISELECT);
			if (value instanceof ArrayList<?> && sv_multi != null && sv_multi) {
				StringBuilder bindVal = new StringBuilder();
				for (String oVal : (ArrayList<String>) value) {
					bindVal.append(oVal + SvConf.getMultiSelectSeparator());
				}
				bindVal.setLength(bindVal.length() - 1);
				value = bindVal.toString();
			}
			if (type.equals(DbFieldType.TEXT) && !SvConf.getDbType().equals(SvDbType.POSTGRES)) {
				ps.setCharacterStream(bindAtPosition, lob.stringReader(value.toString()));
			} else
				ps.setString(bindAtPosition, value.toString());
		}
	}

	/**
	 * Method to get the lazy initialized singleton WKB Writer class instance
	 * 
	 * @return WKBWriter instance
	 */
	private WKBWriter getWKBWriter() {
		if (wkbWriter == null)
			wkbWriter = new WKBWriter();
		return wkbWriter;
	}

	/**
	 * Method to get the lazy initialized singleton WKB Reader class instance
	 * 
	 * @return WKBReader instance
	 */
	private WKBReader getWKBReader() {
		if (wkbReader == null)
			wkbReader = new WKBReader(SvUtil.sdiFactory);
		return wkbReader;
	}

	/**
	 * Return the auto commit flag of the SvCore instance
	 */
	public Boolean getAutoCommit() {
		return autoCommit;
	}

	/**
	 * Set the auto commit flag of the SvCore instance
	 */
	public void setAutoCommit(Boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	/**
	 * Method to get the current User for this SvCore instance
	 * 
	 * @return A {@link DbDataObject} reference holding the object descriptor
	 */
	public DbDataObject getInstanceUser() {
		return instanceUser;
	}

	/**
	 * Method to set the current user of the SvCore. This is possible only for
	 * System or Service users.
	 * 
	 * @param newInstanceUser The descriptor of the new instance user
	 * @throws SvException "system.error.not_authorised" is thrown if the the
	 *                     current user is not system or service
	 */
	public void setInstanceUser(DbDataObject newInstanceUser) throws SvException {
		if (isSystem() || isService()) {
			instanceUser = newInstanceUser;
		} else
			throw (new SvException(Sv.Exceptions.NOT_AUTHORISED, instanceUser));
	}

	public Boolean getIsDebugEnabled() {
		return SvCore.isDebugEnabled;
	}

	public void setIsDebugEnabled(Boolean isDebugEnabled) {
		SvCore.isDebugEnabled = isDebugEnabled;
	}

	public Boolean getIsLongRunning() {
		return isLongRunning;
	}

	public void setIsLongRunning(Boolean isLongRunning) {
		this.isLongRunning = isLongRunning;
	}

	public long getCoreLastActivity() {
		return coreLastActivity;
	}

	public void setCoreLastActivity(long coreLastActivity) {
		this.coreLastActivity = coreLastActivity;
	}

	public long getCoreCreation() {
		return coreCreation;
	}

	public String getCoreTraceInfo() {
		return coreTraceInfo;
	}

	public static ArrayList<Long> getGeometryTypes() {
		return geometryTypes;
	}

	public static void setGeometryTypes(ArrayList<Long> geometryTypes) {
		SvCore.geometryTypes = geometryTypes;
	}

	public String getSessionId() {
		return coreSessionId;
	}

	public DbDataObject getSaveAsUser() {
		return saveAsUser;
	}

	public void setSaveAsUser(DbDataObject saveAsUser) {
		this.saveAsUser = saveAsUser;
	}

	public Boolean getIsInternal() {
		return isInternal;
	}

	public void setIsInternal(Boolean isInternal) throws SvException {
		if (!SvConf.isSystemClass(SvUtil.getCallerClassName(SvCore.class)))
			throw (new SvException("system.error.sysclass_not_registered", instanceUser));

		this.isInternal = isInternal;
	}
}