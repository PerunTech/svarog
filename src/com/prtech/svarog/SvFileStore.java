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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataField;
import com.prtech.svarog_common.DbDataField.DbFieldType;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbDataTable;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog.svCONST;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

public class SvFileStore extends SvCore {
	/**
	 * Log4j instance
	 */
	static final Logger log4j = SvConf.getLogger(SvFileStore.class);

	/**
	 * Static cache instance to hold the system files.
	 */
	private static Cache<Long, byte[]> systemCache = getSystemFilesCache();

	static String fileStorePath = initFileStorePath();
	/**
	 * Variable holding the maximum size of a system file to be cached in mega bytes
	 */
	private static int max_cache_size = initMaxSize();

	/**
	 * private method to get the max file for the cache from the configuration
	 * 
	 * @return
	 */
	private static int initMaxSize() {
		int max_size = 5;
		try {
			max_size = Integer.parseInt(SvConf.getParam("filestore.sys_store.cache_max_filesize"));
		} catch (Exception e) {
			max_size = 5;
		}
		return max_size;
	}

	private static String initFileStorePath() {

		String path = SvConf.getParam("filestore.path");
		if (path == null || path.isEmpty())
			log4j.error("Filestore path not properly configured. Using default path");
		File fPath = new File(path);
		if (!fPath.exists()) {
			log4j.error("Filestore path " + path + " does not exist on the file system!. Using default path");
			path = "./svarog_filestore";
			fPath = new File(path);
			if (!fPath.exists())
				fPath.mkdir();

		}

		if (path != null)
			log4j.info("Filestore path is:" + fPath.getAbsolutePath());

		// TODO Auto-generated method stub
		return path;
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id String UID of the user session under which the SvCore
	 *                   instance will run
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvFileStore(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * 
	 * @param session_id   String UID of the user session under which the SvCore
	 *                     instance will run
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvFileStore(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Shared core Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * 
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvFileStore(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	SvFileStore() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Initialiser method for the file cache
	 * 
	 * @return A cache instance to hold file data
	 */
	private static Cache<Long, byte[]> getSystemFilesCache() {

		int cache_ttl = 10;
		try {
			cache_ttl = Integer.parseInt(SvConf.getParam("filestore.sys_store.cache_ttl"));
		} catch (Exception e) {
			cache_ttl = 10;
		}
		return CacheBuilder.newBuilder().expireAfterAccess(cache_ttl, TimeUnit.MINUTES).<Long, byte[]>build();

	}

	/**
	 * Method to save a file based on byte[] file data.
	 * 
	 * @param fileDescriptor DbDataObject describing the file.
	 * @param objectId       The id of the object to which file should be linked
	 * @param objectType     The type of the object to which the file should be
	 *                       linked
	 * @param fileData       The file data it self. It can be byte[] or InputStream.
	 *                       Svarog will not close the stream after successful save.
	 * @throws SvException Pass through exception from
	 *                     {@link #saveFile(DbDataObject, DbDataObject, Object, Boolean)}
	 */
	@Deprecated
	public void saveFile(DbDataObject fileDescriptor, Long objectId, Long objectType, byte[] fileData)
			throws SvException {

		SvReader svr = new SvReader(this);
		try {
			DbDataObject linkedObject = svr.getObjectById(objectId, objectType, null);
			saveFile(fileDescriptor, linkedObject, fileData, this.autoCommit);
		} finally {
			svr.release();
		}
	}

	/**
	 * Method to save file in the svarog data store. In case you passed InputStream
	 * as file data, you MUST CLOSE IT your self!
	 * 
	 * @param fileDescriptor DbDataObject describing the file.
	 * @param linkedObject   The object to which this file will be linked
	 * @param fileData       The file data it self. It can be byte[] or InputStream.
	 *                       Svarog will not close the stream after successful save.
	 * @param fileStoreId    The file store in which the data should be stored
	 *                       {@link svCONST.SYSTEM_FILESTORE_ID} means it will be
	 *                       stored in the DB
	 * @param autoCommit     If svarog should commit on success or rollback on
	 *                       exception
	 * @throws SvException
	 */
	public void saveFile(DbDataObject fileDescriptor, DbDataObject linkedObject, Object fileData, Boolean autoCommit)
			throws SvException {

		try {
			this.dbSetAutoCommit(false);
			saveFileImpl(fileDescriptor, linkedObject, fileData);
			if (autoCommit)
				dbCommit();
		} catch (SvException e) {
			if (autoCommit)
				dbRollback();
			throw (e);

		}

	}

	/**
	 * 
	 * @param dbo        DbDataObject describing the file (File Metadata)
	 * @param objectId   ID of the object to which the file is associated
	 * @param objectType Type Id of the objectId (can be null)
	 * @param fileData   Binary array (file content)
	 * @return
	 * @throws SvException
	 */
	protected void saveFileImpl(DbDataObject fileDescriptor, DbDataObject linkedObject, Object fileData)
			throws SvException {

		if (fileData == null)
			throw (new SvException("system.error.cant_save_empty_file", instanceUser, fileDescriptor, linkedObject));

		if (!(fileData instanceof InputStream || fileData instanceof byte[]))
			throw (new SvException("system.error.filedata_type_err", instanceUser, null, null));

		SvWriter svw = new SvWriter(this);
		svw.isInternal = true;
		SvLink svl = new SvLink(this);

		Connection fileConn = null;
		try {
			fileDescriptor.setObject_type(svCONST.OBJECT_TYPE_FILE);
			fileConn = this.dbGetConn();
			fileConn.setAutoCommit(false);
			Long fileId = setFileData(fileData, (Long) fileDescriptor.getVal("FILE_STORE_ID"));
			fileDescriptor.setVal("FILE_ID", fileId);
			svw.saveObject(fileDescriptor, false);
			if (linkedObject != null)
				svl.linkObjects(linkedObject, fileDescriptor, "LINK_FILE", "", false);

		} catch (SQLException e) {
			throw (new SvException("system.error.sql_err", instanceUser, fileDescriptor, linkedObject, e));
		} finally {
			svw.release();
			svl.release();
		}

	}

	/**
	 * Overload to take object id and object type to get a list of file descriptors
	 * 
	 * @param objectId     The id of the object to which file descriptors are
	 *                     associated
	 * @param objectTypeId The object type of the linked object
	 * @param fileTypes    The file types
	 * @param refDate      The reference date on which the list of file descriptors
	 *                     should be retrieved
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getFiles(Long objectId, Long objectTypeId, String fileTypes, DateTime refDate)
			throws SvException {

		SvReader dbu = new SvReader(this);
		try {
			DbDataObject linkedObject = dbu.getObjectById(objectId, objectTypeId, refDate);
			return getFiles(linkedObject, fileTypes, refDate);
		} finally {
			dbu.release();
		}

	}

	/**
	 * Method to fetch a list of files filtering by fileType. Uses SvReader and
	 * fetches cached objects
	 * 
	 * @param linkedObject The objet to which the files were linked
	 * @param fileTypes    The file types according to which a filter should be
	 *                     applied
	 * @param refDate      The reference date
	 * @return
	 * @throws SvException
	 */
	@SuppressWarnings("unchecked")
	public DbDataArray getFiles(DbDataObject linkedObject, String fileTypes, DateTime refDate) throws SvException {

		DbDataArray files = new DbDataArray();
		SvReader svr = new SvReader(this);
		try {
			DbDataObject dbl = getLinkType("LINK_FILE", linkedObject.getObject_type(), svCONST.OBJECT_TYPE_FILE);
			DbDataArray allFiles = svr.getObjectsByLinkedId(linkedObject.getObject_id(), dbl, refDate, 0, 0);
			if (fileTypes != null && fileTypes.length() > 0) {
				for (DbDataObject file : allFiles.getItems()) {
					if (file.getVal("FILE_TYPE") != null && ((String) file.getVal("FILE_TYPE")).contains(fileTypes))
						files.addDataItem(file);
				}
			} else
				files.setItems((ArrayList<DbDataObject>) allFiles.getItems().clone());
		} finally {
			svr.release();
		}
		return files;

	}

	/**
	 * Method to get list of file descriptors based on search criteria
	 * 
	 * @param objectTypeId The object type of the linked object
	 * @param fileTypes    The file types
	 * @param refDate      The reference date on which the list of file descriptors
	 *                     should be retrieved * @param fileSearch
	 * @param fileSearch   A DbSearch object which contains the search parameters
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getFilesBySearch(Long objectId, Long objectTypeId, DateTime refDate, DbSearch fileSearch)
			throws SvException {
		SvReader dbu = new SvReader(this);
		try {
			DbDataObject dbo = dbu.getObjectById(objectId, objectTypeId, refDate);
			return getFilesBySearch(dbo, fileSearch, refDate);
		} finally {
			dbu.release();
		}
	}

	/**
	 * Method to fetch a list of file descriptors associated via search criteria.
	 * 
	 * @param linkedObject
	 * @param fileSearch
	 * @param refDate
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getFilesBySearch(DbDataObject linkedObject, DbSearch fileSearch, DateTime refDate)
			throws SvException {
		DbDataArray object = null;
		SvReader dbu = new SvReader(this);

		try {
			DbDataObject dbl = getLinkType("LINK_FILE", linkedObject.getObject_type(), svCONST.OBJECT_TYPE_FILE);

			if (dbl == null)
				throw (new SvException("system.error.invalid_link_type", instanceUser, null, null));

			DbDataObject dbt = getDbt(linkedObject.getObject_type());
			DbDataObject dbtFiles = getDbt(svCONST.OBJECT_TYPE_FILE);

			DbSearch dbs = new DbSearchCriterion("OBJECT_ID", DbCompareOperand.EQUAL, linkedObject.getObject_id());
			DbQueryObject qObjects = new DbQueryObject(repoDbt, repoDbtFields, dbt, getFields(dbt.getObject_id()), dbs,
					DbJoinType.FULL, dbl, LinkType.DBLINK, null);

			DbQueryObject qFiles = new DbQueryObject(repoDbt, repoDbtFields, dbtFiles,
					getFields(dbtFiles.getObject_id()), fileSearch, DbJoinType.FULL, null, LinkType.PARENT, null);
			qFiles.setIsReturnType(true);
			DbQueryExpression q = new DbQueryExpression();

			q.addItem(qObjects);
			q.addItem(qFiles);

			object = dbu.getObjects(q, null, null);
		} finally {
			dbu.release();
		}
		return object;

	}

	/**
	 * Method to save a byte[] to disk based file store. Legacy overload.
	 * 
	 * @param fileId The id of the file data under which it should be stored
	 * @param data   The byte array which contains the file data
	 * @throws SvException
	 */
	@Deprecated
	void fileSystemSaveByte(Long fileId, byte[] data) throws SvException {

		FileOutputStream output = null;
		try {
			output = fileSystemSaveStream(fileId);
			IOUtils.write(data, output);
			output.flush();
		} catch (IOException e) {
			throw (new SvException("system.error.filedata_fs_err", instanceUser, null, fileId, e));
		} finally {
			closeResource((AutoCloseable) output, instanceUser);
		}
	}

	/**
	 * Method to save a file to a disk based file store.
	 * 
	 * @param fileId The id of the file data under which it should be stored
	 * @return
	 * @throws SvException
	 */
	private FileOutputStream fileSystemSaveStream(Long fileId) throws SvException {
		File rootFs = new File(fileStorePath + "/" + Long.toString((fileId / 1000L + 1L) * 1000));
		FileOutputStream output = null;

		try {
			if (!rootFs.exists())
				Files.createDirectory(rootFs.toPath());
			File fileStore = new File(rootFs.getCanonicalPath() + "/" + fileId.toString());
			output = new FileOutputStream(fileStore);
		} catch (IOException e) {
			throw (new SvException("system.error.filedata_fs_err", instanceUser, null, rootFs, e));
		}
		return output;

	}

	/**
	 * Get input stream from a file store
	 * 
	 * @param rbConfig  Global svarog properties
	 * @param fileId    Id of the file to be read
	 * @param errorCode Standard svarog error code as defined in SvCONST
	 * @return
	 * @throws SvException
	 */
	private FileInputStream fileSystemGetStream(Long fileId, HashMap<String, Object> extendedInfo) throws SvException {
		File rootFs = new File(fileStorePath + "/" + Long.toString((fileId / 1000L + 1L) * 1000));
		FileInputStream input = null;

		try {
			if (!rootFs.exists())
				Files.createDirectory(rootFs.toPath());
			File fileStore = new File(rootFs.getCanonicalPath() + "/" + fileId.toString());
			if (fileStore.exists()) {
				if (extendedInfo != null)
					extendedInfo.put("FILE_SIZE", new Long(fileStore.length()));
				input = new FileInputStream(fileStore);
			} else
				throw (new SvException("system.error.filestore_doesnt_exist", instanceUser, null, fileId));

		} catch (IOException e) {
			throw (new SvException("system.error.filedata_fs_err", instanceUser, null, fileId, e));
		}
		return input;

	}

	/**
	 * Method to get an InputStream from a database file store based on file
	 * descriptor
	 * 
	 * @param fileDescriptor The file descriptor for which we want to fetch the data
	 * @return
	 * @throws SvException
	 */
	private InputStream dataBaseGetStream(DbDataObject fileDescriptor, HashMap<String, Object> extendedInfo)
			throws SvException {

		String tblName = SvConf.getParam("filestore.table");
		String schema = SvConf.getParam("filestore.conn.defaultSchema");

		PreparedStatement ps = null;
		ResultSet rs = null;
		InputStream data = null;
		String sqlStr = "select pkid, data from " + (schema != null ? schema : SvConf.getDefaultSchema()) + "."
				+ tblName + " where pkid=?";

		try {
			Connection conn = this.dbGetConn();
			conn = this.dbGetConn();

			ps = conn.prepareStatement(sqlStr);

			ps.setLong(1, (Long) fileDescriptor.getVal("FILE_ID"));
			ps.execute();
			rs = ps.getResultSet();
			rs.next();
			if (extendedInfo != null) {
				Object size = 0;
				if (SvConf.getParam("conn.dbType").equalsIgnoreCase("POSTGRES")) {
					byte[] b = rs.getBytes(2);
					size = b.length;
				} else {
					Blob b = rs.getBlob(2);
					size = b.length();
				}
				extendedInfo.put("FILE_SIZE", size);
			}
			data = rs.getBinaryStream(2);
		} catch (SQLException e) {
			throw (new SvException("system.error.files_db_err", instanceUser, fileDescriptor, sqlStr));
		} finally {
			closeResource((AutoCloseable) rs, instanceUser);
			closeResource((AutoCloseable) ps, instanceUser);
		}
		return data;

	}

	/**
	 * Method to store file data into the database.
	 * 
	 * @param inputData The input data can be of type InputStream or byte[]. Its up
	 *                  to you which data type you will use
	 * @return The id of the stored binary data
	 * @throws SvException
	 */
	Long dataBaseSaveFile(Object inputData) throws SvException {
		Long newFileId = null;
		InputStream streamData = null;
		byte[] byteData = null;

		if (inputData instanceof InputStream)
			streamData = (InputStream) inputData;

		if (inputData instanceof byte[])
			byteData = (byte[]) inputData;

		if (streamData == null && byteData == null)
			throw (new SvException("system.error.cant_save_empty_file", instanceUser, null, null));

		PreparedStatement ps = null;
		ResultSet rs = null;
		String tblName = SvConf.getParam("filestore.table");
		String seqName = tblName + "_pkid";
		String schema = SvConf.getParam("filestore.conn.defaultSchema");
		StringBuilder sqlQuery = new StringBuilder(100);

		sqlQuery.append("insert into " + (schema != null ? schema : SvConf.getDefaultSchema()) + "." + tblName);
		sqlQuery.append("(pkid, data) values(");
		sqlQuery.append(SvConf.getSqlkw().getString("SEQ_NEXTVAL").replace("{SEQUENCE_NAME}",
				(schema != null ? schema : SvConf.getDefaultSchema()) + "." + seqName) + ",?)");
		try {
			Connection conn = this.dbGetConn();
			ps = conn.prepareStatement(sqlQuery.toString(), new String[] { "pkid" });

			if (streamData != null)
				ps.setBinaryStream(1, streamData);
			else
				ps.setBytes(1, byteData);

			int updatedRows = ps.executeUpdate();

			rs = ps.getGeneratedKeys();

			if (rs.next()) {
				newFileId = rs.getLong(1);
			}
			if (newFileId == null || updatedRows != 1)
				throw (new SvException("system.error.filesave_db_err", instanceUser, null, null));
		} catch (SQLException e) {
			throw (new SvException("system.error.filesave_db_err", instanceUser, null, null, e));
		} finally {
			closeResource((AutoCloseable) rs, instanceUser);
			closeResource((AutoCloseable) ps, instanceUser);
		}
		return newFileId;
	}

	/**
	 * Method to get the file Id from a DB sequence. This method is used only when
	 * saving files to disk
	 * 
	 * @return The next available file id
	 * @throws SvException
	 */
	Long getFileId() throws SvException {
		Long newFileId = null;

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Connection conn = this.dbGetConn();
			String tblName = SvConf.getParam("filestore.table");
			String seqName = tblName + "_pkid";
			String schema = SvConf.getParam("filestore.conn.defaultSchema");
			String strNextVal = SvConf.getSqlkw().getString("SEQ_NEXTVAL_SELECT").replace("{SEQUENCE_NAME}",
					(schema != null ? schema : SvConf.getDefaultSchema()) + "." + seqName);

			if (log4j.isDebugEnabled())
				log4j.trace("Selecting a next val from file id sequence" + strNextVal);
			ps = conn.prepareStatement(strNextVal);
			rs = ps.executeQuery();
			if (rs.next()) {
				newFileId = rs.getLong(1);
			}
			if (newFileId == null)
				throw (new SvException("system.error.filesave_db_err", instanceUser, null, null));
		} catch (SQLException e) {
			throw (new SvException("system.error.filesave_db_err", instanceUser, null, null, e));
		} finally {
			closeResource((AutoCloseable) rs, instanceUser);
			closeResource((AutoCloseable) ps, instanceUser);
		}
		return newFileId;
	}

	/**
	 * Method to save the file data in the appropriate storage and return the ID of
	 * the file data
	 * 
	 * @param inputData The file data to be stored (InputDataStream or byte[])
	 * @param isSystem  Flag to store the system files always in the database
	 * @return The id of the stored file data
	 * @throws SvException
	 */
	Long setFileData(Object inputData, Long fileStoreId) throws SvException {

		Long fileId = null;

		FileOutputStream fstr = null;
		try {
			String fsType = SvConf.getParam("filestore.type");

			if ((fileStoreId != null && fileStoreId == svCONST.SYSTEM_FILESTORE_ID) || fsType.equals("DATABASE")) {
				fileId = dataBaseSaveFile(inputData);
			} else if (fsType.equals("FILESYSTEM")) {

				fileId = getFileId();
				if (inputData instanceof InputStream) {
					fstr = fileSystemSaveStream(fileId);
					IOUtils.copy((InputStream) inputData, fstr);
					fstr.flush();
				}
				if (inputData instanceof byte[])
					fileSystemSaveByte(fileId, (byte[]) inputData);
			} else
				throw (new SvException("system.error.filedata_fs_err", instanceUser, null, fsType));

		} catch (IOException e) {
			throw (new SvException("system.error.filedata_fs_err", instanceUser, null, null, e));
		} finally {
			closeResource((Closeable) fstr, instanceUser);
		}

		return fileId;

	}

	/**
	 * Method to get the file data for the requested descriptor as InputStream. You
	 * MUST CLOSE the stream your self.
	 * 
	 * @param dboFile The file descriptor
	 * @return The InputStream holding the file data.
	 * @throws SvException
	 */
	public InputStream getFileAsStream(DbDataObject dboFile) throws SvException {
		InputStream fileData = null;
		// Properties rbConfig = SvConf.getRbConfig();
		fileData = getFileAsStream(dboFile, null);
		return fileData;
	}

	/**
	 * Method to get the file data for the requested descriptor as InputStream. You
	 * MUST CLOSE the stream your self.
	 * 
	 * @param dboFile The file descriptor
	 * @return The InputStream holding the file data.
	 * @throws SvException
	 */
	public InputStream getFileAsStream(DbDataObject dboFile, HashMap<String, Object> extendedInfo) throws SvException {
		InputStream fileData = null;
		// Properties rbConfig = SvConf.getRbConfig();

		String fsType = SvConf.getParam("filestore.type").toUpperCase();
		if (!fsType.equals("FILESYSTEM") || (dboFile.getVal("FILE_STORE_ID") != null
				&& (Long) dboFile.getVal("FILE_STORE_ID") == svCONST.SYSTEM_FILESTORE_ID)) {
			fileData = dataBaseGetStream(dboFile, extendedInfo);
		} else if (fsType.equals("FILESYSTEM")) {
			fileData = fileSystemGetStream((Long) dboFile.getVal("FILE_ID"), extendedInfo);
		}
		return fileData;
	}

	/**
	 * Method to get a file from the system file store, using the system cache for
	 * fast access to system configuration files
	 * 
	 * @param dboFile The file descriptor which we want to get the content
	 * @return The byte array with file content
	 */

	private byte[] getSystemFilestore(DbDataObject dboFile) {
		byte[] data = null;
		if (dboFile.getVal("FILE_STORE_ID") != null
				&& (Long) dboFile.getVal("FILE_STORE_ID") == svCONST.SYSTEM_FILESTORE_ID) {
			data = systemCache.getIfPresent((Long) dboFile.getVal("FILE_ID"));
			if (data != null && (data.length / 1024 / 1024) <= max_cache_size)
				systemCache.put((Long) dboFile.getVal("FILE_ID"), data);
			else
				log4j.warn("System file " + dboFile.getVal("file_name") + " has size "
						+ Integer.toString(data != null ? (data.length / 1024 / 1024) : 0)
						+ " MB. The max file size to be cached is " + max_cache_size
						+ ". The file will not be cached! Verify your file or your size limit");
		}
		return data;

	}

	/**
	 * Method to read a file from disk or db for a descriptor and return a byte
	 * array.
	 * 
	 * @param dboFile The file descriptor
	 * @return
	 * @throws SvException
	 */
	public byte[] getFileAsByte(DbDataObject dboFile) throws SvException {
		byte[] data = null;
		InputStream fstr = null;
		try {
			data = getSystemFilestore(dboFile);
			if (data == null || data.length == 0) {
				HashMap<String, Object> extendedInfo = new HashMap<String, Object>();
				fstr = getFileAsStream(dboFile, extendedInfo);
				if (fstr != null) {
					// Integer curfilesize =
					// Integer.parseInt((dboFile.getVal("FILE_SIZE").toString()));
					data = new byte[Integer.valueOf(extendedInfo.get("FILE_SIZE").toString())];
					IOUtils.read(fstr, data);
				}
			}

		} catch (IOException e) {
			throw (new SvException("system.error.filedata_fs_err", instanceUser, dboFile, null, e));
		} finally {
			closeResource((Closeable) fstr, instanceUser);
		}
		return data;
	}

}
