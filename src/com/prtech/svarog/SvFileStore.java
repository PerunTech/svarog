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
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

public class SvFileStore extends SvCore {
	/**
	 * Log4j instance
	 */
	private static final Logger log4j = SvConf.getLogger(SvFileStore.class);

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

		try (SvReader svr = new SvReader(this)) {
			DbDataObject linkedObject = svr.getObjectById(objectId, objectType, null);
			saveFile(fileDescriptor, linkedObject, fileData, this.autoCommit);
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
	 * @throws SvException Throws system.error.cant_save_empty_file if the file data
	 *                     is null, or system.error.filedata_type_err if data is
	 *                     anything else than byte array or InputStream
	 */
	protected void saveFileImpl(DbDataObject fileDescriptor, DbDataObject linkedObject, Object fileData)
			throws SvException {

		if (fileData == null)
			throw (new SvException(Sv.Exceptions.EMPTY_FILE_SAVE, instanceUser, fileDescriptor, linkedObject));

		if (!(fileData instanceof InputStream || fileData instanceof byte[]))
			throw (new SvException(Sv.Exceptions.FILESAVE_TYPE_ERROR, instanceUser, null, null));

		Connection fileConn = null;
		try (SvWriter svw = new SvWriter(this); SvLink svl = new SvLink(this)) {
			svw.isInternal = true;
			fileDescriptor.setObjectType(svCONST.OBJECT_TYPE_FILE);
			fileConn = this.dbGetConn();
			fileConn.setAutoCommit(false);
			Long fileId = setFileData(fileData, (Long) fileDescriptor.getVal(Sv.FILE_STORE_ID));
			fileDescriptor.setVal(Sv.FILE_ID, fileId);
			svw.saveObject(fileDescriptor, false);
			if (linkedObject != null)
				svl.linkObjects(linkedObject, fileDescriptor, Sv.LINK_FILE, "", false);

		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.SQL_ERR, instanceUser, fileDescriptor, linkedObject, e));
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
	 * @return The list of file objects associated with the object identified by
	 *         objectId and objectTypeId
	 * @throws SvException
	 */
	public DbDataArray getFiles(Long objectId, Long objectTypeId, String fileTypes, DateTime refDate)
			throws SvException {
		try (SvReader svr = new SvReader(this)) {
			DbDataObject linkedObject = svr.getObjectById(objectId, objectTypeId, refDate);
			return getFiles(linkedObject, fileTypes, refDate);
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
	 * @return The list of file objects associated with the object identified by
	 *         objectId and objectTypeId
	 * @throws SvException Any underlying svarog exception
	 */
	@SuppressWarnings("unchecked")
	public DbDataArray getFiles(DbDataObject linkedObject, String fileTypes, DateTime refDate) throws SvException {

		DbDataArray files = new DbDataArray();

		try (SvReader svr = new SvReader(this)) {
			DbDataObject dbl = getLinkType(Sv.LINK_FILE, linkedObject.getObjectType(), svCONST.OBJECT_TYPE_FILE);
			DbDataArray allFiles = svr.getObjectsByLinkedId(linkedObject.getObjectId(), dbl, refDate, 0, 0);
			if (fileTypes != null && fileTypes.length() > 0) {
				for (DbDataObject file : allFiles.getItems()) {
					if (file.getVal(Sv.FILE_TYPE) != null && ((String) file.getVal(Sv.FILE_TYPE)).equals(fileTypes))
						files.addDataItem(file);
				}
			} else
				files.setItems((ArrayList<DbDataObject>) ((ArrayList<DbDataObject>) allFiles.getItems()).clone());
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
	 * @return The list of file objects associated with the object identified by
	 *         objectId and objectTypeId filtered by the fileSearch parameter
	 * @throws SvException
	 */
	public DbDataArray getFilesBySearch(Long objectId, Long objectTypeId, DateTime refDate, DbSearch fileSearch)
			throws SvException {

		try (SvReader svr = new SvReader(this)) {
			DbDataObject dbo = svr.getObjectById(objectId, objectTypeId, refDate);
			return getFilesBySearch(dbo, fileSearch, refDate);
		}
	}

	/**
	 * Method to fetch a list of file descriptors associated via search criteria.
	 * PERFORMANCE WARNING: This method doesn't use Svarog Cache, thus it executes a
	 * query against the database.
	 * 
	 * @param linkedObject The object to which the files are linked
	 * @param fileSearch
	 * @param refDate
	 * @return
	 * @throws SvException
	 */
	public DbDataArray getFilesBySearch(DbDataObject linkedObject, DbSearch fileSearch, DateTime refDate)
			throws SvException {
		DbDataArray object = null;
		try (SvReader svr = new SvReader(this)) {
			DbDataObject dbl = getLinkType(Sv.LINK_FILE, linkedObject.getObjectType(), svCONST.OBJECT_TYPE_FILE);

			if (dbl == null)
				throw (new SvException(Sv.Exceptions.INVALID_LINK_TYPE, instanceUser, null, null));

			DbDataObject dbt = getDbt(linkedObject.getObjectType());
			DbDataObject dbtFiles = getDbt(svCONST.OBJECT_TYPE_FILE);

			DbSearch dbs = new DbSearchCriterion(Sv.OBJECT_ID, DbCompareOperand.EQUAL, linkedObject.getObjectId());
			DbQueryObject qObjects = new DbQueryObject(dbt, dbs, DbJoinType.INNER, dbl, LinkType.DBLINK, null, null);

			DbQueryObject qFiles = new DbQueryObject(dbtFiles, fileSearch, DbJoinType.INNER, null, LinkType.PARENT, null,
					null);
			qFiles.setIsReturnType(true);
			DbQueryExpression q = new DbQueryExpression();

			q.addItem(qObjects);
			q.addItem(qFiles);

			object = svr.getObjects(q, null, null);
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
			throw (new SvException(Sv.Exceptions.FILEDATA_FS_ERROR, instanceUser, null, fileId, e));
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
			throw (new SvException(Sv.Exceptions.FILEDATA_FS_ERROR, instanceUser, null, rootFs, e));
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
					extendedInfo.put(Sv.FILE_SIZE, new Long(fileStore.length()));
				input = new FileInputStream(fileStore);
			} else
				throw (new SvException(Sv.Exceptions.FILESTORE_DOESNT_EXIST, instanceUser, null, fileId));

		} catch (IOException e) {
			throw (new SvException(Sv.Exceptions.FILEDATA_FS_ERROR, instanceUser, null, fileId, e));
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

		String tblName = SvConf.getParam(Sv.FILESTORE_TABLE);
		String schema = SvConf.getParam(Sv.FILESTORE_SCHEMA);

		InputStream data = null;
		String sqlStr = String.format(Sv.SQL.SELECT_FILESTORE, (schema != null ? schema : SvConf.getDefaultSchema()),
				tblName);

		try (PreparedStatement ps = this.dbGetConn().prepareStatement(sqlStr)) {
			ps.setLong(1, (Long) fileDescriptor.getVal(Sv.FILE_ID));
			ps.execute();
			try (ResultSet rs = ps.getResultSet()) {
				rs.next();
				if (extendedInfo != null) {
					Object size = 0;
					if (SvConf.getParam(Sv.DB_TYPE).equalsIgnoreCase(Sv.POSTGRES)) {
						byte[] b = rs.getBytes(2);
						size = b.length;
					} else {
						Blob b = rs.getBlob(2);
						size = b.length();
					}
					extendedInfo.put(Sv.FILE_SIZE, size);
				}
				data = rs.getBinaryStream(2);
			}
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.FILE_DB_ERROR, instanceUser, fileDescriptor, sqlStr));
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
			throw (new SvException(Sv.Exceptions.EMPTY_FILE_SAVE, instanceUser, null, null));

		String tblName = SvConf.getParam(Sv.FILESTORE_TABLE);
		String seqName = tblName + "_" + Sv.PKID.toLowerCase();
		String schema = SvConf.getParam(Sv.FILESTORE_SCHEMA) != null ? SvConf.getParam(Sv.FILESTORE_SCHEMA)
				: SvConf.getDefaultSchema();
		String sqlQuery = String.format(Sv.SQL.INSERT_FILESTORE, schema, tblName, SvConf.getSqlkw()
				.getString(Sv.SQL.SEQ_NEXTVAL).replace("{" + Sv.SQL.SEQUENCE_NAME + "}", schema + "." + seqName));

		try (PreparedStatement ps = this.dbGetConn().prepareStatement(sqlQuery.toString(),
				new String[] { Sv.PKID.toLowerCase() })) {

			if (streamData != null)
				ps.setBinaryStream(1, streamData);
			else
				ps.setBytes(1, byteData);

			int updatedRows = ps.executeUpdate();

			try (ResultSet rs = ps.getGeneratedKeys()) {
				if (rs.next()) {
					newFileId = rs.getLong(1);
				}
			}
			if (newFileId == null || updatedRows != 1)
				throw (new SvException(Sv.Exceptions.FILESAVE_DB_ERROR, instanceUser, null, null));
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.FILESAVE_DB_ERROR, instanceUser, null, null, e));
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

		String tblName = SvConf.getParam(Sv.FILESTORE_TABLE);
		String seqName = tblName + "_" + Sv.PKID.toLowerCase();
		String schema = SvConf.getParam(Sv.FILESTORE_SCHEMA);
		String strNextVal = SvConf.getSqlkw().getString(Sv.SQL.SEQ_NEXTVAL_SELECT).replace(
				"{" + Sv.SQL.SEQUENCE_NAME + "}",
				(schema != null ? schema : SvConf.getDefaultSchema()) + "." + seqName);

		if (log4j.isDebugEnabled())
			log4j.trace(Sv.SQL.SQL_DEBUG + strNextVal);

		try (PreparedStatement ps = this.dbGetConn().prepareStatement(strNextVal)) {
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					newFileId = rs.getLong(1);
				}
			}
			if (newFileId == null)
				throw (new SvException(Sv.Exceptions.FILESAVE_DB_ERROR, instanceUser, null, null));
		} catch (SQLException e) {
			throw (new SvException(Sv.Exceptions.FILESAVE_DB_ERROR, instanceUser, null, null, e));
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
			String fsType = SvConf.getParam(Sv.FILESTORE_TYPE);

			if ((fileStoreId != null && fileStoreId == svCONST.SYSTEM_FILESTORE_ID) || fsType.equals(Sv.DATABASE)) {
				fileId = dataBaseSaveFile(inputData);
			} else if (fsType.equals(Sv.FILESYSTEM)) {

				fileId = getFileId();
				if (inputData instanceof InputStream) {
					fstr = fileSystemSaveStream(fileId);
					IOUtils.copy((InputStream) inputData, fstr);
					fstr.flush();
				}
				if (inputData instanceof byte[])
					fileSystemSaveByte(fileId, (byte[]) inputData);
			} else
				throw (new SvException(Sv.Exceptions.FILEDATA_FS_ERROR, instanceUser, null, fsType));

		} catch (IOException e) {
			throw (new SvException(Sv.Exceptions.FILEDATA_FS_ERROR, instanceUser, null, null, e));
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

		String fsType = SvConf.getParam(Sv.FILESTORE_TYPE).toUpperCase();
		if (!fsType.equals(Sv.FILESYSTEM) || (dboFile.getVal(Sv.FILE_STORE_ID) != null
				&& (Long) dboFile.getVal(Sv.FILE_STORE_ID) == svCONST.SYSTEM_FILESTORE_ID)) {
			fileData = dataBaseGetStream(dboFile, extendedInfo);
		} else if (fsType.equals(Sv.FILESYSTEM)) {
			fileData = fileSystemGetStream((Long) dboFile.getVal(Sv.FILE_ID), extendedInfo);
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
		if (dboFile.getVal(Sv.FILE_STORE_ID) != null
				&& (Long) dboFile.getVal(Sv.FILE_STORE_ID) == svCONST.SYSTEM_FILESTORE_ID) {
			data = systemCache.getIfPresent((Long) dboFile.getVal(Sv.FILE_ID));
			if (data != null && (data.length / 1024 / 1024) <= max_cache_size)
				systemCache.put((Long) dboFile.getVal(Sv.FILE_ID), data);
			else
				log4j.warn(Sv.BIG_FILE_WARN, dboFile.getVal(Sv.FILE_NAME),
						Integer.toString(data != null ? (data.length / 1024 / 1024) : 0), max_cache_size);
		}
		return data;

	}

	/**
	 * Method to read a file from disk or db for a descriptor and return a byte
	 * array.
	 * 
	 * @param dboFile The file descriptor
	 * @return Byte array containing the file data
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
					data = new byte[Integer.valueOf(extendedInfo.get(Sv.FILE_SIZE).toString())];
					IOUtils.read(fstr, data);
				}
			}

		} catch (IOException e) {
			throw (new SvException(Sv.Exceptions.FILEDATA_FS_ERROR, instanceUser, dboFile, null, e));
		} finally {
			closeResource((Closeable) fstr, instanceUser);
		}
		return data;
	}

}
