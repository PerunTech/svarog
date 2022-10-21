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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog.svCONST;

/**
 * The global Svarog caching system. It manages a map of DbCacheTables which in
 * turn manage the cache per different object types
 * 
 * @author XPS13
 *
 */
public class DbCache {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(DbCache.class);

	/**
	 * Map containing all DbCacheTables defined in the system
	 */
	static private Map<Long, DbCacheTable> cacheStorage = new ConcurrentHashMap<Long, DbCacheTable>();

	/**
	 * Static block to initialise the system cache storage
	 */
	static {
		initCache();
	}

	/**
	 * Method that initialises the cache. During initialisation, the cache for
	 * object types: TABLE, FIELD, CODE is prepared.
	 * 
	 * @return True/false if initialisation was successful
	 */
	private static boolean initCache() {
		if (cacheStorage.get(svCONST.OBJECT_TYPE_TABLE) == null) {
			/*
			 * Configure new cache for the SVAROG TABLES object
			 */
			String[] uqFields = new String[] { Sv.SCHEMA.toString(), Sv.TABLE_NAME.toString() };
			DbCacheTable tbl = new DbCacheTable(uqFields, Sv.TABLE);
			cacheStorage.put(svCONST.OBJECT_TYPE_TABLE, tbl);

			/*
			 * Configure new cache for the SVAROG FIELDS object
			 */
			uqFields = new String[] { Sv.FIELD_NAME.toString() };
			cacheStorage.put(svCONST.OBJECT_TYPE_FIELD, new DbCacheTable((uqFields), Sv.PARENT));

			/*
			 * Configure new cache for the SVAROG FIELDS object (Sorted by sort order)
			 */
			uqFields = new String[] { Sv.FIELD_NAME.toString() };
			cacheStorage.put(svCONST.OBJECT_TYPE_FIELD_SORT, new DbCacheTable((uqFields), Sv.PARENT));

			/*
			 * Configure new cache for the SVAROG CODES object
			 */
			uqFields = new String[] { Sv.CODE_VALUE };
			cacheStorage.put(svCONST.OBJECT_TYPE_CODE, new DbCacheTable((uqFields), Sv.PARENT));

		}
		return true;
	}

	/**
	 * Method for fetching an object from the cache based on Id and Type
	 * 
	 * @param objectId   Id of the object which should be fetched
	 * @param objectType Type Id of the object
	 * @return A DbDataObject instance representing the type/id pair
	 */
	static DbDataObject getObject(Long objectId, Long objectType) {
		DbCacheTable dbc = cacheStorage.get(objectType);
		if (dbc != null)
			return dbc.getObject(objectId);
		else
			return null;
	}

	/**
	 * Method for fetching a object from the cache by unique key
	 * 
	 * @param key        The unique key identifying the object
	 * @param objectType the object type
	 * @return A DbDataObject instance identified by the key
	 */
	static DbDataObject getObject(String key, Long objectType) {

		DbCacheTable dbc = cacheStorage.get(objectType);
		if (dbc != null)
			return dbc.getObject(key);
		else
			return null;
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent data
	 * 
	 * @param obj Object to be cached
	 */
	static void addObject(DbDataObject obj) {
		addObject(obj, null, false);
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent data
	 * 
	 * @param obj Object to be cached
	 */
	static void addObject(DbDataObject obj, String key) {
		addObject(obj, key, false);
	}

	/**
	 * Method for fetching a DbCacheTable object from the cache. If there is no
	 * DbCacheTable for the specific type it creates it.
	 * 
	 * @param typeId Id of the type of object which should be stored in the
	 *               DbCacheTable
	 * @return
	 */
	static DbCacheTable getDbCacheTable(Long typeId) {
		DbCacheTable dbc = cacheStorage.get(typeId);
		// if no DbCacheTable exists, then we should create one before adding
		// the object
		if (dbc == null) {
			// we need the object description so we can understand if we need
			// to cache it.
			DbDataObject objectDescriptor = cacheStorage.get(svCONST.OBJECT_TYPE_TABLE).getObject(typeId);

			if ((Boolean) objectDescriptor.getVal("use_cache")) {
				// get the fields for the table
				DbDataArray objectProperties = cacheStorage.get(svCONST.OBJECT_TYPE_FIELD_SORT)
						.getObjectsByParentId(typeId);

				// init a new DbCacheTable with the fields
				dbc = new DbCacheTable(objectDescriptor, objectProperties);

				// add the DbCacheTable into the Cache
				cacheStorage.put(typeId, dbc);

				// finally add the object to the new cache
			}
		}
		return dbc;
	}

	/**
	 * Method to create a cache builder instance, configured from the
	 * objectDescriptor object. If the descriptor doesn't contain TTL/LRU
	 * configuration, system defaults will be used
	 * 
	 * @param objectDescriptor The object type from which the cache should be
	 *                         configured
	 */
	@SuppressWarnings("rawtypes")
	public static CacheBuilder createBuilder(DbDataObject objectDescriptor) {
		return createBuilder(objectDescriptor, null, null);
	}

	/**
	 * Method to create a cache builder instance, configured from the
	 * objectDescriptor object
	 * 
	 * @param objectDescriptor The object type from which the cache should be
	 *                         configured
	 * @param cacheSize        The size of the cache which shall be used in case the
	 *                         object type doesn't specify
	 * @param cacheExpiry      The time in minutes for expiry of the cached objects,
	 *                         if the object descriptor doesn't specify any
	 * @return The CacheBuilder instance configured by objectDescriptor
	 */
	@SuppressWarnings("rawtypes")
	public static CacheBuilder createBuilder(DbDataObject objectDescriptor, Long cacheSize, Long cacheExpiry) {
		CacheBuilder builder = CacheBuilder.newBuilder();
		Long lSize = cacheSize != null ? cacheSize : Sv.DEFAULT_CACHE_SIZE;
		Long lExpiry = cacheExpiry != null ? cacheExpiry : Sv.DEFAULT_CACHE_TTL;
		String cacheType = Sv.LRU_TTL;
		if (objectDescriptor != null) {
			cacheType = (String) objectDescriptor.getVal(Sv.CACHE_TYPE);
			lSize = objectDescriptor.getVal(Sv.CACHE_SIZE) == null ? lSize
					: (Long) objectDescriptor.getVal(Sv.CACHE_SIZE);
			lExpiry = objectDescriptor.getVal(Sv.CACHE_EXPIRY) == null ? lExpiry
					: (Long) objectDescriptor.getVal(Sv.CACHE_EXPIRY);
		}
		if (!Sv.PERM.equals(cacheType)) {
			builder = builder.expireAfterAccess(lSize, TimeUnit.MINUTES);
			builder = builder.maximumSize(lExpiry);
		}
		return builder;
	}

	/**
	 * Method that adds an object to the in-memory cache
	 * 
	 * @param obj Object to be cached
	 */
	static void addObject(DbDataObject obj, String key, Boolean addParentData) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(obj.getObjectType());
		if (dbc != null) {
			dbc.addObject(obj, key);
			if (addParentData)
				dbc.addObjectParentMetaData(obj);
		}
		// if the object which is added is of type OBJECT_TYPE_FIELD then
		// sort the fields and add it to a another cache OBJECT_TYPE_FIELD_SORT
		if (obj.getObjectType() == svCONST.OBJECT_TYPE_FIELD) {
			DbCacheTable dbcs = cacheStorage.get(svCONST.OBJECT_TYPE_FIELD_SORT);
			// perform sort
			dbcs.addObject(obj);
			if (addParentData)
				dbcs.addObjectParentMetaData(obj);
			dbcs.sortParentIdx(obj.getParentId(), Sv.SORT_ORDER, obj.getVal(Sv.SORT_ORDER).getClass());
		}

	}

	/**
	 * Method for fetching array of children objects from the cache based on parent
	 * and type
	 * 
	 * @param parentId   Id of the parent
	 * @param objectType Id of the type of parent
	 * @return
	 */
	static DbDataArray getObjectsByParentId(Long parentId, Long objectType) {
		DbCacheTable tbl = cacheStorage.get(objectType);
		if (tbl != null)
			return tbl.getObjectsByParentId(parentId);
		return null;
	}

	/**
	 * Method for fetching array of linked objects from the cache based on linked
	 * object id , type id and link type id
	 * 
	 * @param parentId   Id of the parent
	 * @param objectType Id of the type of parent
	 * @return
	 */
	static DbDataArray getObjectsByLinkedId(Long linkObjectId, Long linkObjectTypeId, Long dbLinkId, Long objectType,
			String linkStatus) {
		DbCacheTable tbl = cacheStorage.get(objectType);
		if (tbl != null)
			return tbl.getObjectsByLinkedId(linkObjectId, linkObjectTypeId, dbLinkId, linkStatus);
		return null;
	}

	/**
	 * Method which stores an array of items in the cache
	 * 
	 * @param arr
	 */
	static void addArray(DbDataArray arr) {
		for (int z = 0; z < arr.getItems().size(); z++) {
			DbDataObject obj = arr.getItems().get(z);
			DbCache.addObject(obj);
		}
	}

	/**
	 * Method which stores an array of items in the cache, while it updates their
	 * parent links and types. This method is used for initial Svarog initialisation
	 * ONLY, do not use it for anything else !!!
	 * 
	 * @param dboArray the array of system configurations
	 */
	static void addArrayWithParent(DbDataArray dboArray) {
		for (DbDataObject obj : dboArray.getItems()) {
			if (obj != null)
				DbCache.addObject(obj, null, true);
			else
				log4j.warn(
						"Bad system configuration! Adding system config objects to the cache contains null pointers");
		}
	}

	/**
	 * Method which adds a number of DbDataObjects to the cache. All objects by a
	 * single type and parent Elements in the array which are not of same type and
	 * parent are ignored, the objects with parent different than the value of
	 * parentId parameter will be ignored
	 * 
	 * @param dboArray            The array of DbDataObjects of same type
	 * @param objectTypeId        The type of objects in the array, identifying the
	 *                            cache type to which the objects will be added
	 * @param parentId            The ID of the parent under which the objects will
	 *                            be added
	 * @param executeParentChecks Flag if the parent checks shall be executed. If
	 *                            yes, the objects with parent different than the
	 *                            value of parentId parameter will be ignored
	 */
	static void addArrayByParentId(DbDataArray dboArray, Long objectTypeId, Long parentId,
			boolean executeParentChecks) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(objectTypeId);
		if (dbc != null)
			dbc.addArrayByParentId(dboArray, objectTypeId, parentId, executeParentChecks);

	}

	/**
	 * Method which adds a number of DbDataObjects to the cache. All objects by a
	 * single type and parent Elements in the array which are not of same type and
	 * parent are ignored, the objects with parent different than the value of
	 * parentId parameter will be ignored
	 * 
	 * @param dboArray     The array of DbDataObjects of same type
	 * @param objectTypeId The type of objects in the array, identifying the cache
	 *                     type to which the objects will be added
	 * @param parentId     The ID of the parent under which the objects will be
	 *                     added
	 */
	static void addArrayByParentId(DbDataArray arr, Long objectTypeId, Long parentId) {
		addArrayByParentId(arr, objectTypeId, parentId, true);

	}

	/**
	 * Method for adding array of objects of type specified by objectTypeId linked
	 * to a specific Object identified by LinkObjectId of type linkObjectTypeId by
	 * the specified LinkId/Status.
	 * 
	 * @param objects          The array of DbDataObjects to be added to the cache
	 * @param LinkObjectId     The id of the object to which the content of
	 *                         "objects" is linked
	 * @param linkObjectTypeId The type of the left hand side object
	 * @param dbLinkId         The id of link type which is used for linking
	 * @param objectTypeId     the type of objects which are cached
	 * @param linkStatus       The status of the link
	 */
	static void addArrayByLinkedId(DbDataArray arr, Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId,
			Long objectTypeId, String linkStatus) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(objectTypeId);
		if (dbc != null)
			dbc.addArrayByLinkedId(arr, LinkObjectId, linkObjectTypeId, dbLinkId, linkStatus);

	}

	/**
	 * Method to invalidate/remove the objects of type objectTypeId linked to
	 * LinkObjectId via link identified by dbLinkId
	 * 
	 * @param LinkObjectId The object to which the others are linked
	 * @param dbLinkId     The link type which is used to describe the link
	 * @param objectTypeId the type of objects which are cached
	 */
	static void invalidateLinkCache(Long LinkObjectId, Long dbLinkId, Long objectTypeId) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(objectTypeId);
		if (dbc != null)
			dbc.invalidateLinkCache(LinkObjectId, dbLinkId);

	}

	/**
	 * Method for removing object from the cache
	 * 
	 * @param objectId   Id of the object to be removed
	 * @param objectType Type Id of the object to be removed
	 */
	static void removeObject(Long objectId, Long objectType) {
		DbCacheTable tbl = cacheStorage.get(objectType);
		if (tbl != null)
			tbl.removeObject(objectId, null);
	}

	/**
	 * Method for removing object from the cache
	 * 
	 * @param objectId   Id of the object to be removed
	 * @param objectType Type Id of the object to be removed
	 */
	static void removeObject(Long objectId, String key, Long objectType) {
		DbCacheTable tbl = cacheStorage.get(objectType);
		if (tbl != null)
			tbl.removeObject(objectId, key);
	}

	/**
	 * Method for removing supporting object information such as parent/child
	 * relationships which are cached
	 * 
	 * @param dbo The object whose parent list should be removed
	 */
	static void removeObjectSupport(DbDataObject dbo) {
		if (dbo != null) {
			removeObjectSupport(dbo.getParentId(), dbo.getObjectType());
		}

	}

	/**
	 * Method for removing supporting object information such as parent/child
	 * relationships which are cached
	 * 
	 * @param dbo The object whose parent list should be removed
	 */
	static void removeObjectSupport(Long parentId, Long objectType) {
		DbCacheTable tbl = cacheStorage.get(objectType);
		if (tbl != null)
			tbl.removeObjectSupport(parentId);
	}

	/**
	 * Method for removing all children of the specified object and type from the
	 * cache
	 * 
	 * @param objectTypeId The type of children which should be removed
	 * @param parentId     The id of the parent whose children should be removed
	 */
	static void removeByParentId(Long objectTypeId, Long parentId) {
		DbCacheTable tbl = cacheStorage.get(objectTypeId);
		if (tbl != null)
			tbl.removeByParentId(parentId);

	}

	/**
	 * Method which cleans the cache for certain object type
	 * 
	 * @param objectType Id of the object type for which the cache should be cleaned
	 */
	static void cleanObjectType(Long objectType) {
		DbCacheTable tbl = cacheStorage.get(objectType);
		if (tbl != null)
			tbl.clean();
	}

	/**
	 * Method to reset the full Svarog cache
	 */
	static void clean() {
		cacheStorage.clear();
		initCache();
	}
}
