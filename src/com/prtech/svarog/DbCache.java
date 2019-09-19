/*******************************************************************************
 * Copyright (c) 2013, 2017 Perun Technologii DOOEL Skopje.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License
 * Version 2.0 or the Svarog License Agreement (the "License");
 * You may not use this file except in compliance with the License. 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See LICENSE file in the project root for the specific language governing 
 * permissions and limitations under the License.
 *
 *******************************************************************************/
package com.prtech.svarog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
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
	 * Map containing all DbCacheTables defined in the system
	 */
	static private Map<Long, DbCacheTable> cacheStorage = new ConcurrentHashMap<Long, DbCacheTable>();
	/**
	 * Map containing all DbDataTables defined in the system, accessible via
	 * object_id
	 */

	private static Boolean isInitialised = initCache();

	/**
	 * Method that initialises the cache. During initialisation, the cache for
	 * object types: TABLE, FIELD, CODE is prepared.
	 * 
	 * @return True/false if initialisation was successful
	 */
	private static boolean initCache() {
		if (cacheStorage.get(svCONST.OBJECT_TYPE_TABLE) == null) {
			String[] uqFields = new String[] { "SCHEMA", "TABLE_NAME" };
			DbCacheTable tbl = new DbCacheTable(uqFields, "TABLE");
			cacheStorage.put(svCONST.OBJECT_TYPE_TABLE, tbl);
			/*
			 * tableIdCache.put(schema + "." + master_repo + "_tables",
			 * svCONST.getInt("OBJECT_TYPE_TABLE"));
			 */
			uqFields = new String[] { "FIELD_NAME" };
			cacheStorage.put(svCONST.OBJECT_TYPE_FIELD, new DbCacheTable((uqFields), "PARENT"));

			// additional cache for keeping sorted version
			uqFields = new String[] { "FIELD_NAME" };
			cacheStorage.put(svCONST.OBJECT_TYPE_FIELD_SORT, new DbCacheTable((uqFields), "PARENT"));

			/*
			 * tableIdCache.put(schema + "." + master_repo + "_fields",
			 * svCONST.getInt("OBJECT_TYPE_FIELD"));
			 */
			uqFields = new String[] { "CODE_VALUE" };
			cacheStorage.put(svCONST.OBJECT_TYPE_CODE, new DbCacheTable((uqFields), "PARENT"));
			/*
			 * tableIdCache.put(schema + "." + master_repo + "_codes",
			 * svCONST.OBJECT_TYPE_CODE);
			 */

		}
		return true;
	}

	/**
	 * Method for fetching an object from the cache based on Id and Type
	 * 
	 * @param object_id
	 *            Id of the object which should be fetched
	 * @param object_type
	 *            Type Id of the object
	 * @return A DbDataObject instance representing the type/id pair
	 */
	static DbDataObject getObject(Long object_id, Long object_type) {
		DbCacheTable dbc = cacheStorage.get(object_type);
		if (dbc != null)
			return dbc.getObject(object_id);
		else
			return null;
	}

	/**
	 * Method for fetching a object from the cache by unique key
	 * 
	 * @param key
	 *            The unique key identifying the object
	 * @param object_type
	 *            the object type
	 * @return A DbDataObject instance identified by the key
	 */
	static DbDataObject getObject(String key, Long object_type) {

		DbCacheTable dbc = cacheStorage.get(object_type);
		if (dbc != null)
			return dbc.getObject(key);
		else
			return null;
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent
	 * data
	 * 
	 * @param obj
	 *            Object to be cached
	 */
	static void addObject(DbDataObject obj) {
		addObject(obj, null, false);
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent
	 * data
	 * 
	 * @param obj
	 *            Object to be cached
	 */
	static void addObject(DbDataObject obj, String key) {
		addObject(obj, key, false);
	}

	/**
	 * Method for fetching a DbCacheTable object from the cache. If there is no
	 * DbCacheTable for the specific type it creates it.
	 * 
	 * @param typeId
	 *            Id of the type of object which should be stored in the
	 *            DbCacheTable
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
	 * Method that adds an object to the in-memory cache
	 * 
	 * @param obj
	 *            Object to be cached
	 */
	static void addObject(DbDataObject obj, String key, Boolean addParentData) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(obj.getObject_type());
		if (dbc != null) {
			dbc.addObject(obj, key);
			if (addParentData)
				dbc.addObjectParentMetaData(obj);
		}
		// if the object which is added is of type OBJECT_TYPE_FIELD then
		// sort the fields and add it to a another cache OBJECT_TYPE_FIELD_SORT
		if (obj.getObject_type() == svCONST.OBJECT_TYPE_FIELD) {
			DbCacheTable dbcs = cacheStorage.get(svCONST.OBJECT_TYPE_FIELD_SORT);
			// perform sort
			dbcs.addObject(obj);
			if (addParentData)
				dbcs.addObjectParentMetaData(obj);
			dbcs.sortParentIdx(obj.getParent_id(), "SORT_ORDER", obj.getVal("SORT_ORDER").getClass());
		}

	}

	/**
	 * Method for fetching array of children objects from the cache based on
	 * parent and type
	 * 
	 * @param parent_id
	 *            Id of the parent
	 * @param object_type
	 *            Id of the type of parent
	 * @return
	 */
	static DbDataArray getObjectsByParentId(Long parent_id, Long object_type) {
		DbCacheTable tbl = cacheStorage.get(object_type);
		if (tbl != null)
			return tbl.getObjectsByParentId(parent_id);
		return null;
	}

	/**
	 * Method for fetching array of linked objects from the cache based on
	 * linked object id , type id and link type id
	 * 
	 * @param parentId
	 *            Id of the parent
	 * @param object_type
	 *            Id of the type of parent
	 * @return
	 */
	static DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId, Long object_type,
			String linkStatus) {
		DbCacheTable tbl = cacheStorage.get(object_type);
		if (tbl != null)
			return tbl.getObjectsByLinkedId(LinkObjectId, linkObjectTypeId, dbLinkId, linkStatus);
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
	 * Method which stores an array of items in the cache, while it updates
	 * their parent links and types. This method is used for initial Svarog
	 * initialisation ONLY, do not use it for anything else !!!
	 * 
	 * @param arr
	 */
	static void addArrayWithParent(DbDataArray arr) {
		for (int z = 0; z < arr.getItems().size(); z++) {
			DbDataObject obj = arr.getItems().get(z);
			if (obj != null)
				DbCache.addObject(obj, null, true);
			else
				System.out.println("null config?");
		}
	}

	/**
	 * This version is faster compared to addArrayWithParent, but its limited to
	 * add all objects by a single type and parent Elements in the array which
	 * are not of same type and parent are ignored
	 * 
	 * @param arr
	 *            The array of DbDataObjects (MUST BE OF SAME TYPE)
	 *
	 */
	static void addArrayByParentId(DbDataArray arr, Long objectTypeId, Long parentId, boolean executeParentChecks) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(objectTypeId);
		if (dbc != null)
			dbc.addArrayByParentId(arr, objectTypeId, parentId, executeParentChecks);

	}

	static void addArrayByParentId(DbDataArray arr, Long objectTypeId, Long parentId) {
		addArrayByParentId(arr, objectTypeId, parentId, true);

	}

	static void addArrayByLinkedId(DbDataArray arr, Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId,
			Long objectTypeId, String linkStatus) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(objectTypeId);
		if (dbc != null)
			dbc.addArrayByLinkedId(arr, LinkObjectId, linkObjectTypeId, dbLinkId, linkStatus);

	}

	static void invalidateLinkCache(Long LinkObjectId, Long dbLinkId, Long objectTypeId) {
		// get the DbCacheTable which holds the actual in-memory cache
		DbCacheTable dbc = getDbCacheTable(objectTypeId);
		if (dbc != null)
			dbc.invalidateLinkCache(LinkObjectId, dbLinkId);

	}

	/**
	 * Method for removing object from the cache
	 * 
	 * @param object_id
	 *            Id of the object to be removed
	 * @param object_type
	 *            Type Id of the object to be removed
	 */
	static void removeObject(Long object_id, Long object_type) {
		DbCacheTable tbl = cacheStorage.get(object_type);
		if (tbl != null)
			tbl.removeObject(object_id, null);
	}

	/**
	 * Method for removing object from the cache
	 * 
	 * @param object_id
	 *            Id of the object to be removed
	 * @param object_type
	 *            Type Id of the object to be removed
	 */
	static void removeObject(Long object_id, String key, Long object_type) {
		DbCacheTable tbl = cacheStorage.get(object_type);
		if (tbl != null)
			tbl.removeObject(object_id, key);
	}

	static void removeObjectSupport(DbDataObject dbo) {
		if (dbo != null) {
			DbCacheTable tbl = cacheStorage.get(dbo.getObject_type());
			if (tbl != null)
				tbl.removeObjectSupport(dbo);
		}

	}

	/**
	 * Method for removing all children
	 * 
	 * @param objectTypeId
	 * @param parentId
	 */
	static void removeByParentId(Long objectTypeId, Long parentId) {
		DbCacheTable tbl = cacheStorage.get(objectTypeId);
		if (tbl != null)
			tbl.removeByParentId(parentId);

	}

	/**
	 * Method which cleans the cache for certain object type
	 * 
	 * @param object_type
	 *            Id of the object type for which the cache should be cleaned
	 */
	static void cleanObjectType(Long object_type) {
		DbCacheTable tbl = cacheStorage.get(object_type);
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
