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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;

public class DbCacheTable {

	// private final String[] uqFields;
	// private final String unqLevel = "";
	private final String CURRENT_TIME = "NOW";

	ArrayList<String> uqSqlCrit = new ArrayList<String>();
	// private ConcurrentHashMap<Integer, DbDataObject> objCache = new
	// ConcurrentHashMap<Integer, DbDataObject>();

	/**
	 * Internal Cache object storing DbDataObjects
	 */
	private Cache<Long, DbDataObject> objCache = null;

	/**
	 * Internal Cache object storing Historical DbDataObjects
	 */
	private Cache<Long, DbDataObject> objHistoryCache = null;

	/**
	 * Map holding pairs of unique values and object Id
	 */
	private Cache<String, DbDataObject> objKeyCache = null;

	private Cache<String, CopyOnWriteArrayList<Long>> objParentIdCache = null;

	private Cache<String, ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>> objLinkedIdCache = null;

	private Cache<String, Long> objHistoryDateCache = null;
	// when the cache is evicting records we need to remove the object support
	// in form of
	// unique maps and parent lists
	private RemovalListener<Long, DbDataObject> onRemove = new RemovalListener<Long, DbDataObject>() {
		public void onRemoval(RemovalNotification<Long, DbDataObject> removal) {
			removeObjectSupport(removal.getValue());
		}
	};

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void cacheConfig(DbDataObject objectdDescriptor) {
		String cType = "LRU_TTL";

		CacheBuilder builder = CacheBuilder.newBuilder();
		if (objectdDescriptor != null) {
			if (objectdDescriptor.getVal("cache_type") != null)
				cType = (String) objectdDescriptor.getVal("cache_type");

			if (cType.equals("LRU") || cType.equals("LRU_TTL")) {
				if (objectdDescriptor.getVal("cache_size") != null)
					builder = builder.maximumSize((Long) objectdDescriptor.getVal("cache_size"));
				else
					builder = builder.maximumSize(5000);

			}

			if (cType.equals("TTL") || cType.equals("LRU_TTL")) {
				if (objectdDescriptor.getVal("cache_expiry") != null)
					builder = builder.expireAfterAccess((Long) objectdDescriptor.getVal("cache_expiry"),
							TimeUnit.MINUTES);
				else
					builder = builder.expireAfterAccess(10, TimeUnit.MINUTES);

			}

		}
		objHistoryCache = (Cache<Long, DbDataObject>) builder.<Long, DbDataObject>build();
		objLinkedIdCache = (Cache<String, ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>>) builder
				.<String, ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>>build();
		objHistoryDateCache = (Cache<String, Long>) builder.<String, Long>build();
		objParentIdCache = (Cache<String, CopyOnWriteArrayList<Long>>) builder
				.<String, CopyOnWriteArrayList<Long>>build();
		objKeyCache = (Cache<String, DbDataObject>) builder.<String, DbDataObject>build();

		if (!cType.equals("PERM"))
			builder = builder.removalListener(onRemove);

		objCache = (Cache<Long, DbDataObject>) builder.<Long, DbDataObject>build();
	}

	DbCacheTable(String[] uqFields, String unqLevel) {
		// this.uqFields = uqFields;
		cacheConfig(null);
	}

	/**
	 * Construct a DbCacheTable based on the object
	 * 
	 * @param objectHeader     DbDataObject describing the type of objects which
	 *                         will be cached (OBJECT_TYPE_TABLE)
	 * @param objectProperties Array of DbDataObject. Each describing the fields of
	 *                         the object (OBJECT_TYPE_FIELD)
	 */
	DbCacheTable(DbDataObject objectdDescriptor, DbDataArray objectProperties) {
		cacheConfig(objectdDescriptor);
	}

	DbDataObject getObject(Long object_id) {
		return getObject(object_id, null);
	}

	DbDataObject getObject(Long object_id, DateTime refDate) {
		Long oid = object_id;
		boolean isHistorical = refDate != null;
		if (isHistorical)
			oid = objHistoryDateCache.getIfPresent(object_id.toString() + Long.toString(refDate.getMillis()));
		if (oid != null)
			return getObjectImpl(oid, isHistorical);
		else
			return null;

	}

	private DbDataObject getObjectImpl(Long object_id, boolean isHistorical) {
		Cache<Long, DbDataObject> currentCache = isHistorical ? objHistoryCache : objCache;
		DbDataObject dbo = currentCache.getIfPresent(object_id);
		if (dbo != null && !dbo.getIs_dirty())
			return dbo;
		else {
			if (dbo != null)
				currentCache.invalidate(object_id);
			return null;
		}
	}

	DbDataArray getObjectsByParentId(Long parent_id) {
		return getObjectsByParentId(parent_id, null);
	}

	DbDataArray getObjectsByParentId(Long parent_id, DateTime refDate) {
		boolean isHistorical = refDate != null;
		String listId = parent_id.toString() + (isHistorical ? Long.toString(refDate.getMillis()) : CURRENT_TIME);
		CopyOnWriteArrayList<Long> children = null;
		children = objParentIdCache.getIfPresent(listId);

		if (children != null) {
			DbDataArray retval = new DbDataArray();
			synchronized (children) {
				Iterator<Long> it = children.iterator();
				while (it.hasNext()) {
					DbDataObject dbo = getObjectImpl(it.next(), isHistorical);
					if (dbo != null)
						retval.addDataItem(dbo);
					else {

						objParentIdCache.invalidate(listId);
						retval = null;
						break;
					}

				}
			}
			return retval;
		} else
			return null;

	}

	/**
	 * Get all objects from the cache linked to LinkObjectId via dbLinkId
	 * 
	 * @param LinkObjectId
	 * @param linkObjectTypeId
	 * @param dbLinkId
	 * @param linkStatus
	 * @return
	 */
	DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId, String linkStatus) {
		return getObjectsByLinkedId(LinkObjectId, linkObjectTypeId, dbLinkId, linkStatus, null);
	}

	/**
	 * Method to fetch a list of objects from the cache based on a link type which
	 * are linked to a specific object id. This version supports use of reference
	 * date for fetching stored object versions from the past.
	 * 
	 * @param LinkObjectId     The Id of the object to which the list of linked
	 *                         objects is related
	 * @param linkObjectTypeId The type of the left hand side object
	 * @param dbLinkId         The id of the link type which describes the relation
	 * @param linkStatus       The status of the link
	 * @param refDate          The reference date for which we want to get the
	 *                         object list
	 * @return
	 */
	DbDataArray getObjectsByLinkedId(Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId, String linkStatus,
			DateTime refDate) {

		boolean isHistorical = refDate != null;

		String mapId = LinkObjectId.toString() + dbLinkId.toString()
				+ (isHistorical ? Long.toString(refDate.getMillis()) : CURRENT_TIME);
		ConcurrentHashMap<String, CopyOnWriteArrayList<Long>> lnkMap = objLinkedIdCache.getIfPresent(mapId);
		if (lnkMap == null)
			return null;

		synchronized (lnkMap) {
			String lstId = linkStatus != null ? linkStatus : "null";
			CopyOnWriteArrayList<Long> children = lnkMap.get(lstId);
			if (children != null) {
				DbDataArray retval = new DbDataArray();
				Iterator<Long> it = children.iterator();
				while (it.hasNext()) {
					DbDataObject dbo = getObjectImpl(it.next(), isHistorical);
					if (dbo != null)
						retval.addDataItem(dbo);
					else {
						objLinkedIdCache.invalidate(mapId);
						retval = null;
						break;
					}

				}
				return retval;
			} else
				return null;
		}
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent data
	 * 
	 * @param obj Object to be cached
	 */
	void addObject(DbDataObject obj) {
		objCache.put(obj.getObject_id(), obj);
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent data
	 * 
	 * @param obj Object to be cached
	 */
	void addObject(DbDataObject obj, String key) {
		objCache.put(obj.getObject_id(), obj);
		if (key != null)
			objKeyCache.put(key, obj);
	}

	/**
	 * Overrided version for backwards compatibility, which doesn't add parent data
	 * 
	 * @param obj Object to be cached
	 */
	DbDataObject getObject(String key) {
		return objKeyCache.getIfPresent(key);
	}

	/**
	 * Method for synching parent metadata
	 * 
	 * @param obj
	 */
	void addObjectParentMetaData(DbDataObject obj) {
		DbDataObject oldDbo = objCache.getIfPresent(obj.getObject_id());
		String parentListId = (obj.getParent_id() != null ? obj.getParent_id().toString() : "0") + CURRENT_TIME;
		// if the object was cached, check if the parent was changed
		synchronized (objParentIdCache) {
			if (oldDbo != null && obj.getParent_id() != oldDbo.getParent_id()) {
				String oldListId = (oldDbo.getParent_id() != null ? oldDbo.getParent_id().toString() : "0")
						+ CURRENT_TIME;

				CopyOnWriteArrayList<Long> oldChildren = objParentIdCache.getIfPresent(oldListId);
				if (oldChildren != null) {
					int oldIx = oldChildren.indexOf(oldDbo.getObject_id());
					if (oldIx >= 0)
						oldChildren.remove(oldIx);
				}
			}
			CopyOnWriteArrayList<Long> children = objParentIdCache.getIfPresent(parentListId);
			if (children == null) {
				children = new CopyOnWriteArrayList<Long>();
				objParentIdCache.put(parentListId, children);
			}
			children.addIfAbsent(obj.getObject_id());
		}

	}

	void removeByParentId(Long parentId) {
		String parentListId = parentId.toString() + CURRENT_TIME;
		objParentIdCache.invalidate(parentListId);
	}

	void addArrayByParentId(DbDataArray objects, Long objectTypeId, Long parentId) {
		addArrayByParentId(objects, objectTypeId, parentId, null, true);
	}

	void addArrayByParentId(DbDataArray objects, Long objectTypeId, Long parentId, boolean executeParentChecks) {
		addArrayByParentId(objects, objectTypeId, parentId, null, executeParentChecks);
	}

	/**
	 * Method to invalidate the old parent list in case the item was previously
	 * cached. The method checks if the version (PKID) of the object has changed. If
	 * it was changed and there is old version in the cache make sure that we remove
	 * the parent list of object IDs.
	 * 
	 * @param dbo    The new version of the object
	 * @param oldDbo The old version of the object. If there is no old version, null
	 *               is acceptable
	 */
	void updateObjectMetadata(DbDataObject dbo, DbDataObject oldDbo) {
		if (oldDbo != null && oldDbo.getPkid() != dbo.getPkid()) {
			if (dbo.getParent_id() != oldDbo.getParent_id()) {
				String oldListID = (oldDbo.getParent_id() != null ? oldDbo.getParent_id().toString() : "0")
						+ CURRENT_TIME;
				objParentIdCache.invalidate(oldListID);
			}
		}
	}

	/**
	 * Method to update the list of children according to a parent, or according to
	 * a list of linked objects as well
	 * 
	 * @param children        The list object IDs of the children (or linked
	 *                        objects)
	 * @param childrenDbArray The new list of objects which
	 * @param parentId        The parent Id for which we want to cache the children.
	 *                        If its a linked list then its null
	 * @param objectTypeId    The object type (we use it to make sure that we don't
	 *                        cache wrong object types)
	 * @param isHistorical    Flag to signify if the list is current or historical
	 */
	private void updateChildList(CopyOnWriteArrayList<Long> children, DbDataArray childrenDbArray, Long parentId,
			Long objectTypeId, boolean isHistorical, boolean executeParentChecks) {
		children.clear(); // make sure we clear the old children list
		Cache<Long, DbDataObject> currentCache = isHistorical ? objHistoryCache : objCache;
		for (DbDataObject dbo : childrenDbArray.getItems()) {
			if (dbo.getObject_type().equals(objectTypeId)) {
				// if we are adding classic children list, make sure that the
				// parent is correct. Otherwise skip.
				if (parentId != null && !dbo.getParent_id().equals(parentId) && executeParentChecks)
					continue;
				// for historical objects use the pkid. for current object_id.
				Long oid = isHistorical ? dbo.getPkid() : dbo.getObject_id();
				// get the currently cached object
				DbDataObject oldDbo = currentCache.getIfPresent(oid);
				// if we aren't in historical mode than refresh the
				// current time lists
				if (!isHistorical)
					updateObjectMetadata(dbo, oldDbo);

				// if there's no old object or the version was upgrade, replace
				// the old
				if (oldDbo == null || oldDbo.getPkid() != dbo.getPkid())
					currentCache.put(oid, dbo);
				// add the id to the list
				children.add(oid);

			}
		}
	}

	void addArrayByParentId(DbDataArray objects, Long objectTypeId, Long parentId, DateTime refDate,
			boolean executeParentChecks) {
		boolean isHistorical = refDate != null;
		String parentListId = parentId.toString() + (isHistorical ? Long.toString(refDate.getMillis()) : CURRENT_TIME);
		CopyOnWriteArrayList<Long> children = null;
		CopyOnWriteArrayList<Long> value = new CopyOnWriteArrayList<Long>();
		children = objParentIdCache.asMap().putIfAbsent(parentListId, value);
		if (children == null)
			children = value;
		synchronized (children) {
			updateChildList(children, objects, parentId, objectTypeId, isHistorical, executeParentChecks);
		}

	}

	/**
	 * Method to invalidate the current link cache. This method will not invalidate
	 * historical cache
	 * 
	 * @param LinkObjectId The left hand side object id to which objects are linked
	 *                     via specific link type
	 * @param dbLinkId     The Id of the link type which should be used for
	 *                     invalidation
	 */
	void invalidateLinkCache(Long LinkObjectId, Long dbLinkId) {
		String lstId = LinkObjectId.toString() + dbLinkId.toString() + CURRENT_TIME;
		objLinkedIdCache.invalidate(lstId);
	}

	/**
	 * Method for adding array of objects linked to a specific Object ID by the
	 * specified LinkId/Status.
	 * 
	 * @param objects          The array of DbDataObjects to be added to the cache
	 * @param LinkObjectId     The id of the object to which the content of
	 *                         "objects" is linked
	 * @param linkObjectTypeId The type of the left hand side object
	 * @param dbLinkId         The id of link type which is used for linking
	 * @param linkStatus       The status of the link
	 */
	void addArrayByLinkedId(DbDataArray objects, Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId,
			String linkStatus) {
		addArrayByLinkedId(objects, LinkObjectId, linkObjectTypeId, dbLinkId, linkStatus, null);
	}

	/**
	 * Method for adding array of objects linked to a specific Object ID by the
	 * specified LinkId/Status. This version enables usage of reference date in
	 * order to speed up frequent fetching of data from the past
	 * 
	 * @param objects          The array of DbDataObjects to be added to the cache
	 * @param LinkObjectId     The id of the object to which the content of
	 *                         "objects" is linked
	 * @param linkObjectTypeId The type of the left hand side object
	 * @param dbLinkId         The id of link type which is used for linking
	 * @param linkStatus       The status of the link
	 * @param refDate          The reference date for which the list should cached
	 */
	void addArrayByLinkedId(DbDataArray objects, Long LinkObjectId, Long linkObjectTypeId, Long dbLinkId,
			String linkStatus, DateTime refDate) {
		CopyOnWriteArrayList<Long> children = null;
		boolean isHistorical = refDate != null;
		synchronized (objLinkedIdCache) {
			String mapId = LinkObjectId.toString() + dbLinkId.toString()
					+ (isHistorical ? Long.toString(refDate.getMillis()) : CURRENT_TIME);
			String lstId = linkStatus != null ? linkStatus : "null";

			ConcurrentHashMap<String, CopyOnWriteArrayList<Long>> lnkMap = objLinkedIdCache.getIfPresent(mapId);
			if (lnkMap == null) {
				lnkMap = new ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>();
				objLinkedIdCache.put(mapId, lnkMap);
			} else {
				children = lnkMap.get(lstId);
			}
			if (children == null) {
				children = new CopyOnWriteArrayList<Long>();
				lnkMap.put(lstId, children);
			}
		}
		synchronized (children) {
			updateChildList(children, objects, null, linkObjectTypeId, isHistorical, true);
		}
	}

	public class DboComparator implements Comparator<Long> {

		String keyName;
		Class<?> compareBaseClass;

		public DboComparator(String keyName, Class<?> compareBaseClass) {
			this.keyName = keyName;
			this.compareBaseClass = compareBaseClass;
		}

		@Override
		public int compare(Long i1, Long i2) {
			DbDataObject o1 = objCache.getIfPresent(i1);
			DbDataObject o2 = objCache.getIfPresent(i2);
			if (compareBaseClass.equals(String.class))
				return ((String) o1.getVal(keyName)).compareTo((String) o2.getVal(keyName));
			if (compareBaseClass.equals(Long.class))
				return ((Long) o1.getVal(keyName)).compareTo((Long) o2.getVal(keyName));
			if (compareBaseClass.equals(BigDecimal.class))
				return ((BigDecimal) o1.getVal(keyName)).compareTo((BigDecimal) o2.getVal(keyName));
			if (compareBaseClass.equals(DateTime.class))
				return ((DateTime) o1.getVal(keyName)).compareTo((DateTime) o2.getVal(keyName));
			// TODO Auto-generated method stub
			return 0;
		}
	}

	/**
	 * Methog to sort the index by PARENT_ID according to a key name
	 * 
	 * @param parentId Id of the parent which should be sorted
	 * @param keyName  KeyName according to which the comparison should be done
	 */
	synchronized void sortParentIdx(Long parentId, String keyName, Class<?> objType) {
		String parentListId = parentId.toString() + CURRENT_TIME;
		CopyOnWriteArrayList<Long> parentIdx = objParentIdCache.getIfPresent(parentListId);
		if (parentIdx != null)
			Collections.sort(parentIdx, new DboComparator(keyName, objType));
	}

	/**
	 * Method for cleaning the table cache
	 */
	synchronized void clean() {
		objKeyCache.cleanUp();
		objParentIdCache.cleanUp();
		objLinkedIdCache.cleanUp();
		objCache.cleanUp();
		objHistoryCache.cleanUp();
		objHistoryDateCache.cleanUp();
	}

	void removeObject(Long object_id, String key) {
		DbDataObject dbo = objCache.getIfPresent(object_id);
		if (dbo != null) {
			objCache.invalidate(dbo.getObject_id());
			if (key != null)
				objKeyCache.invalidate(key);
			removeObjectSupport(dbo);
		}

	}

	void removeObjectSupport(DbDataObject dbo) {
		if (dbo != null) {
			removeObjectSupport(dbo.getParentId());
		}

	}

	void removeObjectSupport(Long parentId) {
		synchronized (objParentIdCache) {
			String parentListId = parentId.toString() + CURRENT_TIME;
			objParentIdCache.invalidate(parentListId);

		}
	}
}
