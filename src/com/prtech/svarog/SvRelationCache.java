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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryExpression;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.ISvOnSave;
import com.prtech.svarog_common.DbQueryObject.DbJoinType;
import com.prtech.svarog_common.DbQueryObject.LinkType;

/**
 * Class for managing a cache of related objects in order to present them in a
 * unified way.
 * 
 * @author Perun.Tech.01
 *
 */
public class SvRelationCache {

	/**
	 * Callback class to cleanup the cache according to changes in Svarog
	 * 
	 * @author XPS13
	 *
	 */
	public class SvRelationCacheCallback implements ISvOnSave {
		final SvRelationCache cache;

		@Override
		public boolean beforeSave(SvCore parentCore, DbDataObject dbo) {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public void afterSave(SvCore parentCore, DbDataObject dbo) {
			// ensure that the cache invalidation checks are not executed when
			// the cache is already marked as dirty of building the dataset is
			// in progress
			if (cache.isRoot() && !cache.isCacheDirty)
				if (cache.shouldInvalidateCache(dbo))
					cache.invalidateCache();
		}

		SvRelationCacheCallback(SvRelationCache cache) {
			this.cache = cache;
		}

	}

	/**
	 * Array of items result from the query
	 */
	DbDataArray cachedItems = null;

	/**
	 * Flag if there was underlying change in the database and the cache should
	 * refreshed
	 */
	boolean isCacheDirty = true;
	/**
	 * Local variable holding the callback instance used for cleanup
	 */
	protected SvRelationCacheCallback callback = null;

	/**
	 * Method to return the callback for cache cleanup
	 * 
	 * @return
	 */
	public ISvOnSave getCallback() {
		if (callback == null)
			callback = new SvRelationCacheCallback(this);
		return callback;
	}

	/**
	 * Method used to invalidate the currently cached items.
	 */
	public void invalidateCache() {
		isCacheDirty = true;
	}

	/**
	 * The descriptor of the right hand side relation
	 */
	DbDataObject dbt = null;

	/**
	 * The search criteria for loading the objects in the relation
	 */
	DbSearchCriterion search = null;

	/**
	 * The search criteria for loading the objects in the relation
	 */
	DbSearchCriterion searchExt = null;

	/**
	 * Reference to the left hand side relation
	 */
	SvRelationCache parent = null;

	/**
	 * Reference to the root relation
	 */
	SvRelationCache root = null;

	/**
	 * Alias for prefixing the column names in the result object
	 */
	String objectAlias = "";

	/**
	 * A link type to describe the relation of the Cache instance to its parent
	 */
	LinkType relationToParent = null;

	/**
	 * The name of the field used for denormalised relations on the left hand
	 * side (the current Dqo side of the join)
	 */
	String denormFieldName = null;

	
	/**
	 * The name of the field used for denormalised relations on the left hand
	 * side (the previous Dqo side of the join)
	 */
	String denormPreviousFieldName = null;
	
	/**
	 * The link descriptor object if the relation to parent is DBLINK
	 */
	DbDataObject dblt = null;

	/**
	 * The reference date of the node
	 */
	DateTime referenceDate = null;

	/**
	 * The reference date of the node
	 */
	DateTime linkReferenceDate = null;

	/**
	 * The join type used establish the relationship to the parent
	 */
	DbJoinType joinToParent = DbJoinType.INNER;

	/**
	 * List of statuses valid for the link to the parent
	 */
	ArrayList<String> linkStatusList;

	/**
	 * The name of the field used for denormalised relations to parent/left hand
	 * side
	 */
	ArrayList<SvRelationCache> cacheList = new ArrayList<SvRelationCache>();

	/**
	 * Flag to tell the query to include the link objets too
	 */
	Boolean returnLinkObjects = false;

	/**
	 * Flag to tell the query to return or not the columns
	 */
	Boolean isReturnType = true;

	/**
	 * Map holding all object Ids in the complex relation
	 */
	HashMap<String, ArrayDeque<Long>> objectIdMap = new HashMap<String, ArrayDeque<Long>>();

	public SvRelationCache(DbDataObject dbt, DbSearchCriterion search, String objectAlias, LinkType relationToParent,
			String denormFieldName, DbDataObject dblt, DateTime referenceDate) throws SvException {
		configureCache(dbt, search, objectAlias, relationToParent, denormFieldName, dblt, referenceDate);
	}

	/**
	 * Public constructor of the cache item
	 * 
	 * @param dbt
	 *            The table descriptor used to describe the cached objects
	 * @param search
	 *            The criterion for filtering the dataset
	 * @param objectAlias
	 *            The alias to be used for prefixing the objects in the complex
	 *            array
	 * @param relationToParent
	 *            Link type to describe the relation to the paren
	 * @param denormFieldName
	 *            If the relation is denormalized, then register the
	 *            denormalisation field
	 * @param dblt
	 *            The link descriptor in case the relation is by using DbLink
	 * @param referenceDate
	 *            The reference date for this specific part of the dataset
	 * @throws SvException
	 *             Throws exception if basic configuration can not be achieved.
	 */
	public void configureCache(DbDataObject dbt, DbSearchCriterion search, String objectAlias,
			LinkType relationToParent, String denormFieldName, DbDataObject dblt, DateTime referenceDate)
			throws SvException {
		this.dbt = dbt;
		this.search = search;
		this.objectAlias = objectAlias;
		this.relationToParent = relationToParent;
		this.denormFieldName = denormFieldName;
		this.referenceDate = referenceDate;
		this.dblt = dblt;
		if (dbt == null || objectAlias == null)
			throw (new SvException("system.error.relation_cache_no_dbt", svCONST.systemUser, null, this));

		if (relationToParent != null) {
			if ((relationToParent.equals(LinkType.DBLINK) || relationToParent.equals(LinkType.DBLINK_REVERSE))
					&& this.dblt == null)
				throw (new SvException("system.error.relation_link_no_dblt", svCONST.systemUser, null, this));
			if ((relationToParent.equals(LinkType.DENORMALIZED)
					|| relationToParent.equals(LinkType.DENORMALIZED_REVERSE)) && this.denormFieldName == null)
				throw (new SvException("system.error.relation_denorm_no_field", svCONST.systemUser, null, this));

			if (relationToParent.equals(LinkType.CUSTOM) || relationToParent.equals(LinkType.CUSTOM_FREETEXT))
				throw (new SvException("system.error.relation_custom", svCONST.systemUser, null, this));
		}
	}

	/**
	 * Method to check SvRelation list if there already a user defined object
	 * 
	 * @return True if the list contains a user object (is_config_table=false)
	 */
	boolean hasLinkedUserDefinedObject() {
		boolean hasUserObject = false;
		for (SvRelationCache link : this.cacheList) {
			if (link.getDbt().getVal("is_config_table").equals(false)) {
				hasUserObject = true;
				break;
			}
		}
		return hasUserObject;
	}

	/**
	 * Method to match the current search criteria to the updated object. From
	 * the list of repo fields, only the object id and the parent id are
	 * checked.
	 * 
	 * @param dbo
	 *            The object to be matched against the search criterion
	 * @return True if the object is a match.
	 */
	boolean isSearchMatched(DbDataObject dbo) {
		boolean isMatch = true;
		if (this.search != null && search.getFieldName() != null) {
			String searchField = search.getFieldName();
			isMatch = searchField.equals("OBJECT_ID") && dbo.getObject_id().equals(search.getCompareValue());
			if (!isMatch) {
				isMatch = searchField.equals("PARENT_ID") && dbo.getParent_id().equals(search.getCompareValue());
				if (!isMatch)
					isMatch = dbo.getVal(searchField) != null ? dbo.getVal(searchField).equals(search.getCompareValue())
							: false;
			}

		} else if (this.searchExt != null && searchExt.getFieldName() != null) {
			String searchField = searchExt.getFieldName();
			isMatch = searchField.equals("OBJECT_ID") && dbo.getObject_id().equals(searchExt.getCompareValue());
			if (!isMatch) {
				isMatch = searchField.equals("PARENT_ID") && dbo.getParent_id().equals(searchExt.getCompareValue());
				if (!isMatch)
					isMatch = dbo.getVal(searchField) != null
							? dbo.getVal(searchField).equals(searchExt.getCompareValue()) : false;
			}

		} else if (!this.isRoot())
			isMatch = false;
		return isMatch;
	}

	/**
	 * Method to get the root relation
	 * 
	 * @return The reference to the root SvRelationCache object
	 */
	SvRelationCache getRoot() {
		if (root == null) {
			SvRelationCache tmp = this;
			while (!tmp.isRoot())
				tmp = tmp.parent;
			root = tmp;
		}
		return root;
	}

	boolean isLinkSearchMatch(DbDataObject dbo, DbSearchCriterion pSearch) {
		boolean isMatch = false;
		if (dbo.getVal("LINK_TYPE_ID").equals(pSearch.getCompareValue2())) {
			Long lhsObjectId = null;
			if (pSearch.getOperand().equals(DbCompareOperand.DBLINK))
				lhsObjectId = (Long) dbo.getVal("link_obj_id_2");
			if (pSearch.getOperand().equals(DbCompareOperand.DBLINK_REVERSE))
				lhsObjectId = (Long) dbo.getVal("link_obj_id_1");
			if (lhsObjectId != null && lhsObjectId.equals(pSearch.getCompareValue()))
				isMatch = true;
		}
		return isMatch;
	}

	/**
	 * Method to check if the cache should be invalidated
	 * 
	 * @param dbo
	 *            The object which was changed since the last cache loading
	 * @return True if the cache should be invalidated
	 */
	boolean shouldInvalidateCache(DbDataObject dbo) {
		// TODO add handling of denormalized relations!
		boolean shouldInvalidate = false;
		if (dbo.getObject_type().equals(dbt.getObject_id())) {
			// if the object is mapped in the result set always invalidate
			ArrayDeque<Long> ids = getRoot().objectIdMap.get(objectAlias);
			if (ids != null) {
				if (ids.contains(dbo.getObject_id()))
					shouldInvalidate = true;
				else {
					shouldInvalidate = isSearchMatched(dbo);
					if (relationToParent != null && relationToParent.equals(LinkType.PARENT))
						shouldInvalidate = getRoot().objectIdMap.get(parent.objectAlias).contains(dbo.getParent_id());
				}
			}
			// test if the relation is a match too!
		} else if (dbo.getObject_type().equals(svCONST.OBJECT_TYPE_LINK)) {
			if (dblt != null && dbo.getVal("LINK_TYPE_ID").equals(dblt.getObject_id())) {
				Long lhsObjectId = null;
				if (relationToParent.equals(LinkType.DBLINK))
					lhsObjectId = (Long) dbo.getVal("link_obj_id_1");
				if (relationToParent.equals(LinkType.DBLINK_REVERSE))
					lhsObjectId = (Long) dbo.getVal("link_obj_id_2");

				ArrayDeque<Long> oList = getRoot().objectIdMap.get(parent.objectAlias);
				// check if there's a map at all, the cache might be in the
				// process of refresh
				if (oList != null)
					shouldInvalidate = oList.contains(lhsObjectId);
			}
			if (search != null && (search.getOperand().equals(DbCompareOperand.DBLINK)
					|| search.getOperand().equals(DbCompareOperand.DBLINK_REVERSE)))
				shouldInvalidate = isLinkSearchMatch(dbo, search);
			if (searchExt != null && (searchExt.getOperand().equals(DbCompareOperand.DBLINK)
					|| searchExt.getOperand().equals(DbCompareOperand.DBLINK_REVERSE)))
				shouldInvalidate = isLinkSearchMatch(dbo, searchExt);

		}
		if (!shouldInvalidate)
			for (SvRelationCache tmpCache : cacheList) {
				shouldInvalidate = tmpCache.shouldInvalidateCache(dbo);
				if (shouldInvalidate)
					break;
			}
		return shouldInvalidate;

	}

	/**
	 * Method to build a DbQueryExpression from this SvRelationCache.
	 *
	 * @return A DbQueryExpression describing this SvRelationCache with its children
	 * @throws SvException Any thrown exception is forwarded
	 */
	public DbQueryExpression getQueryExpression() throws SvException {
		return buildReverseDQE();
	}
	
	/**
	 * Method to build a DbQueryExpression based on the cache graph but in
	 * reverse order. The forward order had a number of bugs and limitations
	 * 
	 * @return A DbQueryExpression describing the query
	 * @throws SvException
	 *             Any thrown exception is forwarded
	 */
	DbQueryExpression buildReverseDQE() throws SvException {
		DbQueryExpression dqe = new DbQueryExpression();
		dqe.setIsReverseExpression(true);
		dqe.setRootQueryObject(buildDQO(true));
		return dqe;
	}

	/**
	 * Method which builds the DbQuery object for the specific SvRelationCache
	 * 
	 * @return
	 * @throws SvException
	 */
	DbQueryObject buildDQO(boolean traverseChildren) throws SvException {
		return buildDQO(this, traverseChildren, this);
	}

	DbQueryObject buildDQO(SvRelationCache cache, boolean traverseChildren, SvRelationCache rootCache)
			throws SvException {
		if (cache == this)
			rootCache.objectIdMap.clear();

		rootCache.objectIdMap.put(cache.objectAlias, new ArrayDeque<Long>());
		DbSearch qSearch = cache.search;
		if (cache.searchExt != null && cache.search != null) {
			qSearch = new DbSearchExpression().addDbSearchItem(cache.search).addDbSearchItem(cache.searchExt);
		}
		DbQueryObject rootDqo = new DbQueryObject(cache.dbt, qSearch, cache.referenceDate, cache.joinToParent);
		DbQueryObject dqo = rootDqo;
		dqo.setDenormalizedFieldName(cache.denormFieldName);
		dqo.setDenormalizedJoinOnFieldName(cache.denormPreviousFieldName);
		dqo.setSqlTablePrefix(cache.objectAlias);
		dqo.setLinkReferenceDate(cache.linkReferenceDate);
		dqo.setLinkStatusList(cache.linkStatusList);
		dqo.setReverseRelation(true);
		dqo.setIsReturnType(true);
		dqo.setLinkToNextType(cache.relationToParent);
		dqo.setJoinToNext(cache.joinToParent);
		dqo.setLinkToNext(cache.dblt);
		dqo.setReturnLinkObjects(cache.returnLinkObjects);
		dqo.setIsReturnType(cache.isReturnType);
		if (traverseChildren && cache.cacheList != null && cacheList.size() > 0) { // traversing
																					// children
																					// means
																					// that
																					// we
																					// have
																					// a
																					// reverse
																					// expression
			for (SvRelationCache child : cache.cacheList) {
				DbQueryObject nextDqo = this.buildDQO(child, traverseChildren, rootCache);
				dqo.addChild(nextDqo);
			}
		}

		return rootDqo;
	}

	/**
	 * Method which builds the DbQueryExpression containing the full query to be
	 * executed and cached
	 * 
	 * @return DbQueryExpression describing the query used to populate the
	 *         SvRelationCache
	 * @throws SvException
	 */
	DbQueryExpression buildDQE() throws SvException {
		DbQueryExpression dqe = null;
		dqe = new DbQueryExpression();
		DbQueryObject currentDqo = buildDQO(false);
		objectIdMap.clear();
		dqe.addItem(currentDqo);
		objectIdMap.put(this.objectAlias, new ArrayDeque<Long>());
		if (cacheList.size() > 0) {
			SvRelationCache cache = cacheList.get(0);
			objectIdMap.put(cache.objectAlias, new ArrayDeque<Long>());
			DbQueryExpression tmpDq = cache.buildDQE();
			LinkedList<DbQueryObject> dqos = tmpDq.getItems();
			// DbQueryObject firstDqo = dqos.getFirst();
			currentDqo.setLinkToNextType(cache.getRelationToParent());
			currentDqo.setDenormalizedFieldName(this.denormFieldName);
			currentDqo.setJoinToNext(cache.getJoinToParent());
			currentDqo.setLinkToNext(cache.getDblt());
			for (DbQueryObject tmpDqo : dqos) {
				dqe.addItem(tmpDqo);
			}
		}
		return dqe;
	}

	/**
	 * Method to return information if this SvRelationCache instance is the root
	 * instance or not
	 * 
	 * @return True if the parent is null
	 */
	public Boolean isRoot() {
		return parent == null;
	}

	public DbDataArray getData(SvCore core) throws SvException {
		if (!isCacheDirty)
			return cachedItems;

		synchronized (this) {
			// if the cache was already refreshed while we were waiting for the
			// lock just return the set
			if (!isCacheDirty)
				return cachedItems;

			// do the actual loading
			SvReader svr = null;
			try {
				svr = new SvReader(core);
				cachedItems = svr.getObjects(buildReverseDQE(), 0, 0);
				String[][] keys = new String[objectIdMap.keySet().size()][2];
				int i = 0;
				for (String key : objectIdMap.keySet()) {
					keys[i][0] = key;
					keys[i][1] = key + "_OBJECT_ID";
					i++;
				}
				for (DbDataObject dbo : cachedItems.getItems()) {
					for (String[] key : keys) {
						Long oid = (Long) dbo.getVal(key[1]);
						ArrayDeque<Long> cArr = objectIdMap.get(key[0]);
						if (oid != null && !cArr.contains(oid))
							cArr.add(oid);
					}

				}
			} finally {
				if (svr != null)
					svr.release();
			}
			isCacheDirty = false;
			return cachedItems;
		}
	}

	public SvRelationCache(DbDataObject dbt, DbSearchCriterion search, String objectAlias) throws SvException {
		this(dbt, search, objectAlias, null, null, null, null);
	}

	/**
	 * dbt getter
	 * 
	 * @return A DbDataObject describing the table
	 */
	public DbDataObject getDbt() {
		return dbt;
	}

	public void setDbt(DbDataObject dbt) {
		this.dbt = dbt;
	}

	public DbSearchCriterion getSearch() {
		return search;
	}

	public void setSearch(DbSearchCriterion search) {
		this.search = search;
	}

	public SvRelationCache getParent() {
		return parent;
	}

	public void setParent(SvRelationCache parent) {
		this.parent = parent;
	}

	public String getObjectAlias() {
		return objectAlias;
	}

	public void setObjectAlias(String objectAlias) {
		this.objectAlias = objectAlias;
	}

	public LinkType getRelationToParent() {
		return relationToParent;
	}

	public void setRelationToParent(LinkType relationToParent) {
		this.relationToParent = relationToParent;
	}

	public String getDenormFieldName() {
		return denormFieldName;
	}

	public void setDenormFieldName(String denoFieldName) {
		this.denormFieldName = denoFieldName;
	}
	
	public String getDenormPreviousFieldName() {
		return denormPreviousFieldName;
	}

	public void setDenormPreviousFieldName(String denormPreviousFieldName) {
		this.denormPreviousFieldName = denormPreviousFieldName;
	}

	public void removeCache(SvRelationCache o) {
		cacheList.remove(o);
	}

	public void addCache(SvRelationCache cache) throws SvException {
		if (cache != null) {
			if (cache.getDbt().getVal("is_config_table").equals(false)) {
				if (hasLinkedUserDefinedObject())
					throw (new SvException("", svCONST.systemUser, null, this));

				cache.setParent(this);
				this.cacheList.add(0, cache);
			} else {
				cache.setParent(this);
				this.cacheList.add(cache);
			}
		}
	}

	public DbJoinType getJoinToParent() {
		return joinToParent;
	}

	public void setJoinToParent(DbJoinType joinToParent) {
		this.joinToParent = joinToParent;
	}

	public DbDataObject getDblt() {
		return dblt;
	}

	public DateTime getLinkReferenceDate() {
		return linkReferenceDate;
	}

	public void setLinkReferenceDate(DateTime linkReferenceDate) {
		this.linkReferenceDate = linkReferenceDate;
	}

	public ArrayList<String> getLinkStatusList() {
		return linkStatusList;
	}

	public void setLinkStatusList(ArrayList<String> linkStatusList) {
		this.linkStatusList = linkStatusList;
	}

	public Boolean getReturnLinkObjects() {
		return returnLinkObjects;
	}

	public void setReturnLinkObjects(Boolean setValue) {
		this.returnLinkObjects = setValue;
	}

	public Boolean getIsReturnType() {
		return isReturnType;
	}

	public void setIsReturnType(Boolean isReturnType) {
		this.isReturnType = isReturnType;
	}

	public DbSearchCriterion getSearchExt() {
		return searchExt;
	}

	public void setSearchExt(DbSearchCriterion searchExt) {
		this.searchExt = searchExt;
	}

}
