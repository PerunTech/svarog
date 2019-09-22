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

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;

/**
 * Class for cached retrieval of complex objects
 * 
 * @author XPS13
 *
 */
public class SvComplexCache {

	@SuppressWarnings("unchecked")
	private static Cache<String, SvRelationCache> cacheConfig() {
		CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();
		builder = builder.removalListener(onRemove);
		builder = builder.expireAfterAccess(10, TimeUnit.MINUTES);
		builder = builder.maximumSize(100);
		return (Cache<String, SvRelationCache>) builder.<String, SvRelationCache>build();
	}

	@SuppressWarnings("rawtypes")
	static RemovalListener onRemove = new RemovalListener<String, SvRelationCache>() {
		public void onRemoval(RemovalNotification<String, SvRelationCache> removal) {
			SvCore.unregisterOnSaveCallback(removal.getValue().getCallback());
		}
	};

	static Cache<String, SvRelationCache> cachedRelations = cacheConfig();

	/**
	 * Static method to register a new RelationCache tree under a specified key
	 * 
	 * @param uniqueId
	 *            The key under which to register the SvRelationCache
	 * @param cache
	 *            The root SvRelationCache object
	 * @param overwrite
	 *            Flag if old relation object should be overwritten
	 * @throws SvException
	 *             Re-throws any underlying exception
	 */
	static public void addRelationCache(String uniqueId, SvRelationCache cache, Boolean overwrite) throws SvException {
		if (cache.isRoot()) {
			if (cache.getSearch() == null)
				throw (new SvException("system.error.relation_cache_missing_search", svCONST.systemUser, null,
						uniqueId));
			else if (!(cache.getSearch().getOperand().equals(DbCompareOperand.EQUAL)
					|| cache.getSearch().getOperand().equals(DbCompareOperand.DBLINK)
					|| cache.getSearch().getOperand().equals(DbCompareOperand.DBLINK_REVERSE)))
				throw (new SvException("system.error.relation_root_operand_err", svCONST.systemUser, null, uniqueId));

			SvRelationCache oldCache = cachedRelations.getIfPresent(uniqueId);
			if (oldCache == null) {
				cachedRelations.put(uniqueId, cache);
				SvCore.registerOnSaveCallback(cache.getCallback());
			} else {
				if (overwrite) {
					SvCore.unregisterOnSaveCallback(oldCache.getCallback());
					cachedRelations.put(uniqueId, cache);
					SvCore.registerOnSaveCallback(cache.getCallback());
				} else
					throw (new SvException("system.error.complex_cache_item_exists", svCONST.systemUser, null,
							uniqueId));
			}
		} else
			throw (new SvException("system.error.relation_cache_not_root", svCONST.systemUser, null, uniqueId));

	}

	static public DbDataArray getData(String uniqueId, SvCore sharedCore) throws SvException {
		SvRelationCache cache = cachedRelations.getIfPresent(uniqueId);
		if (cache != null)
			return cache.getData(sharedCore);
		else
			return null;
	}

	static public SvRelationCache getCache(String uniqueId) {
		return cachedRelations.getIfPresent(uniqueId);
	}
}
