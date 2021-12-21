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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;

/**
 * The SvSDIDbTile allows loading of standard svarog objects represented by
 * DbDataObject and DbDataArray from the reference database. This class supports
 * loading tiles based on Envelope/BBOX as well as additional search parameters
 * to filter the GIS objects.
 * 
 * @author ristepejov
 *
 */
public class SvSDIDbTile extends SvSDITile {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvSDIDbTile.class.getName());

	DbSearch extSearch;

	public SvSDIDbTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams) throws SvException {
		this.tileTypeId = tileTypeId;
		this.tilelId = tileId;
		this.extSearch = (DbSearch) tileParams.get("DB_SEARCH");
		Object envGeom = tileParams.get("ENVELOPE");
		prepareEnvelope(envGeom);

	}

	/**
	 * Overriden method which loads geometries from the database by using the tile
	 * envelope
	 */
	@Override
	GeometryCollection loadGeometries() throws SvException {

		DbSearch dbs = new DbSearchCriterion(SvGeometry.getGeometryFieldName(tileTypeId), DbCompareOperand.BBOX,
				tileGeometry.getGeometry().getEnvelopeInternal());
		if (extSearch != null) {
			DbSearchExpression dbe = new DbSearchExpression();
			dbe.addDbSearchItem(dbs);
			dbe.addDbSearchItem(extSearch);
			dbs = dbe;
		}
		Geometry geom = null;
		Geometry[] glist = null;
		try (SvReader svr = new SvReader()) {
			svr.includeGeometries = true;
			DbDataArray arr = svr.getObjects(dbs, tileTypeId, null, 0, 0);
			glist = new Geometry[arr.size()];
			if (log4j.isDebugEnabled())
				log4j.debug("Loaded " + arr.size() + " geometries for tile type:" + tileTypeId + ", with search:"
						+ dbs.toSimpleJson().toString());
			int i = 0;
			for (DbDataObject dbo : arr.getItems()) {
				geom = SvGeometry.getGeometry(dbo);
				geom.setUserData(dbo);
				glist[i++] = geom;
			}
		}
		return SvUtil.sdiFactory.createGeometryCollection(glist);
	}

}
