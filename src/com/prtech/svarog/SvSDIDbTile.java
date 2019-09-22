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

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKBReader;

public class SvSDIDbTile extends SvSDITile {

	DbSearch extSearch;

	public SvSDIDbTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams) {
		this.tileTypeId = tileTypeId;
		this.tilelId = tileId;
		this.tileEnvelope = (Envelope) tileParams.get("ENVELOPE");
		this.extSearch = (DbSearch) tileParams.get("DB_SEARCH");
	}

	@Override
	ArrayList<Geometry> loadGeometries() throws SvException {
		ArrayList<Geometry> l = new ArrayList<Geometry>();
		DbSearch dbs = new DbSearchCriterion(SvGeometry.getGeometryFieldName(tileTypeId), DbCompareOperand.BBOX,
				tileEnvelope);
		if (extSearch != null) {
			DbSearchExpression dbe = new DbSearchExpression();
			dbe.addDbSearchItem(dbs);
			dbe.addDbSearchItem(extSearch);
			dbs = dbe;
		}

		WKBReader wkr = new WKBReader();
		Geometry geom = null;
		Point centroid = null;

		SvReader svr = null;

		try {
			svr = new SvReader();
			svr.includeGeometries = true;
			DbDataArray arr = svr.getObjects(dbs, tileTypeId, null, 0, 0);
			for (DbDataObject dbo : arr.getItems()) {
				geom = SvGeometry.getGeometry(dbo);
				geom.setUserData(dbo);
				l.add(geom);
			}
		} finally {
			if (svr != null)
				svr.release();
		}
		// TODO Auto-generated method stub
		return l;
	}

}
