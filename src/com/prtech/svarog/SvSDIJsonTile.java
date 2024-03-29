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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygonal;
import com.vividsolutions.jts.geom.prep.PreparedPolygon;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonReader;

/**
 * The possibility to use a JSON file for population of a tile with geometries
 * is provided by this class. It extends the {@link SvSDITile} and provides
 * means for loading a JSON file.
 * 
 * @author ristepejov
 *
 */
public class SvSDIJsonTile extends SvSDITile {
	private static final Logger log4j = LogManager.getLogger(SvSDIJsonTile.class.getName());

	String jsonFilePath;

	public SvSDIJsonTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams) throws SvException {
		this.tileTypeId = tileTypeId;
		this.tilelId = tileId;
		this.jsonFilePath = (String) tileParams.get("FILE_PATH");
		Object envGeom = tileParams.get("ENVELOPE");
		if (envGeom != null)
			prepareEnvelope(envGeom);
	}

	@Override
	GeometryCollection loadGeometries() throws SvException {

		if (log4j.isDebugEnabled())
			log4j.trace("Loading JSON tile from:" + jsonFilePath);
		String geoJSONBounds = null;
		InputStream is = null;
		try {
			is = SvGeometry.class.getClassLoader().getResourceAsStream(jsonFilePath);
			if (is == null) {
				String path = "./" + jsonFilePath;
				is = new FileInputStream(path);
			}
			if (is != null) {
				geoJSONBounds = IOUtils.toString(is);
			}
		} catch (IOException e) {
			log4j.error("Failed JSON tile from:" + jsonFilePath, e);
		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					log4j.error("Failed JSON. Can't close stream", e);
				}
		}

		GeoJsonReader jtsReader = new GeoJsonReader();
		GeometryCollection layer = null;

		try {
			jtsReader.setUseFeatureType(true);
			layer = (GeometryCollection) jtsReader.read(geoJSONBounds);
			tileGeometry = new PreparedPolygon((Polygonal) layer.getEnvelope());
		} catch (Exception e) {
			log4j.error("Failed parsing JSON tile:" + jsonFilePath, e);

		}
		return layer;
	}

}
