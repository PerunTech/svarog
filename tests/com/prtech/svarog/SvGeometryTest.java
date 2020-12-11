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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog.SvSDITile.SDIRelation;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_interfaces.ISvDatabaseIO;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonWriter;

public class SvGeometryTest {
	private static final Long TEST_LAYER_TYPE_ID = 999L;

	@BeforeClass
	static public void initTestSDI() throws SvException {
		SvGeometry.isSDIInitalized.set(true);
		// create a fake boundary with about 10x10 grid cells
		Envelope boundaryEnv = new Envelope(0, 10 * SvConf.getSdiGridSize() * 1000, 0,
				10 * SvConf.getSdiGridSize() * 1000);
		ArrayList<Geometry> boundGeom = new ArrayList<>();
		boundGeom.add(SvUtil.sdiFactory.toGeometry(boundaryEnv));

		// generate and prepare fake grid
		GeometryCollection grid = DbInit.generateGrid(boundGeom.get(0));
		SvGeometry.prepareGrid(grid);

		// add the fake boundary as system boundary
		Cache<String, SvSDITile> cache = SvGeometry.getLayerCache(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE);
		SvSDITile tile = new SvTestTile(boundaryEnv, svCONST.OBJECT_TYPE_SDI_GEOJSONFILE, boundGeom);
		tile.tilelId = Sv.SDI_SYSTEM_BOUNDARY;
		cache.put(tile.tilelId, tile);

		// now add a test layer with ID TEST_LAYER_TYPE_ID
		CacheBuilder<String, SvSDITile> b = (CacheBuilder<String, SvSDITile>) DbCache.createBuilder(null);
		Cache<String, SvSDITile> newCache = b.<String, SvSDITile>build();
		SvSDITile layerTile = new SvTestTile(
				new Envelope(0, SvConf.getSdiGridSize() * 1000, 0, SvConf.getSdiGridSize() * 1000), TEST_LAYER_TYPE_ID,
				testGeoms(0, 0));
		layerTile.tilelId = "0:0";
		newCache.put(layerTile.tilelId, layerTile);
		cache = SvGeometry.layerCache.putIfAbsent(TEST_LAYER_TYPE_ID, newCache);
		assert (layerTile.getBorderGeometries().size() > 0);
		assert (layerTile.getInternalGeometries().size() > 0);

	}

	@Test
	public void testGrid() {
		Map<String, Geometry> grid = SvGeometry.getGrid();
		if (grid == null || grid.size() < 1)
			fail("Failed fetching base grid");
	}

	@Test
	public void testSDILayerLoad() {
		if (SvCore.getDbtByName("PHYSICAL_BLOCK") == null)
			return;

		SvGeometry svg = null;
		try {
			svg = new SvGeometry();

			Envelope env = new Envelope(7499070.2242, 4542102.0632, 7501509.7277, 4543436.8737);
			List<Geometry> g = SvGeometry.getTileGeometries(env);
			Long tileTypeId = SvCore.getTypeIdByName("PHYSICAL_BLOCK");

			SvSDITile tile = svg.createTile(tileTypeId, g.get(0).getUserData().toString(), null);
			Set<Geometry> geoms = tile.getInternalGeometries();

			System.out.println("Tile envelope from bbox:" + env.toString());
			System.out.println("Area of envelope:" + env.getArea() / 10000 + "Ha");
			System.out.println("Tile geoms:" + geoms.size());
			if (env.getArea() < 1)
				fail("BBOX test failed. Envelope are is less than 1 sqm");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}

	@Test
	public void testBboxParsing() {
		SvGeometry svg = null;
		try {
			svg = new SvGeometry();
			SvSDITile tile = svg.getSysBoundary();
			String bbox = SvGeometry.getBBox(tile.getEnvelope());
			System.out.println("Tile bbox:" + bbox);
			Envelope env = SvGeometry.parseBBox(bbox);
			System.out.println("Tile envelope from bbox:" + env.toString());
			System.out.println("Area of envelope:" + env.getArea() / 10000 + "Ha");
			if (env.getArea() < 1)
				fail("BBOX test failed. Envelope are is less than 1 sqm");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("BBOX test failed");
		}

	}

	@Test
	public void testBoundaryIntersect() {
		if (!SvConf.isSdiEnabled())
			return;

		try {
			GeometryFactory gf = SvUtil.sdiFactory;
			Geometry geom = gf.toGeometry(new Envelope(-100, 200, -100, 20));
			Geometry geomInside = gf.toGeometry(new Envelope(100, 200, 100, 200));

			SvSDITile sysbounds = SvGeometry.getSysBoundary();
			Set<Geometry> gl = sysbounds.getRelations(geom, SDIRelation.INTERSECTS);
			if (gl.size() < 1)
				fail("Cross bounds polygon didn't detect intersection");

			Set<Geometry> gl2 = sysbounds.getRelations(geomInside, SDIRelation.INTERSECTS);
			if (gl2.size() < 1)
				fail("Cross bounds polygon didn't detect intersection");

			Point point = gf.createPoint(new Coordinate(-200, -200));
			gl = sysbounds.getRelations(point, SDIRelation.INTERSECTS);
			if (gl.size() > 0)
				fail("Point detected in boundaries!!!");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised exception:" + e.getFormattedMessage());
		}

	}

	@Test
	public void testBoundsSave() {

		try (SvGeometry svg = new SvGeometry()) {
			GeometryFactory gf = SvUtil.sdiFactory;
			Geometry geom = gf
					.createMultiPolygon(new Polygon[] { (Polygon) gf.toGeometry(new Envelope(-100, 200, -100, 20)) });

			DbDataObject dbounds = new DbDataObject(svCONST.OBJECT_TYPE_SDI_BOUNDS);
			dbounds.setVal("BOUNDS_NAME", "testBounds");
			dbounds.setVal("BOUNDS_ID", "123");
			dbounds.setVal("BOUNDS_CLASS", 1);
			dbounds.setVal("PERIMETER", 0);
			dbounds.setVal("AREA", 0);
			SvGeometry.setGeometry(dbounds, geom);
			// SvGeometry.allowBoundaryIntersect=true;
			// svg.setAllowNullGeometry(true);
			svg.saveGeometry(dbounds);
			String bbox = SvGeometry.getBBox(geom.buffer(100).getEnvelopeInternal());
			Set<Geometry> result = svg.getGeometriesByBBOX(svCONST.OBJECT_TYPE_SDI_BOUNDS, bbox);
			Boolean matched = false;
			for (Geometry g : result) {
				System.out.println(((DbDataObject) g.getUserData()).getObjectId());
				System.out.println(geom.toString());
				if (g.equals(geom))
					matched = true;
			}

			if (!matched)
				fail("Inserted polygon not found in result");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception:" + e.getFormattedMessage());

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetTile() {

		try (SvGeometry svg = new SvGeometry();) {
			GeometryFactory gf = SvUtil.sdiFactory;
			Point point = gf.createPoint(new Coordinate(100, 100));
			System.out.println("Max tile:" + svg.getMaxXtile() + ":" + svg.getMaxYtile());

			List<Geometry> gcl = SvGeometry.getTileGeometries(point.getEnvelopeInternal());
			if (gcl == null)
				fail("Failed getting tile from point");
			else
				for (Geometry tile : gcl) {
					System.out.println("Matched tile:" + tile.getUserData() + ", tile string:" + tile.toText());
					SvSDITile sdiTile = svg.getTile(svCONST.OBJECT_TYPE_SDI_BOUNDS, (String) tile.getUserData(), null);
					System.out.println("Fetched tile data:" + sdiTile.getGeoJson());

				}
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception:" + e.getFormattedMessage());
		}

		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetGIOHandler() {
		ISvDatabaseIO gio;
		try {
			gio = SvConf.getDbHandler();
		} catch (SvException e) {
			e.printStackTrace();
			fail("Can't get geometry handler");
		}

	}

	@Test
	public void testGetBoundaryTile() {
		try {
			SvSDITile sdiTile = SvGeometry.getSysBoundary();
			sdiTile.loadTile();
			System.out.println("Fetched tile data:" + sdiTile.getGeoJson());
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetTileNotExist() {
		try {
			GeometryFactory gf = SvUtil.sdiFactory;
			System.out.println("Max tile:" + SvGeometry.getMaxXtile() + ":" + SvGeometry.getMaxYtile());

			Point point = gf.createPoint(new Coordinate(7469568, 4530337));
			List<Geometry> gcl = SvGeometry.getTileGeometries(point.getEnvelopeInternal());
			if (gcl.size() > 0)
				fail("A point matching tile which is out of the boundary returned a tile");
		} catch (Exception ex) {
			fail("test raised exception");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	static ArrayList<Geometry> testGeoms(double x1, double y1) {
		ArrayList<Geometry> g = new ArrayList<>();
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 10, x1 + 20, y1 + 10, y1 + 20)));
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 20, x1 + 30, y1 + 20, y1 + 30)));
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 - 15, x1 + 5, y1 - 15, y1 + 5)));
		return g;
	}

	@Test
	public void testIntersectsLayer() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Geometry geom = SvUtil.sdiFactory.toGeometry(new Envelope(50, 70, 50, 70));
			if (svg.intersectsLayer(geom, TEST_LAYER_TYPE_ID))
				fail("This should not intersect");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}
}
