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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog.SvSDITile.SDIRelation;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.SvCharId;
import com.prtech.svarog_interfaces.ISvDatabaseIO;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonWriter;

public class SvGeometryTest {
	private static final Long TEST_LAYER_TYPE_ID = 999L;
	private static final Long TEST_LAYER_SECOND_TYPE_ID = 998L;

	private static final long gridX0 = 0L;
	private static final long gridY0 = 0L;

	private static void initFakeSysBoundary() throws SvException {
		// create a fake boundary with about 10x10 grid cells
		Envelope boundaryEnv = new Envelope(gridX0, 10 * SvConf.getSdiGridSize() * 1000, gridY0,
				10 * SvConf.getSdiGridSize() * 1000);
		Geometry[] boundGeom = new Geometry[1];
		boundGeom[0] = SvUtil.sdiFactory.toGeometry(boundaryEnv);

		try (SvReader svr = new SvReader()) {
			// generate and prepare fake grid
			GeometryCollection grid = SvGrid.generateGrid(boundGeom[0], SvConf.getSdiGridSize(), svr);
			Cache<String, SvSDITile> gridCache = SvGeometry.getLayerCache(svCONST.OBJECT_TYPE_GRID);
			SvGrid svg = new SvGrid(grid, Sv.SDI_SYSGRID);
			SvGeometry.setSysGrid(svg);
		}
		// add the fake boundary as system boundary
		Cache<String, SvSDITile> cache = SvGeometry.getLayerCache(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE);
		SvSDITile tile = new SvTestTile(boundaryEnv, svCONST.OBJECT_TYPE_SDI_GEOJSONFILE,
				SvUtil.sdiFactory.createGeometryCollection(boundGeom));
		tile.tilelId = Sv.SDI_SYSTEM_BOUNDARY;
		cache.put(tile.tilelId, tile);

	}

	@AfterClass
	static public void resetSDI() {
		SvGeometry.resetGrid();
	}

	@BeforeClass
	static public void initTestSDI() throws SvException {
		SvGeometry.resetGrid();
		initFakeSysBoundary();
		// now add a test layer with ID TEST_LAYER_TYPE_ID
		CacheBuilder<String, SvSDITile> b = (CacheBuilder<String, SvSDITile>) DbCache.createBuilder(null);
		Cache<String, SvSDITile> newCacheBase = b.<String, SvSDITile>build();
		Cache<String, SvSDITile> newCacheSecond = b.<String, SvSDITile>build();

		SvSDITile layerTile = new SvTestTile(
				new Envelope(gridX0, SvConf.getSdiGridSize() * 1000, gridY0, SvConf.getSdiGridSize() * 1000),
				TEST_LAYER_TYPE_ID, testGeomsBase(gridX0, gridY0));
		layerTile.tilelId = "0:0";

		SvSDITile layerTileSecond = new SvTestTile(
				new Envelope(gridX0, SvConf.getSdiGridSize() * 1000, gridY0, SvConf.getSdiGridSize() * 1000),
				TEST_LAYER_SECOND_TYPE_ID, testGeomsSecond(gridX0, gridY0));
		layerTileSecond.tilelId = "0:0";

		Cache<String, SvSDITile> cache = null;
		newCacheBase.put(layerTile.tilelId, layerTile);
		newCacheSecond.put(layerTileSecond.tilelId, layerTileSecond);
		cache = SvGeometry.layerCache.put(TEST_LAYER_TYPE_ID, newCacheBase);
		cache = SvGeometry.layerCache.put(TEST_LAYER_SECOND_TYPE_ID, newCacheSecond);
		assert (layerTile.getBorderGeometries().size() > 0);
		assert (layerTile.getInternalGeometries().size() > 0);

	}

	@Test
	public void testGrid() {
		Map<String, Geometry> grid;
		try {
			grid = SvGeometry.getGrid();

			if (grid == null || grid.size() < 1)
				fail("Failed fetching base grid");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			fail("Failed fetching base grid");
			e.printStackTrace();
		}
	}

	@Test
	public void testSDILayerLoad() {
		if (SvCore.getDbtByName("PHYSICAL_BLOCK") == null)
			return;

		try (SvGeometry svg = new SvGeometry()) {

			Envelope env = new Envelope(7499070.2242, 4542102.0632, 7501509.7277, 4543436.8737);
			List<Geometry> g = SvGeometry.getTileGeometries(env);
			Long tileTypeId = SvCore.getTypeIdByName("PHYSICAL_BLOCK");

			SvSDITile tile = svg.createTile(tileTypeId, g.get(0).getUserData().toString(), null);
			Collection<Geometry> geoms = tile.getInternalGeometries();

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
		try (SvGeometry svg = new SvGeometry()) {
			;
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
	public void testSpikes() throws ParseException, SvException {
		GeometryFactory gf = SvUtil.sdiFactory;
		String sap = "MULTIPOLYGON (((7479402.8434 4565348.5114, 7479354.0402 4565344.2105, 7479301.9682 4565340.1414, 7479300.0397 4565340.036, 7479278.8263 4565338.7649, 7479277.9074 4565368.7508, 7479277.9074 4565368.7508, 7479344.8897 4565368.7788, 7479375.1521 4565368.3581, 7479442.8025 4565367.4966, 7479390.5765 4565368.0914, 7479395.8834 4565347.8652, 7479402.8434 4565348.5114)))";
		String slu = "POLYGON ((7570896.26 4588986.09, 7570900.02 4588983.91, 7570904.99 4588981.33, 7570905.78 4588978.95, 7570903.97 4588974.17, 7570906.77 4588970.77, 7570911.34 4588972.99, 7570912.93 4588974.18, 7570914.91 4588976.17, 7570918.48 4588982.12, 7570926.02 4588994.03, 7570938.33 4589007.52, 7570940.31 4589010.7, 7570938.72 4589012.68, 7570936.74 4589015.86, 7570930.79 4589022.6, 7570924.04 4589032.52, 7570919.28 4589037.68, 7570917.69 4589039.27, 7570914.12 4589038.08, 7570911.34 4589035.7, 7570906.97 4589030.94, 7570900.62 4589019.03, 7570891.89 4588996.81, 7570892.69 4588989.66, 7570896.26 4588986.09))";
		try (SvGeometry svg = new SvGeometry()) {
			WKTReader wkr = new WKTReader(SvUtil.sdiFactory);
			Geometry geom = wkr.read(sap);
			geom = svg.fixMinVertexDistance(geom, 100);
			geom = svg.fixPolygonSpikes(geom, 2.0);
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
	public void testUnitsSaveNullGeometry() {

		try (SvGeometry svg = new SvGeometry()) {
			GeometryFactory gf = SvUtil.sdiFactory;
			Geometry geom = gf
					.createMultiPolygon(new Polygon[] { (Polygon) gf.toGeometry(new Envelope(-100, 200, -100, 20)) });

			DbDataObject dbounds = new DbDataObject(svCONST.OBJECT_TYPE_SDI_UNITS);
			dbounds.setVal("UNIT_NAME", "testBounds");
			dbounds.setVal("UNIT_ID", "123");
			dbounds.setVal("UNIT_CLASS", "REGION");
			SvGeometry.setGeometry(dbounds, null);
			// SvGeometry.allowBoundaryIntersect=true;
			svg.setAllowNullGeometry(true);
			svg.saveGeometry(dbounds);
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception:" + e.getFormattedMessage());

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
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
			System.out.println("Max tile:" + svg.getSysGrid().getMaxXtile() + ":" + svg.getSysGrid().getMaxYtile());

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
			System.out.println(
					"Max tile:" + SvGeometry.getSysGrid().getMaxXtile() + ":" + SvGeometry.getSysGrid().getMaxYtile());

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

	static Geometry[] testGeomsBaseG(double x1, double y1) {
		Geometry g[] = new Geometry[5];
		g[0] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 10, x1 + 20, y1 + 10, y1 + 20));
		g[1] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 20, x1 + 30, y1 + 20, y1 + 30));
		g[2] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 - 15, x1 + 5, y1 - 15, y1 + 5));

		for (int i = 0; i < 3; i++) {
			DbDataObject dbo = new DbDataObject();
			dbo.setParentId(i + TEST_LAYER_TYPE_ID);
			dbo.setObjectId(i + TEST_LAYER_TYPE_ID + 100);
			g[i].setUserData(dbo);
		}

		WKTReader wkr = new WKTReader();
		String polyHole = "POLYGON ((30 30, 30 40, 40 40, 40 30, 30 30), (35 35, 37 35, 37 37, 35 37, 35 35))";
		try {
			g[3] = wkr.read(polyHole);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		g[4] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 10, x1 + 20, y1 + 20, y1 + 30));

		return g;

	}

	static GeometryCollection testGeomsBase(double x1, double y1) {

		return SvUtil.sdiFactory.createGeometryCollection(testGeomsBaseG(x1, y1));

	}

	static GeometryCollection testGeomsSecond(double x1, double y1) {
		Geometry g[] = new Geometry[3];
		g[0] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 25, x1 + 27, y1 + 20, y1 + 30));
		g[1] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 - 15, x1 + 5, y1 - 15, y1 + 5));
		g[2] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 35, x1 + 40, y1 + 30, y1 + 40));
		return SvUtil.sdiFactory.createGeometryCollection(g);
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

	@Test
	public void testGeomFromPointFullCopy() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Point point = SvUtil.sdiFactory.createPoint(new Coordinate(15, 15));

			Set<Geometry> copied = svg.geometryFromPoint(point, TEST_LAYER_TYPE_ID, TEST_LAYER_SECOND_TYPE_ID, false);

			Set<Geometry> intersected = svg.getRelatedGeometries(point, TEST_LAYER_TYPE_ID, SDIRelation.INTERSECTS,
					null, null, false);
			Geometry g1 = copied.iterator().next();
			Geometry g2 = intersected.iterator().next();
			if (!g1.equalsExact(g2))
				fail("Test did not return a copy of geometry");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGetRelatedGeomsWithFilter() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Point point = SvUtil.sdiFactory.createPoint(new Coordinate(15, 15));

			Set<Geometry> copied = svg.geometryFromPoint(point, TEST_LAYER_TYPE_ID, TEST_LAYER_SECOND_TYPE_ID, false);

			Geometry g1 = copied.iterator().next();
			Set<Geometry> intersected = svg.getRelatedGeometries(point, TEST_LAYER_TYPE_ID, SDIRelation.INTERSECTS,
					new SvCharId("PARENT_ID"), ((DbDataObject) g1.getUserData()).getParentId(), false, true);

			Geometry g2 = intersected.iterator().next();
			if (g2 == null)
				fail("Test did not return a copy of geometry");

			intersected = svg.getRelatedGeometries(point, TEST_LAYER_TYPE_ID, SDIRelation.INTERSECTS,
					new SvCharId("PARENT_ID"), ((DbDataObject) g1.getUserData()).getParentId(), true, true);

			if (intersected.iterator().hasNext())
				fail("Test return a copy of geometry which was supposed to be reversed filter");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomFromPointFullCopyWithHole() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Point point = SvUtil.sdiFactory.createPoint(new Coordinate(32, 32));

			Set<Geometry> copied = svg.geometryFromPoint(point, TEST_LAYER_TYPE_ID, TEST_LAYER_SECOND_TYPE_ID, false);

			Set<Geometry> intersected = svg.getRelatedGeometries(point, TEST_LAYER_TYPE_ID, SDIRelation.INTERSECTS,
					null, null, false);
			Geometry g1 = copied.iterator().next();
			Geometry g2 = intersected.iterator().next();
			if (g1.getArea() != (50.0))
				fail("Test did not return a copy of geometry");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomFromPointEmptyPolygon() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Point point = SvUtil.sdiFactory.createPoint(new Coordinate(3, 3));

			Set<Geometry> copied = svg.geometryFromPoint(point, TEST_LAYER_TYPE_ID, TEST_LAYER_SECOND_TYPE_ID, false);

			Geometry g1 = copied.iterator().next();
			if (!g1.isEmpty())
				fail("Test did not return a copy of geometry");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomFromPointPartialPolygon() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Point point = SvUtil.sdiFactory.createPoint(new Coordinate(21, 21));

			Set<Geometry> copied = svg.geometryFromPoint(point, TEST_LAYER_TYPE_ID, TEST_LAYER_SECOND_TYPE_ID, false);

			Geometry g1 = copied.iterator().next();

			Geometry g2 = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 20, gridX0 + 25, gridY0 + 20, gridY0 + 30));
			if (!g1.equalsExact(g2))
				fail("Test did not return a copy of geometry");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomSplit() throws SvException {
		initTestSDI();
		try (SvGeometry svg = new SvGeometry()) {
			Geometry g2_1 = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 10, gridX0 + 20, gridY0 + 10, gridY0 + 15));
			Geometry g2_2 = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 10, gridX0 + 20, gridY0 + 15, gridY0 + 20));

			Coordinate[] coords = new Coordinate[3];
			coords[0] = new Coordinate(0, 15);
			coords[1] = new Coordinate(25, 15);
			coords[2] = new Coordinate(25, 25);
			LineString line = SvUtil.sdiFactory.createLineString(coords);
			List<DbDataObject> toBeDeleted = new ArrayList<DbDataObject>();

			Set<Geometry> copied = svg.splitGeometries(line, TEST_LAYER_TYPE_ID, toBeDeleted, true, null, null, false);

			// [POLYGON ((10 15, 20 15, 20 10, 10 10, 10 15)), POLYGON ((10 15, 10 20, 20
			// 20, 20 15, 10 15))]
			Iterator<Geometry> it = copied.iterator();
			Geometry g1 = it.next();
			if (!g1.equalsNorm(g2_1))
				fail("Test did not return a copy of geometry");

			g1 = it.next();
			if (!g1.equalsNorm(g2_2))
				fail("Test did not return a copy of geometry");
			if(toBeDeleted.size()<1)
				fail("Test did not return a geometry to be deleted");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomMerge() throws SvException {
		// Geometry g[] = new Geometry[4];
		// g[0] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 10, x1 + 20, y1 + 10,
		// y1 + 20));
		// g[1] = SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 20, x1 + 30, y1 + 20,
		// y1 + 30));

		initTestSDI();
		try (SvGeometry svg = new SvGeometry()) {
			Geometry g2_1 = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 10, gridX0 + 20, gridY0 + 10, gridY0 + 30));

			ArrayList<Point> pts = new ArrayList<Point>();

			pts.add(SvUtil.sdiFactory.createPoint(new Coordinate(15, 15)));
			pts.add(SvUtil.sdiFactory.createPoint(new Coordinate(15, 25)));
			List<DbDataObject> toBeDeleted = new ArrayList<DbDataObject>();

			Geometry g1 = svg.mergeGeometries(pts, TEST_LAYER_TYPE_ID, toBeDeleted, true, null, null, false);
			System.out.println(g1);
			// [POLYGON ((10 15, 20 15, 20 10, 10 10, 10 15)), POLYGON ((10 15, 10 20, 20
			// 20, 20 15, 10 15))]
			// Iterator<Geometry> it = copied.iterator();
			// Geometry g1 = it.next();
			if (!(g2_1.covers(g1) && g1.covers(g2_1)))
				fail("Test did not return a merged geometry");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}
	//TODO FIX THIS
	//[POLYGON ((7588120.22 4590814.87, 7588137.99 4590826.51, 7588144.25 4590830.52, 7588157.15 4590838.77, 7588160.42 4590840.46, 7588173.42 4590810.19, 7588179.24 4590794.32, 7588106.21 4590746.56, 7588102.8 4590760.85, 7588106.22 4590746.58, 7588164.03 4590784.38, 7588120.22 4590814.87))]
	@Test
	public void testDetectSpikes() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {

			Coordinate[] tria = new Coordinate[] { new Coordinate(gridX0 + 10, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 20), new Coordinate(gridX0 + 20, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 10) };

			Coordinate[] spikeSquare = new Coordinate[] { new Coordinate(gridX0 + 10, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 20), new Coordinate(gridX0 + 20, gridX0 + 20),
					new Coordinate(gridX0 + 15, gridX0 + 50), new Coordinate(gridX0 + 20, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 10) };

			Geometry triangle = SvUtil.sdiFactory.createPolygon(tria);
			Geometry square = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 10, gridX0 + 20, gridY0 + 15, gridY0 + 20));

			Double maxAngle = SvParameter.getSysParam(Sv.SDI_SPIKE_MAX_ANGLE, Sv.DEFAULT_SPIKE_MAX_ANGLE);
			svg.testPolygonSpikes(triangle, maxAngle);
			svg.testPolygonSpikes(square, maxAngle);

			svg.testPolygonSpikes(triangle, 44.9);
			svg.testPolygonSpikes(square, 89.1);

			try {
				svg.testPolygonSpikes(triangle, 45.9);
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.SDI_SPIKE_DETECTED))
					fail("Spike was not detected!");
			}
			try {
				svg.testPolygonSpikes(square, 91.9);
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.SDI_SPIKE_DETECTED))
					fail("Spike was not detected!");
			}

			// [POLYGON ((10 15, 20 15, 20 10, 10 10, 10 15)), POLYGON ((10 15, 10 20, 20
			// 20, 20 15, 10 15))]

		} catch (Exception e) {
			fail("Test failed with exception");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testFixSpikes() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {

			Coordinate[] tria = new Coordinate[] { new Coordinate(gridX0 + 10, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 20), new Coordinate(gridX0 + 20, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 10) };

			Coordinate[] spikeSquare = new Coordinate[] { new Coordinate(gridX0 + 10, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 20), new Coordinate(gridX0 + 20, gridX0 + 20),
					new Coordinate(gridX0 + 15, gridX0 + 50), new Coordinate(gridX0 + 20, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 10) };

			Coordinate[] spikeSquareInner = new Coordinate[] { new Coordinate(gridX0 + 10, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 20), new Coordinate(gridX0 + 20, gridX0 + 20),
					new Coordinate(gridX0 + 15, gridX0 + 11), new Coordinate(gridX0 + 20, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 10) };

			Geometry spikeSquareInnerG = SvUtil.sdiFactory.createPolygon(spikeSquareInner);
			Geometry spikeSquareG = SvUtil.sdiFactory.createPolygon(spikeSquare);
			Geometry triangle = SvUtil.sdiFactory.createPolygon(tria);

			try {
				svg.testPolygonSpikes(spikeSquareG, 45.0);
				fail("spike was not detected");
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.SDI_SPIKE_DETECTED))
					fail("Spike was not detected!");
			}
			try {
				spikeSquareG = svg.fixPolygonSpikes(triangle, 46.0);
				fail("spike fix did not fail");
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.SDI_SPIKE_FIX_FAILED))
					fail("Spike fix of triangles shall fail!");
			}

			try {
				spikeSquareG = svg.fixPolygonSpikes(spikeSquareG, 45.0);
			} catch (SvException e) {
				fail("Spike was not fixed!");
			}

			try {
				svg.testPolygonSpikes(spikeSquareG, 45.0);
			} catch (SvException e) {
				fail("Spike was not fixed!");
			}

			try {
				svg.testPolygonSpikes(spikeSquareInnerG, 30.0);
				fail("spike was not detected");
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.SDI_SPIKE_DETECTED))
					fail("Spike fix of triangles shall fail!");
			}
			try {
				spikeSquareInnerG = svg.fixPolygonSpikes(spikeSquareInnerG, 30.0);
			} catch (SvException e) {
				fail("Spike was not fixed!");
			}
			try {
				svg.fixPolygonSpikes(spikeSquareInnerG, 30.0);
			} catch (SvException e) {

				fail("Spikes still exist!");
			}

			// [POLYGON ((10 15, 20 15, 20 10, 10 10, 10 15)), POLYGON ((10 15, 10 20, 20
			// 20, 20 15, 10 15))]

		} catch (Exception e) {
			fail("Test failed with exception");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testMinVertexDistance() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {

			Coordinate[] spikeSquare = new Coordinate[] { new Coordinate(gridX0 + 10, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 20), new Coordinate(gridX0 + 20, gridX0 + 20),
					new Coordinate(gridX0 + 20.3, gridX0 + 20.5), new Coordinate(gridX0 + 20, gridX0 + 10),
					new Coordinate(gridX0 + 10, gridX0 + 10) };

			Geometry spikeSquareG = SvUtil.sdiFactory.createPolygon(spikeSquare);

			Integer minPointDistance = SvParameter.getSysParam(Sv.SDI_MIN_POINT_DISTANCE,
					Sv.DEFAULT_MIN_POINT_DISTANCE);
			svg.testMinVertexDistance(spikeSquareG, minPointDistance);

			try {
				svg.testMinVertexDistance(spikeSquareG, 1000);
			} catch (SvException e) {
				if (!e.getLabelCode().equals(Sv.Exceptions.SDI_VERTEX_DISTANCE_ERR))
					fail("Spike was not detected!");
			}

			Geometry fixed = svg.fixMinVertexDistance(spikeSquareG, 1000);
			if (fixed.getCoordinates().length > 5)
				fail("duplicate coordinate not removed");
			// [POLYGON ((10 15, 20 15, 20 10, 10 10, 10 15)), POLYGON ((10 15, 10 20, 20
			// 20, 20 15, 10 15))]

		} catch (Exception e) {
			fail("Test failed with exception");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomBuffer() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {
			Integer minPointDistance = SvParameter.getSysParam(Sv.SDI_MIN_GEOM_DISTANCE, Sv.DEFAULT_MIN_GEOM_DISTANCE);
			Geometry square = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 5, gridX0 + 9.5, gridY0 + 15, gridY0 + 20));
			// this is with default values and should not raise any exceptions
			svg.testGeomDistance(square, TEST_LAYER_TYPE_ID, minPointDistance);

			try {
				// test with one meter distance
				svg.testGeomDistance(square, TEST_LAYER_TYPE_ID, 1000);
			} catch (SvException ex) {
				if (!ex.getLabelCode().equals(Sv.Exceptions.SDI_GEOM_DISTANCE_ERR))
					fail("Spike was not detected!");
			}

			// fix based on one meter distance
			Geometry result = svg.fixGeomDistance(square, TEST_LAYER_TYPE_ID, 1000);
			// we expect that half meter will be cutoff from 9.5 to 9
			Geometry resultSquare = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 5, gridX0 + 9, gridY0 + 15, gridY0 + 20));

			if (!result.equalsExact(resultSquare))
				fail("Geometry was not cut off properly");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void testGeomHole() throws SvException {

		try (SvGeometry svg = new SvGeometry()) {

			Geometry hole = SvUtil.sdiFactory
					.toGeometry(new Envelope(gridX0 + 15, gridX0 + 17, gridY0 + 15, gridY0 + 17));

			Set<Geometry> gg = svg.getRelatedGeometries(hole, TEST_LAYER_TYPE_ID, SDIRelation.INTERSECTS, null, null,
					false);
			Geometry original = gg.iterator().next();
			Double originalArea = original.getArea();
			// this is with default values and should not raise any exceptions
			Geometry result = svg.holeInPolygon(hole, TEST_LAYER_TYPE_ID, false);

			if ((result.getArea() + hole.getArea()) != originalArea)
				fail("Geometry was not cut off properly");
			// fix based on one meter distance

			// rebuild the tile, now replacing the first geometry with the result with hole
			Geometry[] geoms = testGeomsBaseG(gridX0, gridY0);
			geoms[0] = result;

			SvSDITile layerTile = new SvTestTile(
					new Envelope(gridX0, SvConf.getSdiGridSize() * 1000, gridY0, SvConf.getSdiGridSize() * 1000),
					TEST_LAYER_TYPE_ID, SvUtil.sdiFactory.createGeometryCollection(geoms));
			layerTile.tilelId = "0:0";
			Cache<String, SvSDITile> newCacheBase = SvGeometry.layerCache.get(TEST_LAYER_TYPE_ID);
			newCacheBase.put(layerTile.tilelId, layerTile);

			// this should rebuild the polygon and close the hole
			Geometry resultFill = svg.holeInPolygon(hole, TEST_LAYER_TYPE_ID, true);

			if ((resultFill.getArea()) != originalArea)
				fail("The hole was not filled properly");
		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

	@Test
	public void getGrid() throws SvException {
		try {
			SvGeometry.resetGrid();
			// add the fake boundary as system boundary

			SvGrid sysGrid = new SvGrid(Sv.SDI_SYSGRID);
			if (sysGrid.getInternalGeometries().size() < 1 || sysGrid.getInternalGeomCollection().getArea() < 10)
				fail("Grid init failed");

		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Grid init raised exception");
		}
		initFakeSysBoundary();

	}
	
	
	public void nonNodedTest() throws ParseException
	{
		try {
		String wktPoly1="POLYGON ((7586187.736 4594603.617, 7586187.735 4594603.729, 7586192.732 4594570.022, 7586193.548 4594564.59, 7586199.697 4594567.218, 7586241.788 4594595.046, 7586284.901 4594628.218, 7586274.313 4594640.642, 7586265.454 4594641.202, 7586242.79 4594629.935, 7586230.318 4594632.34, 7586221.811 4594638.347, 7586201.357 4594616.887, 7586187.736 4594603.617))";
		String wktPoly2="POLYGON ((7586187.77 4594603.68, 7586187.77 4594603.69, 7586192.72 4594570.1, 7586193.53 4594564.59, 7586199.7 4594567.2, 7586241.82 4594595.1, 7586284.91 4594628.18, 7586274.33 4594640.68, 7586265.48 4594641.26, 7586242.78 4594629.91, 7586230.28 4594632.41, 7586221.79 4594638.39, 7586201.32 4594616.94, 7586187.77 4594603.68))";

		GeometryFactory gf = SvUtil.sdiFactory;
		WKTReader wkr = new WKTReader(SvUtil.sdiFactory);
		Geometry geom1 = wkr.read(wktPoly1);
		Geometry geom2 = wkr.read(wktPoly2);
		assert(geom1.isValid());
		assert(geom2.isValid());
		Geometry r = geom1.intersection(geom2);
		}catch (Exception e) {
			e.printStackTrace();
			
		}
	}


}
