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
import java.util.Iterator;
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

	private static void initFakeSysBoundary() {
		// create a fake boundary with about 10x10 grid cells
		Envelope boundaryEnv = new Envelope(gridX0, 10 * SvConf.getSdiGridSize() * 1000, gridY0,
				10 * SvConf.getSdiGridSize() * 1000);
		ArrayList<Geometry> boundGeom = new ArrayList<>();
		boundGeom.add(SvUtil.sdiFactory.toGeometry(boundaryEnv));

		// generate and prepare fake grid
		GeometryCollection grid = SvGrid.generateGrid(boundGeom.get(0), SvConf.getSdiGridSize());
		Cache<String, SvSDITile> gridCache = SvGeometry.getLayerCache(svCONST.OBJECT_TYPE_GRID);
		SvGrid svg = new SvGrid(grid, Sv.SDI_SYSGRID);
		SvGeometry.setSysGrid(svg);

		// add the fake boundary as system boundary
		Cache<String, SvSDITile> cache = SvGeometry.getLayerCache(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE);
		SvSDITile tile = new SvTestTile(boundaryEnv, svCONST.OBJECT_TYPE_SDI_GEOJSONFILE, boundGeom);
		tile.tilelId = Sv.SDI_SYSTEM_BOUNDARY;
		cache.put(tile.tilelId, tile);

	}

	@BeforeClass
	static public void initTestSDI() throws SvException {
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

	static ArrayList<Geometry> testGeomsBase(double x1, double y1) {
		ArrayList<Geometry> g = new ArrayList<>();
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 10, x1 + 20, y1 + 10, y1 + 20)));
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 20, x1 + 30, y1 + 20, y1 + 30)));
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 - 15, x1 + 5, y1 - 15, y1 + 5)));

		WKTReader wkr = new WKTReader();
		String polyHole = "POLYGON ((30 30, 30 40, 40 40, 40 30, 30 30), (35 35, 37 35, 37 37, 35 37, 35 35))";
		try {
			g.add(wkr.read(polyHole));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return g;
	}

	static ArrayList<Geometry> testGeomsSecond(double x1, double y1) {
		ArrayList<Geometry> g = new ArrayList<>();
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 25, x1 + 27, y1 + 20, y1 + 30)));
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 - 15, x1 + 5, y1 - 15, y1 + 5)));
		g.add(SvUtil.sdiFactory.toGeometry(new Envelope(x1 + 35, x1 + 40, y1 + 30, y1 + 40)));
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

			Set<Geometry> copied = svg.splitGeometry(line, TEST_LAYER_TYPE_ID, false, true, false);

			// [POLYGON ((10 15, 20 15, 20 10, 10 10, 10 15)), POLYGON ((10 15, 10 20, 20
			// 20, 20 15, 10 15))]
			Iterator<Geometry> it = copied.iterator();
			Geometry g1 = it.next();
			if (!g1.equalsNorm(g2_1))
				fail("Test did not return a copy of geometry");

			g1 = it.next();
			if (!g1.equalsNorm(g2_2))
				fail("Test did not return a copy of geometry");

		}
		if (SvConnTracker.hasTrackedConnections(false, false))
			fail("You have a connection leak, you dirty animal!");
	}

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
			ArrayList<Geometry> geoms = testGeomsBase(gridX0, gridY0);
			geoms.remove(0);
			geoms.add(0, result);
			SvSDITile layerTile = new SvTestTile(
					new Envelope(gridX0, SvConf.getSdiGridSize() * 1000, gridY0, SvConf.getSdiGridSize() * 1000),
					TEST_LAYER_TYPE_ID, geoms);
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
	public void getGrid() {
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

}
