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
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.prtech.svarog.SvSDITile.SDIRelation;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_interfaces.ISvDatabaseIO;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonWriter;

public class SvGeometryTests {
	@Test
	public void testGrid() {
		if(!SvConf.isSdiEnabled())
			return;

		GeoJsonWriter jtsWriter = new GeoJsonWriter();
		jtsWriter.setUseFeatureType(true);
		SvGeometry svg=null;
		try {
			svg = new SvGeometry();
			Map<String, Geometry> grid = SvGeometry.getGrid();
			if(grid==null || grid.size()<1)
				fail("Failed fetching base grid");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception");
		}

	}

	
	@Test
	public void testBboxParsing() {
		if(!SvConf.isSdiEnabled())
			return;

		SvGeometry svg=null;
		try {
			svg= new SvGeometry();
			SvSDITile tile=svg.getSysBoundary();
			String bbox =SvGeometry.getBBox(tile.getEnvelope());
			System.out.println("Tile bbox:"+bbox);
			Envelope env=SvGeometry.parseBBox(bbox);
			System.out.println("Tile envelope from bbox:"+env.toString());
			System.out.println("Area of envelope:"+env.getArea()/10000+"Ha");
			if(env.getArea()<1)
				fail("BBOX test failed. Envelope are is less than 1 sqm");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("BBOX test failed");
		}

	}
	
	
	
	@Test
	public void testBoundaryIntersect()
	{
		if(!SvConf.isSdiEnabled())
			return;

		WKTReader wkt = new WKTReader();
		try {
			Geometry geom = wkt
					.read("MULTIPOLYGON (((7647100 4583980, 7647830 4583980, 7647830 4583460, 7647100 4583460, 7647100 4583980)))");
		
			
			
			SvSDITile sysbounds = SvGeometry.getSysBoundary();
			ArrayList<Geometry> gl=sysbounds.getRelations(geom, SDIRelation.INTERSECTS);
			if(gl.size()<1)
				fail("Cross bounds polygon didn't detect intersection");
			
			GeometryFactory gf= SvUtil.sdiFactory;
			Point point = gf.createPoint(new Coordinate(7597747,4691992));
			gl=sysbounds.getRelations(point, SDIRelation.INTERSECTS);
			if(gl.size()>0)
				fail("Point detected in boundaries!!!");


			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("WKT Parse error, bad polygon?");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test raised exception:"+e.getFormattedMessage());
		}
		
		
	}
	
	
	@Test
	public void testBoundsSave()
	{
		if(!SvConf.isSdiEnabled())
			return;
		
		WKTReader wkt = new WKTReader();
		WKTWriter wkr = new WKTWriter();
		try {
			Geometry geom = wkt
					.read("MULTIPOLYGON (((7647100 4583980, 7647830 4583980, 7647830 4583460, 7647100 4583460, 7647100 4583980)))");
		
			try {
				SvGeometry svg=new SvGeometry();
				DbDataObject dbounds =new DbDataObject(svCONST.OBJECT_TYPE_SDI_BOUNDS);
				dbounds.setVal("BOUNDS_NAME", "testBounds");
				dbounds.setVal("BOUNDS_ID", "123");
				dbounds.setVal("BOUNDS_CLASS", 1);
				dbounds.setVal("PERIMETER", 0);
				dbounds.setVal("AREA", 0);
				SvGeometry.setGeometry(dbounds, geom);
				//SvGeometry.allowBoundaryIntersect=true;
				//svg.setAllowNullGeometry(true);
				svg.saveGeometry(dbounds);
				String bbox =SvGeometry.getBBox(geom.buffer(100).getEnvelopeInternal());
				ArrayList<Geometry> result=svg.getGeometriesByBBOX(svCONST.OBJECT_TYPE_SDI_BOUNDS, bbox);
				Boolean matched=false;
				for(Geometry g: result)
				{
					System.out.println(((DbDataObject)g.getUserData()).getObject_id());
					System.out.println(wkr.write(g));
					if(g.equals(geom))
						matched=true;
						
				}
				
				if(!matched)
					fail("Inserted polygon not found in result");
			} catch (SvException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				fail("Test failed with exception:"+e.getFormattedMessage());

			}
			
				

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("WKT Parse error, bad polygon?");
		}
		
		
	}
	@Test
	public void testGetTile() {
		if(!SvConf.isSdiEnabled())
			return;

		GeoJsonWriter jtsWriter = new GeoJsonWriter();
		SvGeometry svg=null;
		try {
			svg = new SvGeometry();
			GeometryFactory gf= SvUtil.sdiFactory;
			Point point = gf.createPoint(new Coordinate(7550477,4585319));
			System.out.println("Max tile:"+svg.getMaxXtile()+":"+svg.getMaxYtile());
			
			
			List<Geometry> gcl = SvGeometry.getTileGeometries(point.getEnvelopeInternal());
			if(gcl==null)
				fail("Failed getting tile from point");
			else
				for(Geometry tile:gcl)
				{
					System.out.println("Matched tile:"+tile.getUserData()+", tile string:"+tile.toText());
					SvSDITile sdiTile=svg.getTile(svCONST.OBJECT_TYPE_SDI_BOUNDS, (String)tile.getUserData(), null);
					System.out.println("Fetched tile data:"+sdiTile.getGeoJson());

				}
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Test failed with exception:"+e.getFormattedMessage());
		}

	}
	@Test
	public void testGetGIOHandler() {
		if(!SvConf.isSdiEnabled())
			return;


		ISvDatabaseIO gio = SvConf.getDbHandler();
		if(gio==null)
			fail("Can't get geometry handler");
	}


	@Test
	public void testGetUnitsTile() {
		if(!SvConf.isSdiEnabled())
			return;

		GeoJsonWriter jtsWriter = new GeoJsonWriter();
		SvGeometry svg=null;
		try {
			svg = new SvGeometry();
			GeometryFactory gf= SvUtil.sdiFactory;
				{
					SvSDITile sdiTile=svg.getSDIUnitBoundary(1L);
					if(sdiTile.getInternalGeometries().size()!=80)
						fail("Adm boundaries not present or getter failed");

				}
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	@Test
	public void testGetBoundaryTile() {
		if(!SvConf.isSdiEnabled())
			return;

		GeoJsonWriter jtsWriter = new GeoJsonWriter();
		SvGeometry svg=null;
		try {
			svg = new SvGeometry();
			GeometryFactory gf= SvUtil.sdiFactory;
				{
					SvSDITile sdiTile=svg.getSysBoundary();
					sdiTile.loadTile();
					System.out.println("Fetched tile data:"+sdiTile.getGeoJson());

				}
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

	@Test
	public void testGetTileNotExist() {
		if(!SvConf.isSdiEnabled())
			return;

		GeoJsonWriter jtsWriter = new GeoJsonWriter();
		SvGeometry svg=null;
		try {
			svg = new SvGeometry();
			GeometryFactory gf= SvUtil.sdiFactory;
			System.out.println("Max tile:"+svg.getMaxXtile()+":"+svg.getMaxYtile());
			
			Point point = gf.createPoint(new Coordinate(7469568,4530337));
			List<Geometry> gcl = SvGeometry.getTileGeometries(point.getEnvelopeInternal());
			if(gcl.size()>0)
				fail("A point matching tile which is out of the boundary returned a tile");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("test failed with eexception");
		}

	}
	
	
	@Test
	public void testGetGeomHandler() {
		if(!SvConf.isSdiEnabled())
			return;

		ISvDatabaseIO gio = SvConf.getDbHandler();
		if(gio==null)
			fail("Can't load geometry handler");
	}

}
