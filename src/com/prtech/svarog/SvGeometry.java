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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog.SvSDITile.SDIRelation;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonReader;

/**
 * Class for handling all kinds of GIS information attached to the standard
 * DbDataObjects in the system. This class uses the basic {@link DbDataObject}
 * while it focuses only on the fields which are of type GEOMETRY. It provides
 * vast array of caching mechanisms based on tiles.
 * 
 * @author XPS13
 *
 */
public class SvGeometry extends SvCore {

	private static STRtree gridIndex = new STRtree();

	private static Envelope systemBoundsEnvelope = null;

	static private Map<String, Geometry> gridMap = new HashMap<String, Geometry>();

	private static ArrayList<String> borderTiles = new ArrayList<String>();
	static int maxYtile = 0;
	static int maxXtile = 0;

	static long admUnitClass = 0;

	static boolean allowBoundaryIntersect = false;
	static private boolean isSDIInitalized = initSDI();

	static private Map<Long, Cache<String, SvSDITile>> sdiCache = new ConcurrentHashMap<Long, Cache<String, SvSDITile>>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static private Cache<String, SvSDITile> cacheConfig(DbDataObject objectdDescriptor) {
		CacheBuilder builder = CacheBuilder.newBuilder();
		if (objectdDescriptor != null) {
			String cType = (String) objectdDescriptor.getVal("cache_type");

			if (cType.equals("LRU") || cType.equals("LRU_TTL")) {
				if (objectdDescriptor.getVal("cache_size") != null)
					builder = builder.maximumSize((Long) objectdDescriptor.getVal("cache_size"));
				else
					builder = builder.maximumSize((Long) objectdDescriptor.getVal("cache_size"));

			}

			if (cType.equals("TTL") || cType.equals("LRU_TTL")) {
				if (objectdDescriptor.getVal("cache_expiry") != null)
					builder = builder.expireAfterAccess((Long) objectdDescriptor.getVal("cache_expiry"),
							TimeUnit.MINUTES);
				else
					builder = builder.expireAfterAccess(10, TimeUnit.MINUTES);

			}

			// if (!cType.equals("PERM"))
			// builder = builder.removalListener(onRemove);
		} else {
			builder = builder.expireAfterAccess(10, TimeUnit.MINUTES);
			builder = builder.maximumSize(100);
		}
		return (Cache<String, SvSDITile>) builder.<String, SvSDITile>build();
	}

	/**
	 * Method to generate a BBOX string from an envelope. The bbox string is
	 * according to WMS1.1 axis ordering
	 * 
	 * @param env The envelope used to describe the BBOX
	 * @return String version of the boundign box (example BBOX=-180,-90.180,90)
	 */
	public static String getBBox(Envelope env) {
		// BBOX=-180,-90.180,90
		return env.getMinX() + "," + env.getMinY() + "," + env.getMaxX() + "," + env.getMaxY();
	}

	/**
	 * Method to generate a BBOX string from an envelope. The bbox string is
	 * according to WMS1.1 axis ordering
	 * 
	 * @param bbox The envelope used to describe the BBOX
	 * @return Envelope representing the string BBOX
	 * @throws SvException Throws system.error.sdi.cant_parse_bbox if it can't parse
	 *                     the BBOX string
	 */
	public static Envelope parseBBox(String bbox) throws SvException {
		String[] coords = bbox.split(",");
		Envelope retEnv = null;
		if (coords.length != 4)
			throw (new SvException("system.error.sdi.cant_parse_bbox", svCONST.systemUser, null, bbox));

		try {
			Coordinate minLeft = new Coordinate(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
			Coordinate maxRight = new Coordinate(Double.parseDouble(coords[2]), Double.parseDouble(coords[3]));
			retEnv = new Envelope(minLeft, maxRight);
		} catch (Exception e) {
			throw (new SvException("system.error.sdi.cant_parse_bbox", svCONST.systemUser, null, bbox, e));
		}
		// BBOX=-180,-90.180,90
		return retEnv;
	}

	public static Geometry getGeometry(DbDataObject dbo) {
		return (Geometry) dbo.getVal(getGeometryFieldName(dbo.getObject_type()));
	}

	public static Point getCentroid(DbDataObject dbo) {
		return (Point) dbo.getVal(getCentroidFieldName(dbo.getObject_type()));
	}

	public static String getGeometryFieldName(Long objectType) {
		return "GEOM";
	}

	public static String getCentroidFieldName(Long objectType) {
		return "CENTROID";
	}

	public static void setGeometry(DbDataObject dbo, Geometry geom) {
		dbo.setVal(getGeometryFieldName(dbo.getObject_type()), geom);
	}

	public static void setCentroid(DbDataObject dbo, Point centroid) {
		dbo.setVal(getCentroidFieldName(dbo.getObject_type()), centroid);
	}

	// private static Boolean initGrid = initGrid();
	static boolean initSDI() {
		try {
			admUnitClass = Long.parseLong(SvConf.getParam("sys.gis.legal_sdi_unit_type"));

		} catch (Exception e) {
			log4j.error(
					"Can't configure sdi unit legal type via config: sys.gis.legal_sdi_unit_type. Set default to 1");
		}
		allowBoundaryIntersect = SvConf.getParam("sys.gis.allow_boundary_intersect") != null
				&& SvConf.getParam("sys.gis.allow_boundary_intersect").equalsIgnoreCase("TRUE");
		if (log4j.isDebugEnabled())
			log4j.trace("Loading base SDI grid from:./conf/sdi/grid.json");
		String geoJSONBounds = null;
		InputStream is = null;
		try {
			is = SvGeometry.class.getClassLoader().getResourceAsStream("conf/sdi/grid.json");
			if (is == null) {
				String path = "./conf/sdi/grid.json";
				is = new FileInputStream(path);
			}
			if (is != null) {
				geoJSONBounds = IOUtils.toString(is);
			}
		} catch (IOException e) {
			log4j.error("Failed loading base SDI grid from:./conf/sdi/grid.json", e);
			return false;
		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

		GeoJsonReader jtsReader = new GeoJsonReader();
		GeometryCollection grid = null;
		Geometry gridItem = null;
		try {
			jtsReader.setUseFeatureType(true);
			grid = (GeometryCollection) jtsReader.read(geoJSONBounds);
			if (grid != null) {
				for (int i = 0; i < grid.getNumGeometries(); i++) {
					gridItem = grid.getGeometryN(i);
					String TileId = (String) gridItem.getUserData();
					String[] baseId = TileId.split("-");
					if (baseId[1] != null && baseId[1].equals("true"))
						borderTiles.add(baseId[0]);
					gridItem.setUserData(baseId[0]);
					String[] tileColRow = baseId[0].split(":");
					if (maxXtile < Integer.parseInt(tileColRow[0]))
						maxXtile = Integer.parseInt(tileColRow[0]);
					if (maxYtile < Integer.parseInt(tileColRow[1]))
						maxYtile = Integer.parseInt(tileColRow[1]);
					gridIndex.insert(gridItem.getEnvelopeInternal(), gridItem);
					if (systemBoundsEnvelope == null)
						systemBoundsEnvelope = new Envelope(gridItem.getEnvelopeInternal());
					else
						systemBoundsEnvelope.expandToInclude(gridItem.getEnvelopeInternal());
					gridMap.put(baseId[0], gridItem);
				}
			}
		} catch (Exception e) {
			log4j.error("Failed parsing JSON grid:./conf/sdi/grid.json", e);
			return false;
		}

		gridIndex.build();
		if (log4j.isDebugEnabled())
			log4j.trace("Base SDI grid loaded:./conf/sdi/grid.json");
		return true;

	}

	public static Map<String, Geometry> getGrid() {
		return gridMap;
	}

	/**
	 * Method that queries the grid to find the tile for a specific {@link Point}
	 * 
	 * @param point The {@link Point} object for which we need the tile
	 * @return The geometry describing the tile
	 * @throws SvException An exception if the point belongs to multiple tiles
	 */
	public static Geometry getTileGeometry(Point point) throws SvException {
		@SuppressWarnings("unchecked")
		List<Geometry> result = gridIndex.query(point.getEnvelopeInternal());
		if (result.size() == 0)
			return null;
		else if (result.size() == 1)
			return result.get(0);
		else
			throw (new SvException("system.error.sdi.multi_tiles_returned", svCONST.systemUser, null, point));

	}

	/**
	 * Method that returns the grid tiles intersecting a specific envelope
	 * 
	 * @param envelope The envelope which we want to use as bounding box for the
	 *                 returned geometries
	 * @return List of {@link Geometry} objects
	 */
	public static List<Geometry> getTileGeometries(Envelope envelope) {
		List<Geometry> result = gridIndex.query(envelope);

		return result;
	}

	public ArrayList<Geometry> getGeometriesByBBOX(Long typeId, String bbox) throws SvException {
		Envelope envelope = parseBBox(bbox);
		return getGeometries(typeId, envelope);
	}

	public ArrayList<Geometry> getGeometries(Long typeId, Envelope envelope) throws SvException {
		List<Geometry> result = gridIndex.query(envelope);
		ArrayList<Geometry> geoms = new ArrayList<Geometry>();
		for (Geometry tileGeom : result) {
			SvSDITile tile = getTile(typeId, (String) tileGeom.getUserData(), null);
			if (envelope.covers(tile.getEnvelope())) {
				geoms.addAll(tile.getInternalGeometries());
			} else {
				geoms.addAll(tile.getRelations(SvUtil.sdiFactory.toGeometry(envelope), SDIRelation.INTERSECTS, true));
			}
		}
		return geoms;
	}

	public Geometry cutOffLayerImpl(Geometry geom, Long typeId) throws SvException {
		return cutOffLayerImpl(geom, typeId, false);
	}

	public boolean cutOffLayerTest(Geometry geom, Long typeId) throws SvException {
		return cutOffLayerImpl(geom, typeId, true) != null;
	}

	private Geometry cutOffLayerImpl(Geometry geom, Long typeId, boolean testOnly) throws SvException {
		if (geom == null)
			return null;
		boolean intersectionDetected = false;
		Envelope envelope = geom.getEnvelopeInternal();
		List<Geometry> result = gridIndex.query(envelope);
		DbDataObject userData = (DbDataObject) geom.getUserData();
		for (Geometry tileGeom : result) {
			SvSDITile tile = getTile(typeId, (String) tileGeom.getUserData(), null);
			for (Geometry relatedGeom : tile.getRelations(geom, SDIRelation.INTERSECTS, false)) {
				Long relatedOid = ((DbDataObject) relatedGeom.getUserData()).getObjectId();
				if (!testOnly) {
					if (userData == null || (userData != null && !userData.getObjectId().equals(relatedOid)))
						geom = geom.difference(relatedGeom);
				} else {
					intersectionDetected = true;
					break;
				}
			}
		}
		// set the user data //parent Dbo to the modified geometry
		if (!testOnly)
			geom.setUserData(userData);

		return intersectionDetected ? geom : null;
	}

	public Collection<Geometry> cutOffGeometry(Geometry geom, Long typeId) throws SvException {
		return cutOffGeometryImpl(geom, typeId, false);
	}

	public boolean cutOffGeometryTest(Geometry geom, Long typeId) throws SvException {
		return cutOffGeometryImpl(geom, typeId, true) != null;
	}

	public Collection<Geometry> cutOffGeometryImpl(Geometry geom, Long typeId, boolean testOnly) throws SvException {
		if (geom == null)
			return null;
		boolean intersectionDetected = false;
		Envelope envelope = geom.getEnvelopeInternal();
		List<Geometry> result = gridIndex.query(envelope);
		Map<Long, Geometry> updatedGeometries = new HashMap<>();
		DbDataObject userData = (DbDataObject) geom.getUserData();
		for (Geometry tileGeom : result) {
			SvSDITile tile = getTile(typeId, (String) tileGeom.getUserData(), null);
			for (Geometry relatedGeom : tile.getRelations(geom, SDIRelation.INTERSECTS, false)) {
				DbDataObject relatedUserData = ((DbDataObject) relatedGeom.getUserData());
				// if we haven't processed the geometry
				if (!testOnly && !updatedGeometries.containsKey(relatedUserData.getObjectId())) {
					if (userData == null || (userData != null && !userData.getObjectId().equals(relatedUserData.getObjectId()))) {
						Geometry modifiedGeom = relatedGeom.difference(geom);
						modifiedGeom.setUserData(geom.getUserData());
						updatedGeometries.put(relatedUserData.getObjectId(), modifiedGeom);
					}
				} else {
					intersectionDetected = true;
					break;
				}
			}
		}
		return intersectionDetected ? updatedGeometries.values() : null;
	}

	/**
	 * Generate the envelope for the requested tile
	 * 
	 * @param tileTypeId
	 * @param tileId
	 * @param tileParams
	 * @return
	 */
	static Envelope getTileEnvelope(Long tileTypeId, String tileId, HashMap<String, Object> tileParams) {
		Envelope envl = null;
		// if the tileId is found in the grid map, it means we are fetching
		// a grid based tile
		if (gridMap.containsKey(tileId)) {
			Geometry geom = gridMap.get(tileId);
			envl = geom.getEnvelopeInternal();
		} else
			envl = systemBoundsEnvelope;

		return envl;
	}

	public static SvSDITile createTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams) {
		SvSDITile currentTile = null;
		if (tileParams == null)
			tileParams = new HashMap<String, Object>();

		Envelope tileEnv = getTileEnvelope(tileTypeId, tileId, tileParams);
		tileParams.put("ENVELOPE", tileEnv);
		if (tileTypeId == svCONST.OBJECT_TYPE_SDI_GEOJSONFILE)
			currentTile = new SvSDIJsonTile(tileTypeId, tileId, tileParams);
		else
			currentTile = new SvSDIDbTile(tileTypeId, tileId, tileParams);
		return currentTile;
	}

	/**
	 * A method that creates a new {@link SvSDITile} based on the GeoJSON system
	 * boundary
	 * 
	 * @return SvSDITile representing the system boundary
	 */
	public static SvSDITile getSysBoundary() {
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("FILE_PATH", "conf/sdi/boundary.json");
		return getTile(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE, "SYSBOUNDS", params);
	}

	/**
	 * A method that returns all unit boundaries for a specific class. We assume
	 * that the UNIT_CLASS=0 is administrative boundary. All other classes are user
	 * defined
	 * 
	 * @param unitClass The id of the class of units to be returned
	 * @return The SvSDITile instance holding all units of the specific class
	 * @throws SvException Pass through of underlying exceptions
	 */
	public static SvSDITile getSDIUnitBoundary(Long unitClass) throws SvException {
		DbSearchCriterion dbs = new DbSearchCriterion("UNIT_CLASS", DbCompareOperand.EQUAL, unitClass);
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("DB_SEARCH", dbs);
		return getTile(svCONST.OBJECT_TYPE_SDI_UNITS, "SDI_UNITS." + unitClass.toString(), params);
	}

	/**
	 * Method for creating a new {@link SvSDITile} based on tile type id and a set
	 * of parameters
	 * 
	 * @param tileTypeId The tile type ID
	 * @param tileId     The string identification of the tile
	 * @param tileParams Array of strings containing tile id or file path.
	 * @return Instance of the tile, either from the cache of newly created
	 * 
	 */
	public static SvSDITile getTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams) {

		Cache<String, SvSDITile> cache = null;
		synchronized (sdiCache) {
			cache = sdiCache.get(tileTypeId);
			if (cache == null) {
				DbDataObject dbt = null;
				try {
					dbt = getDbt(tileTypeId);
				} catch (SvException ex) {
					if (!tileTypeId.equals(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE))
						log4j.warn("Tile type id: " + tileTypeId.toString()
								+ " is not representing a valid system object in the svarog tables list");
				}
				cache = cacheConfig(dbt);
				((ConcurrentHashMap<Long, Cache<String, SvSDITile>>) sdiCache).putIfAbsent(tileTypeId, cache);
			}
		}

		SvSDITile svTile = null;
		synchronized (cache) {
			svTile = cache.getIfPresent(tileId);
			if (svTile == null) {
				svTile = createTile(tileTypeId, tileId, tileParams);
				cache.put(tileId, svTile);
			}
		}
		return svTile;

	}

	/**
	 * Method to calculate a centroid of a geometry. If the geometry is polygon or
	 * multypolygon it ensures the centroid is inside the bounds It also ensures
	 * that the centroid will not have integer coordinates so it doesn't hit a grid
	 * line
	 * 
	 * @param geom The geometry for which we want to calculate a centroid
	 * @return
	 */
	static Point calculateCentroid(Geometry geom) {
		float factorX = 0.5f;
		float factorY = 0.5f;
		Point centroid = null;
		centroid = (geom).getCentroid();
		Coordinate cor = centroid.getCoordinate();
		if (cor.x % 1 == 0)
			cor.x = cor.x + factorX;
		if (cor.y % 1 == 0)
			cor.y = cor.y + factorY;
		centroid = SvUtil.sdiFactory.createPoint(cor);

		if (!geom.covers(centroid) && (geom instanceof Polygon || geom instanceof MultiPolygon)) {
			if (log4j.isDebugEnabled())
				log4j.info("Geometry centroid " + centroid.toText() + "is out of the polygon:" + geom.toText());
			centroid = geom.getInteriorPoint();
			cor = centroid.getCoordinate();
			if (cor.x % 1 == 0)
				cor.x = cor.x + factorX;
			if (cor.y % 1 == 0)
				cor.y = cor.y + factorY;
			centroid = SvUtil.sdiFactory.createPoint(cor);

			while (!geom.covers(centroid) && (geom instanceof Polygon || geom instanceof MultiPolygon)) {
				if (log4j.isDebugEnabled())
					log4j.info("Geometry centroid " + centroid.toText() + "is out of the polygon:" + geom.toText());
				Envelope env = geom.getEnvelopeInternal();
				// create a random point in the geometry envelope
				cor.x = env.getMinX() + Math.random() * env.getWidth();
				cor.y = env.getMinY() + Math.random() * env.getHeight();
				if (cor.x % 1 == 0)
					cor.x = cor.x + factorX;
				if (cor.y % 1 == 0)
					cor.y = cor.y + factorY;
				centroid = SvUtil.sdiFactory.createPoint(cor);

			}
		}

		// make sure that the coordinates of the centroid are never integers
		// if the coordinates are integers, they might belong on a grid line
		return centroid;
	}

	public static List<Geometry> getGeometries(ArrayList<String> tileIds, Long sdiTypeId) {
		List<Geometry> result = null;
		return result;

	}

	public static int getMaxYtile() {
		return maxYtile;
	}

	public static void setMaxYtile(int maxYtile) {
		SvGeometry.maxYtile = maxYtile;
	}

	public static int getMaxXtile() {
		return maxXtile;
	}

	public static void setMaxXtile(int maxXtile) {
		SvGeometry.maxXtile = maxXtile;
	}

	//////////////////////////////////////////////////////////////////////////
	// Instance specific methods and variables
	//////////////////////////////////////////////////////////////////////////
	/**
	 * Flag to allow saving SDI objects with null geometry
	 */
	private boolean allowNullGeometry = false;

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id The user session which will be used to associate the SvCore
	 *                   instance with the instance user
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvGeometry(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id   The user session which will be used to associate the
	 *                     SvCore instance with the instance user
	 * @param sharedSvCore The shared core which will be used to share the JDBC
	 *                     connection
	 * @throws SvException Pass through of underlying exceptions
	 */
	public SvGeometry(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @param sharedSvCore The shared core which will be used to share the JDBC
	 *                     connection
	 * @throws SvException Pass through of underlying exceptions from the
	 *                     constructor
	 */
	public SvGeometry(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system privileges.
	 * 
	 * @throws SvException Pass through of underlying exceptions from the
	 *                     constructor
	 */
	SvGeometry() throws SvException {
		super(svCONST.systemUser, null);
	}

	/**
	 * Method to execute basic verifications on the geometry 1. Verifies if the
	 * geometry is valid 2. Verified that the geometry intersects at least one grid
	 * tile 3. If the intersecting tile is a border tile then test if the geometry
	 * intersects the system boundary
	 * 
	 * @param dbo The {@link DbDataObject} containing at least GEOMETRY column.
	 * @throws SvException If the geometry is not valid throws
	 *                     "system.error.sdi.invalid_geom". If the geometry is
	 *                     outside of the system grid then
	 *                     "system.error.sdi.geometry_not_in_grid" is thrown. If the
	 *                     geometry intersects the system boundary and svarog is not
	 *                     configured to allow system boundary intersections then
	 *                     "system.error.sdi.geometry_crosses_sysbounds" is thrown.
	 */
	public void verifyBounds(DbDataObject dbo) throws SvException {
		Geometry geom = SvGeometry.getGeometry(dbo);
		if (!geom.isValid())
			throw (new SvException("system.error.sdi.invalid_geom", this.instanceUser, dbo, null));

		List<Geometry> tiles = getTileGeometries(geom.getEnvelopeInternal());
		if (tiles.size() < 1)
			throw (new SvException("system.error.sdi.geometry_not_in_grid", this.instanceUser, dbo, null));

		if (!allowBoundaryIntersect)
			for (Geometry tile : tiles) {
				if (borderTiles.contains((String) tile.getUserData())) {
					if (tile.covers(geom))
						continue;
					else if (tile.intersects(geom)) {
						SvSDITile sysbounds = getSysBoundary();
						if (sysbounds.getRelations(geom, SDIRelation.COVERS).size() < 0)
							throw (new SvException("system.error.sdi.geometry_crosses_sysbounds", this.instanceUser,
									dbo, null));
					}
				}
			}

	}

	/**
	 * Method to get an Administrative Unit based on a point geometry.
	 * 
	 * @param geom The point geometry for which we want to find the administrative
	 *             unit
	 * @return DbDataObject representation of the administrative unit
	 * @throws SvException throws system.error.sdi.adm_bounds_overlaps if the point
	 *                     belongs to more than one Administrative Unit (i.e. there
	 *                     is overlap) or system.error.sdi.adm_bounds_not_found if
	 *                     the point is outside of coverage of the administrative
	 *                     units
	 */
	public DbDataObject getAdmUnit(Point geom) throws SvException {
		SvSDITile admTile = getSDIUnitBoundary(admUnitClass);
		ArrayList<Geometry> units = admTile.getRelations(geom, SDIRelation.COVERS);
		if (units.size() == 1)
			return (DbDataObject) units.get(0).getUserData();
		else if (units.size() > 1)
			throw (new SvException("system.error.sdi.adm_bounds_overlaps", this.instanceUser, null, geom));
		else
			throw (new SvException("system.error.sdi.adm_bounds_not_found", this.instanceUser, null, geom));

	}

	private void prepareGeometry(DbDataObject dbo) throws SvException {

		// test if the geometry is in the system boundaries
		verifyBounds(dbo);
		Geometry geom = getGeometry(dbo);
		Point centroid = calculateCentroid(geom);

		// if area is not set or we have configured to override
		if (dbo.getVal("AREA") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("AREA", geom.getArea());
		if (dbo.getVal("AREA_HA") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("AREA_HA", geom.getArea() / 10000);
		if (dbo.getVal("AREA_HA") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("AREA_KM2", geom.getArea() / 1000000); // needed for
																// moemris
		if (dbo.getVal("AREA_HA") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("PERIMETER", geom.getLength());
		setCentroid(dbo, centroid);

		// admUnitClass dbo.setVal("CENTROID", centroid);
		// DISABLING as per https://192.168.100.130/prtech/svarog/issues/21
		// TODO https://192.168.100.130/prtech/svarog/issues/21
		/*
		 * if (!dbo.getObject_type().equals(svCONST.OBJECT_TYPE_SDI_UNITS) ) {
		 * DbDataObject admUnit = getAdmUnit(centroid);
		 * dbo.setParent_id(admUnit.getObject_id()); }
		 */

	}

	/**
	 * Method to save a DbDataArray of DbDataObjects which are of Geometry Type and
	 * have set the flag hasGeometries. The method will perform basic saveObject on
	 * the alphanumeric fields and further process the GEOMETRY columns
	 * 
	 * @param dba the array of objects to be saved in the database
	 * @throws SvException Throws system.error.sdi.non_sdi_type if the object not of
	 *                     geometry type or system.error.sdi.geom_field_missing if
	 *                     the object has no geometry field while SvGeometry has
	 *                     allowNullGeometry set to false
	 */
	public void saveGeometry(DbDataArray dba, Boolean isBatch) throws SvException {
		SvWriter svw = null;
		try {

			svw = new SvWriter(this);
			svw.isInternal = true; // flag it as internal so can save SDI types
			Geometry currentGeom = null;
			for (DbDataObject dbo : dba.getItems()) {
				if (!SvCore.hasGeometries(dbo.getObjectType()))
					throw (new SvException("system.error.sdi.non_sdi_type", instanceUser, dba, null));
				currentGeom = getGeometry(dbo);
				if (currentGeom != null) {
					prepareGeometry(dbo);
				} else if (!allowNullGeometry)
					throw (new SvException("system.error.sdi.geom_field_missing", instanceUser, dba, null));
			}
			List<Geometry> tileGeomList = null;
			List<SvSDITile> tileList = new ArrayList<SvSDITile>();
			svw.saveObject(dba, isBatch);
			for (DbDataObject dbo : dba.getItems()) {
				Geometry vdataGeom = SvGeometry.getGeometry(dbo);
				if (vdataGeom != null) {
					Envelope env = vdataGeom.getEnvelopeInternal();
					tileGeomList = SvGeometry.getTileGeometries(env);
					for (Geometry tgl : tileGeomList) {
						String tileID = (String) tgl.getUserData();
						SvSDITile tile = SvGeometry.getTile(dbo.getObjectType(), tileID, null);
						tileList.add(tile);
					}
				}
			}

			cacheCleanup(tileList);
		} finally {
			if (svw != null)
				svw.release();
		}
	}

	private void cacheCleanup(List<SvSDITile> tileList) {
		for (SvSDITile tile : tileList) {
			tile.setIsTileDirty(true);
		}
		// broadcast the dirty objects to the cluster
		// if we are coordinator, broadcast through the proxy otherwise
		// broadcast through the client
		if (SvCluster.getIsActive().get()) {
			if (!SvCluster.isCoordinator())
				SvClusterNotifierClient.publishDirtyTileArray(tileList);
			else
				SvClusterNotifierProxy.publishDirtyTileArray(tileList);
		}

	}

	static void markDirtyTile(long tileTypeId, int[] cell) {
		if (cell != null && cell.length > 2) {
			String tileId = Integer.toString(cell[0]) + ":" + Integer.toString(cell[1]);
			SvSDITile tile = getTile(tileTypeId, tileId, null);
			if (tile != null)
				tile.setIsTileDirty(true);
		}
	}

	public void saveGeometry(DbDataArray dba) throws SvException {
		saveGeometry(dba, false);
	}

	/**
	 * Method to save a single geometry to the database. Same as
	 * {@link #saveGeometry(DbDataArray)}, but using a single object instead of
	 * array.
	 * 
	 * @param dbo The object which should be saved
	 * @throws SvException Pass through of underlying exceptions
	 */
	public void saveGeometry(DbDataObject dbo) throws SvException {
		DbDataArray dba = new DbDataArray();
		dba.addDataItem(dbo);
		saveGeometry(dba);
	}

	/**
	 * Getter for allowNullGeometry
	 * 
	 * @return return true if null geometries are allowed
	 */
	public boolean getAllowNullGeometry() {
		return allowNullGeometry;
	}

	/**
	 * If you set the allow null geometry flag, {@link SvGeometry} will allow saving
	 * empty geometries. Your user has to have the svCONST.NULL_GEOMETRY_ACL
	 * permission
	 * 
	 * @param allowNullGeometry True/False
	 */
	public void setAllowNullGeometry(boolean allowNullGeometry) {
		if (this.hasPermission(svCONST.NULL_GEOMETRY_ACL))
			this.allowNullGeometry = allowNullGeometry;
	}

	public static boolean isSDIInitalized() {
		return isSDIInitalized;
	}

}
