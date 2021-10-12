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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.prtech.svarog.SvSDITile.SDIRelation;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.SvCharId;
import com.vividsolutions.jts.algorithm.Angle;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygonal;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonReader;
import com.vividsolutions.jts.operation.polygonize.Polygonizer;
import com.vividsolutions.jts.operation.union.UnaryUnionOp;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;
import com.google.common.math.DoubleMath;

/**
 * Class for handling all kinds of GIS information attached to the standard
 * DbDataObjects in the system. This class uses the basic {@link DbDataObject}
 * while it focuses only on the fields which are of type GEOMETRY. It provides
 * vast array of caching mechanisms based on tiles.
 * 
 * @author XPS13
 *
 */
public class SvGeometry extends SvWriter {

	static final Map<Long, Cache<String, SvSDITile>> layerCache = new ConcurrentHashMap<>();
	public static final String GEOM_STRUCT_TYPE = initStructType();

	static String initStructType() {
		String geomStructType = Sv.EMPTY_STRING;
		try {
			geomStructType = SvCore.getDbHandler().getSQLKeyWordsBundle().getString("GEOMETRY_STRUCT_TYPE");
		} catch (SvException e) {
			log4j.error("Can't get geometry struct type!", e);
		}
		return geomStructType;
	}

	private static volatile SvGrid sysGrid = null;
	private static volatile SvSDITile sysBoundary = null;

	static SvGrid getSysGrid() throws SvException {
		if (sysGrid == null) {
			synchronized (SvGeometry.class) {
				if (sysGrid == null)
					sysGrid = new SvGrid(Sv.SDI_SYSGRID);

			}
		}
		return sysGrid;
	}

	/**
	 * This method shall be used only to enfore reset of the grid and boundary
	 */
	static void resetGrid() {
		sysGrid = null;
		sysBoundary = null;
		Cache<String, SvSDITile> cache = SvGeometry.getLayerCache(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE);
		cache.invalidate(Sv.SDI_SYSTEM_BOUNDARY);
	}

	/**
	 * Method to set a system grid before its initialised. Used for test purposes
	 * 
	 * @param svg
	 */
	static void setSysGrid(SvGrid svg) {
		if (sysGrid == null)
			sysGrid = svg;
	}

	/**
	 * A method that creates a new {@link SvSDITile} based on the GeoJSON system
	 * boundary
	 * 
	 * @return SvSDITile representing the system boundary
	 * @throws SvException
	 */
	public static SvSDITile getSysBoundary() throws SvException {
		if (sysBoundary == null) {
			synchronized (SvGeometry.class) {
				if (sysBoundary == null) {
					HashMap<String, Object> params = new HashMap<String, Object>();
					params.put("FILE_PATH", "conf/sdi/boundary.json");
					sysBoundary = getTile(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE, Sv.SDI_SYSTEM_BOUNDARY, params);
				}
			}
		}
		return sysBoundary;
	}

	static Cache<String, SvSDITile> getLayerCache(Long tileTypeId) {
		Cache<String, SvSDITile> cache = null;
		cache = layerCache.get(tileTypeId);
		if (cache == null) {
			DbDataObject dbt = null;
			try {
				if (!(tileTypeId.equals(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE)
						|| tileTypeId.equals(svCONST.OBJECT_TYPE_GRID)))
					dbt = getDbt(tileTypeId);

				@SuppressWarnings("unchecked")
				CacheBuilder<String, SvSDITile> b = (CacheBuilder<String, SvSDITile>) DbCache.createBuilder(dbt);
				Cache<String, SvSDITile> newCache = b.<String, SvSDITile>build();
				cache = layerCache.putIfAbsent(tileTypeId, newCache);
				cache = cache != null ? cache : newCache;
			} catch (SvException ex) {
				if (!tileTypeId.equals(svCONST.OBJECT_TYPE_SDI_GEOJSONFILE))
					log4j.warn("Tile type id: " + tileTypeId.toString()
							+ " is not representing a valid system object in the svarog tables list");
			}
		}
		return cache;
	}

	/**
	 * Method to generate a BBOX string from an envelope. The bbox string is
	 * according to WMS1.1 axis ordering
	 * 
	 * @param env The envelope used to describe the BBOX
	 * @return String version of the bounding box (example BBOX=-180,-90.180,90)
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
		return (Geometry) dbo.getVal(getGeometryFieldName(dbo.getObjectType()));
	}

	public static Point getCentroid(DbDataObject dbo) {
		return (Point) dbo.getVal(getCentroidFieldName(dbo.getObjectType()));
	}

	public static String getGeometryFieldName(Long objectType) {
		return Sv.GEOM;
	}

	public static String getCentroidFieldName(Long objectType) {
		return Sv.CENTROID;
	}

	public static void setGeometry(DbDataObject dbo, Geometry geom) {
		dbo.setVal(getGeometryFieldName(dbo.getObjectType()), geom);
	}

	public static void setCentroid(DbDataObject dbo, Point centroid) {
		dbo.setVal(getCentroidFieldName(dbo.getObjectType()), centroid);
	}

	public static Map<String, Geometry> getGrid() throws SvException {
		return getSysGrid().getGridMap();
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
		List<Geometry> result = getSysGrid().getGridIndex().query(point.getEnvelopeInternal());
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
	 * @throws SvException
	 */
	public static List<Geometry> getTileGeometries(Envelope envelope) throws SvException {
		@SuppressWarnings("unchecked")
		List<Geometry> result = getSysGrid().getGridIndex().query(envelope);
		return result;
	}

	/**
	 * Generate the envelope for the requested tile
	 * 
	 * @param tileTypeId
	 * @param tileId
	 * @param tileParams
	 * @return
	 * @throws SvException
	 */
	static Envelope getTileEnvelope(Long tileTypeId, String tileId, HashMap<String, Object> tileParams)
			throws SvException {
		Envelope envl = null;
		// if the tileId is found in the grid map, it means we are fetching
		// a grid based tile
		if (getSysGrid().getGridMap().containsKey(tileId)) {
			Geometry geom = getSysGrid().getGridMap().get(tileId);
			envl = geom.getEnvelopeInternal();
		} else
			envl = getSysGrid().getGridEnvelope();

		return envl;
	}

	public static SvSDITile createTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams)
			throws SvException {
		SvSDITile currentTile = null;
		if (tileParams == null)
			tileParams = new HashMap<String, Object>();

		if (tileTypeId == svCONST.OBJECT_TYPE_SDI_GEOJSONFILE)
			currentTile = new SvSDIJsonTile(tileTypeId, tileId, tileParams);
		else {
			Envelope tileEnv = getTileEnvelope(tileTypeId, tileId, tileParams);
			tileParams.put(Sv.ENVELOPE, tileEnv);
			currentTile = new SvSDIDbTile(tileTypeId, tileId, tileParams);
		}
		return currentTile;
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
	public static SvSDITile getSDIUnitBoundary(String unitClass) throws SvException {
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
	 * @throws SvException
	 * 
	 */
	public static SvSDITile getTile(Long tileTypeId, String tileId, HashMap<String, Object> tileParams)
			throws SvException {
		Cache<String, SvSDITile> cache = getLayerCache(tileTypeId);
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

		if (DoubleMath.isMathematicalInteger(cor.x))
			cor.x = cor.x + factorX;
		if (DoubleMath.isMathematicalInteger(cor.y))
			cor.y = cor.y + factorY;
		centroid = SvUtil.sdiFactory.createPoint(cor);

		if (!geom.covers(centroid) && (geom instanceof Polygon || geom instanceof MultiPolygon)) {
			if (log4j.isDebugEnabled())
				log4j.trace("Geometry centroid " + centroid.toText() + "is out of the polygon:" + geom.toText());
			centroid = geom.getInteriorPoint();
			cor = centroid.getCoordinate();
			if (DoubleMath.isMathematicalInteger(cor.x))
				cor.x = cor.x + factorX;
			if (DoubleMath.isMathematicalInteger(cor.y))
				cor.y = cor.y + factorY;
			centroid = SvUtil.sdiFactory.createPoint(cor);

			while (!geom.covers(centroid) && (geom instanceof Polygon || geom instanceof MultiPolygon)) {
				if (log4j.isDebugEnabled())
					log4j.trace("Geometry centroid " + centroid.toText() + "is out of the polygon:" + geom.toText());
				Envelope env = geom.getEnvelopeInternal();
				// create a random point in the geometry envelope
				cor.x = env.getMinX() + Math.random() * env.getWidth();
				cor.y = env.getMinY() + Math.random() * env.getHeight();
				if (DoubleMath.isMathematicalInteger(cor.x))
					cor.x = cor.x + factorX;
				if (DoubleMath.isMathematicalInteger(cor.y))
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

	/**
	 * Method to find all geometries from the layer identified with layerTypeId,
	 * which have a spatial relation of type sdiRelation with Geometry specified by
	 * the geom parameter
	 * 
	 * @param geom            The related geometry
	 * @param layerTypeId     The layer which is test for relation
	 * @param sdiRelation     The spatial relation
	 * @param filterFieldName The field name of the associated DbDataObject of the
	 *                        layer geometry which should be filtered
	 * @param filterValue     The which should be matched as equal. Geometries which
	 *                        don't match the filters are not returned
	 * @param excludeSelf     Flag if you want to exclude the test against self
	 *                        (requires that both geometries have been saved to the
	 *                        DB and have valid object Id)
	 * @return A set of related geometries
	 * @throws SvException Exception if raised by underlying methods
	 */
	public Set<Geometry> getRelatedGeometries(Geometry geom, Long layerTypeId, SDIRelation sdiRelation,
			SvCharId filterFieldName, Object filterValue, boolean excludeSelf) throws SvException {
		return getRelatedGeometries(geom, layerTypeId, sdiRelation, filterFieldName, filterValue, false, excludeSelf);
	}

	/**
	 * Method to find all geometries from the layer identified with layerTypeId,
	 * which have a spatial relation of type sdiRelation with Geometry specified by
	 * the geom parameter
	 * 
	 * @param geom            The related geometry
	 * @param layerTypeId     The layer which is test for relation
	 * @param sdiRelation     The spatial relation
	 * @param filterFieldName The field name of the associated DbDataObject of the
	 *                        layer geometry which should be filtered
	 * @param filterValue     The which should be matched as equal. Geometries which
	 *                        don't match the filters are not returned
	 * @param reverseFilter   If this flag is true, the filter will return only
	 *                        geometries which do not match
	 * @param excludeSelf     Flag if you want to exclude the test against self
	 *                        (requires that both geometries have been saved to the
	 *                        DB and have valid object Id)
	 * @return A set of related geometries
	 * @throws SvException Exception if raised by underlying methods
	 */
	public Set<Geometry> getRelatedGeometries(Geometry geom, Long layerTypeId, SDIRelation sdiRelation,
			SvCharId filterFieldName, Object filterValue, boolean reverseFilter, boolean excludeSelf)
			throws SvException {
		HashSet<Geometry> geoms = new HashSet<>();
		SvCharId objectIdField = new SvCharId(Sv.OBJECT_ID);
		if (geom != null) {
			DbDataObject userData = (DbDataObject) geom.getUserData();
			Object geometryOid = userData != null ? userData.getObjectId() : null;
			@SuppressWarnings("unchecked")
			List<Geometry> gridItems = getSysGrid().getGridIndex().query(geom.getEnvelopeInternal());
			for (Geometry gridGeom : gridItems) {
				SvSDITile tile = getTile(layerTypeId, (String) gridGeom.getUserData(), null);
				Set<Geometry> relatedGeoms = tile.getRelations(geom, sdiRelation, false);
				for (Geometry g : relatedGeoms) {
					// apply the filter by value
					DbDataObject relatedUserData = ((DbDataObject) g.getUserData());
					if (filterFieldName != null && (!reverseFilter)
							^ (!SvUtil.fieldMatchValue(relatedUserData, filterFieldName, filterValue)))
						continue;
					// exclude self if needed
					if (excludeSelf && SvUtil.fieldMatchValue(relatedUserData, objectIdField, geometryOid))
						continue;

					if (!geoms.contains(g))
						geoms.add(g);
				}
			}
		}
		return geoms;
	}

	/**
	 * Method to create a geometry from point selector over layer identified with
	 * layerTypeId. The geometries found in the base layer shall be cut off from the
	 * diff layer and the result will be provided back to the caller
	 * 
	 * @param point                The point from which we can select the source
	 *                             geometries
	 * @param layerTypeId          The layer from which we shall load the geometries
	 * @param diffLayer            The target layer over which we should do
	 *                             difference
	 * @param allowMultiGeometries Flag to allow one point to select more than one
	 *                             geometry
	 * @return A set of resulting geometries
	 * @throws SvException if allowMulti geometries is false, and the base layer has
	 *                     more than one geometry intersecting with the point, a
	 *                     Sv.Exceptions.SDI_MULTIPLE_GEOMS_FOUND exception will be
	 *                     raised
	 */
	public Set<Geometry> geometryFromPoint(Point point, Long layerTypeId, Long diffLayer, boolean allowMultiGeometries)
			throws SvException {
		Set<Geometry> intersected = getRelatedGeometries(point, layerTypeId, SDIRelation.INTERSECTS, null, null, false);
		if (intersected.size() > 1 && !allowMultiGeometries)
			throw (new SvException(Sv.Exceptions.SDI_MULTIPLE_GEOMS_FOUND, svCONST.systemUser, null, point));
		Set<Geometry> result = new HashSet<>();
		for (Geometry originalGeom : intersected) {
			Set<Geometry> related = getRelatedGeometries(originalGeom, diffLayer, SDIRelation.INTERSECTS, null, null,
					false);
			for (Geometry relatedGeom : related) {
				originalGeom = originalGeom.difference(relatedGeom);
			}
			// if the difference resulted in multipolygon, we are interested only in the
			// polygon which covers the point
			if (originalGeom.getNumGeometries() > 1)
				for (int i = 0; i < originalGeom.getNumGeometries(); i++) {
					Geometry gp = originalGeom.getGeometryN(i);
					if (gp.covers(point)) {
						originalGeom = gp;
						break;
					}

				}
			result.add(originalGeom);
		}
		return result;
	}

	/**
	 * Method to select a set of geometries from the database by seleting a point on
	 * the map. Overloaded version for backwards compatibility which doesn't allow
	 * filtering. See
	 * {@link #getGeometryByPoint(Point, Long, boolean, SvCharId, Object)}
	 * 
	 * @param p                    The point which was selected on the map
	 * @param layerTypeId          The layer ID from the geometries shall be
	 *                             selected
	 * @param allowMultiGeometries If the flag is true, the method will return more
	 *                             than one geometry. If the flag is false any
	 *                             overlap will raise exception
	 * @return
	 * @throws SvException
	 */
	Set<Geometry> getGeometryByPoint(Point p, Long layerTypeId, boolean allowMultiGeometries) throws SvException {
		return getGeometryByPoint(p, layerTypeId, allowMultiGeometries, null, null, false);
	}

	/**
	 * Method to select a set of geometries from the database by seleting a point on
	 * the map.
	 * 
	 * @param p                    The point which was selected on the map
	 * @param layerTypeId          The layer ID from the geometries shall be
	 *                             selected
	 * @param allowMultiGeometries If the flag is true, the method will return more
	 *                             than one geometry. If the flag is false any
	 *                             overlap will raise exception
	 * @param filterFieldName      The field name of the associated DbDataObject of
	 *                             the layer geometry which should be filtered
	 * @param filterValue          The which should be matched as equal
	 * @return
	 * @throws SvException
	 */
	Set<Geometry> getGeometryByPoint(Point p, Long layerTypeId, boolean allowMultiGeometries, SvCharId filterFieldName,
			Object filterValue, boolean reverseFilter) throws SvException {
		Set<Geometry> intersected = getRelatedGeometries(p, layerTypeId, SDIRelation.INTERSECTS, filterFieldName,
				filterValue, reverseFilter, false);
		Iterator<Geometry> iterator = intersected.iterator();
		if (intersected.size() > 1 && !allowMultiGeometries)
			throw (new SvException(Sv.Exceptions.SDI_MULTIPLE_GEOMS_FOUND, svCONST.systemUser, null, p));

		if (!iterator.hasNext())
			throw (new SvException(Sv.Exceptions.SDI_MERGE_GEOM_EMPTY, svCONST.systemUser, null, p));

		return intersected;
	}

	/**
	 * Method to split a set of geometries from the layer by a line string. The
	 * starting and ending vertexes of the stringline are considered as disjoint
	 * with the target geometry. All selected geometries (which are between the
	 * start and end vertex of the selector line) are split according to the line.
	 * The method adds all split objects in the splitObjects array in order for the
	 * caller to delete them.This method doesn't perform any database updates
	 * 
	 * @param line                 The line which was drawn on the map. The first
	 *                             and last point are terminators, everything
	 *                             between is a selector.
	 * @param layerTypeId          The layer ID from the geometries shall be
	 *                             selected
	 * @param splitObjects         All DbDataObjects from the geometries which are
	 *                             split are added to this list, for later deletion.
	 * @param allowMultiGeometries If the flag is true, the method will return more
	 *                             than one geometry. If the flag is false any
	 *                             overlap will raise exception
	 * @param filterFieldName      The field name of the associated DbDataObject of
	 *                             the layer geometry which should be filtered
	 * @param filterValue          The which should be matched as equal
	 * @param reverseFilter        If this flag is true, the filter will return only
	 *                             geometries which do not match
	 * @return The resulting geometry with user data assigned from the geometry
	 *         selected via the first point in the set.
	 * @throws SvException Any underlying exceptin raised by the svarog core
	 */
	public Set<Geometry> splitGeometries(LineString line, Long layerTypeId, List<DbDataObject> splitObjects,
			boolean allowMultiGeometries, SvCharId filterFieldName, Object filterValue, boolean reverseFilter)
			throws SvException {
		if (!line.isSimple())
			throw (new SvException("system.error.sdi.line_intersects_self", svCONST.systemUser, null, line));

		Set<Geometry> intersected = this.getRelatedGeometries(line, layerTypeId, SDIRelation.INTERSECTS,
				filterFieldName, filterValue, reverseFilter, false);
		Iterator<Geometry> iterator = intersected.iterator();
		while (iterator.hasNext()) {
			Geometry findGeom = iterator.next();
			if (!findGeom.disjoint(line.getStartPoint()) || !findGeom.disjoint(line.getEndPoint()))
				iterator.remove();
		}

		if (intersected.size() > 1 && !allowMultiGeometries)
			throw (new SvException(Sv.Exceptions.SDI_MULTIPLE_GEOMS_FOUND, svCONST.systemUser, null, line));

		Set<Geometry> result = new HashSet<>();
		for (Geometry originalGeom : intersected) {
			Geometry[] geometries = new Geometry[] { originalGeom.getBoundary(), line };
			GeometryCollection col = SvUtil.sdiFactory.createGeometryCollection(geometries);
			Geometry union = UnaryUnionOp.union(col);
			DbDataObject originalDbo = null;
			if (originalGeom.getUserData() != null && originalGeom.getUserData() instanceof DbDataObject)
				originalDbo = (DbDataObject) originalGeom.getUserData();
			Polygonizer polygonizer = new Polygonizer();
			polygonizer.add(union);
			for (Polygon poly : (Collection<Polygon>) polygonizer.getPolygons()) {
				if (originalDbo != null) {
					DbDataObject dbo = new DbDataObject(originalDbo.getObjectType());
					dbo.setParentId(originalDbo.getParentId());
					dbo.setValuesMap(originalDbo.getValuesMap());
					poly.setUserData(dbo);
				}
				if (originalGeom.covers(poly))
					result.add(poly);
			}
		}
		return result;
	}

	/**
	 * Method to merge a set of geometries from the layer by a set of points on the
	 * map. Each point is treated as selector for a geometry. All selected
	 * geometries are merged and the user data from the first geometry is assigned
	 * to the result. The method adds all other merged objects in the mergedObjects
	 * array in order for the caller to delete them.This method doesn't perform any
	 * database updates
	 * 
	 * @param points               The point list which was selected on the map. The
	 *                             first point is used as reference, all others are
	 *                             merged into it.
	 * @param layerTypeId          The layer ID from the geometries shall be
	 *                             selected
	 * @param mergedObjects        All DbDataObjects from the merged geometries are
	 *                             added to this list, except for the first one.
	 * @param allowMultiGeometries If the flag is true, the method will return more
	 *                             than one geometry. If the flag is false any
	 *                             overlap will raise exception
	 * @param filterFieldName      The field name of the associated DbDataObject of
	 *                             the layer geometry which should be filtered
	 * @param filterValue          The which should be matched as equal
	 * @param reverseFilter        If this flag is true, the filter will return only
	 *                             geometries which do not match
	 * @return The resulting geometry with user data assigned from the geometry
	 *         selected via the first point in the set.
	 * @throws SvException
	 */
	public Geometry mergeGeometries(List<Point> points, Long layerTypeId, List<DbDataObject> mergedObjects,
			boolean allowMultiGeometries, SvCharId filterFieldName, Object filterValue, boolean reverseFilter)
			throws SvException {

		if (points.size() < 2)
			throw (new SvException(Sv.Exceptions.SDI_MERGE_REQUIRES_2PLUS, svCONST.systemUser, null, points));

		Geometry result = getGeometryByPoint(points.get(0), layerTypeId, allowMultiGeometries, filterFieldName,
				filterValue, reverseFilter).iterator().next();
		DbDataObject resultDbo = (DbDataObject) result.getUserData();
		for (int i = 1; i < points.size(); i++) {
			Iterator<Geometry> iterator = getGeometryByPoint(points.get(i), layerTypeId, allowMultiGeometries,
					filterFieldName, filterValue, reverseFilter).iterator();

			while (iterator.hasNext()) {
				Geometry g = iterator.next();
				if (result.disjoint(g))
					throw (new SvException(Sv.Exceptions.SDI_MERGE_GEOM_DISJOINT, svCONST.systemUser, null, g));

				if (!mergedObjects.contains(g.getUserData())) {
					result = result.union(g);
					mergedObjects.add((DbDataObject) g.getUserData());
				}
			}
		}
		result.setUserData(resultDbo);
		return result;
	}

	/**
	 * Method which accepts list of new geometries, which are result of merge or
	 * split, and list of old geometries from the same operation, which should be
	 * deleted
	 * 
	 * @param newGeometries     The list of resulting geometries
	 * @param deletedGeometries The list of transformed/deleted geometries
	 * @param autoCommit        If true, the method will commit to the database
	 * @throws SvException Any underlying exception from the svarog core
	 */
	public void splitMergeGeometryDbUpdate(Set<Geometry> newGeometries, List<DbDataObject> deletedGeometries,
			boolean autoCommit) throws SvException {
		try (SvWriter svw = new SvWriter(this)) {
			DbDataObject dbo = null;
			// delete the others

			DbDataArray dba = new DbDataArray();
			dba.setItems((ArrayList) deletedGeometries);
			svw.deleteObjects(dba, false, false);

			DbDataArray newDba = new DbDataArray();
			for (Geometry g : newGeometries) {
				dbo = (DbDataObject) g.getUserData();
				SvGeometry.setGeometry(dbo, g);
				newDba.addDataItem(dbo);
			}
			this.saveGeometry(newDba, true, false);

			if (autoCommit)
				this.dbCommit();
		}

	}

	/**
	 * Method to merge a set of geometries from the layer by a set of points on the
	 * map. Each point is treated as selector for a geometry.
	 * 
	 * @param points               The point set which was selected on the map
	 * @param layerTypeId          The layer ID from the geometries shall be
	 *                             selected
	 * @param allowMultiGeometries If the flag is true, the method will return more
	 *                             than one geometry. If the flag is false any
	 *                             overlap will raise exception
	 * @param preview              If the preview flag is true, the method will not
	 *                             perform any database updates
	 * @param filterFieldName      The field name of the associated DbDataObject of
	 *                             the layer geometry which should be filtered
	 * @param filterValue          The which should be matched as equal
	 * @return
	 * @throws SvException
	 */
	@Deprecated
	public Geometry mergeGeometries(List<Point> points, Long layerTypeId, boolean allowMultiGeometries, boolean preview,
			SvCharId filterFieldName, Object filterValue, boolean autoCommit) throws SvException {

		List<DbDataObject> mergedObjects = new ArrayList<DbDataObject>();
		Geometry result = mergeGeometries(points, layerTypeId, mergedObjects, allowMultiGeometries, filterFieldName,
				filterValue, false);

		Set<Geometry> resultingSet = new HashSet<Geometry>();
		if (!preview) {
			splitMergeGeometryDbUpdate(resultingSet, mergedObjects, autoCommit);
		}

		return result;
	}

	/**
	 * Method to split a set of geometries from the layer by a line string. The
	 * starting and ending vertexes of the stringline are considered as disjoint
	 * with the target geometry. All selected geometries (which are between the
	 * start and end vertex of the selector line) are split according to the line.
	 * The method performs the final database updates.
	 * 
	 * @param line                 The line which was drawn on the map. The first
	 *                             and last point are terminators, everything
	 *                             between is a selector.
	 * @param layerTypeId          The layer ID from the geometries shall be
	 *                             selected
	 * @param allowMultiGeometries If the flag is true, the method will return more
	 *                             than one geometry. If the flag is false any
	 *                             overlap will raise exception
	 * @param preview              If the preview flag is true, the method will not
	 *                             perform database updates
	 * @param filterFieldName      The field name of the associated DbDataObject of
	 *                             the layer geometry which should be filtered
	 * @param filterValue          The which should be matched as equal
	 * @param autoCommit           If this flag is true, the method will commit the
	 *                             changes to the database
	 * @return The resulting geometry with user data assigned from the geometry
	 *         selected via the first point in the set.
	 * @throws SvException Any underlying exceptin raised by the svarog core
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	public Set<Geometry> splitGeometry(LineString line, Long layerTypeId, boolean allowMultiGeometries, boolean preview,
			SvCharId filterFieldName, Object filterValue, boolean autoCommit) throws SvException {
		List<DbDataObject> splitObjects = new ArrayList<DbDataObject>();
		Set<Geometry> result = splitGeometries(line, layerTypeId, splitObjects, allowMultiGeometries, filterFieldName,
				filterValue, false);

		if (!preview) {
			splitMergeGeometryDbUpdate(result, splitObjects, autoCommit);
		}
		return result;
	}

	/**
	 * Method that returns the all geometries from a specific layer within a
	 * bounding box
	 * 
	 * @param layerTypeId the object id of the layer/object type from which to fetch
	 *                    the geometries
	 * @param bbox        The string representation of the bounding box
	 * @return Set of {@link Geometry} objects
	 */
	public Set<Geometry> getGeometriesByBBOX(Long layerTypeId, String bbox) throws SvException {
		Envelope envelope = parseBBox(bbox);
		return getGeometries(layerTypeId, envelope);
	}

	/**
	 * Method that returns the all geometries from a specific layer within a
	 * bounding box
	 * 
	 * @param layerTypeId the object id of the layer/object type from which to fetch
	 *                    the geometries
	 * @param envelope    The {@link Envelope} of the region we are interested in
	 * @return Set of {@link Geometry} objects
	 */
	public Set<Geometry> getGeometries(Long typeId, Envelope envelope) throws SvException {
		return getRelatedGeometries(SvUtil.sdiFactory.toGeometry(envelope), typeId, SDIRelation.INTERSECTS, null, null,
				false);
	}

	/**
	 * Method that returns the all geometries from a specific layer within a
	 * bounding box
	 * 
	 * @param layerTypeId  the object id of the layer/object type from which to
	 *                     fetch the geometries
	 * @param selector     The {@link Geometry} which we want to use as selector
	 * @param partialCover flag to notify if we want to include partially covered
	 * @return List of {@link Geometry} objects
	 */
	public ArrayList<Geometry> getGeometries(Long typeId, Geometry selector, boolean partialCover) throws SvException {
		@SuppressWarnings("unchecked")
		List<Geometry> result = getSysGrid().getGridIndex().query(selector.getEnvelopeInternal());
		SDIRelation operator = partialCover ? SDIRelation.INTERSECTS : SDIRelation.COVEREDBY;
		ArrayList<Geometry> geoms = new ArrayList<Geometry>();
		for (Geometry tileGeom : result) {
			SvSDITile tile = getTile(typeId, (String) tileGeom.getUserData(), null);
			if (selector.covers(tileGeom)) {
				geoms.addAll(tile.getInternalGeometries());
			} else {
				geoms.addAll(tile.getRelations(selector, operator, true));
			}
		}
		return geoms;
	}

	/**
	 * Method to cut all intersecting geometries of a specific layer from a
	 * geometry. The returning geometry will contain any area of the geometry which
	 * is not cover by the layer features. This method will not cut off the old
	 * version of the geometry in case of update (if the user data of the geometry
	 * contains a valid DbDataObject).
	 * 
	 * @param geom        The geometry to be modified
	 * @param layerTypeId The layer id which should be used to calculate the
	 *                    difference
	 * @return The difference geometry between the original geometry and the layer
	 * @throws SvException underlying exception from layer loading.
	 */
	public Geometry cutLayerFromGeom(Geometry geom, Long layerTypeId) throws SvException {
		return cutLayerFromGeom(geom, layerTypeId, null, null);
	}

	/**
	 * Method to test if the geometry is intersecting any of the geometries of
	 * required layer.
	 * 
	 * @param geom        The geometry to be tested for intersections
	 * @param layerTypeId The layer id which should be used to calculate the
	 *                    intersections
	 * @return True if the geometry intersects the layer
	 * @throws SvException underlying exception from layer loading.
	 */
	public boolean intersectsLayer(Geometry geom, Long layerTypeId) throws SvException {
		return intersectsLayer(geom, layerTypeId, null, null);
	}

	/**
	 * Method to test if the geometry is intersecting any of the geometries of
	 * required layer.
	 * 
	 * @param geom            The geometry to be tested for intersections
	 * @param layerTypeId     The layer id which should be used to calculate the
	 *                        intersections
	 * @param filterFieldName The field name of the associated DbDataObject of the
	 *                        layer geometry which should be filtered
	 * @param filterValue     The which should be matched as equal
	 * @return True if the geometry intersects the layer
	 * @throws SvException underlying exception from layer loading.
	 */
	public boolean intersectsLayer(Geometry geom, Long layerTypeId, SvCharId filterFieldName, Object filterValue)
			throws SvException {
		return cutLayerFromGeomImpl(geom, layerTypeId, true, filterFieldName, filterValue) != null;
	}

	/**
	 * Method to cut all intersecting geometries of a specific layer from a
	 * geometry. The returning geometry will contain any area of the geometry which
	 * is not cover by the layer features. This method will not cut off the old
	 * version of the geometry in case of update (if the user data of the geometry
	 * contains a valid DbDataObject).
	 * 
	 * @param geom            The geometry to be modified
	 * @param layerTypeId     The layer id which should be used to calculate the
	 *                        difference
	 * @param filterFieldName The field name of the associated DbDataObject of the
	 *                        layer geometry which should be filtered
	 * @param filterValue     The which should be matched as equal
	 * @return The difference geometry between the original geometry and the layer
	 * @throws SvException underlying exception from layer loading.
	 */
	public Geometry cutLayerFromGeom(Geometry geom, Long layerTypeId, SvCharId filterFieldName, Object filterValue)
			throws SvException {
		return cutLayerFromGeomImpl(geom, layerTypeId, false, filterFieldName, filterValue);
	}

	/**
	 * Method to calculate the difference of the input geometry against the list of
	 * intersections
	 * 
	 * @param geom          The geometry which should used to calculate the
	 *                      difference from.
	 * @param intersections The set of geometries which are intersecting with the
	 *                      source geometry
	 * @return
	 */
	Geometry calcGeomDiff(Geometry geom, Set<Geometry> intersections) {
		DbDataObject userData = (DbDataObject) geom.getUserData();
		Geometry result = geom;

		for (Geometry relatedGeom : intersections)
			result = result.difference(relatedGeom);

		if (result != null)
			result.setUserData(userData);

		return result;
	}

	private Geometry cutLayerFromGeomImpl(Geometry geom, Long layerTypeId, boolean testOnly, SvCharId filterFieldName,
			Object filterValue) throws SvException {
		Geometry updatedGeometry = null;
		Set<Geometry> intersections = getRelatedGeometries(geom, layerTypeId, SDIRelation.INTERSECTS, filterFieldName,
				filterValue, true);

		if (!testOnly)
			updatedGeometry = calcGeomDiff(geom, intersections);
		else
			updatedGeometry = !intersections.isEmpty() ? geom : null;

		return updatedGeometry;
	}

	public Collection<Geometry> cutGeomFromLayer(Geometry geom, Long layerTypeId) throws SvException {
		return cutGeomFromLayer(geom, layerTypeId, null, null);
	}

	public Collection<Geometry> cutGeomFromLayer(Geometry geom, Long layerTypeId, SvCharId filterFieldName,
			Object filterValue) throws SvException {
		return cutGeomFromLayerImpl(geom, layerTypeId, false, filterFieldName, filterValue);
	}

	public boolean cutGeomFromLayerTest(Geometry geom, Long layerTypeId) throws SvException {
		return cutGeomFromLayerTest(geom, layerTypeId, null, null);
	}

	public boolean cutGeomFromLayerTest(Geometry geom, Long layerTypeId, SvCharId filterFieldName, Object filterValue)
			throws SvException {
		return cutGeomFromLayerImpl(geom, layerTypeId, true, filterFieldName, filterValue) != null;
	}

	/**
	 * Method to calculate the difference between set of geometries and a target
	 * geometry. The set of geometries is updated to exclude any overlaps with the
	 * target geometry.
	 * 
	 * @param geom  The geometry which is cut off from the layer (set of geometries)
	 * @param layer The set of geometries which are subject of difference operation
	 * @return The resulting set of geometries
	 */
	Set<Geometry> calcLayerDiff(Geometry geom, Set<Geometry> layer) {
		DbDataObject userData = (DbDataObject) geom.getUserData();
		HashSet<Geometry> updatedLayer = new HashSet<>();

		for (Geometry relatedGeom : layer) {
			DbDataObject relatedUserData = ((DbDataObject) relatedGeom.getUserData());
			if (userData == null
					|| (userData != null && !userData.getObjectId().equals(relatedUserData.getObjectId()))) {
				Geometry modifiedGeom = relatedGeom.difference(geom);
				modifiedGeom.setUserData(relatedUserData);
				updatedLayer.add(modifiedGeom);
			}

		}
		return updatedLayer;
	}

	/**
	 * /** Method to calculate the difference between a SDI layer and a target
	 * geometry. The set of geometries is updated to exclude any overlaps with the
	 * target geometry.
	 * 
	 * @param geom            The geometry which is cut off from the layer
	 * @param layerTypeId     The id of the layer
	 * @param testOnly        Flag to return only intersecting geometries, without
	 *                        calculating the difference
	 * @param filterFieldName The
	 * @param filterValue
	 * @return
	 * @throws SvException
	 */
	Collection<Geometry> cutGeomFromLayerImpl(Geometry geom, Long layerTypeId, boolean testOnly,
			SvCharId filterFieldName, Object filterValue) throws SvException {
		if (geom == null)
			return null;
		Set<Geometry> updatedGeometries = null;
		Set<Geometry> intersections = getRelatedGeometries(geom, layerTypeId, SDIRelation.INTERSECTS, filterFieldName,
				filterValue, true);

		if (!testOnly)
			updatedGeometries = calcLayerDiff(geom, intersections);
		else
			updatedGeometries = !intersections.isEmpty() ? intersections : null;
		return updatedGeometries;
	}

	//////////////////////////////////////////////////////////////////////////
	// Instance specific methods and variables
	//////////////////////////////////////////////////////////////////////////
	/**
	 * Flag to allow saving SDI objects with null geometry
	 */
	private boolean allowNullGeometry = false;

	/**
	 * Minimum distance between two geometries
	 */
	private Integer minGeomDistance = SvParameter.getSysParam(Sv.SDI_MIN_GEOM_DISTANCE, Sv.DEFAULT_MIN_GEOM_DISTANCE);

	/**
	 * Maximumim spike angle
	 */
	private Double maxAngle = SvParameter.getSysParam(Sv.SDI_SPIKE_MAX_ANGLE, Sv.DEFAULT_SPIKE_MAX_ANGLE);

	/**
	 * Minimum distance between to vertexes
	 */
	private Integer minPointDistance = SvParameter.getSysParam(Sv.SDI_MIN_POINT_DISTANCE,
			Sv.DEFAULT_MIN_POINT_DISTANCE);

	/**
	 * Minimum distance between to vertexes
	 */
	private Double vertexAlignmentTolerance = SvParameter.getSysParam(Sv.SDI_VERTEX_ALIGN_TOLERANCE,
			Sv.DEFAULT_VERTEX_ALIGN_TOLERANCE);

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
		super();
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
		verifyBounds(geom);
	}

	/**
	 * Method to align two line string geometries, vertex by vertex to ensure that
	 * conversion between different projects didn't cause shift in the vertexes,
	 * which would cause re-creation of new geometries
	 * 
	 * @param oldLine The existing line string
	 * @param newLine The new line string
	 * @param isRing  Flag to signify that the string is ring and we guarantee that
	 *                we'll always close it
	 */
	void alignLineString(LineString oldLine, LineString newLine, boolean isRing) {
		for (int i = 0; i < oldLine.getCoordinates().length; i++) {
			Coordinate oc = oldLine.getCoordinates()[i];
			for (int j = 0; j < newLine.getCoordinates().length; j++) {
				Coordinate nc = newLine.getCoordinates()[j];
				if (oc.equals2D(nc, this.vertexAlignmentTolerance)) {
					newLine.getCoordinates()[j] = oc;
					break;
				}
			}

		}
		// ensure the first and last are the same
		if (isRing)
			newLine.getCoordinates()[newLine.getCoordinates().length - 1] = newLine.getCoordinates()[0];
	}

	/**
	 * Method to align two polygon geometries, vertex by vertex to ensure that
	 * conversion between different projects didn't cause shift in the vertexes,
	 * which would cause re-creation of new geometries
	 * 
	 * @param oldG The existing polygon
	 * @param newG The new line polygon
	 */
	void alignPolygon(Polygon oldG, Polygon newG) {
		alignLineString(oldG.getExteriorRing(), newG.getExteriorRing(), true);
		for (int i = 0; i < oldG.getNumInteriorRing(); i++) {
			for (int j = 0; j < newG.getNumInteriorRing(); j++) {
				alignLineString(oldG.getInteriorRingN(i), newG.getInteriorRingN(j), true);
			}
		}
	}

	/**
	 * Method to align two multi polygon geometries, vertex by vertex to ensure that
	 * conversion between different projects didn't cause shift in the vertexes,
	 * which would cause re-creation of new geometries
	 * 
	 * @param oldG The existing polygon
	 * @param newG The new line polygon
	 */
	void alignMultiPolygons(MultiPolygon oldG, MultiPolygon newG) {
		for (int i = 0; i < oldG.getNumGeometries(); i++) {
			for (int j = 0; j < newG.getNumGeometries(); j++) {
				alignPolygon((Polygon) oldG.getGeometryN(i), (Polygon) newG.getGeometryN(j));
			}
		}
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
	public void verifyBounds(Geometry geom) throws SvException {
		if (!geom.isValid())
			throw (new SvException("system.error.sdi.invalid_geom", this.instanceUser, null, geom));

		List<Geometry> tiles = getTileGeometries(geom.getEnvelopeInternal());
		if (tiles.size() < 1)
			throw (new SvException("system.error.sdi.geometry_not_in_grid", this.instanceUser, null, geom));

		if (!SvConf.isIntersectSysBoundary())
			testBoundaryIntersection(geom, tiles);

	}

	/**
	 * Method to test if the geometry in question, points outside of the system
	 * boundary
	 * 
	 * @param geom  The geometry under test
	 * @param tiles The list of tile geometries which intersect with the target
	 *              geometry
	 * @throws SvException SvException of type
	 *                     "system.error.sdi.geometry_crosses_sysbounds" is thrown
	 *                     if the geometry has points out of boundary
	 */
	void testBoundaryIntersection(Geometry geom, List<Geometry> tiles) throws SvException {
		for (Geometry tile : tiles) {
			if (getSysGrid().getBorderTiles().contains((String) tile.getUserData())) {
				if (tile.covers(geom))
					continue;
				else if (tile.intersects(geom)) {
					SvSDITile sysbounds = getSysBoundary();
					if (sysbounds.getRelations(geom, SDIRelation.COVERS).size() < 0)
						throw (new SvException("system.error.sdi.geometry_crosses_sysbounds", this.instanceUser, null,
								geom));
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
		SvSDITile admTile = getSDIUnitBoundary(SvConf.getAdmUnitClass());
		Set<Geometry> units = admTile.getRelations(geom, SDIRelation.COVERS);
		if (units.size() == 1) {
			Iterator<Geometry> i = units.iterator();
			return (DbDataObject) i.next().getUserData();
		} else if (units.size() > 1)
			throw (new SvException("system.error.sdi.adm_bounds_overlaps", this.instanceUser, null, geom));
		else
			throw (new SvException("system.error.sdi.adm_bounds_not_found", this.instanceUser, null, geom));

	}

	/**
	 * Test a polygonal geometry for spikes. A spike is considered angle between
	 * vertices which is lower than a specified system parameter named
	 * SDI.SPIKE_MAX_ANGLE. The unoriented smallest angle between two vectors must
	 * not be lower than the system parameter
	 * 
	 * @param g        The geometry to be tested
	 * @param maxAngle the maximum angle in degrees which is considered as spike
	 * @throws SvException with label "system.error.sdi.spike_detected" to signify
	 *                     that the geometry has spike with angle less then maxAngle
	 */
	public void testPolygonSpikes(Geometry g, Double maxAngle) throws SvException {
		if (!(g instanceof Polygon || g instanceof MultiPolygon) || maxAngle < 0.01)
			return;

		for (int i = 0; i < g.getNumGeometries(); i++) {
			Polygon p = (Polygon) g.getGeometryN(i);
			testPolygonCoordinates(p.getExteriorRing(), maxAngle);
			for (int j = 0; j < p.getNumInteriorRing(); j++) {
				LineString ls = p.getInteriorRingN(j);
				testPolygonCoordinates(ls, maxAngle);
			}
		}
	}

	/**
	 * Test a polygonal geometry for spikes and remove the spikes. A spike test is
	 * specified as in {@link #testPolygonSpikes(Geometry, Double)}. This method
	 * will remove the point forming the spike. If removing the point collapses the
	 * geometry to a line, the method will throw exception
	 * 
	 * @param g        The geometry to be tested
	 * @param maxAngle the maximum angle in degrees which is considered as spike
	 * @throws SvException with label "system.error.sdi.spike_fix_fail" to signify
	 *                     that the geometry spikes can not be fixed automatically
	 */
	public Geometry fixPolygonSpikes(Geometry g, Double maxAngle) throws SvException {
		return fixPolygonSpikes(g, maxAngle, false);
	}

	/**
	 * Remove the spikes from a geometry.
	 * 
	 * @param p        The polygon to be fixed
	 * @param maxAngle the maximum angle in degrees which is considered as spike
	 * @return A list of coordinates which do not contain spikes
	 * @throws SvException with label "system.error.sdi.spike_fix_fail" to signify
	 *                     that the geometry spikes can not be fixed automatically
	 */
	public LinearRing fixPolygonCoordinates(LineString p, Double maxAngle) throws SvException {
		Coordinate[] coords = p.getCoordinates();
		Coordinate[] newCoordinates = new Coordinate[coords.length];
		int startIndex = 0;

		// check if there's spike at the first/last
		double angle = Angle.toDegrees(Angle.angleBetween(coords[1], coords[0], coords[coords.length - 2]));
		if (angle <= maxAngle)
			startIndex = 1;

		// add the zero point, becase we start the loop from the first tail
		newCoordinates[0] = coords[startIndex];
		int iDst = 1;
		for (int cc = startIndex + 1; cc < coords.length; cc++) {
			Coordinate tip1 = coords[cc - 1];
			Coordinate tail = coords[cc];
			int tip2index = (cc == coords.length - 1 ? 1 : cc + 1);
			Coordinate tip2 = coords[tip2index];
			angle = Angle.toDegrees(Angle.angleBetween(tip1, tail, tip2));
			if (angle >= maxAngle)
				newCoordinates[iDst++] = tail;
		}
		if (startIndex == 1)
			newCoordinates[iDst++] = newCoordinates[0];
		if (iDst == coords.length)
			newCoordinates = coords;
		else {
			// if the line string is less than 4 points, means it has collapsed
			if (iDst < 3)
				throw (new SvException(Sv.Exceptions.SDI_SPIKE_FIX_FAILED, instanceUser, null, null));
			newCoordinates = Arrays.copyOf(newCoordinates, iDst);
		}

		return SvUtil.sdiFactory.createLinearRing(newCoordinates);
	}

	/**
	 * Test a polygonal geometry for spikes (one ring at a time).The method will try
	 * to find angles less then max angle and throw
	 * "system.error.sdi.spike_detected"
	 * 
	 * @param p        The polygon linestring to be tested
	 * @param maxAngle the maximum angle in degrees which is considered as spike
	 * @throws SvException with label "system.error.sdi.spike_detected" if spike was
	 *                     detected
	 */
	public void testPolygonCoordinates(LineString p, Double maxAngle) throws SvException {
		Coordinate[] coords = p.getCoordinates();
		for (int cc = 1; cc < coords.length; cc++) {
			Coordinate tip1 = coords[cc - 1];
			Coordinate tail = coords[cc];
			int tip2index = (cc == coords.length - 1 ? 1 : cc + 1);
			Coordinate tip2 = coords[tip2index];
			double angle = Angle.toDegrees(Angle.angleBetween(tip1, tail, tip2));
			if (angle < maxAngle)
				throw (new SvException(Sv.Exceptions.SDI_SPIKE_DETECTED, instanceUser, null, null));

		}
	}

	/**
	 * Test a polygonal geometry for spikes and remove the spikes. Based on the test
	 * only param it will either test or try to fix the spikes.
	 * 
	 * @param g        The geometry to be tested or fixed based on the testOnly
	 *                 flag.
	 * @param maxAngle the maximum angle in degrees which is considered as spike
	 * @param testOnly if this param is set to true, the method will try to find
	 *                 angles less then max angle and throw
	 *                 "system.error.sdi.spike_detected"
	 * @throws SvException with label "system.error.sdi.spike_fix_fail" to signify
	 *                     that the geometry spikes can not be fixed automatically
	 *                     or "system.error.sdi.spike_detected" if spike was
	 *                     detected via test only flag
	 */
	Geometry fixPolygonSpikes(Geometry g, Double maxAngle, boolean testOnly) throws SvException {

		if (!(g instanceof Polygon || g instanceof MultiPolygon) || maxAngle < 0.01)
			return g;

		Polygon[] allPoly = new Polygon[g.getNumGeometries()];
		for (int i = 0; i < g.getNumGeometries(); i++) {
			Polygon p = (Polygon) g.getGeometryN(i);

			LinearRing fixedShellCoordinates = fixPolygonCoordinates(p.getExteriorRing(), maxAngle);
			LinearRing[] fixedInternalRingCoordinates = new LinearRing[p.getNumInteriorRing()];

			for (int j = 0; j < p.getNumInteriorRing(); j++) {
				LineString ls = p.getInteriorRingN(j);
				fixedInternalRingCoordinates[j] = fixPolygonCoordinates(ls, maxAngle);
			}
			if (!testOnly) {
				Polygon pDst = null;
				try {
					pDst = SvUtil.sdiFactory.createPolygon(fixedShellCoordinates, fixedInternalRingCoordinates);
				} catch (Exception e) {
					throw (new SvException(Sv.Exceptions.SDI_SPIKE_FIX_FAILED, instanceUser, null, null, e));
				}
				if (pDst == null || pDst.getArea() < 1)
					throw (new SvException(Sv.Exceptions.SDI_SPIKE_FIX_FAILED, instanceUser, null, null));

				allPoly[i] = pDst;
			}
		}
		if (allPoly.length == 1)
			return allPoly[0];
		else
			return SvUtil.sdiFactory.createMultiPolygon(allPoly);
	}

	/**
	 * Method to test if the geometry has vertexes which are within tolerance
	 * specified by minPointDistance
	 * 
	 * @param geom             The geometry to be validated
	 * @param minPointDistance The distance between two points on the geometry (in
	 *                         millimeters!!!)
	 * @throws SvException The method will raise
	 *                     "system.error.sdi.vertex_min_dist_err" exception if the
	 *                     geometry has points which can be collapsed into each
	 *                     other within the tolerance
	 * 
	 */
	public void testMinVertexDistance(Geometry geom, Integer minPointDistance) throws SvException {
		minVertexDistance(geom, minPointDistance, true);
	}

	/**
	 * Method to ensure that the geometry has no vertexes which are too close.
	 * Public method to simplify the geometry and collapse points within tolerance
	 * 
	 * @param geom             The geometry to be simplified
	 * @param minPointDistance The distance between two points on the geometry (in
	 *                         millimeters!!!)
	 * @return The simplified geometry
	 */
	public Geometry fixMinVertexDistance(Geometry geom, Integer minPointDistance) throws SvException {
		return minVertexDistance(geom, minPointDistance, false);
	}

	/**
	 * Method to ensure that the geometry has no vertexes which are too close
	 * 
	 * @param geom             The geometry to be validated and/or fixed
	 * @param minPointDistance The distance between two points on the geometry (in
	 *                         millimeters!!!). If the tolerance is 0, the test is
	 *                         omitted
	 * @param testOnly         If this method should fix the geometry or raise an
	 *                         exception if the geometry has points within tolerance
	 * @return The simplified geometry
	 * @throws SvException If the flag testOnly is set, the method will raise
	 *                     "system.error.sdi.vertex_min_dist_err" exception if the
	 *                     geometry has been simplified
	 * 
	 */
	Geometry minVertexDistance(Geometry geom, Integer minPointDistance, boolean testOnly) throws SvException {
		if (minPointDistance == 0)
			return geom;
		double tolerance = minPointDistance / 1000.0;
		Geometry g = TopologyPreservingSimplifier.simplify(geom, tolerance);
		if (testOnly && !g.equalsExact(geom))
			throw (new SvException(Sv.Exceptions.SDI_VERTEX_DISTANCE_ERR, instanceUser, null, null));
		return g;

	}

	/**
	 * Method to ensure that the geometry is not too close to another geometry. T
	 * 
	 * @param geom             The geometry to be tested for proximity
	 * @param minPointDistance The distance between the geometry and another
	 *                         geometry (in millimeters!!!). If the tolerance is 0,
	 *                         the test is omitted
	 * @param layerTypeId      The layer id which should be used to calculate the
	 *                         distance
	 * @throws SvException Underlying layer loading exception
	 */
	public Geometry fixGeomDistance(Geometry geom, Long layerTypeId, Integer distanceTolerance) throws SvException {
		return minGeomDistance(geom, layerTypeId, distanceTolerance, false);
	}

	/**
	 * Method to ensure that the geometry is not too close to another geometry. T
	 * 
	 * @param geom             The geometry to be tested for proximity
	 * @param minPointDistance The distance between the geometry and another
	 *                         geometry (in millimeters!!!). If the tolerance is 0,
	 *                         the test is omitted
	 * @param layerTypeId      The layer id which should be used to calculate the
	 *                         distance
	 * @throws SvException If the flag testOnly is set, the method will raise
	 *                     "system.error.sdi.geom_min_dist_err" exception if the
	 *                     geometry is too close to another geometry of the same
	 *                     layer
	 */
	public void testGeomDistance(Geometry geom, Long layerTypeId, Integer distanceTolerance) throws SvException {
		minGeomDistance(geom, layerTypeId, distanceTolerance, true);
	}

	/**
	 * Method to ensure that the geometry has no vertexes which are too close. If
	 * the flag test only is set to false, the original geometry will cut off to the
	 * buffer of tolerance from the geometries in the target layer
	 * 
	 * @param geom             The geometry to be validated and/or fixed
	 * @param minPointDistance The distance between two points on the geometry (in
	 *                         millimeters!!!). If the tolerance is 0, the test is
	 *                         omitted
	 * @param layerTypeId      The layer id which should be used to calculate the
	 *                         distance
	 * @param testOnly         If this method should fix the geometry or raise an
	 *                         exception if the geometry has points within tolerance
	 * @return The simplified geometry
	 * @throws SvException If the flag testOnly is set, the method will raise
	 *                     "system.error.sdi.geom_min_dist_err" exception if the
	 *                     geometry is too close to another geometry of the same
	 *                     layer
	 */
	Geometry minGeomDistance(Geometry geom, Long layerTypeId, Integer distanceTolerance, boolean testOnly)
			throws SvException {
		if (distanceTolerance.intValue() == 0)
			return geom;
		double tolerance = distanceTolerance / 1000.0;

		Geometry resultGeom = geom.buffer(distanceTolerance);
		resultGeom.setUserData(geom.getUserData());
		// find out which ones we are near by with a buffer
		Set<Geometry> neighbours = getRelatedGeometries(resultGeom, layerTypeId, SDIRelation.INTERSECTS, null, null,
				true);

		// the result starts with the source geometry
		resultGeom = geom;
		if (testOnly)
			for (Geometry gg : neighbours) {
				if (gg.disjoint(geom))
					throw (new SvException(Sv.Exceptions.SDI_GEOM_DISTANCE_ERR, instanceUser, null, null));
			}
		else
			for (Geometry gg : neighbours) {
				Geometry neighbourBuff = gg.buffer(tolerance);
				resultGeom = resultGeom.difference(neighbourBuff);
			}

		resultGeom.setUserData(geom.getUserData());
		return resultGeom;
	}

	/**
	 * The method provides means to create/remove a hole from polygon. The polygon
	 * is selected from the underlying layer identified via layerTypeId and assumes
	 * that the underlying layer does not contain overlapping polygons.
	 * 
	 * @param hole        The geometry describing the hole
	 * @param layerTypeId The layer from which we should get the polygon via
	 *                    intersections
	 * @param remove      Flag if we should remove a hole, or create a hole.
	 * @return The modified polygon
	 * @throws SvException
	 */
	public Geometry holeInPolygon(Geometry hole, Long layerTypeId, boolean remove) throws SvException {
		Set<Geometry> result = innerPolygon(hole, layerTypeId, remove, false);
		if (result != null && result.size() == 1)
			return result.iterator().next();
		else
			return null;
	}

	/**
	 * Method to perform filling of holes or cutting off holes from polygons.
	 * 
	 * @param geom                 The geometry of the hole (or cover)
	 * @param layerTypeId          The layer from which we should get the base
	 *                             polygon
	 * @param operationFill        Flag to allow fill of hole or cutoff if false;
	 * @param allowMultiGeometries Boolean flag to allow operation over multiple
	 *                             overlapping geometries
	 * @return Set of modified geometries
	 * @throws SvException Any underlying exception
	 */
	Set<Geometry> innerPolygon(Geometry geom, Long layerTypeId, boolean operationFill, boolean allowMultiGeometries)
			throws SvException {

		Set<Geometry> intersected = getRelatedGeometries(geom, layerTypeId, SDIRelation.INTERSECTS, null, null, true);

		if (intersected.size() > 1 && !allowMultiGeometries)
			throw (new SvException(Sv.Exceptions.SDI_MULTIPLE_GEOMS_FOUND, svCONST.systemUser, null, geom));

		Set<Geometry> result = new HashSet<>();
		for (Geometry g : intersected) {
			Geometry r;
			if (operationFill)
				r = g.union(geom);
			else
				r = g.difference(geom);
			r.setUserData(g.getUserData());
			result.add(r);
		}
		return result;
	}

	/**
	 * Method to prepare the geometry for saving to the database. 1. It tests if the
	 * geometry is within the system boundaries 2. Calculates the centroid 3. Tests
	 * the geometry for spikes 4. Tests the geometries for compliancy with minimal
	 * distance
	 * 
	 * @param dbo
	 * @throws SvException
	 */
	void prepareGeometry(DbDataObject dbo) throws SvException {
		Geometry geom = getGeometry(dbo);
		Point centroid = calculateCentroid(geom);

		minGeomDistance(geom, dbo.getObjectType(), minGeomDistance, true);
		minVertexDistance(geom, minPointDistance, true);
		testPolygonSpikes(geom, maxAngle);

		// if area is not set or we have configured to override
		if (dbo.getVal("AREA") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("AREA", geom.getArea());
		if (dbo.getVal("AREA_HA") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("AREA_HA", geom.getArea() / 10000);
		if (dbo.getVal("AREA_KM2") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("AREA_KM2", geom.getArea() / 1000000); // needed for
																// moemris
		if (dbo.getVal("PERIMETER") == null || SvConf.sdiOverrideGeomCalc)
			dbo.setVal("PERIMETER", geom.getLength());
		setCentroid(dbo, centroid);

	}

	/**
	 * Method to save a DbDataArray of DbDataObjects which are of Geometry Type and
	 * have set the flag hasGeometries. The method will perform basic saveObject on
	 * the alphanumeric fields and further process the GEOMETRY columns
	 * 
	 * @param dba        the array of objects to be saved in the database
	 * @param isBatch    Flag to signal if the inserts in the DB should be batched
	 * @param autoCommit if autocommit is true, the connection will be committed if
	 *                   no exception occurs. if exception occurs, a rollback will
	 *                   be issued
	 * @throws SvException Throws system.error.sdi.non_sdi_type if the object not of
	 *                     geometry type or system.error.sdi.geom_field_missing if
	 *                     the object has no geometry field while SvGeometry has
	 *                     allowNullGeometry set to false
	 * 
	 */
	public void saveGeometry(DbDataArray dba, Boolean isBatch, boolean autoCommit) throws SvException {
		this.isInternal = true; // flag it as internal so can save SDI types
		Geometry currentGeom = null;
		for (DbDataObject dbo : dba.getItems()) {
			if (!SvCore.hasGeometries(dbo.getObjectType()))
				throw (new SvException("system.error.sdi.non_sdi_type", instanceUser, dba, null));
			currentGeom = getGeometry(dbo);
			if (currentGeom != null) {
				verifyBounds(dbo);
				if (currentGeom instanceof Polygonal)
					prepareGeometry(dbo);
			} else if (!allowNullGeometry)
				throw (new SvException("system.error.sdi.geom_field_missing", instanceUser, dba, null));
		}
		super.saveObject(dba, isBatch, autoCommit);
		cacheCleanup(dba);
	}

	/**
	 * Method to invalidate the tiles caching the GIS data related to geometries
	 * held in the DbDataArray
	 * 
	 * @param dba The list of objects containing Geometries
	 * @throws SvException
	 */
	static void cacheCleanup(DbDataArray dba) throws SvException {
		List<Geometry> tileGeomList = null;
		Set<SvSDITile> tileList = new HashSet<SvSDITile>();

		for (DbDataObject dbo : dba.getItems()) {
			Geometry vdataGeom = SvGeometry.getGeometry(dbo);
			if (vdataGeom != null) {
				Envelope env = vdataGeom.getEnvelopeInternal();
				tileGeomList = SvGeometry.getTileGeometries(env);
				for (Geometry tgl : tileGeomList) {
					String tileID = (String) tgl.getUserData();
					SvSDITile tile = SvGeometry.getTile(dbo.getObjectType(), tileID, null);
					if (!tileList.contains(tile))
						tileList.add(tile);
				}
			}
		}
		tilesCleanup(tileList);
	}

	private static void tilesCleanup(Set<SvSDITile> tileList) throws SvException {
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

	static void markDirtyTile(long tileTypeId, int[] cell) throws SvException {
		if (cell != null && cell.length > 2) {
			String tileId = Integer.toString(cell[0]) + ":" + Integer.toString(cell[1]);
			SvSDITile tile = getTile(tileTypeId, tileId, null);
			if (tile != null)
				tile.setIsTileDirty(true);
		}
	}

	public void saveGeometry(DbDataArray dba) throws SvException {
		saveGeometry(dba, false, this.autoCommit);
	}

	/**
	 * Method to save a single geometry to the database. Same as
	 * {@link #saveGeometry(DbDataArray)}, but using a single object instead of
	 * array.
	 * 
	 * @param dbo The object which should be saved
	 * @throws SvException Pass through of underlying exceptions
	 */
	public void saveObject(DbDataObject dbo) throws SvException {
		DbDataArray dba = new DbDataArray();
		dba.addDataItem(dbo);
		saveGeometry(dba);
	}

	public void saveObject(DbDataArray dba, Boolean isBatch) throws SvException {
		saveGeometry(dba, isBatch, this.autoCommit);
	}

	public void saveObject(DbDataArray dba) throws SvException {
		saveGeometry(dba, true, this.autoCommit);
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

}
