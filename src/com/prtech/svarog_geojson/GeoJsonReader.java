/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.prtech.svarog_geojson;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.locationtech.jts.geom.CoordinateSequence;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;

/**
 * Reads a GeoJson Geometry from a JSON fragment into a {@link Geometry}.
 * <p>
 * A specification of the GeoJson format can be found at the GeoJson web site:
 * <a href='http://geojson.org/geojson-spec.html'>http://geojson.org/geojson-
 * spec.html</a>.
 * <p>
 * It is the caller's responsibility to ensure that the supplied
 * {@link PrecisionModel} matches the precision of the incoming data. If a lower
 * precision for the data is required, a subsequent process must be run on the
 * data to reduce its precision.
 * 
 * @author Martin Davis
 * @author Paul Howells, Vivid Solutions.
 * 
 */
public class GeoJsonReader {

	private GeometryFactory gf;

	private boolean useFeatureType = false;

	/**
	 * The default constructor uses the SRID from the Geojson CRS and the
	 * default <code>PrecisionModel</code> to create a
	 * <code>GeometryFactory</code>. If there is no CRS specified then the
	 * default CRS is a geographic coordinate reference system, using the WGS84
	 * datum, and with longitude and latitude units of decimal degrees (SRID =
	 * 4326)
	 */
	public GeoJsonReader() {
		// do nothing
	}

	/**
	 * Configures the reader to expect Features as container for geometries
	 * @param useFeatureType
	 */
	public void setUseFeatureType(boolean useFeatureType) {
		this.useFeatureType = useFeatureType;
	}

	/**
	 * This constructor accepts a <code>GeometryFactory</code> that is used to
	 * create the output geometries and to override the GeoJson CRS.
	 * 
	 * @param geometryFactory
	 *            a GeometryFactory
	 */
	public GeoJsonReader(GeometryFactory geometryFactory) {
		this.gf = geometryFactory;
	}

	/**
	 * Reads a GeoJson Geometry from a <tt>String</tt> into a single
	 * {@link Geometry}.
	 * 
	 * 
	 * @param json
	 *            The GeoJson String to parse
	 * @return the resulting JTS Geometry
	 * 
	 * @throws ParseException
	 *             throws a ParseException if the JSON string cannot be parsed
	 */
	public Geometry read(String json) throws ParseException {
		Geometry result = read(new StringReader(json));
		return result;
	}

	/**
	 * Reads a GeoJson Geometry from a {@link Reader} into a single
	 * {@link Geometry}.
	 * 
	 * 
	 * @param reader
	 *            The input source
	 * @return The resulting JTS Geometry
	 * 
	 * @throws ParseException
	 *             throws a ParseException if the JSON string cannot be parsed
	 */
	public Geometry read(Reader reader) throws ParseException {

		Geometry result = null;

		JSONParser parser = new JSONParser();
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> geometryMap = (Map<String, Object>) parser.parse(reader);

			GeometryFactory geometryFactory = null;
			if (this.gf == null) {
				geometryFactory = this.getGeometryFactory(geometryMap);
			} else {
				geometryFactory = this.gf;
			}

			result = create(geometryMap, geometryFactory);

		} catch (org.json.simple.parser.ParseException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new ParseException(e);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	private Geometry create(Map<String, Object> geometryMap, GeometryFactory geometryFactory) throws ParseException {

		Map<String, Object> geometryMapImpl=geometryMap;
		Geometry result = null;

		String type = (String) geometryMapImpl.get(GeoJsonConstants.NAME_TYPE);
		if(type != null && GeoJsonConstants.NAME_FEATURE.equals(type))
		{
			geometryMapImpl=(Map<String, Object>) geometryMap.get(GeoJsonConstants.NAME_GEOMETRY);
			type = (String) geometryMapImpl.get(GeoJsonConstants.NAME_TYPE);
		}
		if (type == null) {
			throw new ParseException("Could not parse Geometry from Json string.  No 'type' property found.");
		} else {

			if (GeoJsonConstants.NAME_POINT.equals(type)) {
				result = createPoint(geometryMapImpl, geometryFactory);

			} else if (GeoJsonConstants.NAME_LINESTRING.equals(type)) {
				result = createLineString(geometryMapImpl, geometryFactory);

			} else if (GeoJsonConstants.NAME_POLYGON.equals(type)) {
				result = createPolygon(geometryMapImpl, geometryFactory);

			} else if (GeoJsonConstants.NAME_MULTIPOINT.equals(type)) {
				result = createMultiPoint(geometryMapImpl, geometryFactory);

			} else if (GeoJsonConstants.NAME_MULTILINESTRING.equals(type)) {
				result = createMultiLineString(geometryMapImpl, geometryFactory);

			} else if (GeoJsonConstants.NAME_MULTIPOLYGON.equals(type)) {
				result = createMultiPolygon(geometryMapImpl, geometryFactory);

			} else if (GeoJsonConstants.NAME_GEOMETRYCOLLECTION.equals(type) || GeoJsonConstants.NAME_FEATURECOLLECTION.equals(type)) {
				result = createGeometryCollection(geometryMapImpl, geometryFactory);

			} else {
				throw new ParseException("Could not parse Geometry from GeoJson string.  Unsupported 'type':" + type);
			}
			if (geometryMapImpl.get(GeoJsonConstants.NAME_USERDATA) != null)
				result.setUserData(geometryMapImpl.get(GeoJsonConstants.NAME_USERDATA));

		}

		return result;
	}

	@SuppressWarnings("unchecked")
	private Geometry createGeometryCollection(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {
			List<Map<String, Object>> geometriesList = null;
			;
			if(useFeatureType)
				geometriesList = (List<Map<String, Object>>) geometryMap
				.get(GeoJsonConstants.NAME_FEATURES);
			else
				geometriesList = (List<Map<String, Object>>) geometryMap
					.get(GeoJsonConstants.NAME_GEOMETRIES);

			Geometry[] geometries = new Geometry[geometriesList.size()];

			int i = 0;
			for (Map<String, Object> map : geometriesList) {

				geometries[i] = this.create(map, geometryFactory);

				++i;
			}

			result = geometryFactory.createGeometryCollection(geometries);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse GeometryCollection from GeoJson string.", e);
		}

		return result;
	}

	private Geometry createMultiPolygon(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {

			@SuppressWarnings("unchecked")
			List<List<List<List<Number>>>> polygonsList = (List<List<List<List<Number>>>>) geometryMap
					.get(GeoJsonConstants.NAME_COORDINATES);

			Polygon[] polygons = new Polygon[polygonsList.size()];

			int p = 0;
			for (List<List<List<Number>>> ringsList : polygonsList) {

				List<CoordinateSequence> rings = new ArrayList<CoordinateSequence>();

				for (List<List<Number>> coordinates : ringsList) {

					rings.add(createCoordinateSequence(coordinates));
				}

				if (rings.isEmpty()) {
					continue;
				}

				LinearRing outer = geometryFactory.createLinearRing(rings.get(0));
				LinearRing[] inner = null;
				if (rings.size() > 1) {
					inner = new LinearRing[rings.size() - 1];
					for (int i = 1; i < rings.size(); i++) {
						inner[i - 1] = geometryFactory.createLinearRing(rings.get(i));
					}
				}

				polygons[p] = geometryFactory.createPolygon(outer, inner);

				++p;
			}

			result = geometryFactory.createMultiPolygon(polygons);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse MultiPolygon from GeoJson string.", e);
		}

		return result;
	}

	private Geometry createMultiLineString(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {

			@SuppressWarnings("unchecked")
			List<List<List<Number>>> linesList = (List<List<List<Number>>>) geometryMap
					.get(GeoJsonConstants.NAME_COORDINATES);

			LineString[] lineStrings = new LineString[linesList.size()];

			int i = 0;
			for (List<List<Number>> coordinates : linesList) {

				lineStrings[i] = geometryFactory.createLineString(createCoordinateSequence(coordinates));

				++i;
			}

			result = geometryFactory.createMultiLineString(lineStrings);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse MultiLineString from GeoJson string.", e);
		}

		return result;
	}

	private Geometry createMultiPoint(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {

			@SuppressWarnings("unchecked")
			List<List<Number>> coordinatesList = (List<List<Number>>) geometryMap
					.get(GeoJsonConstants.NAME_COORDINATES);

			CoordinateSequence coordinates = this.createCoordinateSequence(coordinatesList);

			result = geometryFactory.createMultiPoint(coordinates);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse MultiPoint from GeoJson string.", e);
		}

		return result;
	}

	private Geometry createPolygon(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {

			@SuppressWarnings("unchecked")
			List<List<List<Number>>> ringsList = (List<List<List<Number>>>) geometryMap
					.get(GeoJsonConstants.NAME_COORDINATES);

			List<CoordinateSequence> rings = new ArrayList<CoordinateSequence>();

			for (List<List<Number>> coordinates : ringsList) {

				rings.add(createCoordinateSequence(coordinates));
			}

			if (rings.isEmpty()) {
				throw new IllegalArgumentException("Polygon specified with no rings.");
			}

			LinearRing outer = geometryFactory.createLinearRing(rings.get(0));
			LinearRing[] inner = null;
			if (rings.size() > 1) {
				inner = new LinearRing[rings.size() - 1];
				for (int i = 1; i < rings.size(); i++) {
					inner[i - 1] = geometryFactory.createLinearRing(rings.get(i));
				}
			}

			result = geometryFactory.createPolygon(outer, inner);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse Polygon from GeoJson string.", e);
		}

		return result;
	}

	private Geometry createLineString(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {

			@SuppressWarnings("unchecked")
			List<List<Number>> coordinatesList = (List<List<Number>>) geometryMap
					.get(GeoJsonConstants.NAME_COORDINATES);

			CoordinateSequence coordinates = createCoordinateSequence(coordinatesList);

			result = geometryFactory.createLineString(coordinates);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse LineString from GeoJson string.", e);
		}

		return result;
	}

	private Geometry createPoint(Map<String, Object> geometryMap, GeometryFactory geometryFactory)
			throws ParseException {

		Geometry result = null;

		try {

			@SuppressWarnings("unchecked")
			List<Number> coordinateList = (List<Number>) geometryMap.get(GeoJsonConstants.NAME_COORDINATES);

			CoordinateSequence coordinate = this.createCoordinate(coordinateList);

			result = geometryFactory.createPoint(coordinate);

		} catch (RuntimeException e) {
			throw new ParseException("Could not parse Point from GeoJson string.", e);
		}

		return result;
	}

	private GeometryFactory getGeometryFactory(Map<String, Object> geometryMap) throws ParseException {

		GeometryFactory result = null;
		@SuppressWarnings("unchecked")
		Map<String, Object> crsMap = (Map<String, Object>) geometryMap.get(GeoJsonConstants.NAME_CRS);
		Integer srid = null;

		if (crsMap != null) {

			try {

				@SuppressWarnings("unchecked")
				Map<String, Object> propertiesMap = (Map<String, Object>) crsMap.get(GeoJsonConstants.NAME_PROPERTIES);
				String name = (String) propertiesMap.get(GeoJsonConstants.NAME_NAME);
				String[] split = name.split(":");
				String epsg = split[1];
				srid = Integer.valueOf(epsg);
			} catch (RuntimeException e) {
				throw new ParseException("Could not parse SRID from Geojson 'crs' object.", e);
			}
		}

		if (srid == null) {
			// The default CRS is a geographic coordinate reference
			// system, using the WGS84 datum, and with longitude and
			// latitude units of decimal degrees. SRID 4326
			srid = Integer.valueOf(4326);
		}

		result = new GeometryFactory(new PrecisionModel(), srid.intValue());
		return result;
	}

	private CoordinateSequence createCoordinateSequence(List<List<Number>> coordinates) {
		CoordinateSequence result = null;

		result = new CoordinateArraySequence(coordinates.size());

		for (int i = 0; i < coordinates.size(); ++i) {
			List<Number> ordinates = coordinates.get(i);

			if (ordinates.size() > 0) {
				result.setOrdinate(i, 0, ordinates.get(0).doubleValue());
			}
			if (ordinates.size() > 1) {
				result.setOrdinate(i, 1, ordinates.get(1).doubleValue());
			}
			if (ordinates.size() > 2) {
				result.setOrdinate(i, 2, ordinates.get(2).doubleValue());
			}

		}

		return result;
	}

	private CoordinateSequence createCoordinate(List<Number> ordinates) {
		CoordinateSequence result = new CoordinateArraySequence(1);

		if (ordinates.size() > 0) {
			result.setOrdinate(0, 0, ordinates.get(0).doubleValue());
		}
		if (ordinates.size() > 1) {
			result.setOrdinate(0, 1, ordinates.get(1).doubleValue());
		}
		if (ordinates.size() > 2) {
			result.setOrdinate(0, 2, ordinates.get(2).doubleValue());
		}

		return result;
	}
}
