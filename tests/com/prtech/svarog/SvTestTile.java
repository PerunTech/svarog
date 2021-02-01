package com.prtech.svarog;

import java.util.ArrayList;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

public class SvTestTile extends SvSDITile {

	final GeometryCollection testGeometries;

	public SvTestTile(Envelope env, long typeTileId, GeometryCollection testGeometries) {
		this.tileEnvelope = env;
		this.tileTypeId = typeTileId;
		this.testGeometries = testGeometries;
	}

	@Override
	GeometryCollection loadGeometries() throws SvException {
		return testGeometries;
	}

}
