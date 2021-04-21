package com.prtech.svarog;

import java.util.ArrayList;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

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
