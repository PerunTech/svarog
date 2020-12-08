package com.prtech.svarog;

import java.util.ArrayList;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class SvTestTile extends SvSDITile {

	final ArrayList<Geometry> testGeometries;

	public SvTestTile(Envelope env, long typeTileId, ArrayList<Geometry> testGeometries) {
		this.tileEnvelope = env;
		this.tileTypeId = typeTileId;
		this.testGeometries = testGeometries;
	}

	@Override
	ArrayList<Geometry> loadGeometries() throws SvException {
		return testGeometries;
	}

}
