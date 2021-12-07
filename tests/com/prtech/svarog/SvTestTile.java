package com.prtech.svarog;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryCollection;


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
