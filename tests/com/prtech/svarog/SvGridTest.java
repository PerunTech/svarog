package com.prtech.svarog;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;

public class SvGridTest {

	@Test
	public void sysGridTest() {
		SvGrid grid = null;
		try {
			grid = new SvGrid(Sv.SDI_SYSGRID);
			if (grid.getInternalGeometries().size() < 1)
				fail("sys grid empty");
		} catch (SvException e) {
			fail("Exception with sys grid initialisation");
		}

	}

	@Test
	public void sysGridGenerate() {
		Geometry boundary = DbInit.getSysBoundaryFromJson();
		try (SvReader svr = new SvReader()) {
				GeometryCollection grid = SvGrid.generateGrid(boundary, SvConf.getSdiGridSize(), svr);
				SvGrid.saveGridToDatabase(grid, "KNT_2020", svr);
		} catch (SvException e) {
			fail("Exception with sys grid initialisation");
		}

	}

	@Test
	public void nonExistingGrid() {
		SvGrid grid = null;
		try {
			grid = new SvGrid("NONEXISTS");
			grid.getInternalGeometries();
			fail("Empty grid initialised");
		} catch (SvException e) {
			if (!e.getLabelCode().equals(Sv.Exceptions.EMPTY_GRID))
				fail("Empty grid initialised");

		}
	}
}
