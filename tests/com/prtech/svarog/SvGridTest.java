package com.prtech.svarog;

import static org.junit.Assert.fail;

import org.junit.Test;

public class SvGridTest {
	@Test
	public void getGrid()
	{
		try {
			SvGrid sysGrid = new SvGrid(Sv.SDI_SYSGRID);
			if(sysGrid.getInternalGeometries().size()<1 || sysGrid.getInternalGeomCollection().getArea()<10)
				fail("Grid init failed");
		} catch (SvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Grid init raised exception");
		}
		
	}
}
