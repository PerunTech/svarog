package com.prtech.svarog;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonReader;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonWriter;

public class SvGrid extends SvSDITile {
	static final Logger log4j = LogManager.getLogger(SvSDITile.class.getName());
	/**
	 * The main index of grid geometries.
	 */
	private STRtree gridIndex = null;

	private String gridName;

	GeometryCollection grid;
	/**
	 * The envelope of the boundaries of the system
	 */
	private Envelope gridEnvelope = null;

	private Map<String, Geometry> gridMap = new HashMap<String, Geometry>();

	private ArrayList<String> borderTiles = new ArrayList<String>();

	static int maxYtile = 0;
	static int maxXtile = 0;

	/**
	 * Method to build the gridIndex and populate the gridmap based on the grid
	 * specified by the internal geometry collection {@link #grid}
	 * 
	 * @param gridName the code name of the grid.
	 * @return
	 */
	SvGrid(String gridName) {
		this.gridName = gridName;
		//TODO load geometries from DB?
	}

	SvGrid(GeometryCollection gridGcl, String gridName) {
		this.gridName = gridName;
		this.grid = gridGcl;
		Geometry gridItem = null;
		gridIndex = new STRtree();
		try {
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
				if (gridEnvelope == null)
					gridEnvelope = new Envelope(gridItem.getEnvelopeInternal());
				else
					gridEnvelope.expandToInclude(gridItem.getEnvelopeInternal());
				gridMap.put(baseId[0], gridItem);
			}
		} catch (Exception e) {
			log4j.error("Failed initialising system grid!", e);
		}
		gridIndex.build();
	}

	public static int getMaxYtile() {
		return maxYtile;
	}

	public static void setMaxYtile(int maxYtile) {
		SvGrid.maxYtile = maxYtile;
	}

	public static int getMaxXtile() {
		return maxXtile;
	}

	public static void setMaxXtile(int maxXtile) {
		SvGrid.maxXtile = maxXtile;
	}

	static GeometryCollection generateGrid(Geometry geo, int gridSize) {
		assert (geo != null);
		Envelope env = geo.getEnvelopeInternal();

		log4j.info("Generating Tiles for system boundary with SRID:" + geo.getSRID() + ", area:" + geo.getArea()
				+ ", and envelope:" + env.toString());
		int row = 0;
		int col = 0;
		GeometryCollection gcl = null;
		try {

			env.expandBy(10);

			Envelope envGrid = new Envelope(java.lang.Math.round(env.getMinX()), java.lang.Math.round(env.getMaxX()),
					java.lang.Math.round(env.getMinY()), java.lang.Math.round(env.getMaxY()));

			ArrayList<Geometry> gridList = new ArrayList<Geometry>();
			Envelope currentGridItem = null;

			Geometry polygon = null;
			double currentMinY = envGrid.getMinY();
			double currentMaxX = envGrid.getMinX();

			Boolean isFinished = false;
			while (!isFinished) {
				currentGridItem = new Envelope(currentMaxX, currentMaxX + gridSize * 1000, currentMinY,
						currentMinY + gridSize * 1000);
				currentMinY = currentGridItem.getMinY();
				currentMaxX = currentGridItem.getMaxX();

				polygon = SvUtil.sdiFactory.toGeometry(currentGridItem);
				if (!polygon.disjoint(geo)) {
					if (polygon.getArea() > 1) {
						polygon.setUserData(row + ":" + col + "-" + (!polygon.within(geo)));
						gridList.add(polygon);
					}
				}
				col++;
				if (currentMaxX > envGrid.getMaxX()) {
					currentMinY = currentGridItem.getMaxY();
					currentMaxX = envGrid.getMinX();
					row++;
					col = 0;
					if (currentMinY > envGrid.getMaxY())
						isFinished = true;
				}

			}
			Geometry[] garr = new Geometry[gridList.size()];
			gridList.toArray(garr);
			gcl = SvUtil.sdiFactory.createGeometryCollection(garr);

		} catch (Exception e) {
			log4j.error("Error generating grid", e);
		}
		return gcl;

	}

	static boolean saveGridToMasterFile(GeometryCollection grid ) {
		try {
			String jtsJson = null;
			GeoJsonWriter jtsWriter = new GeoJsonWriter();
			jtsWriter.setUseFeatureType(true);
			if (SvConf.getSDISrid().equals(Sv.SQL_NULL))
				jtsWriter.setEncodeCRS(false);
			jtsJson = jtsWriter.write(grid);
			SvUtil.saveStringToFile(SvConf.getConfPath() + SvarogInstall.masterSDIPath + SvarogInstall.sdiGridFile,
					jtsJson);
			log4j.info("Number of tiles written:" + grid.getNumGeometries());
			log4j.info("Generating Tiles finished successfully (" + SvConf.getConfPath() + SvarogInstall.masterSDIPath
					+ SvarogInstall.sdiGridFile + ")");
			return true;
		} catch (Exception e) {
			log4j.error("Error saving grid to file:" + SvConf.getConfPath() + SvarogInstall.masterSDIPath
					+ SvarogInstall.sdiGridFile, e);
		}
		return false;
	}

	@Override
	ArrayList<Geometry> loadGeometries() throws SvException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Method to initialise the system grid from a JSON file
	 * 
	 * @param gridFileName The filename containing the grid
	 * @return collection of square geometries forming the system grid
	 */
	static GeometryCollection loadGridFromJson(String gridFileName) {
		GeometryCollection grid = null;
		if (log4j.isDebugEnabled())
			log4j.trace("Loading base SDI grid from:" + gridFileName);
		String geoJSONBounds = null;
		InputStream is = null;
		try {
			String path = gridFileName;
			is = new FileInputStream(path);
			geoJSONBounds = IOUtils.toString(is);
			GeoJsonReader jtsReader = new GeoJsonReader();
			jtsReader.setUseFeatureType(true);
			grid = (GeometryCollection) jtsReader.read(geoJSONBounds);
			if (log4j.isDebugEnabled())
				log4j.trace("Base SDI grid loaded:" + gridFileName);
		} catch (Exception e) {
			log4j.error("Failed loading base SDI grid from:" + gridFileName, e);
		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					log4j.error("Failed loading base SDI grid from:" + gridFileName, e);
				}
		}
		return grid;
	}

	public STRtree getGridIndex() {
		return gridIndex;
	}

	public void setGridIndex(STRtree gridIndex) {
		this.gridIndex = gridIndex;
	}

	public String getGridName() {
		return gridName;
	}

	public void setGridName(String gridName) {
		this.gridName = gridName;
	}

	public Map<String, Geometry> getGridMap() {
		return gridMap;
	}

	public void setGridMap(Map<String, Geometry> gridMap) {
		this.gridMap = gridMap;
	}

	public ArrayList<String> getBorderTiles() {
		return borderTiles;
	}

	public void setBorderTiles(ArrayList<String> borderTiles) {
		this.borderTiles = borderTiles;
	}

	public Envelope getGridEnvelope() {
		return gridEnvelope;
	}

	public void setGridEnvelope(Envelope gridEnvelope) {
		this.gridEnvelope = gridEnvelope;
	}

}
