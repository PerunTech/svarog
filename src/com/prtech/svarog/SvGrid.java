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

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbSearch;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonReader;
import com.vividsolutions.jts.io.svarog_geojson.GeoJsonWriter;

public class SvGrid extends SvSDITile {
	static final Logger log4j = LogManager.getLogger(SvSDITile.class.getName());
	private static final String GRID_NAME = "GRID_NAME";
	private static final String GRIDTILE_ID = "GRIDTILE_ID";
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

	protected Long tileTypeId = svCONST.OBJECT_TYPE_GRID;

	protected String getTileId() {
		return tileTypeId.toString() + "." + gridName;
	}

	/**
	 * Method to build the gridIndex and populate the gridmap based on the grid
	 * specified by the internal geometry collection {@link #grid}
	 * 
	 * @param gridName the code name of the grid.
	 * @return
	 * @throws SvException
	 */
	SvGrid(String gridName) throws SvException {
		this.gridName = gridName;
		SvSDITile boundary = SvGeometry.getSysBoundary();
		boundary.loadTile();
		Envelope env = boundary.getEnvelope();
		env.expandBy(2000);
		this.tileEnvelope = env;
		buildGrid(this.getInternalGeomCollection(), gridName);
	}

	public SvGrid(GeometryCollection gridGcl, String sdiSysgrid) {
		buildGrid(gridGcl, gridName);
	}

	void buildGrid(GeometryCollection gridGcl, String gridName) {
		this.gridName = gridName;
		this.grid = gridGcl;
		Geometry gridItem = null;
		gridIndex = new STRtree();
		try {
			for (int i = 0; i < grid.getNumGeometries(); i++) {
				gridItem = grid.getGeometryN(i);
				Object ud = gridItem.getUserData();
				String tileId = null;
				if (ud instanceof DbDataObject)
					tileId = (String) ((DbDataObject) ud).getVal(GRIDTILE_ID);
				else
					tileId = (String) ud;
				String[] baseId = tileId.split("-");
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

	static boolean saveGridToMasterFile(GeometryCollection grid) {
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

	/**
	 * Method to ensure the old grid items are invalidated
	 * 
	 * @param gridName The name of the grid which should be deleted
	 * @param core     The parent ISvCore to ensure all happens in a single
	 *                 transaction
	 * @throws SvException Underlying exception if the deletion failed
	 */
	static void deleteOldGrid(String gridName, ISvCore core) throws SvException {

		try (SvReader svr = new SvReader((SvCore) core); SvWriter svw = new SvWriter((SvCore) core)) {
			DbSearch dbs = new DbSearchCriterion(GRID_NAME, DbCompareOperand.EQUAL, gridName);
			DbDataArray arr = svr.getObjects(dbs, svCONST.OBJECT_TYPE_GRID, null, 0, 0);
			if (arr.size() > 0)
				svw.deleteObjects(arr);
		}

	}

	static boolean shouldUpgradeGrid(GeometryCollection grid) {
		SvGrid g = null;
		boolean result = false;
		try {
			g = SvGeometry.getSysGrid();
			Map<String, Geometry> gmap = g.getGridMap();
			for (int i = 0; i < grid.getNumGeometries(); i++) {
				Geometry gg = SvUtil.sdiFactory.createMultiPolygon(new Polygon[] { (Polygon) grid.getGeometryN(i) });
				String tileId = (String) grid.getGeometryN(i).getUserData();
				tileId = tileId.substring(0, tileId.indexOf("-"));
				Geometry existing = gmap.get(tileId);
				if (existing == null || !gg.equalsExact(existing)) {
					log4j.info("Tile " + tileId + " has updated geometry, upgrade needed");
					result = true;
				}

			}
		} catch (SvException e) {
			log4j.debug("System grid is empty. Initial install required");
			result = true;
		}
		if (!result)
			log4j.info("System grid is up to date");
		return result;

	}

	/**
	 * Save a new version of a grid into the database. It will invalidate the old
	 * grid!
	 * 
	 * @param grid     The list of geometries forming the grid
	 * @param gridName The name of the grid
	 * @return
	 */
	static boolean saveGridToDatabase(GeometryCollection grid, String gridName) {
		try (SvWriter svg = new SvWriter()) {
			// if its the system grid test if we should upgrade
			if (gridName.equals(Sv.SDI_SYSGRID) && !shouldUpgradeGrid(grid))
				return true;
			// first lets delete the existing grid if any
			deleteOldGrid(gridName, svg);
			// now prepare the new set of geometries for saving to the database
			DbDataArray gridArray = new DbDataArray();
			for (int i = 0; i < grid.getNumGeometries(); i++) {
				DbDataObject gridCell = new DbDataObject(svCONST.OBJECT_TYPE_GRID);
				Geometry g = SvUtil.sdiFactory.createMultiPolygon(new Polygon[] { (Polygon) grid.getGeometryN(i) });
				gridCell.setVal(GRID_NAME, gridName);
				gridCell.setVal(GRIDTILE_ID, grid.getGeometryN(i).getUserData());
				gridCell.setVal(Sv.AREA, g.getArea());
				SvGeometry.setGeometry(gridCell, g);
				Point centroid = SvGeometry.calculateCentroid(g);
				SvGeometry.setCentroid(gridCell, centroid);
				gridArray.addDataItem(gridCell);
			}
			svg.isInternal = true;
			svg.saveObject(gridArray, true);
			log4j.info("Number of tiles written:" + grid.getNumGeometries());
			return true;
		} catch (Exception e) {
			log4j.error("Error saving grid to file:" + SvConf.getConfPath() + SvarogInstall.masterSDIPath
					+ SvarogInstall.sdiGridFile, e);
		}
		return false;
	}

	@Override
	ArrayList<Geometry> loadGeometries() throws SvException {
		ArrayList<Geometry> geometries = new ArrayList<>();
		DbSearch dbs = new DbSearchCriterion(GRID_NAME, DbCompareOperand.EQUAL, gridName);
		Geometry geom = null;
		try (SvReader svr = new SvReader()) {

			svr.includeGeometries = true;
			DbDataArray arr = svr.getObjects(dbs, tileTypeId, null, 0, 0);
			if (log4j.isDebugEnabled())
				log4j.debug("Loaded " + arr.size() + " geometries for tile type:" + tileTypeId + ", with search:"
						+ dbs.toSimpleJson().toString());
			for (DbDataObject dbo : arr.getItems()) {
				geom = SvGeometry.getGeometry(dbo);
				geom.setUserData(dbo);
				geometries.add(geom);
			}
		}
		if (geometries.size() < 1)
			throw (new SvException(Sv.Exceptions.EMPTY_GRID, svCONST.systemUser));
		return geometries;
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
