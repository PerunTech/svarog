package com.prtech.svarog;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.felix.main.AutoProcessor;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvConfigurationMulti;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvConfiguration.UpdateType;

/**
 * Class used to support the execution of objects implementing
 * {@link ISvConfiguration} interface. The class will read the list of all
 * executed configurations and ensure that all available objects are executed
 * unless they were already called during a previous upgrade. In case of
 * {@link ISvConfigurationMulti} the class shall check the version of the object
 * to ensure that the currently available version is executed
 * 
 * @author ristepejov
 *
 */
public class SvConfigurationUpgrade {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvConfigurationUpgrade.class);
	/**
	 * The list of previous upgrades
	 */
	static DbDataArray upgradeHistory = getUpgradeHistory();

	/**
	 * Static block to initialise the upgrade history
	 */
	static DbDataArray getUpgradeHistory() {
		if (SvarogInstall.isSvarogInstalled()) {
			DbDataArray history = null;
			try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr)) {
				DbQueryObject q = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_CONFIGURATION_LOG), null, null,
						null);
				history = svr.getObjects(q, null, null);
				history.rebuildIndex(Sv.CONFIGURATION_CLASS, true);
			} catch (SvException e) {
				log4j.fatal("Failed reading upgrade history", e);
			}
			return history;
		} else
			return new DbDataArray();

	}

	/**
	 * List of SvConfigurations available in the system
	 */
	static ArrayList<ISvConfiguration> iSvCfgs = null;

	/**
	 * Lazy loader of the ISvconfiguration instances from the directory used for
	 * AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY
	 * 
	 * @return List of objects implementing the {@link ISvConfiguration} interface
	 */
	static List<ISvConfiguration> getConfig() {
		if (iSvCfgs == null) {
			synchronized (SvConfigurationUpgrade.class) {
				if (iSvCfgs == null) {
					iSvCfgs = new ArrayList<ISvConfiguration>();
					// add the system implementation
					iSvCfgs.add(new SvConfigurationImpl());

					ArrayList<Object> cfgs = DbInit.loadClass(SvConf.getParam(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY),
							ISvConfiguration.class);
					if (cfgs != null)
						for (Object o : cfgs)
							if (o instanceof ISvConfiguration)
								iSvCfgs.add((ISvConfiguration) o);
				}
			}
		}
		return iSvCfgs;
	}

	/**
	 * Method to sort an array of ISvConfiguration objects by different update types
	 * 
	 * @param cfgs       The array of ISvConfiguration objects
	 * @param updateType The specific update type to be used for sorting
	 * @return The sorted ArrayList
	 */
	static List<ISvConfiguration> getSortedCfgs(List<ISvConfiguration> cfgs,
			final ISvConfiguration.UpdateType updateType) {
		Collections.sort(cfgs, new Comparator<ISvConfiguration>() {
			public int compare(ISvConfiguration o1, ISvConfiguration o2) {
				return o1.executionOrder(updateType) - o2.executionOrder(updateType);
			}
		});
		return cfgs;
	}

	/**
	 * Method to perform the save of the configuration log to the database.
	 * 
	 * @param svc          The SvCore instance used for the transaction
	 * @param conf         The configuration object which was executed
	 * @param isSuccessful The result of the execution
	 * @param errorMsg     Any error message/stack trace.
	 * @param updateType   The update type which was executed.
	 * @throws SvException
	 */
	static void saveConfigLog(SvCore svc, ISvConfiguration conf, boolean isSuccessful, String errorMsg,
			ISvConfiguration.UpdateType updateType) throws SvException {
		DbDataObject executionLog = upgradeHistory
				.getItemByIdx(conf.getClass().getName() + "-" + updateType.toString());

		DbDataObject dbo = new DbDataObject();
		dbo.setObjectType(svCONST.OBJECT_TYPE_CONFIGURATION_LOG);
		dbo.setVal(Sv.CONFIGURATION_CLASS, conf.getClass().getName() + "-" + updateType.toString());
		int version = 1;
		if (conf instanceof ISvConfigurationMulti) {
			int executedVerion = executionLog != null ? ((Long) executionLog.getVal(Sv.VERSION)).intValue() : 0;
			version = ((ISvConfigurationMulti) conf).getVersion(executedVerion);
		}
		dbo.setVal(Sv.VERSION, version);
		dbo.setVal(Sv.IS_SUCCESSFUL, isSuccessful);
		dbo.setVal(Sv.EXECUTION_TIME, DateTime.now());
		try (SvWriter svw = new SvWriter(svc); SvNote note = new SvNote(svw)) {
			svw.isInternal = true;
			svw.saveObject(dbo, false);
			if (!isSuccessful) {
				note.setAutoCommit(false);
				note.setNote(dbo.getObjectId(), "EXECUTION_ERROR", errorMsg);
			}
			svw.dbCommit();
		}
	}

	/**
	 * Method to check if an ISvConfiguration instance was already executed or not.
	 * 
	 * @param conf       The configuration instances which is subject of execution
	 * @param updateType The type of update which shall be executed
	 * @return Boolean result if the configuration should be executed
	 */
	static boolean shouldExecute(ISvConfiguration conf, ISvConfiguration.UpdateType updateType) {
		boolean shouldExecute = true;
		DbDataObject executionLog = upgradeHistory
				.getItemByIdx(conf.getClass().getName() + "-" + updateType.toString());

		if (conf instanceof ISvConfigurationMulti) {
			ISvConfigurationMulti multi = ((ISvConfigurationMulti) conf);
			List<UpdateType> types = multi.getUpdateTypes();
			if (types == null || !types.contains(updateType))
				shouldExecute = false;
			else {
				int executedVerion = executionLog != null ? ((Long) executionLog.getVal(Sv.VERSION)).intValue() : 0;
				int nextVersion = multi.getVersion(executedVerion);
				shouldExecute = nextVersion > executedVerion;
			}
		} else
			shouldExecute = executionLog == null;
		return shouldExecute;
	}

	/**
	 * Static method which executes all existing configurations for the required
	 * updateType. See {@link ISvConfiguration.UpdateType}
	 * 
	 * @param updateType The update type to be executed
	 * @throws SQLException in case any connection issues arise
	 */
	static void executeConfiguration(ISvConfiguration.UpdateType updateType) throws SQLException {
		Connection conn = null;
		String schema = null;
		String msg = "";
		ISvCore svc = null;

		// pre-install db handler call
		try {

			schema = SvConf.getDefaultSchema();
			if (!updateType.equals(UpdateType.SCHEMA) || SvarogInstall.isSvarogInstalled()) {
				svc = new SvReader();
				conn = svc.dbGetConn();
			} else
				conn = SvConf.getDBConnection();

			// iterate over the available configurations
			for (ISvConfiguration conf : getSortedCfgs(getConfig(), updateType)) {
				// if the configuration was executed
				if (!shouldExecute(conf, updateType))
					continue;

				boolean isSuccessful = true;
				try {
					switch (updateType) {
					case SCHEMA:
						msg = conf.beforeSchemaUpdate(conn, svc, schema);
						break;
					case LABELS:
						msg = conf.beforeLabelsUpdate(conn, svc, schema);
						break;
					case CODES:
						msg = conf.beforeCodesUpdate(conn, svc, schema);
						break;
					case TYPES:
						msg = conf.beforeTypesUpdate(conn, svc, schema);
						break;
					case LINKTYPES:
						msg = conf.beforeLinkTypesUpdate(conn, svc, schema);
						break;
					case ACL:
						msg = conf.beforeAclUpdate(conn, svc, schema);
						break;
					case SIDACL:
						msg = conf.beforeSidAclUpdate(conn, svc, schema);
						break;
					case FINAL:
						msg = conf.afterUpdate(conn, svc, schema);
						break;
					default:
						break;
					}
				} catch (Exception e) {
					isSuccessful = false;
					if (e instanceof SvException)
						msg = ((SvException) e).getFormattedMessage();
					msg += ExceptionUtils.getStackTrace(e);
				}
				saveConfigLog((SvCore) svc, conf, isSuccessful, msg, updateType);
			}
		} catch (SvException e) {
			log4j.error("Before " + updateType.toString() + " raised an unexpected exception", e);
		} finally {
			if (svc != null)
				svc.release();
			if (conn != null)
				conn.close();
		}
	}
}
