package com.prtech.svarog;

import java.sql.Connection;
import java.util.ArrayList;

import org.apache.felix.main.AutoProcessor;
import org.apache.logging.log4j.Logger;

import com.prtech.svarog_interfaces.ISvConfiguration;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvConfiguration.UpdateType;

public class SvConfigurationUpgrade {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = SvConf.getLogger(SvConfigurationUpgrade.class);

	/**
	 * List of SvConfigurations available in the system
	 */
	static ArrayList<ISvConfiguration> iSvCfgs = null;
	
	
	static void prepareConfig() {
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
	
	static void executeConfiguration(ISvConfiguration.UpdateType updateType) throws Exception {
		Connection conn = null;
		String schema = null;
		String msg = "";
		ISvCore svc = null;
		prepareConfig();

		// pre-install db handler call
		try {
			conn = SvConf.getDBConnection();
			schema = SvConf.getDefaultSchema();
			if (!updateType.equals(UpdateType.SCHEMA) || isSvarogInstalled())
				svc = new SvReader();
			for (ISvConfiguration conf : getSortedCfgs(iSvCfgs, updateType)) {
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
				log4j.info(msg);
			}
		} finally {
			if (svc != null)
				svc.release();
			if (conn != null)
				conn.close();
		}
	}
}
