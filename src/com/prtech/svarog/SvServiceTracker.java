package com.prtech.svarog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import com.prtech.svarog_interfaces.IPerunPlugin;
import com.prtech.svarog_interfaces.ISvExecutor;
import com.prtech.svarog_interfaces.ISvExecutorGroup;

/**
 * Internal class to implement the service tracker customizer. we need it to
 * detect services removal.
 * 
 * @author ristepejov
 *
 */
public class SvServiceTracker implements ServiceTrackerCustomizer {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvServiceTracker.class.getName());

	@Override
	public Object addingService(ServiceReference arg0) {
		// Nothing to do here we will anyway lazy load the services
		log4j.debug("Adding service with reference: " + arg0.getClass().getCanonicalName());
		@SuppressWarnings("unchecked")
		Object result = arg0.getBundle().getBundleContext().getService(arg0);
		if (result != null && result instanceof IPerunPlugin) {
			try {
				SvPerunManager.addPlugin((IPerunPlugin) result);
			} catch (Exception e) {
				log4j.error("Error loading plugin:" + ((IPerunPlugin) result).getContextName(), e);
				result = null;
			}
		}
		return result;
	}

	@Override
	public void modifiedService(ServiceReference arg0, Object arg1) {
		if (arg1 instanceof IPerunPlugin) {
			SvPerunManager.reloadPlugin((IPerunPlugin) arg1);
		}
		log4j.debug("Modified service with reference: " + arg0.getClass().getCanonicalName());

	}

	@Override
	public void removedService(ServiceReference arg0, Object arg1) {
		if (arg1 instanceof ISvExecutor) {
			ISvExecutor exec = (ISvExecutor) arg1;
			SvExecManager.invalidateExecutor(exec.getCategory(), exec.getName());
		} else if (arg1 instanceof ISvExecutorGroup) {
			ISvExecutorGroup execg = (ISvExecutorGroup) arg1;

			for (String key : SvExecManager.getKeys(execg))
				SvExecManager.invalidateExecutor(key);
		} else if (arg1 instanceof IPerunPlugin) {
			SvPerunManager.removePlugin(((IPerunPlugin) arg1).getContextName());
		}
		log4j.debug("Removed service with reference: " + arg0.getClass().getCanonicalName());

	}

}