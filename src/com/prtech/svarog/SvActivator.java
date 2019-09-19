package com.prtech.svarog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class SvActivator implements BundleActivator {
	/**
	 * Log4j instance used for logging
	 */
	static final Logger log4j = LogManager.getLogger(SvActivator.class.getName());
	private BundleContext bundleContext = null;

	public void start(BundleContext context) {
		
		bundleContext = context;
		log4j.info("Starting bundle "+bundleContext.getBundle().getSymbolicName());
		
	}

	public void stop(BundleContext context) {
		bundleContext = null;
		log4j.info("Stopping bundle "+bundleContext.getBundle().getSymbolicName());

	}

	public BundleContext getContext() {
		return bundleContext;
	}
}
