/*******************************************************************************
 *   Copyright (c) 2013, 2019 Perun Technologii DOOEL Skopje.
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Apache License
 *   Version 2.0 or the Svarog License Agreement (the "License");
 *   You may not use this file except in compliance with the License. 
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See LICENSE file in the project root for the specific language governing 
 *   permissions and limitations under the License.
 *  
 *******************************************************************************/
package com.prtech.svarog;

import org.apache.logging.log4j.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.BundleListener;

public class SvActivator implements BundleActivator, BundleListener {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = SvConf.getLogger(SvActivator.class);
	private BundleContext bundleContext = null;

	public void start(BundleContext context) {

		bundleContext = context;
		log4j.info("Starting bundle " + bundleContext.getBundle().getSymbolicName());

	}

	public void stop(BundleContext context) {
		log4j.info("Stopping bundle " + bundleContext.getBundle().getSymbolicName());
		bundleContext = null;
	}

	public BundleContext getContext() {
		return bundleContext;
	}

	@Override
	public void bundleChanged(BundleEvent arg0) {
		// Nothing to do here
		
	}
}
