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

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import org.apache.felix.framework.Felix;
import org.apache.felix.framework.util.FelixConstants;
import org.apache.felix.framework.util.Util;
import org.apache.felix.main.AutoProcessor;
import org.apache.logging.log4j.Logger;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.osgi.util.tracker.ServiceTracker;

import com.prtech.svarog_interfaces.IPerunPlugin;
import com.prtech.svarog_interfaces.ISvExecutor;
import com.prtech.svarog_interfaces.ISvExecutorGroup;

/**
 * <p>
 * This class is the default way to instantiate and execute the framework. Its
 * recycled from the Felix project. It reuses some of its property handling
 * capabilities.
 * </p>
 **/
public class SvarogDaemon {

	/**
	 * Log4j object to log issues
	 */
	private static final Logger log4j = SvConf.getLogger(SvarogDaemon.class);
	/**
	 * Service tracker to list all SvarogExecutors loaded from bundles
	 */

	static ServiceTracker<?, ?> svcTracker = null;

	/**
	 * The property name used to specify an URL to the system property file.
	 **/
	public static final String SYSTEM_PROPERTIES_PROP = "felix.system.properties";
	/**
	 * The default name used for the system properties file.
	 **/
	public static final String SYSTEM_PROPERTIES_FILE_VALUE = "system.properties";
	/**
	 * The property name used to specify an URL to the configuration property file
	 * to be used for the created the framework instance.
	 **/
	public static final String CONFIG_PROPERTIES_PROP = "felix.config.properties";
	/**
	 * The default name used for the configuration properties file.
	 **/
	public static final String CONFIG_PROPERTIES_FILE_VALUE = "config.properties";
	/**
	 * Name of the configuration directory.
	 */
	public static final String CONFIG_DIRECTORY = "conf";

	/**
	 * The felix instance reference
	 */
	static Framework osgiFramework = null;

	/**
	 * Satus code used by svarog installed to notify that the daemon should run
	 */
	private static int svarogDaemonStatus = -13;

	/**
	 * Method to provide public information about the status of the svarog daemon
	 * 
	 * @return
	 */
	public static int getDaemonStatus() {
		return svarogDaemonStatus;
	}

	/**
	 * Local field to keep reference to the internal hearbeat client
	 */
	static Thread internalClient = null;

	/**
	 * <p>
	 * This method performs the main task of constructing an framework instance and
	 * starting its execution or running the install/management process. The
	 * following functions are performed when invoked:
	 * </p>
	 * <ol>
	 * <li><i><b>Examine and verify command-line arguments.</b></i> The launcher
	 * will pass the command line to the SvarogInstaller. If the installer finds -dm
	 * or --daemon, it will skipinstall and run the SvarogDaemon to host the OSGI
	 * Framework</li>
	 * <li><i><b>Copy configuration properties specified as system properties into
	 * the set of configuration properties.</b></i> Even though the Felix framework
	 * does not consult system properties for configuration information, sometimes
	 * it is convenient to specify them on the command line when launching Felix. To
	 * make this possible, the SvarogDaemon launcher copies any configuration
	 * properties specified as system properties into the set of configuration
	 * properties passed into Felix.</li>
	 * <li><i><b>Add shutdown hook.</b></i> To make sure the framework shutdowns
	 * cleanly, the launcher installs a shutdown hook; this can be disabled with the
	 * <tt>felix.shutdown.hook</tt> configuration property.</li>
	 * <li><i><b>Create and initialize a framework instance.</b></i> The OSGi
	 * standard <tt>FrameworkFactory</tt> is retrieved from
	 * <tt>META-INF/services</tt> and used to create a framework instance with the
	 * configuration properties.</li>
	 * <li><i><b>Auto-deploy bundles.</b></i> All bundles in the auto-deploy
	 * directory are deployed into the framework instance.</li>
	 * <li><i><b>Start the framework.</b></i> The framework is started and the
	 * launcher thread waits for the framework to shutdown.</li>
	 * </ol>
	 * <p>
	 * It should be noted that simply starting an instance of the framework is not
	 * enough to create an interactive session with it. It is necessary to install
	 * and start bundles that provide a some means to interact with the framework;
	 * this is generally done by bundles in the auto-deploy directory or specifying
	 * an "auto-start" property in the configuration property file. If no bundles
	 * providing a means to interact with the framework are installed or if the
	 * configuration property file cannot be found, the framework will appear to be
	 * hung or deadlocked. This is not the case, it is executing correctly, there is
	 * just no way to interact with it.
	 * </p>
	 * <p>
	 * The launcher provides two ways to deploy bundles into a framework at startup,
	 * which have associated configuration properties:
	 * </p>
	 * <ul>
	 * <li>Bundle auto-deploy - Automatically deploys all bundles from a specified
	 * directory, controlled by the following configuration properties:
	 * <ul>
	 * <li><tt>felix.auto.deploy.dir</tt> - Specifies the auto-deploy directory from
	 * which bundles are automatically deploy at framework startup. The default is
	 * the <tt>bundle/</tt> directory of the current directory.</li>
	 * <li><tt>felix.auto.deploy.action</tt> - Specifies the auto-deploy actions to
	 * be found on bundle JAR files found in the auto-deploy directory. The possible
	 * actions are <tt>install</tt>, <tt>update</tt>, <tt>start</tt>, and
	 * <tt>uninstall</tt>. If no actions are specified, then the auto-deploy
	 * directory is not processed. There is no default value for this property.</li>
	 * </ul>
	 * </li>
	 * <li>Bundle auto-properties - Configuration properties which specify URLs to
	 * bundles to install/start:
	 * <ul>
	 * <li><tt>felix.auto.install.N</tt> - Space-delimited list of bundle URLs to
	 * automatically install when the framework is started, where <tt>N</tt> is the
	 * start level into which the bundle will be installed (e.g.,
	 * felix.auto.install.2).</li>
	 * <li><tt>felix.auto.start.N</tt> - Space-delimited list of bundle URLs to
	 * automatically install and start when the framework is started, where
	 * <tt>N</tt> is the start level into which the bundle will be installed (e.g.,
	 * felix.auto.start.2).</li>
	 * </ul>
	 * </li>
	 * </ul>
	 * <p>
	 * These properties should be specified in the <tt>svarog.properties</tt> so
	 * that they can be processed by the launcher during the framework startup
	 * process.
	 * </p>
	 * 
	 * @param args Accepts arguments to install, upgrade or configure svarog.
	 * @throws Exception If an error occurs.
	 **/
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		// if svarog is initialised just exit
		if (SvCore.isInitialized.get()) {
			System.err.println("Svarog is already initialized as library");
			System.exit(-1);
		} else
			// set svarog daemon flag, to prevent cleanup on its own.
			SvCore.svDaemonRunning.compareAndSet(false, true);

		// if the daemon was started without params, assume -dm to start the
		// daemon and enable integration with the Svarog Install
		if (args.length == 0)
			args = new String[] { "-dm" };

		// since the Daemon main should be a single point of entry, we simply
		// pass it to the install class. If the install class returns the daemon
		// status then no SvarogInstall operation was required
		int cmdLineStatus = SvarogInstall.main(args);
		if (cmdLineStatus != svarogDaemonStatus)
			System.exit(cmdLineStatus);

		// initalise Svarog
		if (!SvCore.initSvCore())
			System.exit(-1);

		// Load system properties.
		SvarogDaemon.loadSystemProperties();

		// Read configuration properties.
		final Map<String, Object> configProps = SvarogDaemon.loadConfigProperties();

		// Copy framework properties from the system properties.
		SvarogDaemon.copySystemProperties(configProps);

		try {

			List<SvActivator> list = new ArrayList<SvActivator>();
			list.add(new SvActivator());
			configProps.put(FelixConstants.SYSTEMBUNDLE_ACTIVATORS_PROP, list);

			osgiFramework = new Felix(configProps); // factory.newFramework(configProps);
			// Initialize the framework, but don't start it yet.
			osgiFramework.init();

			// track executors, executor groups and perun plugings
			String serviceFilter = "(|(objectClass=" + ISvExecutor.class.getName() + ")(|(objectClass="
					+ ISvExecutorGroup.class.getName() + ")(objectClass=" + IPerunPlugin.class.getName() + ")))";

			// Use the framework bundle context to register a service tracker
			// in order to track Svarog Executors coming and going in the system
			svcTracker = new ServiceTracker<Object, Object>(osgiFramework.getBundleContext(),
					osgiFramework.getBundleContext().createFilter(serviceFilter), new SvServiceTracker());
			svcTracker.open();

			// Use the system bundle context to process the auto-deploy
			// and auto-install/auto-start properties.
			log4j.info("Starting OSGI auto-deploy from directory:"
					+ configProps.get(AutoProcessor.AUTO_DEPLOY_DIR_PROPERTY));
			AutoProcessor.process(configProps, osgiFramework.getBundleContext());
			FrameworkEvent event = null;
			// Start the OSGI framework.
			osgiFramework.start();

			// Prepare the maintenance thread. If we are daemon we will run in
			// this thread and the osgi Framework will do wait. So we dont wait
			// and we set the current thread as maintenance
			SvMaintenance.setMaintenanceThread(Thread.currentThread());
			// Set the proxy to process the notifications it self
			SvClusterNotifierProxy.processNotification = true;
			// set the client to rejoin on failed beat
			SvClusterClient.rejoinOnFailedHeartBeat = true;

			boolean shutdown = false;
			while (!shutdown) {
				// Wait for framework to stop to exit the VM. Wait just enough
				// to run the cluster maintenance as well as tracked connection
				// cleanup.
				try {
					shutdown = false;
					long timeout = SvMaintenance.performMaintenance();
					event = osgiFramework.waitForStop(timeout);

					if (event.getType() == FrameworkEvent.STOPPED_UPDATE)
						osgiFramework.start();

					if (event.getType() != FrameworkEvent.WAIT_TIMEDOUT
							&& event.getType() != FrameworkEvent.STOPPED_UPDATE)
						shutdown = true;

				} catch (InterruptedException e) {
					// if our thread was interrupted, set the flag and back to
					// maintenance
					Thread.currentThread().interrupt();
				}

			}
			log4j.info("OSGI Framework stopped. Shutting down SvarogDaemon");
			// Otherwise, exit.
			System.exit(0);
		} catch (Exception ex) {
			log4j.info("Could not start OSGI framework!", ex);
			System.exit(-1);
		}
	}

	/**
	 * Simple method to parse META-INF/services file for framework factory.
	 * Currently, it assumes the first non-commented line is the class name of the
	 * framework factory implementation.
	 * 
	 * @return The created <tt>FrameworkFactory</tt> instance.
	 * @throws Exception if any errors occur.
	 **/
	static FrameworkFactory getFrameworkFactory() throws Exception {
		URL url = SvarogDaemon.class.getClassLoader()
				.getResource("META-INF/services/org.osgi.framework.launch.FrameworkFactory");
		if (url != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
			try {
				for (String s = br.readLine(); s != null; s = br.readLine()) {
					s = s.trim();
					// Try to load first non-empty, non-commented line.
					if ((s.length() > 0) && (s.charAt(0) != '#')) {
						return (FrameworkFactory) Class.forName(s).newInstance();
					}
				}
			} finally {
				if (br != null)
					br.close();
			}
		}

		throw new Exception("Could not find framework factory.");
	}

	/**
	 * <p>
	 * Loads the properties in the system property file associated with the
	 * framework installation into <tt>System.setProperty()</tt>. These properties
	 * are not directly used by the framework in anyway. By default, the system
	 * property file is located in the <tt>conf/</tt> directory of the Felix
	 * installation directory and is called "<tt>system.properties</tt>". The
	 * installation directory of Felix is assumed to be the parent directory of the
	 * <tt>felix.jar</tt> file as found on the system class path property. The
	 * precise file from which to load system properties can be set by initializing
	 * the "<tt>felix.system.properties</tt>" system property to an arbitrary URL.
	 * </p>
	 **/
	public static void loadSystemProperties() {
		// The system properties file is either specified by a system
		// property or it is in the same directory as the Felix JAR file.
		// Try to load it from one of these places.

		// See if the property URL was specified as a property.
		URL propURL = null;
		String custom = System.getProperty(SYSTEM_PROPERTIES_PROP);
		if (custom != null) {
			try {
				propURL = new URL(custom);
			} catch (MalformedURLException ex) {
				System.err.print("Main: " + ex);
				return;
			}
		} else {
			// Determine where the configuration directory is by figuring
			// out where felix.jar is located on the system class path.
			File confDir = null;
			String classpath = System.getProperty("java.class.path");
			int index = classpath.toLowerCase().indexOf("felix.jar");
			int start = classpath.lastIndexOf(File.pathSeparator, index) + 1;
			if (index >= start) {
				// Get the path of the felix.jar file.
				String jarLocation = classpath.substring(start, index);
				// Calculate the conf directory based on the parent
				// directory of the felix.jar directory.
				confDir = new File(new File(new File(jarLocation).getAbsolutePath()).getParent(), CONFIG_DIRECTORY);
			} else {
				// Can't figure it out so use the current directory as default.
				confDir = new File(System.getProperty("user.dir"), CONFIG_DIRECTORY);
			}

			try {
				propURL = new File(confDir, SYSTEM_PROPERTIES_FILE_VALUE).toURI().toURL();
			} catch (MalformedURLException ex) {
				System.err.print("Main: " + ex);
				return;
			}
		}

		// Read the properties file.
		Properties props = new Properties();
		InputStream is = null;
		try {
			is = propURL.openConnection().getInputStream();
			props.load(is);
			is.close();
		} catch (FileNotFoundException ex) {
			// Ignore file not found.
		} catch (Exception ex) {
			System.err.println("Main: Error loading system properties from " + propURL);
			System.err.println("Main: " + ex);
			try {
				if (is != null)
					is.close();
			} catch (IOException ex2) {
				// Nothing we can do.
			}
			return;
		}

		// Perform variable substitution on specified properties.
		for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements();) {
			String name = (String) e.nextElement();
			System.setProperty(name, Util.substVars(props.getProperty(name), name, null, null));
		}
	}

	/**
	 * <p>
	 * Loads the configuration properties in the configuration property file
	 * associated with the Svarog framework installation; these properties are
	 * accessible to the framework and to bundles and are intended for configuration
	 * purposes. By default, the configuration property file is located in the
	 * <tt>root</tt> directory of the Svarog installation directory and is called
	 * "<tt>svarog.properties</tt>". The installation directory of Svarog is assumed
	 * to be the parent directory of the <tt>svarog.jar</tt> file as found on the
	 * system class path property. The precise file from which to load configuration
	 * properties can be set by initializing the "<tt>felix.config.properties</tt>"
	 * system property to an arbitrary URL.
	 * </p>
	 * 
	 * @return A <tt>Properties</tt> instance or <tt>null</tt> if there was an
	 *         error.
	 **/
	public static Map<String, Object> loadConfigProperties() {
		// The config properties file is share with Svarog, so all configuration
		// is in svarog.properties.

		Properties props = SvConf.config;
		// Perform variable substitution for system properties and
		// convert to dictionary.
		Map<String, Object> map = new HashMap<String, Object>();
		for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements();) {
			String name = (String) e.nextElement();
			map.put(name, Util.substVars(props.getProperty(name), name, null, props));
		}

		return map;
	}

	public static void copySystemProperties(Map<String, Object> configProps) {
		for (Enumeration<?> e = System.getProperties().propertyNames(); e.hasMoreElements();) {
			String key = (String) e.nextElement();
			if (key.startsWith("felix.") || key.startsWith("org.osgi.framework.")) {
				configProps.put(key, System.getProperty(key));
			}
		}
	}

}
