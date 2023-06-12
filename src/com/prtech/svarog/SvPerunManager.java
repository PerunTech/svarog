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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.prtech.svarog_common.DbDataArray;
import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_common.DbQueryObject;
import com.prtech.svarog_common.DbSearch.DbLogicOperand;
import com.prtech.svarog_common.DbSearchCriterion;
import com.prtech.svarog_common.DbSearchCriterion.DbCompareOperand;
import com.prtech.svarog_common.DbSearchExpression;
import com.prtech.svarog_common.ISvOnSave;
import com.prtech.svarog_interfaces.IPerunPlugin;
import com.prtech.svarog_interfaces.ISvCore;
import com.prtech.svarog_interfaces.ISvExecutorGroup;

/**
 * Svarog Execution Manager class is inherited from the SvCore and extends it
 * with interfacing the OSGI framework to find a list of services which
 * implement the ISvExecutor interface
 * 
 * @author ristepejov
 *
 */
public class SvPerunManager extends SvCore {
	static final String CONTEXT_NAME = "CONTEXT_NAME";
	static final String MENU_CONF = "MENU_CONF";
	static final String CONTEXT_MENU_CONF = "CONTEXT_MENU_CONF";

	static final List<IPerunPlugin> pendingRegistration = new CopyOnWriteArrayList<IPerunPlugin>();

	/**
	 * Callback class to refresh the plugin according to changes in Svarog database
	 * 
	 * @author XPS13
	 *
	 */
	static public class SvPerunCallback implements ISvOnSave {

		@Override
		public boolean beforeSave(SvCore parentCore, DbDataObject dbo) {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public void afterSave(SvCore parentCore, DbDataObject dbo) {
			String name = (String) dbo.getVal(CONTEXT_NAME);
			if (name != null) {
				SvPerunInstance inst = pluginMap.get(name);
				if (inst != null) {
					SvPerunInstance newInstance;
					try {
						newInstance = new SvPerunInstance(inst.getPlugin(), dbo);
						pluginMap.put(newInstance.getPlugin().getContextName(), newInstance);
					} catch (SvException e) {
						log4j.error("Error loading plugin:" + name, e);
					}

				}
			}
		}

	}

	/**
	 * Global init block to set the call back for plugin refresh from the database.
	 */
	static {
		ISvOnSave callback = new SvPerunCallback();
		SvCore.registerOnSaveCallback(callback, svCONST.OBJECT_TYPE_PERUN_PLUGIN);
	}

	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvPerunManager.class.getName());

	/**
	 * Cache with all executors responding to a specific command string. The Key is
	 * represents a CATEGORY.NAME
	 */
	static ConcurrentHashMap<String, SvPerunInstance> pluginMap = new ConcurrentHashMap<String, SvPerunInstance>();

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id String UID of the user session under which the SvCore
	 *                   instance will run
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvPerunManager(String session_id) throws SvException {
		super(session_id);
	}

	/**
	 * Constructor to create a SvUtil object according to a user session. This is
	 * the default constructor available to the public, in order to enforce the
	 * svarog security mechanisms based on the logged on user.
	 * 
	 * @param session_id   String UID of the user session under which the SvCore
	 *                     instance will run
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvPerunManager(String session_id, SvCore sharedSvCore) throws SvException {
		super(session_id, sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @param sharedSvCore The SvCore instance which will be used for JDBC
	 *                     connection sharing (i.e. parent SvCore)
	 * @throws SvException Pass through exception from the super class constructor
	 */
	public SvPerunManager(SvCore sharedSvCore) throws SvException {
		super(sharedSvCore);
	}

	/**
	 * Default Constructor. This constructor can be used only within the svarog
	 * package since it will run with system priveleges.
	 * 
	 * @throws SvException Pass through exception from the super class constructor
	 */
	SvPerunManager() throws SvException {
		super();
	}

	/**
	 * Method to invalidate a plugin entry in the system map
	 * 
	 * @param name
	 */
	static void removePlugin(String name) {
		synchronized (SvPerunManager.class) {
			if (pluginMap != null)
				pluginMap.remove(name);
		}
	}

	/**
	 * Method to add a plugin instance to the plugin map
	 * 
	 * @param name
	 */
	static void addPlugin(IPerunPlugin plugin) {
		synchronized (SvPerunManager.class) {
			// if the cluster is active we just register the plugin, if it isn't we add it
			// to the pending list
			if (SvCluster.getIsActive().get()) {
				List<IPerunPlugin> plugins = new ArrayList<IPerunPlugin>();
				plugins.add(plugin);
				reloadPluginInstances(plugins);
			} else {
				if (!pendingRegistration.contains(plugin))
					pendingRegistration.add(plugin);
			}
		}
	}

	/**
	 * Method to refresh/reload a plugin instance to the plugin map
	 * 
	 * @param name
	 */
	static void reloadPlugin(IPerunPlugin plugin) {
		synchronized (SvPerunManager.class) {
			if (SvCluster.getIsActive().get()) {
				List<IPerunPlugin> plugins = new ArrayList<IPerunPlugin>();
				plugins.add(plugin);
				reloadPluginInstances(plugins);
			} else {
				if (!pendingRegistration.contains(plugin))
					pendingRegistration.add(plugin);
			}

		}
	}

	/**
	 * Method to refresh/reload a plugin instance to the plugin map
	 * 
	 * @param name
	 */
	static void registerPendingPlugins() {
		if (SvCluster.getIsActive().get() && pendingRegistration.size() > 0) {
			synchronized (SvPerunManager.class) {
				for (IPerunPlugin p : pendingRegistration) {
					List<IPerunPlugin> plugins = new ArrayList<IPerunPlugin>();
					plugins.add(p);
					reloadPluginInstances(plugins);
				}
				pendingRegistration.clear();
			}
		}
	}

	/**
	 * Method to get a plugin instance by context name
	 * 
	 * @param context the http context name assigned to the plugin
	 * @return The plugin instance
	 */
	SvPerunInstance getPlugin(String context) {
		SvPerunInstance instance = pluginMap.get(context);
		if (this.hasPermission(instance.getPermissionCode()))
			return instance;
		else
			return null;

	}

	/**
	 * Method to build a DbDataObject instance from a IPerunPlugin class
	 * 
	 * @param plugin The pluing instance
	 * @return Populated DbDataObject
	 */
	@SuppressWarnings("unchecked")
	static DbDataObject buildDboPlugin(IPerunPlugin plugin) {
		DbDataObject dboPlugin = new DbDataObject(svCONST.OBJECT_TYPE_PERUN_PLUGIN);

		ISvCore svc = null;
		try {
			svc = new SvReader();
			JsonObject jso = plugin.getMenu((JsonObject) null, svc);
			dboPlugin.setVal(MENU_CONF, jso);
			JsonObject jsoContext = plugin.getContextMenu((HashMap) null, (JsonObject) null, svc);
			dboPlugin.setVal(CONTEXT_MENU_CONF, jsoContext);
			dboPlugin.setVal("PERMISSION_CODE", plugin.getPermissionCode());
			dboPlugin.setVal("LABEL_CODE", plugin.getLabelCode());
			dboPlugin.setVal("IMG_PATH", plugin.getIconPath());
			dboPlugin.setVal("JAVASCRIPT_PATH", plugin.getJsPluginUrl());
			dboPlugin.setVal("SORT_ORDER", plugin.getSortOrder());
			dboPlugin.setVal("VERSION", plugin.getVersion());
			dboPlugin.setVal(CONTEXT_NAME, plugin.getContextName());
			dboPlugin.setStatus(svCONST.STATUS_VALID);

		} catch (SvException e) {
			log4j.error("Error generating perun pluing structure", e);
		} finally {
			if (svc != null)
				svc.release();
		}
		return dboPlugin;
	}

	/**
	 * Method which initialises the plugin metadata from the database. This part is
	 * important to support the changing certain data of the plugin by the admin
	 * user in the database in order override the dates coming from the plugin
	 * itself.
	 * 
	 * @param plugins List of plugins available in the OSGI
	 * @return SvExecInstance object to be stored in the execution cache
	 */
	private static void reloadPluginInstances(List<IPerunPlugin> plugins) {
		String unqNameField = CONTEXT_NAME;
		try (SvReader svr = new SvReader(); SvWriter svw = new SvWriter(svr);) {
			svr.switchUser(svCONST.serviceUser);
			svw.isInternal = true;
			svw.setAutoCommit(false);

			// add all plugin names in one big OR list
			DbSearchExpression search = new DbSearchExpression();
			for (IPerunPlugin plugin : plugins) {
				search.addDbSearchItem(new DbSearchCriterion(unqNameField, DbCompareOperand.EQUAL,
						plugin.getContextName(), DbLogicOperand.OR));
			}

			DbQueryObject dqo = new DbQueryObject(SvCore.getDbt(svCONST.OBJECT_TYPE_PERUN_PLUGIN), search, null, null);
			DbDataArray dboPlugins = svr.getObjects(dqo, 0, 0);
			// build gui meta data for each
			buildGuiMetaData(dboPlugins);
			// switch to service user in order to be able to manage permissions
			dboPlugins.rebuildIndex(unqNameField, true);

			DbDataArray upgradedList = configurePlugins(plugins, dboPlugins);
			svw.saveObject(upgradedList);
			svw.dbCommit();
		} catch (SvException e) {
			log4j.error("Error registering list of plugins", e);

		}
	}

	private static void buildGuiMetaData(DbDataArray dboPlugins) {
		for (DbDataObject dbo : dboPlugins.getItems()) {
			String strGuiMeta = (String) dbo.getVal(Sv.GUI_METADATA);
			if (strGuiMeta != null) {
				Gson g = new Gson();
				try {
					JsonObject guiMeta = g.fromJson(strGuiMeta, JsonObject.class);
					dbo.setVal(Sv.GUI_METADATA, guiMeta);
				} catch (Exception e) {
					dbo.setVal(Sv.GUI_METADATA, null);
					log4j.warn("Can't parse gui metadata for object:" + dbo.toJson().toString(), e);
				}
			}
		}

	}

	/**
	 * Method to math the plugin instances loaded in the OSGI container with the
	 * database configuration and reconfigure if needed.
	 * 
	 * @param plugins    The list of pluings loaded in the OSGI
	 * @param dboPlugins The list of database objects configured in the database
	 * @return List of database objects which should be used for upgrade of the
	 *         database
	 */
	static DbDataArray configurePlugins(List<IPerunPlugin> plugins, DbDataArray dboPlugins) {
		DbDataArray upgradedList = new DbDataArray();

		for (IPerunPlugin plugin : plugins) {
			try {
				DbDataObject pluginDbo = dboPlugins.getItemByIdx(plugin.getContextName());
				// get the new version of the descriptor
				DbDataObject newVersion = buildDboPlugin(plugin);
				if (pluginDbo != null) {
					if (plugin.getVersion() > (Long.valueOf(pluginDbo.getVal("VERSION").toString())).intValue()) {
						// if we should keep the old menu, copy from old
						if (!plugin.replaceMenuOnNew())
							newVersion.setVal(MENU_CONF, pluginDbo.getVal(MENU_CONF));
						// if we should keep the old context menu, copy from old
						if (!plugin.replaceContextMenuOnNew())
							newVersion.setVal(CONTEXT_MENU_CONF, pluginDbo.getVal(CONTEXT_MENU_CONF));
						newVersion.setPkid(pluginDbo.getPkid());
						newVersion.setObjectId(pluginDbo.getObjectId());
						if (pluginDbo.getVal(Sv.GUI_METADATA) != null)
							newVersion.setVal(Sv.GUI_METADATA, pluginDbo.getVal(Sv.GUI_METADATA));
						pluginDbo = newVersion;
						upgradedList.addDataItem(pluginDbo);
					}

				} else {
					pluginDbo = newVersion;
					upgradedList.addDataItem(pluginDbo);
				}
				SvPerunInstance inst = new SvPerunInstance(plugin, pluginDbo);
				pluginMap.put(plugin.getContextName(), inst);
			} catch (Exception e) {
				log4j.error("Failed to register a Perun Pluging on following context:"
						+ (plugin.getContextName() != null ? plugin.getContextName() : "NO_CONTEXT"), e);
			}

		}
		return upgradedList;
	}

	/**
	 * Method to return perun plugins sorted by sort order
	 */
	public List<Map.Entry<String, SvPerunInstance>> getPerunPlugins() {
		// sort the map
		List<Map.Entry<String, SvPerunInstance>> entries = new ArrayList<Map.Entry<String, SvPerunInstance>>(
				pluginMap.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<String, SvPerunInstance>>() {
			public int compare(Map.Entry<String, SvPerunInstance> a, Map.Entry<String, SvPerunInstance> b) {
				return Integer.compare(a.getValue().getSortOrder(), b.getValue().getSortOrder());
			};
		});
		// check the permissions if not have, remove
		Iterator<Map.Entry<String, SvPerunInstance>> iter = entries.iterator();
		while (iter.hasNext()) {
			Map.Entry<String, SvPerunInstance> instance = iter.next();
			if (!this.hasPermission(instance.getValue().getPermissionCode())
					|| !instance.getValue().getStatus().equals(svCONST.STATUS_VALID))
				iter.remove();
		}
		return entries;
	}

	/**
	 * Method to get the main plugin menu based on the authorisations
	 * 
	 * @param context the context name for which we want to get the menu
	 * @return JsonObject containing the menu of the module
	 */
	public JsonObject getMenu(String context) {
		SvPerunInstance instance = pluginMap.get(context);
		if (this.hasPermission(instance.getPermissionCode()))
			return instance.getMainMenu(this);
		else
			return null;

	}

	/**
	 * Method to get the plugin context menu based on the authorisations and context
	 * 
	 * @param context    the context name for which we want to get the menu
	 * @param contextMap the context parameters to parameterise the menu
	 * @return JsonObject containing the menu of the module
	 */
	public JsonObject getContextMenu(String context, HashMap<String, String> contextMap) {
		SvPerunInstance instance = pluginMap.get(context);
		if (this.hasPermission(instance.getPermissionCode()))
			return instance.getContextMenu(contextMap, this);
		else
			return null;
	}

}
