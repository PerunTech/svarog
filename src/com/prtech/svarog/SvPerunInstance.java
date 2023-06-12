package com.prtech.svarog;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.prtech.svarog_common.DbDataObject;
import com.prtech.svarog_interfaces.IPerunPlugin;
import com.prtech.svarog_interfaces.ISvCore;

/**
 * Internal SvPerunInstance class to wrap the PerunPlugin in order to allow
 * changing of the default start and end dates coming from the executor
 * itself.The real start and end dates are provided by configuration
 */

public class SvPerunInstance {
	/**
	 * Log4j instance used for logging
	 */
	private static final Logger log4j = LogManager.getLogger(SvPerunInstance.class.getName());

	String status;
	JsonObject mainMenu;
	JsonObject contextMenu;
	String permissionCode;
	String labelCode;
	String imgPath;
	String jsPath;
	int sortOrder;
	IPerunPlugin plugin;
	DbDataObject dboPlugin;

	JsonObject setMenuJson(DbDataObject dboPlugin, String confFieldName) throws SvException {
		Gson g = new Gson();
		JsonObject menu = null;
		if (dboPlugin.getVal(confFieldName) != null) {
			if (dboPlugin.getVal(confFieldName) instanceof JsonObject)
				menu = (JsonObject) dboPlugin.getVal(confFieldName);
			else if (dboPlugin.getVal(confFieldName) instanceof String) {
				String menuStr = (String) dboPlugin.getVal(confFieldName);
				try {
					menu = g.fromJson(menuStr, JsonObject.class);
				} catch (Exception e) {
					log4j.error("Can't parse context menu config:" + menuStr, e);
				}
			} else
				log4j.error("Can't parse context menu config");
		} else {
			try (SvReader svr = new SvReader()) {
				if (confFieldName.equals("MENU_CONF"))
					menu = plugin.getMenu((JsonObject) null, svr);
				else
					menu = plugin.getContextMenu((HashMap) null, (JsonObject) null, svr);
			}
		}
		return menu;
	}

	/**
	 * Constructor to set a
	 * 
	 * @param plugin
	 * @param dboPlugin
	 * @throws SvException 
	 */
	SvPerunInstance(IPerunPlugin plugin, DbDataObject dboPlugin) throws SvException {
		if (plugin == null || dboPlugin == null)
			throw new NullPointerException("Plugin can't be null");
		this.status = dboPlugin.getStatus();
		this.plugin = plugin;

		this.mainMenu = setMenuJson(dboPlugin, "MENU_CONF");
		this.contextMenu = setMenuJson(dboPlugin, "CONTEXT_MENU_CONF");

		this.permissionCode = (String) (dboPlugin.getVal("PERMISSION_CODE") != null
				? dboPlugin.getVal("PERMISSION_CODE")
				: plugin.getPermissionCode());
		this.labelCode = (String) (dboPlugin.getVal("LABEL_CODE") != null ? dboPlugin.getVal("LABEL_CODE")
				: plugin.getLabelCode());
		this.imgPath = (String) (dboPlugin.getVal("IMG_PATH") != null ? dboPlugin.getVal("IMG_PATH")
				: plugin.getIconPath());
		this.jsPath = (String) (dboPlugin.getVal("JAVASCRIPT_PATH") != null ? dboPlugin.getVal("JAVASCRIPT_PATH")
				: plugin.getJsPluginUrl());
		this.sortOrder = (dboPlugin.getVal("SORT_ORDER") != null
				? (Long.valueOf(dboPlugin.getVal("SORT_ORDER").toString())).intValue()
				: plugin.getSortOrder());
		this.plugin = plugin;
		this.dboPlugin = dboPlugin;

	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		if (this.plugin == null) {
			return false;
		}
		final SvPerunInstance other = (SvPerunInstance) obj;
		if (other.plugin == null) {
			return false;
		}

		if (!this.plugin.getContextName().equals(other.plugin.getContextName())) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		if (this.plugin != null) {
			hash = this.plugin.hashCode();
		}
		return hash;
	}

	public JsonObject getMainMenu(ISvCore core) {
		return plugin.getMenu(mainMenu, core);
	}

	public JsonObject getContextMenu(HashMap<String, String> contextMap, ISvCore core) {
		return plugin.getContextMenu(contextMap, contextMenu, core);
	}

	public String getPermissionCode() {
		return permissionCode;
	}

	public String getLabelCode() {
		return labelCode;
	}

	public String getImgPath() {
		return imgPath;
	}

	public String getJsPath() {
		return jsPath;
	}

	public int getSortOrder() {
		return sortOrder;
	}

	public IPerunPlugin getPlugin() {
		return plugin;
	}

	public String getStatus() {
		return status;
	}

	public DbDataObject getDboPlugin() {
		return dboPlugin;
	}
}
