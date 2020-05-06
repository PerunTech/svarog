package com.prtech.svarog;

import java.util.HashMap;

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

	String status;
	JsonObject mainMenu;
	JsonObject contextMenu;
	String permissionCode;
	String labelCode;
	String imgPath;
	String jsPath;
	int sortOrder;
	IPerunPlugin plugin;

	SvPerunInstance(IPerunPlugin plugin, DbDataObject dboPlugin) {
		if (plugin == null || dboPlugin == null)
			throw new NullPointerException("Plugin can't be null");
		this.status = dboPlugin.getStatus();
		Gson g = new Gson();
		if (dboPlugin.getVal("MENU_CONF") != null) {
			String menuStr = (String) dboPlugin.getVal("MENU_CONF");
			this.mainMenu = g.fromJson(menuStr, JsonObject.class);
		} else
			this.mainMenu = plugin.getMenu((JsonObject) null, null);

		if (dboPlugin.getVal("CONTEXT_MENU_CONF") != null) {
			String menuStr = (String) dboPlugin.getVal("CONTEXT_MENU_CONF");
			this.contextMenu = g.fromJson(menuStr, JsonObject.class);
		} else
			this.contextMenu = plugin.getContextMenu((HashMap) null, (JsonObject) null, null);

		this.permissionCode = (String) (dboPlugin.getVal("PERMISSION_CODE") != null
				? dboPlugin.getVal("PERMISSION_CODE") : plugin.getPermissionCode());
		this.labelCode = (String) (dboPlugin.getVal("LABEL_CODE") != null ? dboPlugin.getVal("LABEL_CODE")
				: plugin.getLabelCode());
		this.imgPath = (String) (dboPlugin.getVal("IMG_PATH") != null ? dboPlugin.getVal("IMG_PATH")
				: plugin.getIconPath());
		this.jsPath = (String) (dboPlugin.getVal("JAVASCRIPT_PATH") != null ? dboPlugin.getVal("JAVASCRIPT_PATH")
				: plugin.getJsPluginUrl());
		this.sortOrder = (int) (dboPlugin.getVal("SORT_ORDER") != null ? dboPlugin.getVal("SORT_ORDER")
				: plugin.getSortOrder());
		this.plugin = plugin;

	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (!SvPerunInstance.class.isAssignableFrom(obj.getClass())) {
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

}
