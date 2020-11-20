package com.prtech.svarog;

import com.prtech.svarog_common.SvCharId;

/**
 * Class holding the constants
 * 
 * @author ristepejov
 *
 */
public class Sv {

	private Sv() {
	};

	public static final String Y2K_START_DATE = "2000-01-01T00:00:00";
	public static final String CODE_LIST_ID = "CODE_LIST_ID";
	public static final SvCharId LABEL_CODE = new SvCharId("LABEL_CODE");
	public static final String VERSION = "VERSION";
	public static final String SORT_ORDER = "SORT_ORDER";
	public static final SvCharId TABLE_NAME = new SvCharId("table_name");
	public static final String PARENT = "PARENT";
	public static final String MASTER_REPO = "master_repo";
	public static final String REPO_TABLE_NAME = "{REPO_TABLE_NAME}";
	public static final String MASTER_REPO_NAME = "{" + MASTER_REPO.toUpperCase() + "}";
	public static final String DEFAULT_SCHEMA = "{DEFAULT_SCHEMA}";
	public static final String EXECUTOR_KEY = "EXECUTOR_KEY";
	public static final String NOTES = "NOTES";
	public static final String GUI_METADATA = "GUI_METADATA";
	public static final String EXTENDED_PARAMS = "EXTENDED_PARAMS";
	public static final SvCharId FIELD_NAME = new SvCharId("FIELD_NAME");
	public static final String FIELDS = "FIELDS";
	public static final String DOT = ".";

	public static final String TABLE_META_PKID = "table_meta_pkid";
	public static final String LABEL_CODE_LC = "label_code";
	public static final String IS_VISIBLE_UI = "is_visible_ui";
	public static final SvCharId LINK_TYPE = new SvCharId("link_type");
	public static final String LINK_TYPE_DESCRIPTION = "link_type_description";
	public static final String E_MAIL = "e_mail";
	public static final String ACL_OBJECT_ID = "acl_object_id";
	public static final String ACL_OBJECT_TYPE = "acl_object_type";
	public static final String LOCALE_ID = "locale_id";
	public static final String LABEL_TEXT = "label_text";
	public static final String CACHE_TYPE = "cache_type";
	public static final String BOUNDS_CLASS = "BOUNDS_CLASS";
	public static final String CENTROID = "CENTROID";
	public static final String GEOMETRY = "GEOMETRY";
	public static final String BOUNDS_AREA_HA = "bounds_area_ha";
	public static final String SQL_NULL = "NULL";
	public static final String EMPTY_STRING = "";
	public static final String VALUE = "VALUE";
	public static final String NVARCHAR = "NVARCHAR";
	public static final String DATA_TYPE = "DATA_TYPE";
	public static final String PARAM_TYPE_ID = "PARAM_TYPE_ID";
	public static final String PARAM_ORDER = "PARAM_ORDER";
	public static final String TEXT_AREA = "TEXT_AREA";
	public static final String INPUT_TYPE = "INPUT_TYPE";
	public static final String SID_OBJECT_ID = "sid_object_id";
	public static final String SID_TYPE_ID = "sid_type_id";
	public static final String ACL_LABEL_CODE = "ACL_LABEL_CODE";
	public static final String STATUS = "status";
	public static final String GROUP_NAME = "group_name";
	public static final String ADMIN = "ADMIN";
	public static final String ACLS = "ACLS";
	public static final String FIRST_NAME = "first_name";

	public static final SvCharId FIELD_TYPE = new SvCharId("FIELD_TYPE");
	public static final SvCharId ACCESS_TYPE = new SvCharId("ACCESS_TYPE");
	public static final SvCharId CONFIG_UNQ_ID = new SvCharId("CONFIG_UNQ_ID");
	public static final SvCharId FIELD_SCALE = new SvCharId("FIELD_SCALE");
	public static final SvCharId SV_MULTISELECT = new SvCharId("SV_MULTISELECT");
	public static final SvCharId REPO_NAME = new SvCharId("REPO_NAME");

	public static final String FILE = "File";
	public static final String COLON = ":";
	public static final String CLASS = "Class";
	public static final String SEMICOLON = ";";
	public static final String METHOD = "Method";
	public static final String LINE = "Line";
	public static final String DEBUG_INFO = "setDebugInfo";
	public static final String STACK_TRACE = "getStackTrace";
	public static final String INIT = "<init>";
	public static final SvCharId SCHEMA = new SvCharId("SCHEMA");
	public static final String FIELD = "FIELD";
	public static final String LINK = "LINK";
	public static final SvCharId CONFIG_TYPE_ID = new SvCharId("config_type_id");
	public static final SvCharId REPO_TABLE = new SvCharId("REPO_TABLE");
	public static final String POA = "POA";
	public static final SvCharId LINK_OBJ_TYPE_1 = new SvCharId("link_obj_type_1");
	public static final SvCharId LINK_OBJ_TYPE_2 = new SvCharId("link_obj_type_2");
	public static final String V = "V";
	public static final SvCharId LAST_REFRESH = new SvCharId("last_refresh");
	public static final String USER_DEFAULT_GROUP = "USER_DEFAULT_GROUP";
	public static final String USER_GROUP = "USER_GROUP";
	public static final String OBJECT_ID = "OBJECT_ID";
	public static final SvCharId GROUP_SECURITY_TYPE = new SvCharId("GROUP_SECURITY_TYPE");
	public static final String SPACE = " ";
	public static final String AND = "AND";
	public static final String ORDER_BY = "ORDER BY";
	public static final String LIMIT_OFFSET = "LIMIT_OFFSET";
	public static final String bracketOFFSET = "{OFFSET}";
	public static final String bracketLIMIT= "{LIMIT}";
	public static final String OR = "OR";
	public static final SvCharId USER_NAME = new SvCharId("USER_NAME");
	public static final SvCharId ACL_CONFIG_UNQ = new SvCharId("acl_config_unq");
	public static final String LOCALE = "LOCALE";
	public static final String PKID = "PKID";
	public static final SvCharId SV_ISLABEL = new SvCharId("SV_ISLABEL");
	public static final SvCharId SV_LOADLABEL = new SvCharId("SV_LOADLABEL");
	public static final String BOOLEAN = "BOOLEAN";
	public static final String TEXT = "TEXT";
	public static final String ZERO = "0";
	public static final SvCharId CONFIG_RELATION_TYPE =new SvCharId("CONFIG_RELATION_TYPE");
	public static final SvCharId CONFIG_RELATION = new SvCharId("CONFIG_RELATION");
	public static final SvCharId CONFIG_TYPE = new SvCharId("CONFIG_TYPE");
	public static final SvCharId IS_CONFIG_TABLE = new SvCharId("is_config_table");
	public static final String TBL = "TBL";

}
