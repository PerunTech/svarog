package com.prtech.svarog;

import org.joda.time.DateTime;

import com.prtech.svarog_common.SvCharId;

/**
 * Class holding the constants
 * 
 * @author ristepejov
 *
 */
public class Sv {
	public static class Exceptions {

		/**
		 * Different exception type constants
		 */
		public static final String SDI_SHORTSPIKE_DETECTED = "system.error.sdi.shortspike_detected";
		public static final String SDI_SPIKE_DETECTED = "system.error.sdi.spike_detected";
		public static final String SDI_SPIKE_FIX_FAILED = "system.error.sdi.spike_fix_fail";
		public static final String SDI_VERTEX_DISTANCE_ERR = "system.error.sdi.vertex_min_dist_err";
		public static final String SDI_GEOM_DISTANCE_ERR = "system.error.sdi.geom_min_dist_err";
		public static final String SDI_VERTEX_DIST_FIX_FAILED = "system.error.sdi.vertex_dist_fix_fail";
		public static final String SDI_MULTIPLE_GEOMS_FOUND = "system.error.sdi.multi_geoms_found";
		public static final String SDI_MERGE_REQUIRES_2PLUS = "system.error.sdi.merge_requires_2plus";
		public static final String SDI_MERGE_GEOM_EMPTY = "system.error.sdi.geometry_empty";
		public static final String SDI_MERGE_GEOM_DISJOINT = "system.error.sdi.geometries_disjoint";
		public static final String SQL_ERR = "system.error.sql_err";
		public static final String NULL_OBJECT = "system.error.null_object";
		public static final String MULTI_TYPES_NOT_ALLOWED = "system.error.multi_types_not_allowed";
		public static final String OBJECT_NOT_PERSISTENT = "system.error.object_not_persistent";
		public static final String OBJECT_NOT_FOUND = "system.error.object_not_found";
		public static final String OBJECT_COUNT_ERROR = "system.error.wrong_row_count_update";
		public static final String OBJECT_NOT_UPDATEABLE = "system.error.obj_not_updateable";
		public static final String EMPTY_GRID = "system.error.gis_grid_empty";
		public static final String CLUSTER_INACTIVE = "system.error.cluster_inactive";
		public static final String CLUSTER_COMMUNICATION_ERROR = "system.error.cluster_socket_error";
		public static final String IP_BLOCKED = "system.error.ip_blocked";
		public static final String INVALID_LINK_TYPE = "system.error.invalid_link_type";
		public static final String FILEDATA_FS_ERROR = "system.error.filedata_fs_err";
		public static final String FILESAVE_DB_ERROR = "system.error.filesave_db_err";
		public static final String FILESTORE_DOESNT_EXIST = "system.error.filestore_doesnt_exist";
		public static final String FILE_DB_ERROR = "system.error.files_db_err";
		public static final String EMPTY_FILE_SAVE = "system.error.cant_save_empty_file";
		public static final String FILESAVE_TYPE_ERROR = "system.error.filedata_type_err";
		public static final String NO_DBT_FOUND = "system.error.no_dbt_found";
		public static final String BAD_JDBC_HANDLER = "system.error.jdbc_bad_database_handler";
		public static final String JDBC_CANT_RELEASE = "system.error.jdbc_cant_release";
		public static final String REPOSAVE_ERR = "system.error.reposave_err";
		public static final String BATCH_SIZE_ERR = "system.error.batch_size_err";
		public static final String SDI_TYPE_LIMIT = "system.error.sdi.sdi_type_limit";
		public static final String NOT_AUTHORISED = "system.error.not_authorised";

		private Exceptions() {
		}

	}
	public static class SQL {

		public static final String SEQ_NEXTVAL_SELECT = "SEQ_NEXTVAL_SELECT";
		public static final String SEQUENCE_NAME = "SEQUENCE_NAME";
		public static final String SEQ_NEXTVAL = "SEQ_NEXTVAL";
		public static final String NULL = "NULL";
		public static final String SELECT_FILESTORE = "select pkid, data from %s.%s where pkid=?";
		public static final String SQL_DEBUG = "SQL QUERY DEBUG: ";
		public static final String INSERT_FILESTORE = "insert into %s.%s (pkid, data) values(%s,?)";
		public static final String GEOMETRY_STRUCT_TYPE = "GEOMETRY_STRUCT_TYPE";
		
	}

	public static class Link {

		/**
		 * Different exception type constants
		 */
		public static final SvCharId LINK_TYPE = new SvCharId("link_type");
		public static final String LINK_TYPE_DESCRIPTION = "link_type_description";
		public static final String LINK = "LINK";
		public static final SvCharId LINK_OBJ_TYPE_1 = new SvCharId("link_obj_type_1");
		public static final SvCharId LINK_OBJ_TYPE_2 = new SvCharId("link_obj_type_2");
		public static final SvCharId DEFER_SECURITY = new SvCharId("DEFER_SECURITY");

		private Link() {
		}

	}

	public static class Security {

		/**
		 * Different exception type constants
		 */

		public static final SvCharId FIRST_REQUEST_TIME = new SvCharId("FIRST_REQUEST_TIME");
		public static final SvCharId REQUEST_COUNT = new SvCharId("REQUEST_COUNT");
		public static final SvCharId BLOCKED_UNTIL = new SvCharId("BLOCKED_UNTIL");
		public static final SvCharId BLOCKED_COUNT = new SvCharId("BLOCKED_COUNT");

		private Security() {
		}

	}

	private Sv() {
	}

	public static final DateTime Y2K_START_DATE = new DateTime("2000-01-01T00:00:00");
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
	public static final SvCharId CONFIG_TYPE_ID = new SvCharId("config_type_id");
	public static final SvCharId REPO_TABLE = new SvCharId("REPO_TABLE");
	public static final String POA = "POA";
	public static final String POA_OU = "POA_OU";
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
	public static final String OFFSET = "{OFFSET}";
	public static final String LIMIT = "{LIMIT}";
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
	public static final SvCharId CONFIG_RELATION_TYPE = new SvCharId("CONFIG_RELATION_TYPE");
	public static final SvCharId CONFIG_RELATION = new SvCharId("CONFIG_RELATION");
	public static final SvCharId CONFIG_TYPE = new SvCharId("CONFIG_TYPE");
	public static final SvCharId IS_CONFIG_TABLE = new SvCharId("is_config_table");
	public static final String TBL = "TBL";
	public static final String PARAM_NAME = "PARAM_NAME";
	public static final String PARAM_VALUE = "PARAM_VALUE";
	public static final long DEFAULT_CACHE_SIZE = 5000;
	public static final long DEFAULT_CACHE_TTL = 10;
	public static final String SYS_PARAMS = "SYS_PARAMS";
	public static final String USER_PARAMS = "USER_PARAMS";
	public static final String USCORE = "_";
	public static final String CONFIG_FILENAME = "svarog.properties";
	public static final String LRU = "LRU";
	public static final String LRU_TTL = "LRU_TTL";
	public static final String TTL = "TTL";
	public static final String CODE_VALUE = "CODE_VALUE";
	public static final String TABLE = "TABLE";
	public static final String CACHE_SIZE = "CACHE_SIZE";
	public static final String CACHE_EXPIRY = "CACHE_EXPIRY";
	public static final String SDI_SYSTEM_BOUNDARY = "SDI_SYSTEM_BOUNDARY";
	public static final String ENVELOPE = "ENVELOPE";
	public static final String PERM = "PERM";
	public static final String GEOM = "GEOM";
	public static final String PARAM_TYPE = "PARAM_TYPE";
	public static final String SDI_SPIKE_MAX_ANGLE = "SDI_SPIKE_MAX_ANGLE";
	public static final Double DEFAULT_SPIKE_MAX_ANGLE = 0.0;
	/**
	 * Code of system grid. This is the default grid used for the SDI subsystem
	 */
	public static final String SDI_SYSGRID = "SDI_SYSGRID";
	public static final String SDI_MIN_POINT_DISTANCE = "SDI_MIN_POINT_DISTANCE";
	public static final Double DEFAULT_MIN_POINT_DISTANCE = 0.05;

	public static final String SDI_VERTEX_ALIGN_TOLERANCE = "SDI_VERTEX_ALIGN_TOLERANCE";
	public static final Double DEFAULT_VERTEX_ALIGN_TOLERANCE = 0.05;
	
	public static final String SDI_MIN_GEOM_DISTANCE = "SDI_MIN_GEOM_DISTANCE";
	public static final Integer DEFAULT_MIN_GEOM_DISTANCE = 0;
	public static final String ERROR = "ERROR:";
	public static final String USER_NAME_LABEL = "User name ";
	public static final String CANNOT = " can not be ";
	public static final String SDI_GRID = "SDI_GRID";
	public static final String AREA = "AREA";
	public static final String STRING_CAST = "STRING_CAST";
	public static final String OBJECT_QUALIFIER_LEFT = "OBJECT_QUALIFIER_LEFT";
	public static final String OBJECT_QUALIFIER_RIGHT = "OBJECT_QUALIFIER_RIGHT";
	public static final String STRING_CONCAT = "STRING_CONCAT";
	public static final SvCharId IS_NULL = new SvCharId("IS_NULL");
	public static final String PARENT_ID = "PARENT_ID";
	public static final SvCharId UNQ_LEVEL = new SvCharId("UNQ_LEVEL");
	public static final String PARENT_CODE_VALUE = "PARENT_CODE_VALUE";
	public static final String INVALID_SESSION = "error.invalid_session";
	public static final String LINK_FILE = "LINK_FILE";
	public static final String FILE_SIZE = "FILE_SIZE";
	public static final SvCharId FILE_STORE_ID = new SvCharId("FILE_STORE_ID");
	public static final SvCharId FILE_ID = new SvCharId("FILE_ID");
	public static final String BIG_FILE_WARN = "System file {} has size {} MB. The max file size to be cached is {}. The file will not be cached! Verify your file or your size limit";
	public static final SvCharId FILE_NAME = new SvCharId("FILE_NAME");
	public static final String FILESYSTEM = "FILESYSTEM";
	public static final String FILESTORE_TYPE = "filestore.type";
	public static final String DATABASE = "DATABASE";
	public static final String FILESTORE_TABLE = "filestore.table";
	public static final String FILESTORE_SCHEMA = "filestore.conn.defaultSchema";
	public static final String DB_TYPE = "conn.dbType";
	public static final String POSTGRES = "POSTGRES";
	public static final SvCharId FILE_TYPE = new SvCharId("FILE_TYPE");
	public static final String CONFIGURATION_CLASS = "CONFIGURATION_CLASS";
	public static final String EXECUTION_TIME = "EXECUTION_TIME";
	public static final String IS_SUCCESSFUL = "IS_SUCCESSFUL";
	public static final String REFERENCE_DATE = "REFERENCE_DATE";
	public static final String GEOMETRY_TYPE = "GEOMETRY_TYPE";
	public static final String POLYGON = "POLYGON";
	public static final String KEY_TOO_LONG = "KEY_TOO_LONG";

}
