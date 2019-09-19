conn.type={{ db_conn_type }}
driver.name={{ db_driver_name }}

conn.string={{ db_connstring }}
user.name={{ db_username }}
user.password={{ db_password }}

conn.dbType={{ db_type }}
conn.defaultSchema={{ db_schema }}
sys.masterRepo=svarog

sys.core.cleanup_time=30
sys.core.is_debug=false
sys.service_class=
sys.force_timezone=

sys.jdbc.batch_size=10
sys.conf.path=conf
sys.lock.max_wait_time = 10

sys.defaultLocale=mk_MK
sys.defaultDateFormat=dd/MM/yyyy
sys.defaultTimeFormat=HH:mm:ss
sys.defaultJSDateFormat=d/m/Y
sys.defaultJSTimeFormat=H:i


sys.gis.default_srid=6316
sys.gis.geom_handler=POSTGIS
sys.gis.grid_size=10
sys.gis.tile_cache=100
sys.gis.precision_scale=1000
sys.gis.allow_boundary_intersect=false
sys.gis.legal_sdi_unit_type=1

sys.codes.multiselect_separator=;

oracle.jdbc.J2EE13Compliant=false

filestore.conn.type=DEFAULT
filestore.sys_store.cache_ttl=30
filestore.sys_store.cache_max_filesize=5
filestore.table=svarog_filestore
filestore.type=FILESYSTEM
filestore.path=c:/svarog_fs
custom.jar=./svarog_custom_afsard_dp-1.0_dev.jar
frontend.hostname=
mail.from = admim@admin.com
mail.username =admim@admin.com
mail.password =mailpass

mail.host = smtp.gmail.com
mail.smtp.auth=true
mail.smtp.starttls.enable=true
mail.smtp.port=587
mail.format=text/html; charset=UTF-8;
security.crypto_type=hsm

security.hsm_cfg_file=C\:\\SoftHSM\\softhsm_svarog.cfg
