conn.type={{ db_conn_type }}
driver.name={{ db_driver_name }}

conn.string={{ db_connstring }}
user.name={{ db_username }}
user.password={{ db_password }}

conn.dbType={{ db_type }}
conn.defaultSchema={{ db_schema }}
conn.dbHandlerClass={{ db_handler }}
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


sys.gis.default_srid=NULL
sys.gis.geom_handler=POSTGIS
sys.gis.grid_size=10
sys.gis.tile_cache=100
sys.gis.precision_scale=1000
sys.gis.allow_boundary_intersect=false
sys.gis.legal_sdi_unit_type=1
sys.gis.override_user_area_perim=false
sys.gis.enable_spatial=true

sys.codes.multiselect_separator=;

oracle.jdbc.J2EE13Compliant=false

filestore.conn.type=DEFAULT
filestore.sys_store.cache_ttl=30
filestore.sys_store.cache_max_filesize=5
filestore.table=svarog_filestore
filestore.type=FILESYSTEM
filestore.path=c:/svarog_fs

felix.auto.deploy.dir=osgi-bundles
org.osgi.framework.storage=osgi-cache
felix.auto.deploy.action=install,start
felix.log.level=1
org.osgi.service.http.port=8091
batch.max_thread_pool_size=25
obr.repository.url=http://felix.apache.org/obr/releases.xml
org.osgi.framework.system.packages.extra=com.prtech.svarog,com.prtech.svarog_common,com.prtech.svarog_interfaces,org.easybatch.core.job;version=5.2.0,org.easybatch.core.processor;version=5.2.0,org.easybatch.core.reader;version=5.2.0,org.easybatch.core.record;version=5.2.0,org.easybatch.core.writer;version=5.2.0,com.github.os72.protobuf.dynamic,com.google.protobuf,com.google.gson,com.google.gson.reflect,javax.ws.rs,javax.ws.rs.core,org.springframework.batch.core,org.springframework.batch.core.job.builder,org.springframework.context.annotation,org.springframework.beans.factory.annotation,org.springframework.batch.repeat,org.springframework.batch.core.step.tasklet,org.springframework.batch.core.step.builder,org.springframework.batch.core.scope.context,org.springframework.batch.core.launch.support,org.springframework.batch.core.launch,org.springframework.batch.core.configuration.annotation,org.apache.logging.log4j;version=2.11.0,org.glassfish.jersey.media.multipart,org.joda.time,org.joda.time.format,com.vividsolutions.jts.io.svarog_geojson,com.vividsolutions.jts.geom



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
