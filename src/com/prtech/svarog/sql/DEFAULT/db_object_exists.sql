 SELECT count(*) 
   FROM   pg_class     c
   JOIN   pg_namespace n ON n.oid = c.relnamespace
   WHERE  upper(c.relname) = upper('{OBJECT_NAME}')      -- sequence name here
   AND    upper(n.nspname) = upper('{SCHEMA_NAME}')