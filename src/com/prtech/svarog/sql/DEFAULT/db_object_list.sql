 SELECT * 
   FROM   pg_class     c
   JOIN   pg_namespace n ON n.oid = c.relnamespace
   WHERE  upper(c.relname) LIKE upper('{OBJECT_FILTER}')      -- sequence name here
   AND    upper(n.nspname) = upper('{SCHEMA_NAME}')