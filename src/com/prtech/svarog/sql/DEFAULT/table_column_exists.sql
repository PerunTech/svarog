SELECT COUNT (*)
FROM information_schema.columns 
WHERE upper(table_name)=upper('{TABLE_NAME}')
and upper(column_name)=upper('{COLUMN_NAME}')
and upper(table_schema)=upper('{SCHEMA_NAME}')