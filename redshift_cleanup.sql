/* Table ddl for holding various specs like username, schema_name, table_name and column_names 
in order to generate table headers for backup onto Amazon S3 */
create table redshift_cleanup_table_specs
(
	schema_name varchar(100) encode lzo,
	table_name varchar(200) encode lzo,
	column_name varchar(100) encode lzo,
	column_num int encode delta,
	username varchar(100) encode lzo
)
diststyle even;

/* Records insertion using postgres system tables. attnum is the most important attribute as 
it gives us column order in a table. Very useful for table header generation */
insert into redshift_cleanup_table_specs
select n.nspname, c.relname, a.attname, a.attnum, u.usename from pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
JOIN pg_user u ON c.relowner = u.usesysid
JOIN pg_attribute a ON c.oid = a.attrelid
LEFT JOIN pg_attrdef adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
WHERE c.relkind = 'r' AND a.attnum > 0 
/* Exclude or include any schema for backup */
and n.nspname not in ()
ORDER BY n.nspname,c.relname,a.attnum;

/* Table ddl for holding the exact table header with a specific delimited 
which will be added to the top of the backup file */
create table redshift_cleanup_table_headers
(
	schema_name varchar(100) encode lzo,
	table_name varchar(200) encode lzo,
	table_header varchar(2000) encode lzo,
	username varchar(100) encode lzo
)
diststyle even;

/* header generation using column_names and using column_num as the ordering 
for the column_name in the header description */
insert into redshift_cleanup_table_headers
select schema_name, table_name, listagg(column_name,'|') within group (order by column_num), username 
from redshift_cleanup_table_specs group by 1,2,4;

