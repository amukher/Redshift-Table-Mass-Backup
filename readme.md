# AWS Redshift Mass Backup Script

When to use this script:

 * Under any space cleanup activity, when table backup is required before purging.
 * Backup your schema with an easy script.

Why to use this script:

 * Automatic script loads all the tables to Amazon S3 bucket with table headers.
 * The script also follows a hierarchy of Schema>Username>backed up table (with headers)
 * A well design storage for backup, where any user can easily locate their tables.


### Stuff used to make this:

 * Some Postgres system tables.
 * Some sql queries.
 * Some shell scripting.
