# Get schema name as an argument for backup
#echo $1;

export PGPASSWORD='<YOUR PASSWORD>';
user_list=`psql -h <YOUR REDSHIFT HOSTNAME> -p 5439 -U <YOUR USERNAME> -w -d <YOUR DB NAME> -Atc "select username from redshift_cleanup_table_headers where schema_name = '$1' group by 1"`

# Nested loops for backing up for all user's tables under the schema names passed as an argument
for user_name in $user_list
do
	#echo $user_name;
	
	export PGPASSWORD='<YOUR PASSWORD>';
	tbl_list=`psql -h <YOUR REDSHIFT HOSTNAME> -p 5439 -U <YOUR USERNAME> -w -d <YOUR DB NAME> -Atc "select table_name from redshift_cleanup_table_headers where schema_name = '$1' and username='$user_name' group by 1"`

	for tbl_name in $tbl_list
	do
		#echo $tbl_name;

		# Generating S3 path for header files
		header_path='s3://<YOUR AMAZON S3 BUCKET>/redshift_cleanup/'
		header_path=$header_path$1"/"$user_name"/"$tbl_name"_header.csv"
		#echo $header_path;

		# Generating S3 path for the backup files containing records
		backup_path='s3://<YOUR AMAZON S3 BUCKET>/redshift_cleanup/'
		backup_path=$backup_path$1"/"$user_name"/"$tbl_name".csv"
		#echo $backup_path;

		# Unloading header and record file seperately to Amazon S3 location.
		export PGPASSWORD='<YOUR PASSWORD>';
		psql -h <YOUR REDSHIFT HOSTNAME> -p 5439 -U <YOUR USERNAME> -w -d <YOUR DB NAME> -Atc "unload('select table_header from redshift_cleanup_table_headers where schema_name=''$1'' and table_name=''$tbl_name'' and username=''$user_name''') to '$header_path' credentials 'aws_access_key_id=<YOUR AWS ACCESS KEY ID>;aws_secret_access_key=<YOUR AWS SECRET ACCESS KEY>' allowoverwrite parallel off gzip"
		export PGPASSWORD='<YOUR PASSWORD>';
		psql -h <YOUR REDSHIFT HOSTNAME> -p 5439 -U <YOUR USERNAME> -w -d <YOUR DB NAME> -Atc "unload('select * from $1.$tbl_name') to '$backup_path' credentials 'aws_access_key_id=<YOUR AWS ACCESS KEY ID>;aws_secret_access_key=<YOUR AWS SECRET ACCESS KEY>' allowoverwrite delimiter '|' parallel off gzip"
		
		# Change directory to a post processing or merging folder in unix for merging both header and records file.
		cd /<YOUR EC2 UNIX ENVIRONMENT>/redshift_cleanup

		# I am using s3cmd here, so s3cmd should be installed earlier for use.
		# Get unloaded header file to unix environment
		s3cmd get -f $header_path*
		# Rename file and remove those annoying 000 that gets attached whenever we unload to s3.
		mv $tbl_name"_header.csv000.gz" $tbl_name"_header.csv.gz"

		# Get unloaded record file to unix environment
		s3cmd get -f $backup_path*
		# Rename file and remove those annoying 000 that gets attached whenever we unload to s3.
		mv $tbl_name".csv000.gz" $tbl_name".csv.gz"

		# Finally the main reason for doing all this workaround. Merging header and record file together.
		zcat $tbl_name"_header.csv.gz" $tbl_name".csv.gz" | gzip > $tbl_name"_merged.csv.gz"

		# Putting the merged file back to S3 location
		s3cmd put -f $tbl_name"_merged.csv.gz" $backup_path".gz"

		# Cleaning up and removing the processed or raw files.
		s3cmd del $header_path"000.gz"
		s3cmd del $backup_path"000.gz"
		rm $tbl_name"_header.csv.gz"
		rm $tbl_name".csv.gz"
		rm $tbl_name"_merged.csv.gz"

	done
done