"""
This version of the processing code uses Glue managed bookmarking to avoid reprocessing files
that have alcready been processed in the same directory. It uses a configuration file (configforv3.json)
to specify the composite primary keys for tables and other configuration parameters.

Author: Frankfurt Ogunfunminiyi
Email: olawole.ogunfunminiyi@shellenergy.com
Date: 2023-05-25
"""
import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.context import SparkConf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException

def initialize_spark_session():
    # return spark object
    conf_list = [
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.glue_catalog.warehouse", "s3://mp2appsrvdevshell"),
        ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
        ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        #("datalake-formats", "iceberg")
    ]
    spark_conf = SparkConf().setAll(conf_list)
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return spark

def is_s3_path_empty(s3_path):
    """
    Check if the given S3 path contains any subdirectory that starts with 'partition'.
    
    Args:
    s3_path (str): The full S3 path to check, e.g., 's3://mp2appsrvdevshell/esg/'
    
    Returns:
    bool: True if the path contains a directory starting with 'partition', False otherwise.
    """
    s3 = boto3.client('s3', region_name='us-east-1')
    
    # Parse S3 bucket and prefix from the S3 path
    s3_path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket_name = s3_path_parts[0]
    prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ""

    # Ensure the prefix ends with a slash to only check immediate "subdirectories"
    if not prefix.endswith('/'):
        prefix += '/'

    # List objects in the directory, using Delimiter to return only immediate subdirectories
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    # Check for directories (CommonPrefixes) that start with 'partition'
    if 'CommonPrefixes' in response:
        for common_prefix in response['CommonPrefixes']:
            directory_name = common_prefix['Prefix'].split('/')[-2]  # Get the immediate directory name
            if directory_name.startswith('partition'):
                return False  # Found a directory that starts with 'partition'  
    return True  # No directory starting with 'partition' was found

def ensure_last_data_read_table_exists(spark,path,config):
        if not spark.catalog.tableExists(path):
            sql_stmt = create_last_data_read_tb(path,config)
            spark.sql(sql_stmt)
            print(f'Table {path} successefully created.')
        #Table already exist and this is not the first time of runing jobs
        print(f'Table {path} already exist and it will be populated.')
        
def populate_list_of_files_processed_info(spark, df, path, table_path):
    # Create a list of Rows
    rows = []
    process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for file_path in df.inputFiles():
        rows.append(Row(last_processed_date=process_time, data_directory=path, file_path=file_path))
    
    # Convert the list of Rows to a DataFrame
    processed_files_df = spark.createDataFrame(rows)
    
    # Create a temporary view from the DataFrame
    processed_files_df.createOrReplaceTempView("temp_files_processed")

    # Use Spark SQL to insert the data into the table
    spark.sql(f"""
    INSERT INTO {table_path} (last_processed_date, data_directory, file_path)
    SELECT TIMESTAMP '{process_time}', '{path}', file_path
    FROM temp_files_processed
    """)

    print(f'List of files processed in this directory loaded into {table_path} successfully.')

def create_last_data_read_tb(path,config):
    return f"""
    CREATE TABLE IF NOT EXISTS {path} (
        last_processed_date TIMESTAMP,
        data_directory STRING, 
        file_path STRING
        
    )
    USING ICEBERG
    PARTITIONED BY (data_directory, months(last_processed_date))
    OPTIONS (
        'format-version'='2'
        
    )
    LOCATION '{config["bucket_name"]}/{config["database"]}'  

    """
def move_file(s3, source_bucket, source_key, destination_bucket, destination_key):
    # Copy the file to the new destination
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)

    # Delete the original file after copying
    s3.delete_object(Bucket=source_bucket, Key=source_key)

    # Return the destination path without the 's3://' prefix
    return f"{destination_bucket}/{destination_key}"

def move_processed_files(source_files, destination_base_path):
    """
    Move files from the list of source S3 paths to the destination S3 path.
    """
    s3 = boto3.client('s3', region_name='us-east-1')
    futures = []
    
    # Use ThreadPoolExecutor to perform operations in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        for file_path in source_files:
            # Extract the bucket and key from the source file path
            source_bucket = file_path.split('/')[2]
            source_key = '/'.join(file_path.split('/')[3:])
            
            # Construct the destination key using the destination_base_path
            destination_key = destination_base_path + '/' + source_key
            
            # Submit the move_file task to the executor
            futures.append(executor.submit(move_file, s3, source_bucket, source_key, source_bucket, destination_key))
        
        # Process the results as they complete
        for future in as_completed(futures):
            print(future.result())

        print(f"All files moved to {destination_base_path} successfully.")


def load_config(bucket_name, key):
    """
    Load the configuration file from S3.
    """
    s3_client = boto3.client('s3', region_name='us-east-1')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    config_content = response['Body'].read().decode('utf-8')
    return json.loads(config_content)

def process_directory(s3_path, table_name, config, spark):
    """
    Process a single directory and upsert data to the Iceberg table.
    """
    # Read data from S3 into a DataFrame using Spark
    try:
 
        df = spark.read.load(
                s3_path,
                compression="gzip",
                format="json",
                inferSchema="true"
            )
    except Exception as e: 
         print(f"Exception : {str(e)}. That directory is likely to be empty")
         return 
    
    full_table_name = f"glue_catalog.{config['database']}.{table_name}"
    # Deduplicate the DataFrame using the composite primary keys (FileID and FileSequence)
    df.createOrReplaceTempView("source_table")
    deduplicated_df = spark.sql("""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY FileID, FileSequence ORDER BY FileID) AS rownum
            FROM source_table
        ) AS subquery
        WHERE rownum = 1
    """)
    # Drop the temporary view to clean up the Spark session
    spark.catalog.dropTempView("source_table")

    # Check if the Iceberg table exists
    table_exists = False
    try:
        target_df = spark.read.format("iceberg").load(full_table_name)
        table_exists = True
    except:
        print(f"Table {full_table_name} does not exist. It will be created.")

    if table_exists:
        # Get sorted list of columns
        source_columns = sorted(deduplicated_df.columns)
        target_columns = sorted(target_df.columns)

        # Ensure the number of columns and names are the same
        if source_columns != target_columns:
            raise ValueError(f"Source and target tables do not have matching columns. "
                             f"Source columns: {source_columns}, Target columns: {target_columns}")

        # Select the columns in sorted order
        deduplicated_df = deduplicated_df.select(*source_columns)
        target_df = target_df.select(*target_columns)

        # Register the deduplicated DataFrame as a temporary view to use in the MERGE INTO statement
        deduplicated_df.createOrReplaceTempView("updates")

        # Use Spark SQL to perform the merge operation with Iceberg's MERGE INTO
        merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING updates AS source
            ON target.FileID = source.FileID AND target.FileSequence = source.FileSequence
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        # Execute the SQL query
        spark.sql(merge_sql)

        # Drop the temporary view after the merge
        spark.catalog.dropTempView("updates")
        return df
    else:
        # Register the deduplicated DataFrame as a temporary view to be used in the SQL statement
        deduplicated_df.createOrReplaceTempView("temp_table")

        # Construct the SQL statement for creating the table if it doesn't exist
        sql_stmt = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING iceberg 
            OPTIONS (
                'format-version'='2' 
            )
            LOCATION '{config['bucket_name']}/{config['database']}/{table_name}'
            AS SELECT * FROM temp_table
        """
        # Execute the SQL statement
        spark.sql(sql_stmt)

        # Drop the temporary view after table creation
        spark.catalog.dropTempView("temp_table")

        print(f"Table {full_table_name} has been created with the initial data and partitioned.")

        return df

def main():
    # Get command line arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONFIG_S3_BUCKET', 'CONFIG_S3_KEY'])
    # Initialize Spark and Glue contexts with Iceberg and Glue Catalog configurations
    spark = initialize_spark_session()
    glueContext = GlueContext(spark.sparkContext)
    # Initialize Glue job
    job = Job(glueContext)
    job_name = args['JOB_NAME']
    # Load configuration from S3
    config = load_config(args['CONFIG_S3_BUCKET'], args['CONFIG_S3_KEY'])
    # Check if the processed file metadata table is available or create it
    full_metadata_tb = f"glue_catalog.{config['database']}.last_data_read_tb" 
    ensure_last_data_read_table_exists(spark,full_metadata_tb,config)
    # Extract job properties and initialize the job
    job_properties = config['job_properties']
    job.init(job_name, job_properties)

    # Process each directory and corresponding table
    directories_to_tables = config['source_directory']
    
    for s3_path, table_name in directories_to_tables.items():
        ## Check if directory is not empty if empty "continue" to next iteration
        if is_s3_path_empty(s3_path):
            print(f"Skipping {s3_path} as it is empty")
            continue  # Skip to the next iteration if empty
        originalDF = process_directory(s3_path, table_name, config, spark)
        populate_list_of_files_processed_info(spark,originalDF, s3_path,full_metadata_tb)
        move_processed_files(originalDF.inputFiles(), config["processed_directory"])    
    # Commit the job
    job.commit()

if __name__ == "__main__":
    main()
