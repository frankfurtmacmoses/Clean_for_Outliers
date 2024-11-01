''' 
This version of processing code use glue managed book marking to avoid reprocessing files that already 
processed in the same directory but uses spark optimized approach because of in-efficient join. Time complexity is hig
because of section. Needed to be optimized with process_table_by_schema_v2 or v3. 

Author: Frankfurt Ogunfunminiyi
Email:  olawole.ogunfunminiyi@shellenergy.com
Date:   2023-05-25

'''



import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

class GlueIcebergJob:
    def __init__(self, job_name, config_s3_bucket, config_s3_key):
        # Initialize Spark and Glue contexts with Iceberg and Glue Catalog configurations
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session.builder \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", "s3://mp2appsrvdevshell") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.glue_catalog.client.region", "us-east-1") \
            .getOrCreate()

        self.job = Job(self.glueContext)
        
        # Initialize job name
        self.job_name = job_name
        
        # Load configuration from S3
        self.config = self.load_config(config_s3_bucket, config_s3_key)
        
        # Extract job properties
        self.job_properties = self.config['job_properties']
        
        # Extract topic prefix
        self.topic_prifix = self.config['topic_prifix']
        
        # Initialize Glue job with properties from config
        self.job.init(self.job_name, self.job_properties)
        
    def load_config(self, bucket_name, key):
        """Load configuration from S3."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        config_content = response['Body'].read().decode('utf-8')
        return json.loads(config_content)
    
    def process_directory(self, s3_path, table_name):
        """Process a single directory and upsert data to the Iceberg table."""
        # Read data from S3 into a DynamicFrame using Glue's Spark library with Glue bookmarks enabled
        dynamic_frame = self.glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [s3_path],
                "recurse": True,
                "groupFiles": "inPartition",  # Bookmark setting
                "groupSize": "10485760",  # Bookmark setting
                "enableGlueDataCatalog": True,  # Enable Glue Data Catalog
                "jobBookmarkKeys": ["path"],  # Bookmark key (depends on your use case)
                "jobBookmarkKeysSortOrder": "asc"  # Bookmark sort order
            },
            format="json",  # Specify the format of the input files (e.g., json, csv, parquet)
            transformation_ctx="dynamic_frame"
        )
        
        # Convert DynamicFrame to Spark DataFrame
        df = dynamic_frame.toDF()

        # Define the full table name
        full_table_name = f"glue_catalog.{self.config['database']}.{table_name}"

        # Check if the Iceberg table exists
        table_exists = False
        try:
            target_df = self.spark.read.format("iceberg").load(full_table_name)
            table_exists = True
        except:
            print(f"Table {full_table_name} does not exist. It will be created.")

        if table_exists:
            # Get sorted list of columns
            source_columns = sorted(df.columns)
            target_columns = sorted(target_df.columns)
            
            # Ensure the number of columns and names are the same
            if source_columns != target_columns:
                raise ValueError(f"Source and target tables do not have matching columns. "
                                 f"Source columns: {source_columns}, Target columns: {target_columns}")
            
            # Select the columns in sorted order
            df = df.select(*source_columns)
            target_df = target_df.select(*target_columns)
            
            # Determine which table to broadcast
            if df.count() < target_df.count():
                small_df = df
                large_df = target_df
            else:
                small_df = target_df
                large_df = df
            
            # Perform an upsert operation manually using broadcast join for the smaller table
            # First, find the rows that need to be updated (i.e., matching keys)
            updated_rows = large_df.alias("large").join(broadcast(small_df).alias("small"),
                                                        (large_df.FileID == small_df.FileID) & 
                                                        (large_df.FileSequence == small_df.FileSequence),
                                                        "inner") \
                                                   .selectExpr("small.*")

            # Find the rows that need to be inserted (i.e., non-matching keys)
            inserted_rows = small_df.alias("small").join(broadcast(large_df).alias("large"),
                                                         (small_df.FileID == large_df.FileID) & 
                                                         (small_df.FileSequence == large_df.FileSequence),
                                                         "left_anti")

            # Combine the updated rows with the non-updated ones
            final_df = large_df.alias("large").join(broadcast(updated_rows).alias("updates"),
                                                    (large_df.FileID == updated_rows.FileID) & 
                                                    (large_df.FileSequence == updated_rows.FileSequence),
                                                    "left_anti").union(inserted_rows)

            # Write the result back to the Iceberg table
            final_df.write.format("iceberg").mode("overwrite").save(full_table_name)

        else:
            # Register the DataFrame as a temporary view to be used in the SQL statement
            df.createOrReplaceTempView("temp_table")

            # Construct the SQL statement for creating the table if it doesn't exist
            sql_stmt = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name}
                USING iceberg 
                TBLPROPERTIES (
                    'format-version'='2' 
                )
                LOCATION '{self.config['bucket_name']}/{self.config['database']}/{table_name}'
                AS SELECT * FROM temp_table
            """

            # Execute the SQL statement
            self.spark.sql(sql_stmt)
            print(f"Table {full_table_name} has been created with the initial data and partitioned.")
    
    def run(self):
        """Main method to run the job."""
        # Process each directory and corresponding table
        directories_to_tables = self.config['source_directory']
        for s3_path, table_name in directories_to_tables.items():
            self.process_directory(s3_path, table_name)
        
        # Commit the job
        self.job.commit()

if __name__ == "__main__":
    # Get command line arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONFIG_S3_BUCKET', 'CONFIG_S3_KEY'])
    
    # Instantiate and run the GlueIcebergJob class
    glue_iceberg_job = GlueIcebergJob(args['JOB_NAME'], args['CONFIG_S3_BUCKET'], args['CONFIG_S3_KEY'])
    glue_iceberg_job.run()
