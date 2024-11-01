'''
This version of processing code use  custom bookmark to manage how S3 files are proccessed

Author: Frankfurt Ogunfunminiyi
Email:  olawole.ogunfunminiyi@shellenergy.com
Date:   2023-05-25

'''
import os
import boto3
from contextlib import contextmanager
from datetime import datetime
from typing import List, Tuple, Optional
import watchtower
import logging
from botocore.exceptions import ClientError
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Table
from awsglue.context import GlueContext
from awsglue.job import Job
import json

class DataTableProcessor:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.bucket_name = self.config['bucket_name']
        self.database = self.config['database']
        self.catalog = self.config['catalog']
        self.topic_dir = self.config["topic_dir"]
        #self.topic_prefix = self.config["topic_prefix"]
        self.spark = self.initialize_spark_session()
        self.logger = self.setup_logger(f'{self.database}/{self.topic_dir}')
        self.region = self.config["region"]

    def run(self):
        tables_list = self.load_tables_list()
        self.ensure_last_data_read_table_exists()

        for table_name in tables_list:
            job_name = f"{table_name}_job"
            self.configure_job_properties(job_name, table_name)

            with self.glue_job_manager(self.setup_glue_job(job_name, self.config["job_properties"])) as job:
                source_dir = self.get_source_dir(table_name)
                last_processed_path = self.get_last_file_processed_path(source_dir)
                bucket, filename = self.get_bucket_and_file_name(last_processed_path)
                last_file_modified_date = self.get_last_modified_date(bucket, filename)
                df = self.process_new_files(last_processed_path, last_file_modified_date, source_dir)

                if df is not None:
                    temp_table = self.create_temp_table(df)
                    self.process_data_table(temp_table, table_name)
                    self.populate_list_of_files_processed_info(df, source_dir)
                else:
                    self.logger.info(f"No new file processed in the source directory: {source_dir}")

                if not self.is_glue_job_scheduled(job_name):
                    self.schedule_glue_job(job_name)

    @contextmanager
    def glue_job_manager(self, job):
        try:
            yield job
        finally:
            if job:
                job.commit()

    def load_config(self, config_path):
        with open(config_path, 'r') as file:
            return json.load(file)

    def load_tables_list(self) -> List[str]:
        with open("tables_config/tablelist.txt") as tables:
            return [line.strip() for line in tables]

    def ensure_last_data_read_table_exists(self):
        if not self.spark.catalog.tableExists(f"{self.catalog}.{self.database}.last_data_read_tb"):
            sql_stmt = self.create_last_data_read_tb()
            self.spark.sql(sql_stmt)
        

    def configure_job_properties(self, job_name, table_name):
        self.config["job_properties"]["LogGroupName"] = f's3://{self.bucket_name}/{self.database}/{self.topic_dir}_log_gp'
        self.config["job_properties"]["LogStreamNamePrefix"] = f'{table_name}_log-{datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}'

    def get_source_dir(self, table_name):
        return f"{self.bucket_name}/{self.config['topic_dir']}/{table_name}"

    def get_last_file_processed_path(self, curr_source_data_dir):
        try:
            curr_source_data_dir_safe = curr_source_data_dir if curr_source_data_dir is not None else ''
            query = f"""
            SELECT file_path
            FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY COALESCE(last_processed_date, '1970-01-01') DESC) as row_number
                FROM {self.catalog}.{self.database}.last_data_read_tb
                WHERE data_directory = '{curr_source_data_dir_safe}'
            ) tmp
            WHERE row_number = 1
            """
            result = self.spark.sql(query).collect()
            return result[0]["file_path"] if result else None

        except Exception as e:
            if "RAISE CONVERTED FROM NONE" in str(e):
                # Handle the specific 'RAISE CONVERTED FROM NONE' exception
                print(f"Handled 'RAISE CONVERTED FROM NONE' exception for source directory: {curr_source_data_dir}")
                return None  # or some other default value or behavior
            else:
                # Re-raise the exception if it's not the one we want to handle
                raise

    def get_bucket_and_file_name(self, last_processed_path) -> Tuple[str, str]:
        if last_processed_path and last_processed_path.startswith("s3://"):
            s3path = last_processed_path[5:]
            parts = s3path.split('/', 1)
            return parts[0], parts[1]
        return "", ""

    def get_last_modified_date(self, bucket_name, file_path) -> Optional[str]:
        s3 = boto3.client("s3")
        try:
            response = s3.head_object(Bucket=bucket_name, Key=file_path)
            modified_date = response['LastModified']
            return modified_date.strftime('%Y-%m-%dT%H:%M:%S')
        except Exception as e:
            #self.logger.error(f"Error getting last modified date for {file_path} in bucket {bucket_name}: {e}")
            print(f"Error getting last modified date for {file_path} in bucket {bucket_name}: {e}")
            return None

    def process_new_files(self, last_file_process_path, last_raw_file_modified_date, raw_source):
        if last_file_process_path:
            if last_raw_file_modified_date:
                return self.spark.read.load(
                    raw_source,
                    compression="gzip",
                    format="json",
                    inferSchema="true",
                    modifiedAfter=last_raw_file_modified_date
                )
        else:
            return self.spark.read.load(
                raw_source,
                compression="gzip",
                format="json",
                inferSchema="true"
            )

    def create_temp_table(self, df):
        df.createOrReplaceTempView("temp_table")
        return self.spark.sql("SELECT * FROM temp_table")

    def process_data_table(self, temp_table, table_name):
        full_table_name = f"{self.catalog}.{self.database}.{self.topic_prefix}/{table_name}"
        if not self.spark.catalog.tableExists(full_table_name):
            self.create_iceberg_table(full_table_name, temp_table)
        else:
            self.merge_into_iceberg_table(full_table_name, temp_table)

    def create_iceberg_table(self, full_table_name, temp_table):
        sql_stmt = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING iceberg 
            TBLPROPERTIES (
                'table_type'='ICEBERG',
                'format-version'='2',
                'write.delete.mode'='copy-on-write',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read',
                'write.object-storage.enabled'=true
            )
            LOCATION 's3://{self.bucket_name}/{self.database}.{self.topic_prefix}'
            AS SELECT * FROM {temp_table}
        """
        self.spark.sql(sql_stmt)

    def merge_into_iceberg_table(self, full_table_name, temp_table):
        sql_stmt = f"""
            MERGE INTO {full_table_name} target
            USING (SELECT * FROM {temp_table}) source
            ON target.FileID = source.FileID  
            AND target.FileSequence = source.FileSequence
            WHEN MATCHED THEN UPDATE 
            SET *
        """
        self.spark.sql(sql_stmt)

    def populate_list_of_files_processed_info(self, df, source_dir):
        for file_path in df.inputFiles():
            process_time = datetime.now()
            self.spark.sql(f"INSERT INTO {self.catalog}.{self.database}.last_data_read_tb VALUES ('{process_time}','{file_path}','{source_dir}')")

    

    job_properties):
        glue_context = GlueContext(self.spark.sparkContext
    def setup_glue_job(self, job_name, )
        job = Job(glue_context)
        job.init(job_name, job_properties)
        return job

    def create_last_data_read_tb(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.database}.last_data_read_tb (
            last_processed_date TIMESTAMP,
            file_path STRING,
            data_directory STRING 
        )
        USING ICEBERG
        PARTITIONED BY (data_directory, days(last_processed_date))
        TBLPROPERTIES (
            'table_type'='ICEBERG',
            'format-version'='2',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.object-storage.enabled'=true,
            'key'='file_path'
        )
        LOCATION '{self.bucket_name}/{self.database}'  
        """

    def initialize_spark_session(self):
        conf_list = [
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
            ("spark.sql.catalog.glue_catalog.warehouse", "s3://mp2appsrvdevshell/esgdb/my_warehouse_dir"),
            ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
            ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            
            #Enable Glue Data Catalog as Hive Metastore
            # ("spark.sql.catalogImplementation", "hive"),
            # ("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"),
            # ("spark.sql.warehouse.dir", "s3://mp2appsrvdevshell/esgdb/my_warehouse_dir"),
            # ("spark.sql.hive.metastore.sharedPrefixes", "com.amazonaws.services.glue"),
            # ("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4"),
            # ("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4"),
            # # Optional: Specify AWS region (if needed)
            # ("spark.hadoop.hive.metastore.glue.region", "us-east-1")
            ]
        spark_conf = SparkConf().setAll(conf_list)
        return SparkSession.builder.config(conf=spark_conf).getOrCreate()

    

if __name__ == "__main__":
    # sample source directory: s3://mp2appsrvdevshell/esg/tpc.ESG.CTG.USG_6100_IDRQuantity/
    # sample last_processed_path: s3://mp2appsrvdevshell/esg/tpc.ESG.CTG.USG_6100_IDRQuantity/partition=0/tpc.ESG.CTG.USG_6100_IDRQuantity+0+0000000000.json.gz
    # sample file_name: esg/tpc.ESG.CTG.USG_6100_IDRQuantity/partition=0/tpc.ESG.CTG.USG_6100_IDRQuantity+0+0000000000.json.gz
    
    config_settings = "config/glue-job-config/config.json"
    processor = DataTableProcessor(config_settings)
    processor.run()