import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, expr

# Initialize Boto3 S3 client
s3_client = boto3.client('s3')

# Helper function to read a file from S3 and return its content
def read_s3_file(bucket_name, key):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response['Body'].read().decode('utf-8')

# Read config.json from S3
config_json_content = read_s3_file('mp2appsrvdevshell', 'deploysetup/config.json')
config = json.loads(config_json_content)

# Extract job properties for job.init
job_properties = config["job_properties"]

# Extract connection details from config
connection_config = config["connection_config"]
connection_name = connection_config["connection_name"]
vpc_id = connection_config["vpc_id"]
security_group_id = connection_config["security_group_id"]
subnet_id = connection_config["subnet_id"]

# Extract bootstrap servers from config
bootstrap_servers = config["bootstrap_servers"]

# Read topic_list from S3
topic_list_content = read_s3_file('mp2appsrvdevshell', 'deploysetup/topic_list.json')
topics_tables = json.loads(topic_list_content)  # Assuming topic_list is a JSON string containing a dictionary

# Initialize Glue Context and Spark Session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], job_properties)  # Pass only job_properties to job.init
#job.init('JOB_NAME', job_properties)  # Pass only job_properties to job.init
# Function to create a Kafka connection using Boto3
# def create_glue_kafka_connection(connection_name, vpc_id, security_group_id, subnet_id):
#     # Initialize a Boto3 client for Glue
#     glue_client = boto3.client('glue')

#     # Define the connection properties
#     connection_properties = {
#         'Name': connection_name,
#         'ConnectionType': 'NETWORK',  # This indicates a VPC-based connection
#         'ConnectionProperties': {
#             'KAFKA_BOOTSTRAP_SERVERS': bootstrap_servers,  # Use bootstrap servers from config
#             'KAFKA_SSL_ENABLED': 'true',  # Set to true if you're using SSL for Kafka
#             'KAFKA_SASL_MECHANISM': 'AWS_IAM',  # Use AWS_IAM if using IAM authentication, otherwise configure appropriately
#             'KAFKA_SECURITY_PROTOCOL': 'SASL_SSL',  # This is for IAM auth; adjust as per your setup
#         },
#         'PhysicalConnectionRequirements': {
#             'SubnetId': subnet_id,
#             'SecurityGroupIdList': [
#                 security_group_id
#             ],
#             'AvailabilityZone': 'us-east-1a'  # Specify the availability zone, if necessary
#         }
#     }

#     # Create the connection
#     response = glue_client.create_connection(
#         ConnectionInput=connection_properties
#     )

#     return response

# Create the Kafka connection
# response = create_glue_kafka_connection(connection_name, vpc_id, security_group_id, subnet_id)
# print("Kafka connection created:", response)

# Function to process each batch and write to Iceberg
# def processBatch(data_frame, batchId, table_name):
#     if data_frame.count() > 0:
#         # Parse the JSON data if needed (assuming the data is JSON format)
#         df = data_frame.withColumn("value", col("value").cast("string"))

#         # Check if the Iceberg table exists
#         table_exists = False
#         try:
#             spark.table(table_name)
#             table_exists = True
#         except Exception as e:
#             print(f"Table {table_name} does not exist. It will be created.")

#         if table_exists:
#             # Read the existing Iceberg table
#             existing_df = spark.table(table_name)

#             # Merge logic based on primary keys: FileID, Sequential_ID
#             merged_df = df.alias("updates").join(
#                 existing_df.alias("existing"),
#                 (df["FileID"] == existing_df["FileID"]) & (df["Sequential_ID"] == existing_df["Sequential_ID"]),
#                 "full_outer"
#             ).select(
#                 col("updates.*")
#             )

#             # Write the merged data back to the Iceberg table
#             merged_df.write.format("iceberg").mode("append").saveAsTable(table_name)
#         else:
#             # Create the Iceberg table with partitioning and properties
#             df.write.format("iceberg") \
#                 .mode("overwrite") \
#                 .option("path", f"{config['bucket_name']}/{config['database']}/{table_name}") \
#                 .option("format-version", "2") \
#                 .option("write.delete.mode", "copy-on-write") \
#                 .option("write.update.mode", "merge-on-read") \
#                 .option("write.merge.mode", "merge-on-read") \
#                 .option("write.object-storage.enabled", "true") \
#                 .partitionBy("data_directory", expr("days(last_processed_date)")) \
#                 .saveAsTable(table_name)
#             print(f"Table {table_name} has been created with the initial data and partitioned.")

# Loop through each topic and process the data
# for topic, table_name in topics_tables.items():
#     # Kafka Source as Spark DataFrame
#     kafka_df = glueContext.create_data_frame.from_options(
#         connection_type="kafka",
#         connection_options={
#             "connectionName": connection_name,  # Use the connection created above
#             "classification": "json",
#             "startingOffsets": "earliest",
#             "topicName": topic,
#             "inferSchema": "true",
#             "kafka.bootstrap.servers": bootstrap_servers,  # Use bootstrap servers from config
#             "typeOfData": "kafka"
#         },
#         transformation_ctx=f"dataframe_{topic.replace('.', '_')}"
#     ).toDF()  # Convert DynamicFrame to Spark DataFrame

#     # Execute the batch processing using forEachBatch
#     kafka_df.writeStream.foreachBatch(
#         lambda df, batchId: processBatch(df, batchId, table_name)
#     ).option(
#         "checkpointLocation", f"{job_properties['TempDir']}/{args['JOB_NAME']}/{topic.replace('.', '_')}/checkpoint/"
#     ).start()

# # Wait for the streams to finish
# spark.streams.awaitAnyTermination()

# Commit the job
job.commit()
