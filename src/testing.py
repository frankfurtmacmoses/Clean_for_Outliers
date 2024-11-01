import boto3

# Initialize the S3 client
s3 = boto3.client('s3')

def has_s3_folders(s3_path):
    """
    Check if the given S3 path has any folders.

    Args:
    s3_path (str): The full S3 path to check, e.g., 's3://bucket-name/path/'

    Returns:
    bool: True if the path has folders, False otherwise.
    """
    # Parse S3 bucket and prefix from the S3 path
    s3_path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket_name = s3_path_parts[0]
    prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ""

    # Check for folders (common prefixes) in the S3 path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    
    # Return True if the path has folders (common prefixes)
    if 'CommonPrefixes' in response and len(response['CommonPrefixes']) > 0:
        return True
    return False

# Example usage in your loop
if __name__ == '__main__':
    if not has_s3_folders(s3_path):
        print(f"Skipping {s3_path} as it has no folders")
        continue  # Skip to the next iteration if no folders

    # Proceed with processing if folders are present
    originalDF = process_directory(s3_path, table_name, config, spark)
