import requests
import boto3
from io import BytesIO

def upload_file_to_s3(url, bucket_name, object_name):
    """
    Downloads a file from a given URL and uploads it to an S3 bucket.

    :param url: URL of the file to download
    :param bucket_name: Name of the S3 bucket
    :param object_name: S3 object name. If not specified, the file name is used
    """
    # Create a session using boto3
    session = boto3.Session(
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY',
        region_name='YOUR_REGION'
    )
    s3 = session.resource('s3')

    # Download the file
    response = requests.get(url)
    if response.status_code == 200:
        # Upload the file to S3
        s3.Object(bucket_name, object_name).put(Body=BytesIO(response.content))
        print(f"File uploaded to {bucket_name}/{object_name}")
    else:
        print("Failed to download the file")

# Example usage
# upload_file_to_s3('http://example.com/myfile.jpg', 'mybucket', 'myfile.jpg')


import boto3
import zipfile
from io import BytesIO

def unzip_s3_file(bucket_name, zip_object_name, target_prefix=''):
    """
    Downloads a zip file from S3, unzips it, and uploads the contents back to S3.

    :param bucket_name: Name of the S3 bucket
    :param zip_object_name: Key of the zip file in S3
    :param target_prefix: Prefix for the unzipped files in S3
    """
    # Create a session using boto3
    session = boto3.Session(
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY',
        region_name='YOUR_REGION'
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # Download the zip file from S3
    zip_obj = bucket.Object(zip_object_name)
    buffer = BytesIO(zip_obj.get()["Body"].read())

    # Unzip the file and upload contents back to S3
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            file_info = zip_ref.getinfo(filename)
            if file_info.is_dir():  # Skip directories
                continue
            unzipped_file = zip_ref.open(filename)
            # Define the target key for each unzipped file
            target_key = f"{target_prefix}{filename}"
            # Upload the file
            bucket.put_object(Key=target_key, Body=unzipped_file.read())
            print(f"Uploaded {target_key} to S3 bucket {bucket_name}")

# Example usage
# unzip_s3_file('mybucket', 'path/to/myzipfile.zip', 'unzipped/')
