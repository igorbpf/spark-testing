import boto3

def create_folders_in_s3(session, bucket_name, folder_names):
    """
    Create multiple folders in an S3 bucket.

    :param session: A boto3 session object
    :param bucket_name: Name of the S3 bucket
    :param folder_names: A list of folder names to create
    """
    # Get the S3 resource from the provided session
    s3 = session.resource('s3')
    
    # Iterate over the list of folder names
    for folder_name in folder_names:
        # Ensure the folder name ends with a '/'
        folder_key = f'{folder_name}/' if not folder_name.endswith('/') else folder_name
        
        # Use the S3 resource to put an empty object for the folder
        s3.Object(bucket_name, folder_key).put()

# Usage example
if __name__ == "__main__":
    # Create a boto3 session
    session = boto3.Session(
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY',
        region_name='YOUR_REGION'
    )

    # Bucket name
    bucket_name = 'your-bucket-name'

    # List of folders you want to create
    folder_names = ['folder1', 'folder2', 'folder3']

    # Create folders
    create_folders_in_s3(session, bucket_name, folder_names)
