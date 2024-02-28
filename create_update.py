import boto3

def create_or_update_glue_job(job_name, script_location, iam_role, region_name='your-region', **kwargs):
    """
    Create a new AWS Glue job, or update the existing one if it already exists.

    Parameters:
    - job_name (str): Name of the Glue job.
    - script_location (str): S3 path to the Glue job script.
    - iam_role (str): IAM role ARN for the Glue job.
    - region_name (str): AWS region name. Default is 'your-region'.
    - **kwargs: Additional keyword arguments for job configuration (e.g., MaxCapacity, GlueVersion).

    Returns:
    - response (dict): AWS Glue response for the create_job or update_job call.
    """

    # Create an AWS Glue client
    glue_client = boto3.client('glue', region_name=region_name)

    # Define the job arguments
    job_args = {
        'Name': job_name,
        'Role': iam_role,
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': script_location,
            'PythonVersion': '3'
        },
        **kwargs  # Include additional job configuration arguments
    }

    try:
        # Attempt to create the Glue job
        response = glue_client.create_job(**job_args)
        print(f"Glue job '{job_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        # If the job already exists, update it with the provided parameters
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_args)
        print(f"Glue job '{job_name}' updated successfully.")

    return response


job_name = "your-glue-job-name"
script_location = "s3://path-to-your-script"
iam_role = "your-iam-role-arn"

# Optional: Define additional keyword arguments for job configuration
job_kwargs = {
    'MaxCapacity': 10.0,
    'GlueVersion': '2.0',
    'WorkerType': 'G.1X',
    'NumberOfWorkers': 10,
    'DefaultArguments': {
        '--TempDir': 's3://path-for-temp-data',
        '--job-language': 'python'
    }
}

# Replace 'your-region' with the actual AWS region you are using
create_or_update_glue_job(job_name, script_location, iam_role, region_name='your-region', **job_kwargs)
