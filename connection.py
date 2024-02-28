import boto3

def create_glue_connection(connection_name, connection_properties, connection_type='JDBC', description='Description of the connection', region_name='your-region'):
    """
    Create an AWS Glue connection.

    Parameters:
    - connection_name (str): Name of the connection.
    - connection_properties (dict): Properties of the connection, e.g., JDBC connection URL, username, password.
    - connection_type (str): Type of the connection (e.g., 'JDBC', 'S3', etc.). Default is 'JDBC'.
    - description (str): Description of the connection. Default is 'Description of the connection'.
    - region_name (str): AWS region name.

    Returns:
    - response (dict): AWS Glue response for the create_connection call.
    """

    # Create an AWS Glue client
    glue_client = boto3.client('glue', region_name=region_name)

    # Attempt to create the connection
    try:
        response = glue_client.create_connection(
            ConnectionInput={
                'Name': connection_name,
                'Description': description,
                'ConnectionType': connection_type,
                'ConnectionProperties': connection_properties,
                # Add other parameters like 'PhysicalConnectionRequirements' if needed
            }
        )
        print(f"Connection '{connection_name}' created successfully.")
        return response
    except Exception as e:
        print(f"Error creating connection '{connection_name}': {e}")
        return None




connection_name = "your-connection-name"
connection_properties = {
    'CONNECTION_TYPE': 'JDBC',
    'JDBC_CONNECTION_URL': 'jdbc:mysql://database-url:3306/database-name',
    'JDBC_ENFORCE_SSL': 'false',
    'USERNAME': 'your-username',
    'PASSWORD': 'your-password'
}

# Replace 'your-region' with the actual AWS region you are using
create_glue_connection(connection_name, connection_properties, region_name='your-region')
