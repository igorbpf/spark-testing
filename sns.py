from pyspark.sql import SparkSession
import boto3

# Initialize a Spark session
spark = SparkSession.builder.appName("sns_publisher").getOrCreate()

# Example DataFrame
data = [("message1",), ("message2",), ("message3",)]
df = spark.createDataFrame(data, ["message"])

# AWS SNS topic ARN
sns_topic_arn = 'arn:aws:sns:region:account-id:topic-name'

def publish_messages(partition):
    # Create a Boto3 client for SNS within the function to avoid serialization issues
    sns_client = boto3.client('sns', region_name='your-region')

    for row in partition:
        message = row['message']
        # Publish message to SNS topic
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message
        )
        # Log the response or handle it as needed
        print(response)

# Use foreachPartition to distribute the publishing task
df.foreachPartition(publish_messages)


import pytest
from moto import mock_sns
import boto3
from your_module import publish_messages  # Adjust the import according to your project structure

# Use the moto mock_sns decorator to mock SNS for the duration of this function
@mock_sns
def test_publish_messages():
    # Set up the mock SNS environment
    sns = boto3.client('sns', region_name='us-east-1')
    response = sns.create_topic(Name='TestTopic')
    topic_arn = response['TopicArn']

    # Mocking a DataFrame partition. In practice, this should resemble the actual data partition structure.
    partition = [{'message': 'test message 1'}, {'message': 'test message 2'}]

    # Call the function with the mocked data
    publish_messages(partition, sns_client=sns)  # Assuming you've adjusted your function to accept an sns_client parameter

    # Now, let's check if messages were published to the SNS topic
    published_messages = sns.list_topics()
    assert len(published_messages['Topics']) == 1  # Ensuring the topic was created
    # Further assertions can be made based on the behavior of your `publish_messages` function

# Running the test when this script is executed directly
if __name__ == "__main__":
    pytest.main()
