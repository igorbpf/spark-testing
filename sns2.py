from unittest.mock import patch
import boto3
from moto import mock_sns
import your_module  # Adjust this to the module where your publish function is

@mock_sns
def test_publish_messages():
    with mock_sns():
        # Create an SNS client and a topic for testing
        sns = boto3.client('sns', region_name='us-east-1')
        response = sns.create_topic(Name='TestTopic')
        topic_arn = response['TopicArn']

        # Patch the publish method on the SNS client
        with patch.object(sns, 'publish') as mock_publish:
            # Assuming your publish_messages function accepts a topic ARN and a list of messages
            your_module.publish_messages(topic_arn, ['message1', 'message2'])

            # Now you can assert that publish was called
            mock_publish.assert_called()
            # And you can get even more specific, like checking how many times it was called
            assert mock_publish.call_count == 2
            # Or inspecting the arguments it was called with
            mock_publish.assert_any_call(Message='message1', TopicArn=topic_arn)
            mock_publish.assert_any_call(Message='message2', TopicArn=topic_arn)


vimport pytest
from moto import mock_sqs
import boto3
from your_module import publish_messages_to_sqs  # Adjust this import to your project structure

@mock_sqs
def test_publish_messages_to_sqs():
    # Create a mock SQS environment
    sqs = boto3.client('sqs', region_name='us-east-1')
    response = sqs.create_queue(QueueName='TestQueue')
    queue_url = response['QueueUrl']

    # Mocking a DataFrame partition. In practice, this should resemble the actual data partition structure.
    partition = [{'message': 'test message 1'}, {'message': 'test message 2'}]

    # Call your function to publish messages to the queue
    publish_messages_to_sqs(partition)

    # Initialize an empty list to hold the received messages
    received_messages = []

    # Loop to receive messages until all are retrieved
    while True:
        # Receive messages from the queue
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)

        # If no messages are returned, break the loop
        if 'Messages' not in messages:
            break

        # Extend the list of received messages
        received_messages.extend(messages['Messages'])

        # More robust implementations might handle message visibility and deletion for real-world scenarios

    # Now you can assert the number of received messages matches the number of messages sent
    assert len(received_messages) == len(partition), "The number of received messages does not match the expected count."

    # Further assertions can be made based on the content of the messages
