import boto3
from datetime import datetime, timedelta

# Initialize a boto3 client for Cost Explorer
client = boto3.client('ce', region_name='us-east-1')

# Define the time period for the report
end_date = datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

# Use the get_cost_and_usage method to fetch the cost and usage data
response = client.get_cost_and_usage(
    TimePeriod={
        'Start': start_date,
        'End': end_date
    },
    Granularity='MONTHLY',
    Metrics=["UnblendedCost"],
    GroupBy=[
        {'Type': 'DIMENSION', 'Key': 'SERVICE'},
        {'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'}
    ]
)

# Process the response to generate your report
for result in response['ResultsByTime']:
    for group in result['Groups']:
        amount = group['Metrics']['UnblendedCost']['Amount']
        service = group['Keys'][0]
        account_id = group['Keys'][1]
        print(f"Account ID: {account_id}, Service: {service}, Amount: ${amount}")
