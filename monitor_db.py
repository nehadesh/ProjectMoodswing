import json
import boto3
import time
from botocore.config import Config


def lambda_handler(event, context):
    # connect to processed_tweets SQS
    sqs = boto3.resource('sqs', region_name='us-east-1')
    queue = sqs.get_queue_by_name(QueueName='processed_tweets')

    # get approximate number of messages
    message_count = int(queue.attributes.get('ApproximateNumberOfMessages'))
    message_count += int(queue.attributes.get('ApproximateNumberOfMessagesNotVisible'))

    while (True):
        # connect to the load_db lambda function
        lam = boto3.client('lambda', region_name='us-east-1')
        # check that the process_tweets SQS is empty
        if (message_count == 0):
            # invoke the load_db lambda function
            response = lam.invoke(FunctionName='load_db', InvocationType='Event')
        # exit the while loop
        break
    else:
        # wait 10 seconds before checking the message count again
        time.sleep(10)