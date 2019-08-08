import json
import boto3

"""
@author : Harrison Banh
Uploads to a queue a message for each search string file that contains Twitter
searches for a specified group
"""


def lambda_handler(event, context):
    # AWS SQS resources
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='search_groups')
    bucket = 'tweetdump'
    filepath = 'search-group-files/'

    s3 = boto3.client('s3')
    bucket = s3.list_objects_v2(Bucket=bucket, Prefix=filepath)

    # Upload all the names of the search groups within the bucket as messages within SQS
    for object in bucket['Contents']:
        filename = object['Key']

        # Define the messages's structure
        response = queue.send_message(
            MessageBody=str(filename)
        )

        # Print failures
        print(response.get('MessageId'))
        print("Failure: " + str(response.get('Failure')))

    return None


