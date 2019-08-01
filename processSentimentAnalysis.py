import json
import pymysql
import sys
import boto3
import csv
import numpy as np

db_host = "pmoodswingdb.c2mtswumiqvy.us-east-1.rds.amazonaws.com"
db_user = "pmuser"
db_password = "butterfly"
db_name = "pmoodswing"


def lambda_handler(event, context):
    # Download the processed tweet from the queue
    # sqs = boto3.resource('sqs')

    # Get the queue
    # queue = sqs.get_queue_by_name(QueueName='processed_tweets')        
    # messages = queue.receive_messages(MessageAttributeNames=['SentimentData'], MaxNumberOfMessages=1)
    # message = messages[0]
    # sentimentData = message.message_attributes.get('SentimentData').get('StringValue')

    # message.delete()
    for record in event['Records']:
        dbString = record['body']

    # dbString = dbString[1:-1]
    # arrays = dbString.split("]@@@projectmoodswing@@@[")
    # dbStringRow = arrays[0].split("', '")
    # dbNumsRow = arrays[1].split(", ")
    # dbRow = [dbStringRow[0][1:]]
    # dbRow.append(dbStringRow[1])
    # dbRow.append(int(dbNumsRow[0]))
    # dbRow.append(int(dbNumsRow[1]))
    # dbRow.extend(dbStringRow[2:])
    # dbRow[-1] = dbRow[-1][:-1]
    # for field in dbNumsRow[2:]:
    #     dbRow.append(float(field))
    # print(dbRow)

    # dbEntry = str(dbRow)[1:-1]
    print(dbString)
    print("Begin connection")
    connection = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name, connect_timeout=10)
    print("Connected")
    with connection.cursor() as cur:
        sql = """insert into tweets (id_str, tweet_text, retweet_count, favorite_count, created_at, coordinates, 
        in_reply_to_screen_name, user_id_str, user_location, user_screen_name, hashtags, mentions, product_group, pos_sent, ntrl_sent, neg_sent, mixed_sent, overall_score)
                values (%s);""" % (dbString)

        # values (%s, %s, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %f, %f, %d)
        cur.execute(sql)
        connection.commit()
        cur.close()
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }