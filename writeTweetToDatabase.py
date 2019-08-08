import json
import pymysql
import sys
import boto3
import csv
import numpy as np

db_host = "pmoodswingdb.c2mtswumiqvy.us-east-1.rds.amazonaws.com"
db_user = "pmuser"
db_password = "275UNSdNnh9ZcSJX"
db_name = "pmoodswing"


def lambda_handler(event, context):
    # Unpacking the message for the sentiment data
    dataMap = {}
    for record in event['Records']:
        message_attributes = record['messageAttributes']

        print(message_attributes)
        ids = message_attributes['ids']['StringValue']
        ids = ids.split(' ')
        dataMap['id_str'] = ids[0]
        dataMap['id_str'] = '"' + dataMap['id_str'] + '"'
        dataMap['in_reply_to_screen_name'] = ids[1]
        dataMap['in_reply_to_screen_name'] = '"' + dataMap['in_reply_to_screen_name'] + '"'
        dataMap['user_id_str'] = ids[2]
        dataMap['user_id_str'] = '"' + dataMap['user_id_str'] + '"'
        dataMap['user_screen_name'] = ids[3]
        dataMap['user_screen_name'] = '"' + dataMap['user_screen_name'] + '"'

        scores = message_attributes['scores']['StringValue']
        scores = scores.split(' ')
        dataMap['pos_sent'] = float(scores[0])
        dataMap['ntrl_sent'] = float(scores[1])
        dataMap['neg_sent'] = float(scores[2])
        dataMap['mixed_sent'] = float(scores[3])
        dataMap['overall_score'] = float(scores[4])

        dataMap['tweet_text'] = message_attributes['tweet_text']['StringValue']
        dataMap['tweet_text'] = '"' + dataMap['tweet_text'] + '"'

        counts = message_attributes['counts']['StringValue']
        counts = counts.split(' ')
        dataMap['retweet_count'] = int(counts[0])
        dataMap['favorite_count'] = int(counts[1])
        print(type(dataMap['retweet_count']), type(dataMap['favorite_count']))

        dataMap['created_at'] = message_attributes['created_at']['StringValue']
        dataMap['created_at'] = '"' + dataMap['created_at'] + '"'

        dataMap['coordinates'] = message_attributes['coordinates']['StringValue']
        dataMap['coordinates'] = '"' + dataMap['coordinates'] + '"'

        dataMap['user_location'] = message_attributes['user_location']['StringValue']
        dataMap['user_location'] = '"' + dataMap['user_location'] + '"'

        dataMap['hashtags'] = message_attributes['hashtags']['StringValue']
        dataMap['hashtags'] = '"' + dataMap['hashtags'] + '"'

        dataMap['mentions'] = message_attributes['mentions']['StringValue']
        dataMap['mentions'] = '"' + dataMap['mentions'] + '"'

        dataMap['product_group'] = message_attributes['group']['StringValue']
        dataMap['product_group'] = '"' + dataMap['product_group'] + '"'

    print(dataMap)

    print("Begin connection")
    connection = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name, connect_timeout=10)
    print("Connected")
    with connection.cursor() as cur:
        query = """insert into tweets (id_str, tweet_text, retweet_count, favorite_count, created_at, coordinates, 
        in_reply_to_screen_name, user_id_str, user_location, user_screen_name, hashtags, mentions, product_group, 
        pos_sent, ntrl_sent, neg_sent, mixed_sent, overall_score)
                values (%s, %s, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %f, %f, %f);""" % (
        dataMap['id_str'], dataMap['tweet_text'], int(dataMap['retweet_count']), int(dataMap['favorite_count']),
        dataMap['created_at'], dataMap['coordinates'], dataMap['in_reply_to_screen_name'], dataMap['user_id_str'],
        dataMap['user_location'], dataMap['user_screen_name'], dataMap['hashtags'], dataMap['mentions'],
        dataMap['product_group'], dataMap['pos_sent'], dataMap['ntrl_sent'], dataMap['neg_sent'], dataMap['mixed_sent'],
        dataMap['overall_score'])

        # tweetValuesTuple = (dataMap['id_str'], dataMap['tweet_text'], int(dataMap['retweet_count']), int(dataMap['favorite_count']), dataMap['created_at'],dataMap['coordinates'], dataMap['in_reply_to_screen_name'], dataMap['user_id_str'], dataMap['user_location'], dataMap['user_screen_name'], dataMap['hashtags'], dataMap['mentions'], dataMap['product_group'], dataMap['pos_sent'], dataMap['ntrl_sent'], dataMap['neg_sent'], dataMap['mixed_sent'], dataMap['overall_score'])
        # cur.execute(query, tweetValuesTuple)
        cur.execute(query)
        connection.commit()
        cur.close()
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }