import json
import pymysql
import sys
import boto3
import csv
import numpy as np
import base64
from botocore.config import Config

db_host = "pmoodswingdb.c2mtswumiqvy.us-east-1.rds.amazonaws.com"
db_user = "pmuser"
db_password = "275UNSdNnh9ZcSJX"
db_name = "pmoodswing"
aws_region = "us-east-1"


def lambda_handler(event, context):
    # Unpacking the message for the sentiment data
    dataMap = {}
    for record in event['Records']:
        message_attributes = record['messageAttributes']

        print(message_attributes)
        ids = message_attributes['ids']['stringValue']
        ids = ids.split(' ')
        dataMap['id_str'] = ids[0]
        dataMap['id_str'] = '"' + dataMap['id_str'] + '"'
        dataMap['in_reply_to_screen_name'] = ids[1]
        dataMap['in_reply_to_screen_name'] = '"' + dataMap['in_reply_to_screen_name'] + '"'
        dataMap['user_id_str'] = ids[2]
        dataMap['user_id_str'] = '"' + dataMap['user_id_str'] + '"'
        dataMap['user_screen_name'] = ids[3]
        dataMap['user_screen_name'] = '"' + dataMap['user_screen_name'] + '"'

        scores = message_attributes['scores']['stringValue']
        scores = scores.split(' ')
        dataMap['pos_sent'] = float(scores[0])
        dataMap['ntrl_sent'] = float(scores[1])
        dataMap['neg_sent'] = float(scores[2])
        dataMap['mixed_sent'] = float(scores[3])
        dataMap['sentiment'] = str(scores[4])
        dataMap['sentiment'] = '"' + dataMap['sentiment'] + '"'

        dataMap['tweet_text'] = message_attributes['tweet_text']['stringValue']
        dataMap['tweet_text'] = '"' + dataMap['tweet_text'] + '"'

        counts = message_attributes['counts']['stringValue']
        counts = counts.split(' ')
        dataMap['retweet_count'] = int(counts[0])
        dataMap['favorite_count'] = int(counts[1])
        print(type(dataMap['retweet_count']), type(dataMap['favorite_count']))

        dataMap['created_at'] = message_attributes['created_at']['stringValue']
        dataMap['created_at'] = '"' + dataMap['created_at'] + '"'

        dataMap['coordinates'] = message_attributes['coordinates']['stringValue']
        dataMap['coordinates'] = '"' + dataMap['coordinates'] + '"'

        dataMap['user_location'] = message_attributes['user_location']['stringValue']
        dataMap['user_location'] = '"' + dataMap['user_location'] + '"'

        dataMap['hashtags'] = message_attributes['hashtags']['stringValue']
        dataMap['hashtags'] = '"' + dataMap['hashtags'] + '"'

        dataMap['mentions'] = message_attributes['mentions']['stringValue']
        dataMap['mentions'] = '"' + dataMap['mentions'] + '"'

        dataMap['product_group'] = message_attributes['group']['stringValue']
        dataMap['product_group'] = '"' + dataMap['product_group'] + '"'

    print(dataMap)

    print("Begin connection")
    secret = get_secret()
    secret_dict = eval(secret)
    connection = pymysql.connect(host=secret_dict['host'], user=secret_dict['username'],
                                 password=secret_dict['password'], database=secret_dict['dbname'], connect_timeout=10)
    print("Connected")
    with connection.cursor() as cur:
        insertQuery = """insert into tweets (id_str, tweet_text, retweet_count, favorite_count, created_at, coordinates, 
        in_reply_to_screen_name, user_id_str, user_location, user_screen_name, hashtags, mentions, product_group, 
        pos_sent, ntrl_sent, neg_sent, mixed_sent, sentiment)
                values (%s, %s, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %f, %f, %s);""" % (
        dataMap['id_str'], dataMap['tweet_text'], int(dataMap['retweet_count']), int(dataMap['favorite_count']),
        dataMap['created_at'], dataMap['coordinates'], dataMap['in_reply_to_screen_name'], dataMap['user_id_str'],
        dataMap['user_location'], dataMap['user_screen_name'], dataMap['hashtags'], dataMap['mentions'],
        dataMap['product_group'], dataMap['pos_sent'], dataMap['ntrl_sent'], dataMap['neg_sent'], dataMap['mixed_sent'],
        dataMap['sentiment'])

        updateQuery = """update tweets set tweet_text = %s, retweet_count = %d, favorite_count = %d, created_at =  %s, coordinates = %s, in_reply_to_screen_name = %s, user_id_str = %s, user_location = %s, user_screen_name = %s, hashtags = %s, mentions = %s, product_group = %s, pos_sent = %f, ntrl_sent = %f, neg_sent = %f, mixed_sent = %f, sentiment = %s where id_str = %s;""" % (
        dataMap['tweet_text'], int(dataMap['retweet_count']), int(dataMap['favorite_count']), dataMap['created_at'],
        dataMap['coordinates'], dataMap['in_reply_to_screen_name'], dataMap['user_id_str'], dataMap['user_location'],
        dataMap['user_screen_name'], dataMap['hashtags'], dataMap['mentions'], dataMap['product_group'],
        dataMap['pos_sent'], dataMap['ntrl_sent'], dataMap['neg_sent'], dataMap['mixed_sent'], dataMap['sentiment'],
        dataMap['id_str'])
        print("QUERY: " + updateQuery)
        try:
            cur.execute(insertQuery)
        except pymysql.IntegrityError as error:
            print("Detected error: " + str(error))
            print("Updating table instead.... ")
            cur.execute(updateQuery)
        connection.commit()
        cur.close()
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def get_secret():
    secret_name = "ProjMoodswing/MySQL"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',
                            region_name=aws_region,
                            config=Config(proxies={'https': 'http://fdcproxy.1dc.com:8080'}))
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return secret