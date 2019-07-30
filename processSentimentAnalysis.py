# import tar
# make sure runtime = Python 3.7
import csv
import json
import os
# import pandas as pd
import boto3
from datetime import datetime
import numpy
import pandas as pd
import re
import pymysql
import sys

db_host = "pmoodswingdb.c2mtswumiqvy.us-east-1.rds.amazonaws.com"
db_user = "pmuser"
db_password = "butterfly"
db_name = "pmoodswing"

# calls the AMazon comprehend api
api = boto3.client('comprehend')

# input bucket
s3 = boto3.client('s3')


# input_path= "tweetdump/csv/files/tmp/" + " "
# outputpath = "negativetweets/"

def lambda_handler(event, context):
    bucket = 'tweetdump'
    if event:
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key'])
        fileObj = s3.get_object(Bucket=bucket, Key=filename)
        file_content = fileObj["Body"].read().decode('utf-8')

        entries = file_content.splitlines()
        entries = entries[1:]
        for entry in entries:
            print("ENTRY: " + entry)
            # fields = entry.split(",")
            fields = ['{}'.format(x) for x in list(csv.reader([entry], delimiter=',', quotechar='"'))[0]]
            text = fields[2]
            print(text)

            client = boto3.client('comprehend')
            sentiment = client.detect_sentiment(Text=text, LanguageCode='en')['Sentiment']

            # TODO: Generate the alert score
            alert_score = 0

            break
        # df = pd.DataFrame.from_csv(fileObj)
        # print(df)

        # print(file_content)

    return {"message - ": "hi"}


def insert_csv_into_db(event, context):
    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 database=db_name,
                                 connect_timeout=10)
    with connection.cursor() as cur:
        sql = """insert into tweets (id_str, tweet_text, retweet_count, favorite_count, created_at, coordinates, in_reply_to_screen_name, user_id_str, user_location, user_screen_name, hashtags, mentions, product_group, score)
values (%s);"""
        cur.execute(sql, (insert_values))
        connection.commit()
        cur.close()
    connection.close()
