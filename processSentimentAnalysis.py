# import tar
# make sure runtime = Python 3.7
import csv
import json
import os
# import pandas as pd
import boto3
from datetime import datetime
import numpy as np
import pandas as pd
import re

# calls the AMazon comprehend api
api = boto3.client('comprehend')

# input bucket
s3 = boto3.client('s3')


# input_path= "tweetdump/csv/files/tmp/" + " "
# outputpath = "negativetweets/"

def lambda_handler(event, context):
    negCount = 0
    posCount = 0
    neuCount = 0
    bucket = 'tweetdump'
    if event:
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key'])
        fileObj = s3.get_object(Bucket=bucket, Key=filename)
        file_content = fileObj["Body"].read().decode('utf-8')

        rows = csv.reader(file_content.split("\n"))
        next(rows)
        for row in rows:
            if len(row) == 0:
                break

            text = row[2]
            # generate sentiment score for the text
            client = boto3.client('comprehend')
            sentiment = client.detect_sentiment(Text=text, LanguageCode='en')['SentimentScore']

            positive = sentiment["Positive"]
            neutral = sentiment["Neutral"]
            negative = sentiment["Negative"]
            mixed = sentiment["Mixed"]

            # TODO: Generate the alert score
            alert_score = 0
            print(row)
            # # formatting list of db row entries
            # dbRow = row[1:]

            # dbRow[1] = dbRow[1].replace("'", "\\\\'")
            # dbRow[10] = dbRow[10].replace("'", "\\\\'")
            # dbRow[11] = dbRow[11].replace("'", "\\\\'")

            # dbRow[2] = int(dbRow[2])
            # dbRow[3] = int(dbRow[3])

            # tweetdate = dbRow[4]
            # tokens = tweetdate.split(' ')
            # date_string = tokens[-1] + '-' + month_to_numbers(tokens[1]) + '-' + tokens[2] + ' ' + tokens[3]
            # dbRow[4] = date_string

            # numbers = []
            # numbers.append(int(dbRow[2]))
            # numbers.append(int(dbRow[3]))
            # numbers.append(positive)
            # numbers.append(neutral)
            # numbers.append(negative)
            # numbers.append(mixed)
            # numbers.append(alert_score)

            # dbRow.pop(2)
            # dbRow.pop(2)
            # print(dbRow)
            # print(numbers)

            # # Move to write_tweet_to_db
            # dbString = str(dbRow)
            # dbString = dbString.replace('"', "'")
            # body = dbString + "@@@projectmoodswing@@@" + str(numbers)
            # print(body)

            # ---------------------------------------- REMOVE ---------------------------------------

            # string formatting to generate row of values to insert into the db
            dbRow = "\"" + row[1] + "\",\"" + row[2] + "\"," + row[3] + "," + row[4] + ",\""
            tweetdate = row[5]
            tokens = tweetdate.split(' ')
            date_string = tokens[-1] + '-' + month_to_numbers(tokens[1]) + '-' + tokens[2] + ' ' + tokens[3]
            dbRow += date_string + "\",\""
            remString = "\",\"".join([str(x) for x in row[6:]])
            print("REM: " + remString)
            dbRow += remString + "\","
            sentimentString = "{0},{1},{2},{3},{4}".format(positive, neutral, negative, mixed, alert_score)
            dbRow += sentimentString
            print(dbRow)
            dbRow = dbRow.replace("'", "\\\\'")
            dbRow = dbRow.replace('"', "'")
            body = dbRow
            print(dbRow)
            break
            # TODO: If timeout error rerun it

            # Upload the processed sentiment and the tweet to a queue
            sqs = boto3.resource('sqs')
            queue = sqs.get_queue_by_name(QueueName='processed_tweets')

            # Define the messages's structure
            response = queue.send_message(MessageBody=body)

            print(response.get('MessageId'))
            print("Failure: " + str(response.get('Failure')))

    return {"message - ": "hi"}


def month_to_numbers(month):
    switcher = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    return switcher[month]