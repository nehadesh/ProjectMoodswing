import tweepy
import json
import numpy
import pandas as pd
import boto3
import datetime
import re
import random
import base64
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    """ This function downloads tweets that are pulled up based on a particular search string"""
    # gets (group, search query list) tuple for 1 search group from the SQS queue
    search_group = download_from_queue(event, context)
    api_key, api_secret, access_token, access_token_secret = get_secret()

    # ----------------------- REMOVE RETURN--------------------
    api = authenticate(api_key, api_secret, access_token, access_token_secret)

    # extract the group/category and query from each search string
    group = search_group[0]
    query_list = search_group[1]
    file_path = search_group[2]

    for query in query_list:
        print("Group: " + group + " \nQuery:" + query)

        # try to search twitter for the current query, and if it fails due to a rate limit error, wait 15 mins
        print("Starting search upload...")
        search_tweets(query, group, api, file_path)
        print("Search upload complete...")
    return {
        'statusCode': 200,
        'message': "hi"
    }


def download_from_queue(event, context):
    search_strings_list = []
    s3 = boto3.resource('s3')
    print("EVENT: " + str(event))
    for record in event['Records']:
        filePath = record['body']

        obj = s3.Object('tweetdump', filePath)
        body = obj.get()['Body'].read()
        body = str(body)
        group = filePath[filePath.find('/') + 1: filePath.find('.txt')]
        raw_strings = body.split('{')
        for string in raw_strings:
            string = string[:string.find("}")]

            string = string.rstrip("\\r\\n").replace("\\r\\n", " ")
            string = string.strip()
            search_strings_list.append(string)

    return (group, search_strings_list[1:], filePath)


def authenticate(api_key, api_secret, access_token, access_token_secret):
    """ Authenticates access to API and returns api object """

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api


def clean_text(text):
    print("CLEANING TEXT---------------------------------------------------")
    tweet = text.lower()
    # stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])
    tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', '', tweet)  # remove URLs
    tweet = re.sub('@[^\s]+', '', tweet)  # remove usernames
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet)  # remove the # in #hashtag
    return tweet


def search_tweets(query, group, api, filePath):
    """ Saves search results of the twitter search query in a csv and stores it in an s3 bucket """
    print("------------------ SEARCHING TWEETS--------------------")
    json_list = []
    user_list = []
    entities_list = []
    hashtag_list = []
    mention_list = []
    group_list = []
    status_list = []
    try:
        status_list = api.search(q=query, count=100, lang="en")
        if len(status_list) == 0:
            print("NOTHING IN PAST 7 DAYS ----------------------------------------------------------------")
            # status list might be empty because we use the Standard Search API
            # which limits our search history to within the past 7 days
            # So in this case... do nothing
            return False
    except tweepy.RateLimitError as limitErr:
        api_key, api_secret, access_token, access_token_secret = get_secret()
        api = authenticate(api_key, api_secret, access_token, access_token_secret)
        status_list = api.search(q=query, count=100, lang="en")
        if len(status_list) == 0:
            print("NOTHING IN PAST 7 DAYS ----------------------------------------------------------------")
            # status list might be empty because we use the Standard Search API
            # which limits our search history to within the past 7 days
            # So in this case... do nothing
            return False

    # Data structuring --> removing nested jsons
    if len(status_list) != 0:
        for status in status_list:
            print("STATUS exists -----------------------------------")

            group_list.append(group)
            json_str = json.dumps(status._json)
            json_data = json.loads(json_str)

            # extracting required fields from nested user json
            user = json_data['user']

            del user['entities']
            user_list.append(user)
            entities = json_data['entities']

            # extracting hashtags from nested entities json
            hashtag_text = []
            hashtags = entities['hashtags']
            for hashtag in hashtags:
                hashtag_text.append(hashtag['text'])
            hashtag_list.append(hashtag_text)

            # extracting screen namesfrom nested entities json
            screen_names = []
            user_mentions = entities['user_mentions']
            for mention in user_mentions:
                screen_names.append(mention['screen_name'])
            mention_list.append(screen_names)

            entities_list.append(entities)

            del json_data['user']
            del json_data['entities']

            # remove special characters from tweet text
            text = json_data['text']
            print("ENTERING CLEAN TEXT--------------------------------------")
            tweet_string = clean_text(text)
            json_data['text'] = tweet_string
            print("DONE CLEANING--------------------------------------")

            # final list of tweet jsons with nested jsons removed
            json_list.append(json_data)

        # create a dataframe based on the list of tweet jsons
        json_df = pd.DataFrame(json_list)

        # extracting required fields
        json_df = json_df[['id_str', 'text', 'retweet_count', 'favorite_count', 'created_at', 'coordinates',
                           'in_reply_to_screen_name']]

        # creating user_df for nested user json
        user_df = pd.DataFrame(user_list)
        hashtag_series = pd.Series(hashtag_list)
        mention_series = pd.Series(mention_list)
        group_series = pd.Series(group_list)
        user_df = user_df[['id_str', 'location', 'screen_name']]

        # adding required user & entity fields to main json_df
        json_df['user_id_str'] = user_df['id_str']
        json_df['user_location'] = user_df['location']
        json_df['user_screen_name'] = user_df['screen_name']
        json_df['hashtags'] = hashtag_series
        json_df['mentions'] = mention_series
        json_df['group'] = group_series

        # convert data frame to csv and store in s3 bucket
        print("SAVING AS CSV-----------------------------")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('tweetdump')
        currDate = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        group = group.replace(' ', '-')
        filepath = '/tmp/tweets' + currDate + '.csv'
        with open(filepath, 'w+') as data:
            # write the csv string to a file called tweets(insert timestamp here).csv
            data.write(json_df.to_csv(encoding='utf-8'))
        key = 'csv/files' + filepath
        bucket.upload_file(filepath, key)


def get_secret():
    secret_name = "twitter/APIkeys/" + str(1 + random.randrange(2))
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            print("SECRET: " + secret)
            secrets = json.loads(secret)
            api_key = secrets["api_key"]
            api_secret = secrets['api_secret']
            access_token = secrets['access_token']
            access_token_secret = secrets['access_token_secret']

        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        return (api_key, api_secret, access_token, access_token_secret)