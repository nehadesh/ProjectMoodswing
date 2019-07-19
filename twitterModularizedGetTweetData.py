import tweepy
import json
import numpy
import pandas as pd
import boto3
import datetime


def lambda_handler(event, context):
    """ This function downloads tweets that are pulled up based on a particular search string"""

    # gets 10 search strings from the SQS search string queue
    search_strings = download_from_queue(event, context)
    api = authenticate()

    # extract the group/category and query from each search string
    for search_string in search_strings:
        group = search_string[0]
        query = '\"' + search_string[1] + '\"'
        print("Group: " + group + " \nQuery:" + query)

        # try to search twitter for the current query, and if it fails due to a rate limit error, wait 15 mins
        try:
            print("Starting search upload...")
            search_tweets(query, group, api)
            print("Search upload complete...")
        except tweepy.RateLimitError:
            time.sleep(15 * 60)

    return {
        'statusCode': 200,
        'message': "hi"
    }


def download_from_queue(event, context):
    # Get the service resource
    sqs = boto3.resource('sqs')

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName='twitter_searches.fifo')
    search_strings_list = []
    count = 0

    messages = queue.receive_messages(MessageAttributeNames=['Group', 'Query'], MaxNumberOfMessages=10)

    for message in messages:
        count += 1
        if message.message_attributes is not None:
            group = message.message_attributes.get('Group').get('StringValue')
            query = message.message_attributes.get('Query').get('StringValue')
            search_strings_list.append((group, query))

        # Let the queue know that the message is processed
        message.delete()

    return search_strings_list


def authenticate():
    """ Authenticates access to API and returns api object """

    auth = tweepy.OAuthHandler('NzodV06H6DSmox23nXUru5HsX', 'IEDGvE2p0gMho1n5MwaXDCDdDkNC9iwlxtNhtnVVCI3RE2Fr95')
    auth.set_access_token('951565558788608005-nKcZkVRC5v1XD54rnrDYihVUxOwG6p4',
                          'wBjZU436kNqCpANj2NNIs9CVQfSpAGwSKjMrzZnnINqOE')
    api = tweepy.API(auth)
    return api


def search_tweets(query, group, api):
    """ Saves search results of the twitter search query in a csv and stores it in an s3 bucket """
    print("------------------ SEARCHING TWEETS--------------------")
    json_list = []
    user_list = []
    entities_list = []
    hashtag_list = []
    mention_list = []
    group_list = []

    # calls the search function of the API and gets the 100 most recent tweets
    status_list = api.search(q=query, count=100)
    if not status_list:
        # status list might be empty because we use the Standard Search API
        # which limits our search history to within the past 7 days
        # So in this case... do nothing
        return 0

    status_count = 0

    # Data structuring --> removing nested jsons
    for status in status_list:
        status_count += 1

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

        # final list of tweet jsons with nested jsons removed
        json_list.append(json_data)

    # create a dataframe based on the list of tweet jsons
    json_df = pd.DataFrame(json_list)

    if status_list:
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
        print("----------------------------- SAVING AS CSV-----------------------------")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('tweetdump')
        filepath = '/tmp/tweets' + str(datetime.datetime.now()) + '.csv'
        with open(filepath, 'w+') as data:
            # write the csv string to a file called tweets(insert timestamp here).csv
            data.write(json_df.to_csv())
        key = 'csv/' + filepath
        bucket.upload_file(filepath, key)

    return status_count