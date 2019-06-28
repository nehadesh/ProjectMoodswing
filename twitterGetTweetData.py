import tweepy
import json
import pandas as pd
from pandas import DataFrame
auth = tweepy.OAuthHandler('NzodV06H6DSmox23nXUru5HsX','IEDGvE2p0gMho1n5MwaXDCDdDkNC9iwlxtNhtnVVCI3RE2Fr95')
auth.set_access_token('951565558788608005-nKcZkVRC5v1XD54rnrDYihVUxOwG6p4', 'wBjZU436kNqCpANj2NNIs9CVQfSpAGwSKjMrzZnnINqOE')
api = tweepy.API(auth)


# (top 20) from our home timeline (posts of all accounts we've followed
#public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet.id, tweet.retweet_count, tweet.favorite_count, tweet.text)

# TODO - SAVE THE LINK TO THE TWEET SO THAT WE CAN GET BACK TO IT
#---------------------- get a list of tweet objects ------------------------------------
status_list = api.home_timeline()

#---------------------- get tweet objects in json format ------------------------------------
json_list = []
user_list = []
entities_list = []
hashtag_list = []
mention_list = []
for status in status_list:
    
    #status = status_list[0]
    json_str = json.dumps(status._json)
    json_data = json.loads(json_str)
    user = json_data['user']
    del user['entities']
    user_list.append(user)
    entities = json_data['entities']

    hashtag_text = []
    hashtags = entities['hashtags']
    for hashtag in hashtags: 
        hashtag_text.append(hashtag['text'])
    hashtag_list.append(hashtag_text)

    screen_names = []
    user_mentions = entities['user_mentions']
    for mention in user_mentions:
        screen_names.append(mention['screen_name'])
    mention_list.append(screen_names)

    entities_list.append(entities)
    # TODO - extract info from user & entities and append it to json_data
    del json_data['user']
    del json_data['entities']
    json_list.append(json_data)

# ---------------------- create a data frame of the list of tweets -----------------------------
json_df = pd.DataFrame(json_list)
# ---------------------- extract required fields ----------------------------------------------
json_df = json_df[['id_str','text','retweet_count','favorite_count','created_at','coordinates']]

user_df = pd.DataFrame(user_list)
hashtag_series = pd.Series(hashtag_list)
mention_series = pd.Series(mention_list)
user_df =  user_df[['id_str', 'location', 'screen_name']]
# entities_df = pd.DataFrame(entities_list)

json_df['user_id_str'] = user_df['id_str']
json_df['user_location'] = user_df['location']
json_df['user_screen_name'] = user_df['screen_name']
json_df['hashtags'] = hashtag_series
json_df['mentions'] = mention_series


# ---------------------- save as a csv file ----------------------------------------------
json_df.to_csv('TweetData.csv')


#df.drop(columns=['user'])
#df = df.reset_index(drop=True)
#df = pd.DataFrame(json_data)



