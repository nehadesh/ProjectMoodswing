import tweepy
auth = tweepy.OAuthHandler('NzodV06H6DSmox23nXUru5HsX','IEDGvE2p0gMho1n5MwaXDCDdDkNC9iwlxtNhtnVVCI3RE2Fr95')
auth.set_access_token('951565558788608005-nKcZkVRC5v1XD54rnrDYihVUxOwG6p4', 'wBjZU436kNqCpANj2NNIs9CVQfSpAGwSKjMrzZnnINqOE')
api = tweepy.API(auth)


# (top 20) from our home timeline (posts of all accounts we've followed
#public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet.id, tweet.retweet_count, tweet.favorite_count, tweet.text)


#---------------------- get a list of tweet objects ------------------------------------
status_list = api.home_timeline()

#---------------------- get tweet objects in json format ------------------------------------
json_list = []
for status in status_list:
    #status = status_list[0]
    json_str = json.dumps(status._json)
    json_data = json.loads(json_str)
    user = json_data['user']
    entities = json_data['entities']
    # TODO - extract info from user & entities and append it to json_data
    del json_data['user']
    del json_data['entities']
    json_list.append(json_data)

# ---------------------- create a data frame of the list of tweets -----------------------------
df = pd.DataFrame(json_list)
# ---------------------- extract required fields ----------------------------------------------
df = df[['id_str','text','retweet_count','favorite_count','created_at','coordinates']]
# ---------------------- save as a csv file ----------------------------------------------
df.to_csv('TweetData.csv')


#df.drop(columns=['user'])
#df = df.reset_index(drop=True)
#df = pd.DataFrame(json_data)



