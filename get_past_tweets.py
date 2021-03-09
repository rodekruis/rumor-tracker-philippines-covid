# -*- coding: utf-8 -*-
try:
    import json
except ImportError:
    import simplejson as json
import tweepy
import pandas as pd
import datetime
from requests.exceptions import Timeout, ConnectionError
from requests.packages.urllib3.exceptions import ReadTimeoutError
import ssl
import time
import os
import logging
logging.basicConfig(filename='tweets/get_past_tweets.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


# get credentials to use Twitter API
with open('twitter-api-keys.json') as f:
    keys = json.load(f)
    ACCESS_TOKEN = keys['ACCESS_TOKEN']
    ACCESS_SECRET = keys['ACCESS_SECRET']
    CONSUMER_KEY = keys['CONSUMER_KEY']
    CONSUMER_SECRET = keys['CONSUMER_SECRET']

# initialize Twitter API
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
# ---------------------------------------------------------------------------------------------------------------------
# wait_on_rate_limit= True; API automatically waits for rate limits to replenish
# wait_on_rate_limit_notify = True; API prints a notification when Tweepy is waiting for rate limits to replenish
# ---------------------------------------------------------------------------------------------------------------------

# input queries to search on Twitter
queries = ['bakuna vaccine', 'vaccine geocode:12.879721,121.774017,575mi']
# get today's date and one week ago      
today = datetime.date.today()
week_ago = today - datetime.timedelta(days=7)
os.makedirs('tweets', exist_ok=True)

# loop over queries and search
for query in queries:
    # save output as
    save_file = 'tweets/tweets_'+query[:6]+'_['+today.strftime("%d-%m-%Y")+', '+week_ago.strftime("%d-%m-%Y")+'].json'
    n = 0
    try:
        for page in tweepy.Cursor(api.search, q=query, tweet_mode='extended', include_entities=True,
                                  max_results=100).pages():
            print(f'processing page {n}')
            for tweet in page:
                try:
                    with open(save_file, 'a') as tf:
                        tf.write('\n')
                        json.dump(tweet._json, tf)
                except tweepy.TweepError:
                    logging.warning("Some error occurred, skipping tweet")
                    pass
            n += 1
    except:
        logging.warning("Some error occurred again, skipping", query)
