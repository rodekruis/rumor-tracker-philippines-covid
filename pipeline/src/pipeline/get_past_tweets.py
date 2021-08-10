# -*- coding: utf-8 -*-
# import datetime
from azure.storage.blob import BlobServiceClient, BlobClient
import os
import json
import tweepy
import pandas as pd
from requests.exceptions import Timeout, ConnectionError
from requests.packages.urllib3.exceptions import ReadTimeoutError
import ssl
import time
import logging


def get_past_tweets(blob_service_client):

    # initialize twitter API
    with open("../credentials/twitter_secrets.json") as file:
        twitter_secrets = json.load(file)
    auth = tweepy.OAuthHandler(twitter_secrets['CONSUMER_KEY'], twitter_secrets['CONSUMER_SECRET'])
    auth.set_access_token(twitter_secrets['ACCESS_TOKEN'], twitter_secrets['ACCESS_SECRET'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
    # ---------------------------------------------------------------------------------------------------------------------
    # wait_on_rate_limit= True; API automatically waits for rate limits to replenish
    # wait_on_rate_limit_notify = True; API prints a notification when Tweepy is waiting for rate limits to replenish
    # ---------------------------------------------------------------------------------------------------------------------

    # input queries to search on Twitter
    # "bakuna vaccine": all tweets mentioning vaccine and bakuna
    # "vaccine geocode:12.879721,121.774017,575mi": all tweets mentioning vaccine geo-located in the Philippines
    queries = ["bakuna vaccine", "vaccine geocode:12.879721,121.774017,575mi"]

    # Create a local directory to hold blob data
    local_path = "./tweets"
    os.makedirs(local_path, exist_ok=True)
    local_file_name = 'tweets_latest.json'
    upload_file_path = os.path.join(local_path, local_file_name)
    tweets = []

    # loop over queries and search
    for query in queries:
        n = 0
        try:
            for page in tweepy.Cursor(api.search,
                                      q=query,
                                      tweet_mode='extended',
                                      include_entities=True,
                                      max_results=100).pages():
                logging.info('processing page {0}'.format(n))
                try:
                    for tweet in page:
                        tweets.append(tweet)
                except Exception as e:
                    logging.warning("Some error occurred, skipping page {0}:".format(n))
                    logging.warning(e)
                    pass
                n += 1
        except Exception as e:
            logging.warning("Some error occurred, skipping query {0}:".format(query))
            logging.warning(e)
            pass

    with open(upload_file_path, 'a') as tf:
        for tweet in tweets:
            try:
                tf.write('\n')
                json.dump(tweet._json, tf)
            except Exception as e:
                logging.warning("Some error occurred, skipping tweet:")
                logging.warning(e)
                pass

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='tweets-raw/'+local_file_name)
    # Upload the created file
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # save a timestamped copy for future reference
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)
    reference_filename = 'tweets_' + today.strftime("%d-%m-%Y") + '_' + week_ago.strftime("%d-%m-%Y") + '.json'
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='tweets-raw/'+reference_filename)
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)