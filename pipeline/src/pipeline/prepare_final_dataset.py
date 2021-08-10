import os
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient
import logging
import datetime
import random


def prepare_final_dataset(blob_service_client):

    logging.info('prepare_final_dataset: merging')
    in_file = './tweets/tweets_latest_topic.csv'
    if not os.path.exists(in_file):
        logging.error("Error: no processed tweets found")
    df_tweets = pd.read_csv(in_file)

    # datetime to date
    df_tweets['created_at'] = pd.to_datetime(df_tweets['created_at'])
    df_tweets['created_at'] = df_tweets['created_at'].dt.date
    df_tweets['full_text_clean'] = df_tweets['full_text_clean'].str.replace(': ', '')
    df_tweets['full_text_en'] = df_tweets['full_text_en'].str.replace(': ', '')
    df_tweets = df_tweets[(~df_tweets['full_text_clean'].str.contains('#NAME?')) & (~df_tweets['full_text_en'].str.contains('#NAME?'))]

    if len(df_tweets) > df_tweets.id.nunique():
        logging.info('re-assigning id')
        for ix, row in df_tweets.iterrows():
            ran_id = random.randint(1, 1E18)
            if ran_id not in df_tweets.id.unique():
                df_tweets.at[ix, 'id'] = ran_id
        logging.info(f"{len(df_tweets)}, {df_tweets.id.nunique()}")

    # save locally
    os.makedirs('./powerbi', exist_ok=True)
    out_file = './powerbi/powerbi_latest.xlsx'
    df_tweets.to_excel(out_file, index=False, encoding='utf8')

    # merge with existing datasets
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='powerbi/powerbi_latest.xlsx')
    try:
        with open('./powerbi/powerbi_old.xlsx', "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        df_old = pd.read_excel('./powerbi/powerbi_old.xlsx')
        if 'Unnamed: 0' in df_old.columns:
            df_old = df_old.drop(columns=['Unnamed: 0'])
        res = pd.concat([df_tweets, df_old])
        res = res.drop_duplicates(subset=['id'])
        res.to_excel('./powerbi/powerbi_merged.xlsx', index=False)
    except:
        df_tweets.to_excel('./powerbi/powerbi_merged.xlsx', index=False, encoding='utf8')

    # upload processed tweets
    with open('./powerbi/powerbi_merged.xlsx', "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # upload a timestamped copy for future reference
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)
    reference_filename = 'powerbi_' + today.strftime("%d-%m-%Y") + '_' + week_ago.strftime("%d-%m-%Y") + '.xlsx'
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='powerbi/'+reference_filename)
    with open('./powerbi/powerbi_latest.xlsx', "rb") as data:
        blob_client.upload_blob(data, overwrite=True)



