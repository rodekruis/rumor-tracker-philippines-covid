import datetime
from azure.storage.blob import BlobServiceClient, BlobClient
import logging
import os
# -*- coding: utf-8 -*-
try:
    import json
except ImportError:
    import simplejson as json
import ssl
import time
import pandas as pd
import geopandas as gpd
import numpy as np
import preprocessor as tp
import reverse_geocoder as rg
import random
from shapely.geometry import Polygon, Point
import logging


def extract_location(x):
    if not pd.isna(x):
        bbox = Polygon(x['bounding_box']['coordinates'][0])
        coords = bbox.centroid.coords[0]
        coords = (coords[1], coords[0])
        match = rg.search(coords, mode=1)[0]['admin2'].lower()
        match = match.replace('province of ', '')
        match = match.replace('makati city', 'makati')
        match = match.replace('city of ', '')
        ri = random.randint(1, 4)
        if ri == 1:
            match = match.replace('manila', 'ncr, manila, first district')
        elif ri == 2:
            match = match.replace('manila', 'ncr, second district')
        elif ri == 3:
            match = match.replace('manila', 'ncr, third district')
        elif ri == 4:
            match = match.replace('manila', 'ncr, fourth district')
        return match
    else:
        return np.nan


def extract_coordinates(x):
    if not pd.isna(x):
        bbox = Polygon(x['bounding_box']['coordinates'][0])
        centroid = bbox.centroid.coords
        return Point(centroid)
    else:
        return np.nan


def match_list_to_df(row, matching, df):
    loc = matching[0]
    for col in ['ADM1_EN', 'ADM2_EN', 'ADM3_EN', 'ADM3_REF', 'ADM3ALT1EN', 'ADM3ALT2EN']:
        df_loc = df[df[col].str.lower() == loc]
        if not df_loc.empty:
            row['ADM1_EN'] = df_loc['ADM1_EN'].values[0]
            row['ADM2_EN'] = df_loc['ADM2_EN'].values[0]
            return row
    return row


def match_location(row, loc_list, df):
    x = row['full_text_clean']
    row['ADM1_EN'] = np.nan
    row['ADM2_EN'] = np.nan
    if not pd.isna(x):
        matching = [s for s in loc_list if s in x]
        if len(matching) > 0:
            return match_list_to_df(row, matching, df)
        else:
            if not pd.isna(row['place']):
                x = extract_location(row['place'])
                matching = [s for s in loc_list if s in x]
                if len(matching) > 0:
                    return match_list_to_df(row, matching, df)
                else:
                    return row
            else:
                return row
    else:
        return row


def parse_geolocate_tweets(blob_service_client):

    # download locations
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='locations/locations.csv')
    locations_path = "./locations"
    os.makedirs(locations_path, exist_ok=True)
    location_file_path = os.path.join(locations_path, 'locations.csv')
    if os.path.exists(location_file_path):
        os.remove(location_file_path)
    with open(location_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    df = pd.read_csv(location_file_path, encoding='utf8')
    locations_all = df['ADM1_EN'].unique().tolist() + \
                    df['ADM2_EN'].unique().tolist() + \
                    df['ADM3_EN'].unique().tolist() + \
                    df['ADM3_REF'].dropna().unique().tolist() + \
                    df['ADM3ALT1EN'].dropna().unique().tolist() + \
                    df['ADM3ALT2EN'].dropna().unique().tolist()
    locations_all = [x.lower() for x in locations_all]
    locations_all.remove('bakun')

    # download geodata
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='geodata/phl_admbnda_adm2_psa_namria_20200529.json')
    geodata_path = "./geodata"
    os.makedirs(geodata_path, exist_ok=True)
    geodata_file_path = os.path.join(geodata_path, 'phl_admbnda_adm2_psa_namria_20200529.json')
    if os.path.exists(geodata_file_path):
        os.remove(geodata_file_path)
    with open(geodata_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    gdf_adm = gpd.read_file(geodata_file_path)

    # load tweets
    df_tweets = pd.DataFrame()
    tweets_path = './tweets/tweets_latest.json'
    if not os.path.exists(tweets_path):
        blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                          blob='tweets-raw/tweets_latest.json')
        tweetdata_path = "./tweets"
        os.makedirs(tweetdata_path, exist_ok=True)
        with open(tweets_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
    if not os.path.exists(tweets_path):
        logging.error("Error: no raw tweets found")
    df_tweets_ = pd.read_json(tweets_path, lines=True)
    df_tweets = df_tweets.append(df_tweets_, ignore_index=True)
    print(f'{len(df_tweets)}')
    # drop duplicates
    df_tweets = df_tweets.drop_duplicates(subset=['id'])
    print(f'---> {len(df_tweets)}')

    # parse locations (geolocation)
    df_tweets['full_text_clean'] = df_tweets['full_text'].apply(tp.clean)
    df_tweets['full_text_clean'] = df_tweets['full_text_clean'].str.lower()
    df_tweets['full_text_clean'] = df_tweets['full_text_clean'].str.replace(': ', '')

    df_tweets = df_tweets.apply(lambda x: match_location(x, locations_all, df), axis=1)

    # add geolocated tweets
    df_tweets['coord'] = df_tweets['place'].apply(extract_coordinates)
    gdf_tweets = gpd.GeoDataFrame(df_tweets[~pd.isna(df_tweets.coord)], geometry='coord', crs="EPSG:4326")
    if len(gdf_tweets) > 0:
        gdf_tweets = gdf_tweets[['id', 'coord']]
        res_union = gpd.overlay(gdf_tweets, gdf_adm, how='intersection')
        for ix, row in df_tweets.iterrows():
            if row['id'] in res_union['id'].unique():
                df_tweets.at[ix, 'ADM1_EN'] = res_union[res_union['id'] == row['id']]['ADM1_EN'].values[0]
                df_tweets.at[ix, 'ADM2_EN'] = res_union[res_union['id'] == row['id']]['ADM2_EN'].values[0]
            else:
                df_tweets.at[ix, 'ADM1_EN'] = np.nan
                df_tweets.at[ix, 'ADM2_EN'] = np.nan

    # remove unnecessary data
    for col in ['full_text', 'truncated', 'display_text_range', 'entities', 'extended_entities',
                'metadata', 'source', 'in_reply_to_status_id', 'in_reply_to_status_id_str',
                'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name',
                'user', 'geo', 'coordinates', 'contributors', 'is_quote_status',
                'possibly_sensitive', 'place', 'retweet_count', 'favorite_count',
                'retweeted_status', 'quoted_status_id', 'quoted_status_id_str', 'quoted_status',
                'favorited', 'retweeted', 'coord']:
        if col in df_tweets.columns:
            df_tweets = df_tweets.drop(columns=[col])
    logging.info(df_tweets.head())
    logging.info(f'{len(df_tweets)} tweets found, {df_tweets["ADM1_EN"].count()} geo-located')

    # save processed tweets locally
    processed_tweets_path = "./tweets/tweets_latest_geolocated.csv"
    df_tweets.to_csv(processed_tweets_path)

