import pandas as pd
import numpy as np
import os
from google.cloud import language_v1
from google.cloud import translate_v2 as translate
from apiclient import discovery
from google.oauth2 import service_account
import preprocessor as tp
from time import sleep
from requests.exceptions import ReadTimeout, ConnectionError
import logging


def translate_to_english(row, translate_client):
    text = row['full_text_clean']
    lang = row['lang']
    if lang != 'en':
        try:
            result = translate_client.translate(text, target_language="en")
        except ReadTimeout or ConnectionError:
            sleep(60)
            try:
                result = translate_client.translate(text, target_language="en")
            except ReadTimeout or ConnectionError:
                sleep(60)
                result = translate_client.translate(text, target_language="en")
        trans = result["translatedText"]
        return trans
    else:
        return text


def detect_sentiment(row, nlp_client, TYPE_, ENCODING_):
    text = row['full_text_en']
    document = {"content": text, "type_": TYPE_, "language": "en"}
    response = nlp_client.analyze_sentiment(request={'document': document, 'encoding_type': ENCODING_})
    return response.document_sentiment.score, response.document_sentiment.magnitude


def predict_sentiment_tweets():

    # get Google API credentials
    TYPE_ = language_v1.Document.Type.PLAIN_TEXT
    ENCODING_ = language_v1.EncodingType.UTF8
    service_account_info = "../credentials/google_service_account_secrets.json"
    credentials = service_account.Credentials.from_service_account_file(service_account_info)
    translate_client = translate.Client(credentials=credentials)
    nlp_client = language_v1.LanguageServiceClient(credentials=credentials)

    # load geolocated tweets
    input_path = "./tweets/tweets_latest_geolocated.csv"
    if not os.path.exists(input_path):
        logging.error("predict_sentiment_tweets: error: no processed tweets found")
    df_tweets = pd.read_csv(input_path, index_col=0)
    df_tweets = df_tweets.dropna(subset=['full_text_clean'])

    df_texts = df_tweets.drop_duplicates(subset=['full_text_clean'])

    # translate to english
    logging.info('predict_sentiment_tweets: translating to english')
    df_texts['full_text_en'] = df_texts.apply(lambda x: translate_to_english(x, translate_client), axis=1)

    # detect sentiment
    logging.info('predict_sentiment_tweets: detecting sentiment')
    df_texts['sentiment_score'], df_texts['sentiment_magnitude'] = \
        zip(*df_texts.apply(lambda x: detect_sentiment(x, nlp_client, TYPE_, ENCODING_), axis=1))

    logging.info('predict_sentiment_tweets: saving results')
    for ix, row in df_tweets.iterrows():
        df_texts_ = df_texts[df_texts['full_text_clean']==row['full_text_clean']]
        df_tweets.at[ix, 'full_text_en'] = df_texts_['full_text_en'].values[0]
        df_tweets.at[ix, 'sentiment_score'] = df_texts_['sentiment_score'].values[0]
        df_tweets.at[ix, 'sentiment_magnitude'] = df_texts_['sentiment_magnitude'].values[0]

    # save processed tweets locally
    processed_tweets_path = './tweets/tweets_latest_sentiment.csv'
    df_tweets.to_csv(processed_tweets_path, index=False)




