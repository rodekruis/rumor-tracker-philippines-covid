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
from tqdm import tqdm
tqdm.pandas()

# get Google API credentials
TYPE_ = language_v1.Document.Type.PLAIN_TEXT
ENCODING_ = language_v1.EncodingType.UTF8
credentials = service_account.Credentials.from_service_account_file('vaccination-rumors-service-account-key.json')
translate_client = translate.Client(credentials=credentials)
nlp_client = language_v1.LanguageServiceClient(credentials=credentials)


def translate_to_english(row):
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


def detect_sentiment(text):
    document = {"content": text, "type_": TYPE_, "language": "en"}
    response = nlp_client.analyze_sentiment(request={'document': document, 'encoding_type': ENCODING_})
    return response.document_sentiment.score, response.document_sentiment.magnitude


# load geolocated tweets
tweets = 'tweets/tweets_latest2_geolocated_select.csv'
df_tweets = pd.read_csv(tweets, index_col=0)
print(df_tweets.head())
df_tweets = df_tweets.dropna(subset=['full_text_clean'])

# translate to english
print('translating to english')
df_tweets['full_text_en'] = df_tweets.progress_apply(translate_to_english, axis=1)
df_tweets = df_tweets[df_tweets.lang == 'en']
df_tweets['full_text_en'] = df_tweets['full_text_clean']
# save to csv
out_file = 'tweets/tweets_latest2_english_select.csv'
df_tweets.to_csv(out_file)

# detect sentiment
print('detecting sentiment')
df_tweets['sentiment_score'], df_tweets['sentiment_magnitude'] = zip(*df_tweets['full_text_en'].progress_apply(detect_sentiment))
# save to csv
out_file = 'tweets/tweets_latest2_sentiment_select.csv'
df_tweets.to_csv(out_file)


