import pandas as pd
import os
import operator
import gensim
import numpy as np
import matplotlib.pyplot as plt
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
np.random.seed(2018)
import nltk
nltk.download('wordnet')
import pickle
stemmer = PorterStemmer()
from spellchecker import SpellChecker
spell = SpellChecker()
from pipeline.GSDMM import MovieGroupProcess
from pipeline.utils import preprocess, BreakIt, produce_mapping
from apiclient import discovery
from google.oauth2 import service_account
from datetime import timedelta, date
from numpy.random import multinomial
from numpy import log, exp
from numpy import argmax
import logging
from azure.storage.blob import BlobServiceClient, BlobClient


class CustomUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        try:
            return super().find_class(__name__, name)
        except AttributeError:
            return super().find_class(module, name)


def keywords_to_topic(df, df_topics):
    """assign a description ('theme') to each topic based on keywords"""
    for ix, row in df.iterrows():
        df_topics_ = df_topics[df_topics['topic number'] == row['topic number']]
        df.at[ix, 'topic'] = df_topics_['topic'].values[0]
    return df


def predict_topic_tweets(blob_service_client):

    refit = False
    model_filename = 'gsdmm-model-v1.pickle'
    keys_to_topic_filename = 'keys-to-topics-v1.csv'
    models_path = "./models"
    os.makedirs(models_path, exist_ok=True)
    model_filepath = os.path.join(models_path, model_filename)

    # load tweets
    input_path = "./tweets/tweets_latest_sentiment.csv"
    if not os.path.exists(input_path):
        logging.error("Error: no processed tweets found")
    df_tweets = pd.read_csv(input_path)

    text = df_tweets["full_text_en"]
    text = text[text != 'None'].astype(str)
    text = text[text.str.len() > 4]
    text = text.drop_duplicates()
    len_original = len(text)

    processed_ser = text.map(preprocess)
    processed_docs = [item[0] for item in processed_ser]
    mapping_list = [item[1] for item in processed_ser]
    mapping121, mapping12many = produce_mapping(mapping_list)

    # initialize and fit GSDMM model
    if not refit:
        # download topic model
        blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                          blob='models/' + model_filename)
        if os.path.exists(model_filepath):
            os.remove(model_filepath)
        with open(model_filepath, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        if os.path.exists(model_filepath):
            logging.info('loading existing topic model')
            model = CustomUnpickler(open(model_filepath, "rb")).load()
        else:
            logging.error("Error: no topic model found")
    else:
        logging.info('initialize and fit topic model')
        model = MovieGroupProcess(K=6, alpha=0.3, beta=0.05, n_iters=500)
        y = model.fit(processed_docs, len(processed_docs))
        pickle.dump(model, open(model_filepath, "wb"))

    # create list of topic descriptions (lists of keywords) and scores
    matched_topic_score_list = [model.choose_best_label(i) for i in processed_docs]
    matched_topic_list = [t[0] for t in matched_topic_score_list]
    score_list = [t[1] for t in matched_topic_score_list]
    text = pd.DataFrame({'text': text.values, 'topic_num': matched_topic_list, 'score': score_list})

    # create list of human-readable topic descriptions (de-lemmatize)
    logging.info('create list of human-readable topics (de-lemmatize)')
    topic_list = [list(reversed(sorted(x.items(), key=operator.itemgetter(1))[-5:])) for x in model.cluster_word_distribution]
    topic_list_flat = [[l[0] for l in x] for x in topic_list]
    topic_list_human_readable = topic_list_flat.copy()
    for ixt, topic in enumerate(topic_list_human_readable):
        for ixw, word in enumerate(topic):
            try:
                for raw in text.text.values:
                    for token in gensim.utils.simple_preprocess(raw):
                        if word in token:
                            topic_list_human_readable[ixt][ixw] = token
                            raise BreakIt
            except BreakIt:
                pass
    topic_list_human_readable = [[spell.correction(t) for t in l] for l in topic_list_human_readable]

    # create dataframe with best example per topic and topic description
    logging.info('create dataframe with best example per topic and topic description')
    df = pd.DataFrame()
    for topic_num, topic in enumerate(topic_list_human_readable):
        text_topic = text[text.topic_num == topic_num].sort_values(by=['score'], ascending=False).reset_index()
        frequency = len(text[text.topic_num == topic_num]) / len_original
        responses = len(text[text.topic_num == topic_num])
        if not text_topic.empty:
            representative_text = ';'.join(text_topic.iloc[:10]['text'].values.tolist())

            df = df.append(pd.Series({"topic number": int(topic_num),
                                      "example": representative_text,
                                      "keywords": ', '.join(topic),
                                      "frequency (%)": frequency * 100.,
                                      "number of responses": responses}), ignore_index=True)
    df = df.sort_values(by=['frequency (%)'], ascending=False)

    if not refit:
        # add topic descriptions and save topics locally
        blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                          blob='models/' + keys_to_topic_filename)
        topics_file_path = os.path.join(models_path, keys_to_topic_filename)
        if os.path.exists(topics_file_path):
            os.remove(topics_file_path)
        with open(topics_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        df_topics = pd.read_csv(topics_file_path)
        df = keywords_to_topic(df, df_topics)

    topic_dir = './topics'
    os.makedirs(topic_dir, exist_ok=True)
    df.to_csv(os.path.join(topic_dir, 'topics_latest_select.csv'))

    # assign topic to tweets
    logging.info('assign topic to tweets')
    for ix, row in text.iterrows():
        topic = df[df['topic number']==row['topic_num']]["topic"].values[0]
        df_tweets.at[df_tweets["full_text_en"]==row["text"], 'topic'] = topic

    # save processed tweets locally
    tweets_dir = './tweets'
    processed_tweets_path = os.path.join(tweets_dir, 'tweets_latest_topic.csv')
    df_tweets.to_csv(processed_tweets_path, index=False)

    # upload processed tweets
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='tweets-processed/tweets_latest.csv')
    with open(processed_tweets_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # upload a timestamped copy for future reference
    today = date.today()
    week_ago = today - timedelta(days=7)
    reference_filename = 'tweets_' + today.strftime("%d-%m-%Y") + '_' + week_ago.strftime("%d-%m-%Y") + '.csv'
    blob_client = blob_service_client.get_blob_client(container='covid-phl-rumor-tracker',
                                                      blob='tweets-processed/'+reference_filename)
    with open(processed_tweets_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)





