import datetime
import json
import os
from pipeline.get_past_tweets import get_past_tweets
from pipeline.parse_geolocate_tweets import parse_geolocate_tweets
from pipeline.predict_sentiment_tweets import predict_sentiment_tweets
from pipeline.predict_topic_tweets import predict_topic_tweets
from pipeline.prepare_final_dataset import prepare_final_dataset
from azure.storage.blob import BlobServiceClient, BlobClient
import logging
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG, filename='ex.log')
# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)


def main():
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    with open("../credentials/blobstorage_secrets.json") as file:
        blobstorage_secrets = json.load(file)
    blob_service_client = BlobServiceClient.from_connection_string(blobstorage_secrets['connection_string'])

    try:
        get_past_tweets(blob_service_client)
    except Exception as e:
        logging.error(f'Error in get_past_tweets: {e}')
    try:
        parse_geolocate_tweets(blob_service_client)
    except Exception as e:
        logging.error(f'Error in parse_geolocate_tweets: {e}')
    try:
        predict_sentiment_tweets()
    except Exception as e:
        logging.error(f'Error in predict_sentiment_tweets: {e}')
    try:
        predict_topic_tweets(blob_service_client)
    except Exception as e:
        logging.error(f'Error in predict_topic_tweets: {e}')
    try:
        prepare_final_dataset(blob_service_client)
    except Exception as e:
        logging.error(f'Error in prepare_final_dataset: {e}')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


if __name__ == "__main__":
    main()