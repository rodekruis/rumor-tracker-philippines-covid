# rumor-tracker-philippines-covid
Scripts to group tweets about COVID-19 into topics and do [sentiment analysis](https://en.wikipedia.org/wiki/Sentiment_analysis).

Built to support Philippines Red Cross (PRC) during COVID-19 response. 

## Introduction
This repo contains the code to:
1. Download tweets on a specific topic (e.g. COVID-19 vaccines)
2. Detect sentiment of the tweets (positive or negative?)
3. Group tweets into topics
4. Assign a topic and a representative example to each group

Built on top of [GSDMM: short text clustering](https://github.com/rwalk/gsdmm) and [Google Cloud Natural Language](https://cloud.google.com/natural-language).

N.B. the creation of groups (a.k.a. clustering) is automated, but the topic description is not. You need a human to read some representative examples of each group and come up with a meaningful, human-readable description.

## Setup
Generic requirements:
-   [Twitter developer account](https://developer.twitter.com/en/apply-for-access)
-   OPTIONAL (translate): [Google Cloud account](https://cloud.google.com/)
-   OPTIONAL (upload to Azure datalake): [Azure account](https://azure.microsoft.com/en-us/get-started/) and 

For 510: Google cloud service account credentials are accessible [here](https://console.cloud.google.com/apis/credentials?project=vaccination-rumors&folder=&organizationId=&supportedpurview=project), login credentials in Bitwarden.

**N.B. If you clone this repo for a new project, please create NEW credentials for Twitter, a NEW project in Google cloud (and activate needed APIs) and a NEW resource group in Azure**. Follow [these instructions](https://docs.google.com/document/d/182aQPVRZkXifHDNjmE66tj5L1l4IvAt99rxBzpmISPU/edit?usp=sharing) to make it operational in Azure and check a working example at [this link](https://portal.azure.com/#@rodekruis.nl/resource/subscriptions/b2d243bd-7fab-4a8a-8261-a725ee0e3b47/resourceGroups/510Global-Covid/providers/Microsoft.Logic/workflows/covid-phl-rumor-tracker/logicApp).

### with Docker
1. Install [Docker](https://www.docker.com/get-started)
3. Download vector input data from [here](https://rodekruis.sharepoint.com/sites/510-CRAVK-510/_layouts/15/guestaccess.aspx?docid=09ee1386e97b54b7cbd9399c730181efa&authkey=AelH_jSEguHCrGEp5gh2oyI&expiration=2022-07-04T22%3A00%3A00.000Z&e=OBsIge), unzip and move it to
```
vector/
```
5. Copy your Google, Twitter and Azure credentials in
```
credentials/
```
3. Build the docker image from the root directory
```
docker build -t rodekruis/rumor-tracker-philippines-covid .
```
4. Run and access the docker container
```
docker run -it --entrypoint /bin/bash rodekruis/rumor-tracker-philippines-covid
```
5. Check that everything is working by running the pipeline (see [Usage](https://github.com/rodekruis/rumor-tracker-philippines-covid#usage) below)


### Manual Setup
TBI

## Usage
```
Usage: run-pipeline
```

## Structure

1. Get tweets of past week
```
python get_past_tweets.py
```
2. Geolocate tweets based on locations mentioned
```
python parse_geolocate_tweets.py
```
3. Translate to English, detect sentiment
```
python get_sentiment_tweets.py
```
4. Group tweets in topics
```
python topic_modelling 
```
