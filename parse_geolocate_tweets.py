import pandas as pd
import numpy as np
import preprocessor as tp
from shapely.geometry import Polygon, Point
import geopandas as gpd
import json
import os


def match_location(x, loc_list, gdf):
    matching = [s for s in loc_list if s in x]
    if len(matching) > 0:
        loc = matching[0]
        for col in ['ADM1_EN', 'ADM2_EN', 'ADM3_EN', 'ADM3_REF', 'ADM3ALT1EN', 'ADM3ALT2EN']:
            gdf_loc = gdf[gdf[col].str.lower() == loc]
            if not gdf_loc.empty:
                return gdf_loc['geometry'].values[0]
        return np.nan
    else:
        return np.nan


def extract_location(x):
    if not pd.isna(x):
        bbox = Polygon(x['bounding_box']['coordinates'][0])
        centroid = bbox.centroid.coords
        return Point(centroid)
    else:
        return np.nan


# load vector file containing location names
gdf = gpd.read_file(r"C:\Users\JMargutti\OneDrive - Rode Kruis\Rode Kruis\ERA\shapefiles\phl_admbndp_admALL_psa_namria_itos_20200529.shp", encoding='utf8')
locations_all = gdf['ADM1_EN'].unique().tolist() + gdf['ADM2_EN'].unique().tolist() + gdf['ADM3_EN'].unique().tolist() + gdf['ADM3_REF'].dropna().unique().tolist() + gdf['ADM3ALT1EN'].dropna().unique().tolist() + gdf['ADM3ALT2EN'].dropna().unique().tolist()
print(f'locations: {len(locations_all)}')
locations_all = [x.lower() for x in locations_all]
locations_all.remove('bakun')

# load tweets
tweets = 'tweets/tweets_latest2_geolocated_select.csv'
df_tweets = pd.DataFrame()
for file in os.listdir('tweets'):
    if file.endswith('.json'):# and ('15-02' in file or '22-02' in file):
        print(file)
        df_tweets_ = pd.read_json(os.path.join('tweets', file), lines=True)
        df_tweets = df_tweets.append(df_tweets_, ignore_index=True)

# drop duplicates
df_tweets = df_tweets.drop_duplicates(subset=['id'])
print(df_tweets.head())

# parse locations (geolocation)
df_tweets['coord'] = df_tweets['place'].apply(extract_location)
df_tweets['full_text_clean'] = df_tweets['full_text'].apply(tp.clean)
df_tweets['full_text_clean'] = df_tweets['full_text_clean'].str.lower()
df_tweets['location'] = df_tweets['full_text_clean'].apply(lambda x: match_location(x, locations_all, gdf))
df_tweets['coord'] = df_tweets['coord'].fillna(df_tweets['location'])
df_tweets = df_tweets.drop(columns=['location', 'place'])
print(df_tweets.head())
print(f'{len(df_tweets)} tweets, {df_tweets.coord.count()} geo-located')

# save as csv
df_tweets.to_csv(tweets)

# save as geojson
gdf = gpd.GeoDataFrame(df_tweets[~pd.isna(df_tweets.coord)], geometry='coord')
out_file = tweets.replace('.csv', '.geojson')
gdf[['id', 'coord']].to_file(out_file, driver='GeoJSON')


