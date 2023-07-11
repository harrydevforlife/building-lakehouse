import os

import pandas as pd
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import constants as const
from script.s3_file import download, connect

spark = SparkSession.builder \
    .appName("app") \
    .master("local") \
    .getOrCreate()

pwd_dir = os.getcwd()

s3_connection = connect()

if os.path.exists(pwd_dir+"/tmp/photos.json") == False:
    download(s3_connection, "raw-data", "yelp/images/photos.json")

imageJson=spark.read.json(pwd_dir+"/tmp/photos.json")

def fetch_poster(res_id):
    try:
        photo=imageJson.filter(col('business_id')==res_id).take(1)[0][3]
        if os.path.exists(pwd_dir+"/tmp/"+photo+".jpg") == False:
            download(s3_connection, "raw-data", "yelp/images/photos/"+photo+".jpg")
        return pwd_dir+"/tmp/"+photo+".jpg"
    except:
        return "https://toohotel.com/wp-content/uploads/2022/09/TOO_restaurant_Panoramique_vue_Paris_nuit_v2-scaled.jpg"


def get_recommendations(res, names, cosine_sim):
    """in this function we find similarity score for specific res sorted
    and gets all metadata for it"""
    indices = pd.Series(res.index, index=res['name']).drop_duplicates()
    idx = {indices[t] for t in names}
    sim_scores = dict()
    for res_idx in idx:
        sim = cosine_sim[res_idx]
        for i, s in enumerate(sim):
            sim_scores[i] = s if s > sim_scores.get(i, 0) else sim_scores.get(i, 0)

    for i in idx:
        del sim_scores[i]

    sim_scores = list(sorted(sim_scores.items(), key=lambda item: item[1], reverse=True))[:const.RES_NUMBER]

    res_indices = [i[0] for i in sim_scores]
    res_similarity = [i[1] for i in sim_scores]
    return pd.DataFrame(zip(res['business_id'].iloc[res_indices], res['name'].iloc[res_indices], res_similarity),
                        columns=["business_id", "name", "score"])
