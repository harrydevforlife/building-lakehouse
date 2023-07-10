import pandas as pd
import requests
import constants as const

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .appName("app") \
    .master("local") \
    .getOrCreate()

imageJson=spark.read.json('../../temp/yelp_image_photos.json')

def fetch_poster(res_id):
    try:
        full_path="D:/temp/photos/"
        photo=imageJson.filter(col('business_id')==res_id).take(1)[0][3]
        return full_path+photo+".jpg"
    except:
        return "https://thuvienlogo.com/data/03/logo-nha-hang-dep-07.jpg"


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
