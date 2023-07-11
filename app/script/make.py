
import pandas as pd

from script.api import request_recommend
from script.connect import Connect


def make_card_element(userid, res_number):

    recommended_res = request_recommend({
        "userid": userid,
        "res_num": res_number,
        "token": "systemapi"
        }
    )

    hive_conn = Connect()
    
    results = {
        "business_id": [],
        "name": [],
        "score": [],
        }
    for res in recommended_res["results"]:
        businessid = hive_conn.get_fetchone(f"SELECT DISTINCT(business_id) FROM bronze.restaurant_transform WHERE businessid = '{res.get('businessid')}'")
        name = hive_conn.get_fetchone(f"SELECT DISTINCT(name) FROM bronze.restaurant_transform WHERE businessid = '{res.get('businessid')}'")

        results["business_id"].append(businessid)
        results["name"].append(name)
        results["score"].append(res.get("rating"))

    hive_conn.close()

    print(results)
    return pd.DataFrame.from_dict(results)