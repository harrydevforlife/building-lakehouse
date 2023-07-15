
import pandas as pd

from script.api import request_recommend
from script.connect import Connect


def make_card_element(userid, res_number,model,StructType=StructType,StructField=StructField,IntegerType=IntegerType,explode=explode,col=col,spark=spark):

    recommended_res = request_recommend({
        "userid": userid,
        "res_num": res_number*100,
        "token": "systemapi"
        }
    )

    hive_conn = Connect()
    
    results = {
        "business_id": [],
        "name": [],
        "score": [],
        }

    # recommend for user
    userid = [ [res.get('userid')]]
    useridColumns = StructType([StructField("userid", IntegerType())])
    userdf = spark.createDataFrame(data=userid, schema = useridColumns)
    ## predict by model
    userSubsetRecs = model.recommendForUserSubset(userdf, res_number*100)
    nrecommendations = userSubsetRecs\
        .withColumn("rec_exp", explode("recommendations"))\
        .select('userid', col("rec_exp.businessid"), col("rec_exp.rating"))
    
    lst = nrecommendations.select('businessid').rdd.flatMap(lambda x: x).collect()
    recomList = list(lst)
    
    for res in recommended_res["results"]:
        businessid = hive_conn.get_fetchone(f"SELECT DISTINCT(business_id) FROM bronze.restaurant_transform WHERE businessid = '{res.get('businessid')}' and businessid IN ({','.join(str(id) for id in recomList)}) ORDER BY CASE WHEN customer_id IN ({','.join(str(id) for id in recomList)}) THEN 0 ELSE 1 END, businessid")
        name = hive_conn.get_fetchone(f"SELECT DISTINCT(name) FROM bronze.restaurant_transform WHERE businessid = '{res.get('businessid')}' and businessid IN ({','.join(str(id) for id in recomList)}) ORDER BY CASE WHEN customer_id IN ({','.join(str(id) for id in recomList)}) THEN 0 ELSE 1 END, businessid")
        results["business_id"].append(businessid)
        results["name"].append(name)
        results["score"].append(res.get("rating"))
        if len(results)==res_number:
            break

    hive_conn.close()

    print(results)
    return pd.DataFrame.from_dict(results)