import scipy
import pickle
from sklearn.metrics.pairwise import linear_kernel, cosine_similarity
import constants as const
from script.utils import get_recommendations


def weighted_average_based_recommendations(ress):
    """we have already saved dataframe which is sorted based on scores.
    and just read and get top res"""

    ress = ress.head(const.RES_NUMBER)
    ress = ress[["business_id", "name", "score"]]
    ress.columns = ["business_id", "name", "score"]
    return ress

def read_item(name,fullRes):
    selected_rows = fullRes.loc[fullRes["name"] == name]
    business_id=selected_rows.iloc[0]['business_id']
    return business_id,selected_rows

def contend_based_recommendations(res,titles):
    """read matrix create similarity function and call main function"""
    tfidf_matrix = scipy.sparse.load_npz('data/res_matrix.npz')
    print(tfidf_matrix.shape)
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
    print(cosine_sim.shape)
    return get_recommendations(res, titles, cosine_sim)


# def contend_based_recommendations_extra(res, titles):
#     """read matrix create similarity function and call main function"""
#     count_matrix = scipy.sparse.load_npz("data/count_matrix.npz")
#     cosine_sim = cosine_similarity(count_matrix, count_matrix)
#     return get_recommendations(res, titles, cosine_sim)

