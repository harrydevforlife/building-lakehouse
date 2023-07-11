import pickle
from script.recommender import contend_based_recommendations, weighted_average_based_recommendations,read_item
from UI.widgets import initialize_res_widget, show_recommended_res_info, detail_item
import constants as const
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("app") \
    .master("local") \
    .getOrCreate()

st=const.st
components=st.components.v1
st.set_page_config(page_title="Recommender system", layout="wide")


# add styling
with open('./assets/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# # load main movie dataframe
with open('data/res_df.pickle', 'rb') as handle:
    res = pickle.load(handle)

with open('data/res_scores.pickle', 'rb') as handle:
    fullRes = pickle.load(handle)
    
# result=spark.read.json('./data/ress_df.json')
# res = [row[0] for row in result.select('name').distinct().collect()]

st.markdown('# Restaurant Recommender system')
social_components = open("assets/social_components.html", 'r', encoding='utf-8')
components.html(social_components.read())

# add search panel and search button
main_layout, search_layout = st.columns([10, 1])
options = main_layout.multiselect('Which restaurant do you like?', res["name"].unique())
show_recommended_res_btn = search_layout.button("search")

# add widgets on sidebar
recommended_res_num = st.sidebar.slider("Recommended restaurant number", min_value=5, max_value=10)
if recommended_res_num:
    const.RES_NUMBER = recommended_res_num
show_score = st.sidebar.checkbox("Show score")

def emptyStreamlit(res,st,name=None,col1=None,col2=None):
    if const.name!=None:
        if const.name in res["name"].values:
            Detail(const.name,res)
        else:
            st.warning("The restaurant was remove from visible!")

def Detail(name,res=res):
    # An Item
    streamlit=st
    const.name=None
    business_id,selected_rows=read_item(name,fullRes)
    detail_item(business_id,selected_rows,streamlit) 

    ## Related Item
    col_for_content_based = initialize_res_widget(streamlit)
    contend_based_recommended_res = contend_based_recommendations(res,[selected_rows.iloc[0]['name']])
    show_recommended_res_info(contend_based_recommended_res, col_for_content_based, show_score,streamlit)
    emptyStreamlit(res,streamlit,name)

def main():
    streamlit=st
    # create horizontal layouts for res
    col_for_score_based = initialize_res_widget(streamlit)
    col_for_content_based = initialize_res_widget(streamlit)
    # col_for_content_based_extra = initialize_res_widget(content_extra_based_cfg)

    # show recommended res based on weighted average (this is same for all res)
    score_based_recommended_res = weighted_average_based_recommendations(fullRes)
    show_recommended_res_info(score_based_recommended_res, col_for_score_based, show_score,streamlit)

    # when search clicked
    if show_recommended_res_btn:
        contend_based_recommended_res = contend_based_recommendations(res,options)
        show_recommended_res_info(contend_based_recommended_res, col_for_content_based, show_score,streamlit)

    emptyStreamlit(res,streamlit,None,col_for_score_based,col_for_content_based)

if __name__ == "__main__":
    print(const.name)
    if const.name==None:
        main()
    else:
        Detail(const.name)