import streamlit as st
import streamlit.components.v1 as components
import pickle
from script.utils import fetch_poster
from UI.widgets import initialize_res_widget, show_recommended_res_info
from script.recommender import contend_based_recommendations
from config import content_based_cfg
import constants as const
import os

with open('data/res_scores.pickle', 'rb') as handle:
    fullRes = pickle.load(handle)

with open('data/res_df.pickle', 'rb') as handle:
    res = pickle.load(handle)

recommended_res_num = st.sidebar.slider("Recommended restaurant number", min_value=5, max_value=10)
if recommended_res_num:
    const.RES_NUMBER = recommended_res_num
show_score = st.sidebar.checkbox("Show score")

def Detail(name,res=res):
    # An Item
    const.name=None
    st.empty()
    selected_rows = fullRes.loc[fullRes["name"] == name]
    business_id=selected_rows.iloc[0]['business_id']
    image=fetch_poster(business_id)
    with open('assets/style.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    # Use the container class to wrap your elements
    detail = open("assets/detail.html", 'r', encoding='utf-8').read()
    detail=detail.replace("{{ img }}", image)
    detail=detail.replace("{{ name }}", selected_rows.iloc[0]['name'])
    detail=detail.replace("{{ categories }}", selected_rows.iloc[0]['categories'])
    detail=detail.replace("{{ add }}", selected_rows.iloc[0]['address'])
    detail=detail.replace("{{ score }}", str(round(selected_rows.iloc[0]['score'],2)))
    is_open="Opening" if selected_rows.iloc[0]['is_open']==1 else 'Closed'
    detail=detail.replace("{{ is_open }}", is_open)
    st.markdown(detail, unsafe_allow_html=True)

    ## Related Item
    col_for_content_based = initialize_res_widget(content_based_cfg)
    contend_based_recommended_res = contend_based_recommendations(res,[selected_rows.iloc[0]['name']])
    show_recommended_res_info(contend_based_recommended_res, col_for_content_based, show_score)
    if const.name!=None:
        if const.name in res["name"].values:
            Detail(const.name,res)
        else:
            st.warning("The restaurant was remove from visible!")

# Detail('Sustainable Wine Tours',res)
Detail('Southside  Hardware',res)