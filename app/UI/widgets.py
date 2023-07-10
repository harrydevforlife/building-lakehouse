import constants as const
from script.utils import fetch_poster
from PIL import Image
import sys
sys.path.append('../')
import constants as const

def initialize_res_widget(st):
    """here we create empty blanks for all recommended restaurants
    and add description and title from appropriate config file"""

    res_cols = st.columns(const.RES_NUMBER)
    for c in res_cols:
        with c:
            st.empty()
    return res_cols

def detail_item(business_id,selected_rows,st):
    image=fetch_poster(business_id)
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

def show_recommended_res_info(recommended_res, res_cols, show_score,st):
    """in this function we get all data what we want to show and put in on webpage"""
    res_ids = recommended_res["business_id"]
    res_name = recommended_res["name"]
    res_scores = recommended_res["score"]
    posters = [fetch_poster(i) for i in res_ids]
    
    for index,(c, name, score, img) in enumerate(zip(res_cols, res_name, res_scores, posters)):
        with c:  
            if st.button(name) and name:
                const.name=name
            st.markdown("""<style>.element-container.css-1047zxe.e1tzin5v3 .row-widget.stButton>* {
                                padding: 0;
                                border: 0;
                                margin: 0;
                                color: white;
                                background-color: transparent;
                        }
                        </style>
                        """,unsafe_allow_html=True)
            try:
                img = Image.open(img)
            except:
                pass
            st.image(img)
            if show_score:
                st.write(round(score, 3))
