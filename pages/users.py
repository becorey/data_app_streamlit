import streamlit as st
from st_keyup import st_keyup
from df_global_search import DataFrameSearch
from st_aggrid import AgGrid

st.title("Users")

db = st.session_state["db"]

users_data = db.df(db.users.find())
users_data['_id'] = users_data['_id'].astype(str)
users_data = users_data.sort_values('name')

search_text = st_keyup(
    "Search", label_visibility = "collapsed", placeholder = "Search", debounce = 500
)

with DataFrameSearch(
        dataframe = users_data,
        text_search = search_text,
        case_sensitive = False,
        regex_search = False,
        highlight_matches = False,
) as df:
    st.dataframe(
        data = df,
        column_config = {
            #'_index': None,
            '_id': None,
            'history': None
        },
        use_container_width = True,
        hide_index = True
    )