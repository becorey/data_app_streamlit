import streamlit as st
import streamlit_authenticator as stauth
import pymongo
import sys
import traceback

from db_mongo import DB_Handler

st.set_page_config(layout = "wide")

env = 'prod'

if env == 'dev':
    authenticator = stauth.Authenticate(
        credentials = './.streamlit/credentials.yaml'
    )
elif env == 'prod':
    authenticator = stauth.Authenticate(
        credentials = st.secrets['credentials'].to_dict(),
        cookie_name = st.secrets['cookie']['name'],
        cookie_key = st.secrets['cookie']['key'],
        cookie_expiry_days = st.secrets['cookie']['expiry_days']
    )
st.session_state["authenticator"] = authenticator

try:
    authenticator.login(
        location = 'main',
        max_login_attempts = 15,
        single_session = False
    )
except Exception as e:
    st.error(e)


@st.cache_resource
def load_db():
    return DB_Handler()

try:
    db = load_db()
    st.session_state['db'] = db
except pymongo.errors.ConfigurationError as e:
    traceback.print_exc()
    print('Check the mongodb cluster is active')
    sys.exit()


# session_state initialization
if 'selected_tool' not in st.session_state:
    st.session_state['selected_tool'] = None


if st.session_state.get('authentication_status'):
    pg = st.navigation([
        st.Page(
            "./pages/home.py",
            title = 'Home',
            icon = ":material/home:"
        ),
        #st.Page(
            #"./pages/events.py",
            #title = 'Events',
            #icon = ":material/data_thresholding:"
        #),
        st.Page(
            "./pages/tools.py",
            title = 'Tools',
            icon = ":material/tools_power_drill:"
        ),
        st.Page(
            "./pages/users.py",
            title = 'Users',
            icon = ":material/account_circle:"
        ),
        st.Page(
            "pages/settings.py",
            title = 'Settings',
            icon = ":material/settings_account_box:"
        )],
        position = 'top'
    )
else:
    pg = st.navigation([st.Page("pages/settings.py")], position = 'top')

if 'selected_tool' in st.session_state and st.session_state['selected_tool'] is not None:
    with st.sidebar:
        st.write(st.session_state['selected_tool']['datalogger'])
        st.write(st.session_state['selected_tool']['brand'])
        st.write(st.session_state['selected_tool']['model'])
        st.write(st.session_state['selected_tool']['SN'])
        st.write(st.session_state['selected_tool']['description'])


pg.run()
