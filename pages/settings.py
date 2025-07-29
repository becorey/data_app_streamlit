import streamlit as st
import streamlit_authenticator as stauth

# Pre-hashing all plain text passwords once
# cred_hashed = stauth.Hasher.hash_passwords(credentials['credentials'])

if 'authenticator' not in st.session_state:
    st.stop()

authenticator = st.session_state["authenticator"]

st.write('authentication_status', st.session_state.get('authentication_status'))

if st.session_state.get('authentication_status'):
    st.title("Settings")
    st.write(f'Welcome *{st.session_state.get("name")}*')
    authenticator.logout()
elif st.session_state.get('authentication_status') is False:
    st.error('Username/password is incorrect')
elif st.session_state.get('authentication_status') is None:
    st.warning('Please enter your username and password')