import streamlit_authenticator.hasher as stauth

password = "123"
hashed_password = stauth.Hasher([password]).generate()[0]
print(hashed_password)