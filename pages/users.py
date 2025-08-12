import streamlit as st
from st_keyup import st_keyup
import st_aggrid
import datetime
import ast

st.title("Users")

if 'db' not in st.session_state:
    st.write('Loading database...')
    st.stop()

db = st.session_state["db"]

users_data = db.df(db.users.find())
users_data['_id'] = users_data['_id'].astype(str)
users_data = users_data.sort_values('name')

gob = st_aggrid.GridOptionsBuilder.from_dataframe(users_data)
gob.configure_default_column(
    filter = True,
    groupable = True,
    enableCellTextSelection = True,
    wrapText = True,
)

sel_index = None
if 'new_user_id' in st.session_state:
    sel_index = users_data[users_data['_id'] == str(st.session_state['new_user_id'])].index.tolist()
    if len(sel_index) > 0:
        sel_index = sel_index[0]


gob.configure_selection('single', use_checkbox = False, pre_selected_rows = sel_index)

columns_to_hide = ['history'] #  '_id',
for c in columns_to_hide:
    gob.configure_column(c, hide = True)

user_cols = st.columns([.6, .4])

with user_cols[0]:
    event = st_aggrid.AgGrid(
        users_data,
        gridOptions = gob.build(),
        key = 'users_dataframe',
        update_on = 'MODEL_CHANGED',
        fit_columns_on_grid_load = True
    )

    add_user_button = st.button('Add User', icon = ':material/person:')
    if add_user_button:
        new_id = db.insert(
            'users',
            {
                'description': f'Added {datetime.datetime.today().strftime('%m-%d-%Y %H:%M:%S')}'
            }
        )
        st.session_state['new_user_id'] = str(new_id)
        st.toast('Added new user')
        st.rerun()

if event.selected_data is None or event.selected_data.empty:
    st.stop()

user = event.selected_data.iloc[0]
user = user.replace('nan', '')

if 'history' not in user:
    user['history'] = list()
user['history'] = ast.literal_eval(user['history'])

with user_cols[1]:
    with st.form('users_form') as form:
        st.subheader('Update User Info')

        vals = dict()

        text_fields = [
            'name', 'address', 'phone', 'email',
            'company', 'description'
        ]
        for text_field in text_fields:
            vals[text_field] = st.text_input(text_field, value = user[text_field])

        submitted = st.form_submit_button("Submit")
        if submitted:
            with st.spinner('Updating user info...', show_time = True):
                # st.write(vals)
                db.update(collection = 'users', _id = user['_id'], data = vals)
                users_data = db.df(db.users.find())

            st.markdown(
                """
                <style>
                div[data-testid=stToast] {
                    background-color: #ccddc4; /* Example: Orange-red background */
                    color: black; /* Example: White text color */
                }
                </style>
                """,
                unsafe_allow_html = True
            )
            st.toast(f'Updated info for {user['name']}', icon = ':material/person_add:')
            st.rerun()

    with st.expander('Change history'):
        for row in user['history'][::-1]:
            when = datetime.datetime.strptime(row['when'], "%Y-%m-%dT%H:%M:%S.%f")
            with st.container():
                cols = st.columns([.3, .7])
                with cols[0]:
                    st.write(when.strftime('%Y-%m-%d %a'))
                    st.write(when.strftime('%I:%M:%S %p'))
                with cols[1]:
                    st.table(row['what'])