import streamlit as st
import st_aggrid
import time
import datetime
import pytz


def user_format_func(users, id):
    user = users.loc[users['_id'] == id]
    if user.empty:
        return ""
    user = user.iloc[0]
    return f"{user['name']} ({user['company']})"


st.title("Tools")

if 'db' not in st.session_state:
    st.write('Loading database...')
    st.stop()

db = st.session_state["db"]
tools_data = db.df(db.tools.find())
tools_data['_id'] = tools_data['_id'].astype(str)

users = db.df(db.users.find())
users['_id'] = users['_id'].astype(str)
users = users.sort_values('name').reset_index()

tools_data['User Name'] = tools_data['user'].apply(lambda x: user_format_func(users, x))

gob = st_aggrid.GridOptionsBuilder.from_dataframe(tools_data)
gob.configure_default_column(
    #alwaysShowHorizontalScroll = True,
    #alwaysShowVerticalScroll = False,
    filter = True,
    groupable = True,
    enableCellTextSelection = True,
    #rowHeight = 100,
    #wrapText = wrap_text,
    #autoHeight = auto_height,
    #enableRangeSelection = True,
)
gob.configure_selection('single', use_checkbox = False)

columns_to_hide = ['_id', 'history', 'gps', 'gnss', 'shape_height_in', 'shape_width_in', 'user']
for c in columns_to_hide:
    gob.configure_column(c, hide = True)

event = st_aggrid.AgGrid(
    tools_data,
    gridOptions = gob.build(),
    key = 'tools_dataframe',
    update_on = 'MODEL_CHANGED',
)

if event.selected_data is None:
    st.stop()

tool = event.selected_data.iloc[0]


with st.form('tools_form') as form:
    st.subheader('Update Tool Info')

    vals = dict()

    vals['brand'] = st.pills('Brand', ['', 'EGO', 'FLEX', 'SKIL', 'KOBALT'], selection_mode = 'single', default = tool['brand'])

    text_fields = [
        'model', 'description', 'SN',
        'datalogger', 'shape_width_in', 'shape_height_in'
    ]
    for text_field in text_fields:
        vals[text_field] = st.text_input(text_field, value = tool[text_field])

    vals['user'] = st.selectbox(
        'user',
        users['_id'],
        format_func = lambda x: user_format_func(users, x),
        index = (users[users['_id'] == tool['user']].index.tolist()[0] if tool['user'] else None)
    )

    timezones = [t for t in pytz.common_timezones if t.startswith('US') or t in ['UTC']]
    utc_offsets = [datetime.datetime.now(pytz.timezone(t)).strftime('%z') for t in timezones]
    timezone_options = [str(tz) + ' ' + str(os) for tz, os in zip(timezones, utc_offsets)]
    timezone_choices = {'': ''}
    timezone_choices.update(dict(zip(timezones, timezone_options)))

    vals['timezone'] = st.selectbox(
        'timezone',
        timezones,
        format_func = lambda x: timezone_choices[x],
        index = timezones.index(tool['timezone'])
    )

    submitted = st.form_submit_button("Submit")
    if submitted:
        with st.spinner('Updating tool info...', show_time = True):
            # st.write(vals)
            db.update(collection = 'tools', _id = tool['_id'], data = vals)
            tools_data = db.df(db.tools.find())
            tools_data['_id'] = tools_data['_id'].astype(str)
        st.success('Tool info updated')