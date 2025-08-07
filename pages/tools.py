import ast

import pandas as pd
import streamlit as st
import st_aggrid
import time
import datetime
import pytz
import plotly.express as px

import hologram


def user_format_func(users, id):
    user = users.loc[users['_id'] == id]
    if user.empty:
        return ""
    user = user.iloc[0]
    user = user.fillna('')
    return f"{user['name']} ({user['company']})"


st.title("Tools")

if 'db' not in st.session_state:
    st.write('Loading database...')
    st.stop()

db = st.session_state["db"]
tools_data = db.df(db.tools.find())
tools_data['_id'] = tools_data['_id'].astype(str) # weird bug, db.df should already be doing this

users = db.df(db.users.find())
users['_id'] = users['_id'].astype(str)
users = users.sort_values('name').reset_index()

tools_data['User Name'] = tools_data['user'].apply(lambda x: user_format_func(users, x))
tools_data['Last Updated'] = tools_data['history'].apply(lambda x: x[-1]['when'].strftime('%m-%d-%Y %I:%M:%S %p') if len(x) > 0 else '')
tools_data = tools_data.drop(columns = ['history'])

gob = st_aggrid.GridOptionsBuilder.from_dataframe(tools_data)
gob.configure_default_column(
    filter = True,
    groupable = True,
    enableCellTextSelection = True
)
gob.configure_selection('single', use_checkbox = False)

columns_to_hide = ['_id', 'history', 'gps', 'gnss', 'shape_height_in', 'shape_width_in', 'user', 'schedule']
for c in columns_to_hide:
    gob.configure_column(c, hide = True)

tool_cols = st.columns([.65, .35])

with tool_cols[0]:
    event = st_aggrid.AgGrid(
        tools_data,
        gridOptions = gob.build(),
        key = 'tools_dataframe',
        update_on = 'MODEL_CHANGED',
        fit_columns_on_grid_load = True,

    )

    add_tool_button = st.button('Add Tool', icon = ':material/add:')
    if add_tool_button:
        db.insert(
            'tools',
            {
                'description': f'New, added {datetime.datetime.today().strftime('%m-%d-%Y %H:%M:%S')}'
            }
        )
        st.toast('Added new tool')
        st.rerun()

if event.selected_data is None:
    st.stop()

tool = event.selected_data.iloc[0]
tool = tool.replace('nan', '')

text_fields = [
    'model', 'description', 'SN',
    'datalogger', 'hotspot', 'shape_width_in', 'shape_height_in'
]
for text_field in text_fields:
    if text_field not in tool:
        tool[text_field] = ''

if 'schedule' not in tool or isinstance(tool['schedule'], float):
    tool['schedule'] = [{'User': '', 'Start Date': None, 'End Date': None, 'Timezone': ''}]

tool = tool.fillna('')
tool = tool.replace({None: ''})

timezones = [t for t in pytz.common_timezones if t.startswith('US') or t in ['UTC']]
utc_offsets = [datetime.datetime.now(pytz.timezone(t)).strftime('%z') for t in timezones]
timezone_options = [str(tz) + ' ' + str(os) for tz, os in zip(timezones, utc_offsets)]
timezone_choices = {'': ''}
timezone_choices.update(dict(zip(timezones, timezone_options)))

timezones = [''] + timezones

with tool_cols[0]:
    if tool['hotspot']:
        lat_long = hologram.link_last_location(name = tool['hotspot'])
        if lat_long:
            st.subheader('Latest Hotspot Location')
            df = pd.DataFrame(
                [lat_long]
            )
            fig = px.scatter_map(df, lat = 'latitude', lon = 'longitude')
            st.plotly_chart(fig)

    st.subheader(f"Schedule for {tool['model']} {tool['SN']}")

    if isinstance(tool['schedule'], str):
        tool['schedule'] = ast.literal_eval(tool['schedule'])

    sch_df = pd.DataFrame(tool['schedule'])

    sch_df['Start Date'] = pd.to_datetime(sch_df['Start Date'])
    sch_df['End Date'] = pd.to_datetime(sch_df['End Date'])
    #sch_df = sch_df.fillna(0)

    users['disp'] = users.apply(lambda row: user_format_func(users, row['_id']), axis = 1)

    sch_val = st.data_editor(
        sch_df,
        key = 'tool_schedule',
        num_rows = 'dynamic',
        column_config = {
            'User': st.column_config.SelectboxColumn(
                'User',
                options = [''] + users['disp'].dropna(axis = 'index').tolist(),
            ),
            'Start Date': st.column_config.DateColumn(
                'Start Date',
                format = 'MM-DD-YYYY'
            ),
            'End Date': st.column_config.DateColumn(
                'End Date',
                format = 'MM-DD-YYYY'
            ),
            'Timezone': st.column_config.SelectboxColumn(
                'Timezone',
                options = timezones
            )
        }
    )
    submit_sch = st.button('Submit')
    if submit_sch:
        sch_val = sch_val.replace({pd.NaT: None})
        sch_list = sch_val.to_dict(orient = 'records')
        db.update(collection = 'tools', _id = tool['_id'], data = {'schedule': sch_list})

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
        st.toast(f'Updated schedule for {tool['brand']} {tool['model']} {tool['SN']}', icon = ':material/event_available:')



with tool_cols[1]:
    with st.form('tools_form') as form:
        st.subheader('Update Tool Info')
        vals = dict()

        vals['brand'] = st.pills('Brand', ['', 'EGO', 'FLEX', 'SKIL', 'KOBALT'], selection_mode = 'single', default = tool['brand'])

        for text_field in text_fields:
            vals[text_field] = st.text_input(text_field, value = tool[text_field])

        match_list = users[users['_id'] == tool['user']].index.tolist()
        if tool['user'] and len(match_list) > 0:
            user_index = match_list[0]
        else:
            user_index = None

        vals['user'] = st.selectbox(
            'user',
            users['_id'],
            format_func = lambda x: user_format_func(users, x),
            index = user_index
        )

        vals['timezone'] = st.selectbox(
            'timezone',
            timezones,
            format_func = lambda x: timezone_choices[x],
            index = timezones.index(tool['timezone'])
        )

        submitted = st.form_submit_button("Submit")
        if submitted:
            with st.spinner('Updating tool info...', show_time = True):
                db.update(collection = 'tools', _id = tool['_id'], data = vals)

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
            st.toast('Tool info updated', icon = ':material/task:')

            st.rerun()