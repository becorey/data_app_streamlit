import streamlit as st
import datetime
import st_aggrid
import pandas as pd
import ast
import plotly.express as px
import math

import bigquery
import event
import functions
import data_from_cloud

st.title('Home')

if 'db' not in st.session_state:
    st.write('Loading database...')
    st.stop()

db = st.session_state["db"]

today = datetime.date.today()
earlier_date = today - datetime.timedelta(weeks = 2)
date_range = st.slider(
    'Begin by selecting a Date Range',
    min_value = today - datetime.timedelta(weeks = 26),
    max_value = today,
    value = (earlier_date, today),
    step = datetime.timedelta(weeks = 1),
    format = "MM/DD/YYYY"
)
start_date, end_date = date_range

# -----
st.header(f':material/earthquake: Active Tools {start_date} to {end_date}', divider = True)

# start with info from events in bigquery
query = (f"SELECT "
         f"`datalogger`, "
         f"SUM(duration) as sum_duration, "
         f"MAX(date) as `Last Active` "
         f"FROM `{st.secrets['bigquery_project_id']}.{st.secrets['bigquery_dataset']}.events` "
         f"WHERE `date` >= \"{start_date}\" AND `date` <= \"{end_date}\" "
         f"GROUP BY `datalogger` "
         f"ORDER BY `sum_duration` DESC "
         )
active_tools = bigquery.df_from_query(query)
active_tools['Total Time'] = active_tools.apply(lambda row: functions.seconds_to_string(row['sum_duration']), axis = 1)

# add info from tools in mongodb
tools_data = db.df(db.tools.find())
# tools_data['_id'] = tools_data['_id'].astype(str)
tools_data = tools_data.drop(columns = ['_id', 'history',])
active_tools = pd.merge(active_tools, tools_data, on = 'datalogger', how = 'left')
active_tools['Tool Name'] = active_tools['model'] + ' ' + active_tools['SN']

# add info from users in mongodb
users_data = db.df(db.users.find())
users_data['_id'] = users_data['_id'].astype(str)
users_data = users_data[['_id', 'name']]
active_tools = pd.merge(active_tools, users_data, left_on = 'user', right_on = '_id', how = 'left')
active_tools = active_tools.drop(columns = ['user'])

gob = st_aggrid.GridOptionsBuilder.from_dataframe(active_tools)
gob.configure_selection('single', use_checkbox = False)
gob.configure_column('datalogger', headerCheckboxSelection = True, checkboxSelection = False)
gob.configure_column("Last Active", type=["customDateTimeFormat"], custom_format_string='yyyy-MM-dd')
columns_to_hide = ['sum_duration', '_id', 'history', 'gps', 'gnss', 'shape_height_in', 'shape_width_in', 'Tool Name']
for c in columns_to_hide:
    gob.configure_column(c, hide = True)

active_tools_cols = st.columns([0.6, 0.4])
with active_tools_cols[0]:
    active_tools_selection = st_aggrid.AgGrid(
        active_tools,
        gridOptions = gob.build(),
        update_on = ('modelUpdated', 500),
        key = 'active_tools'
    )

with active_tools_cols[1]:
    st.plotly_chart(
        px.bar(
            active_tools,
            x = 'Tool Name',
            y = 'sum_duration'
        )
    )

if active_tools_selection.selected_data is None:
    st.stop()

selected_tool = active_tools_selection.selected_data.iloc[0]


# ------
st.header(f":material/calendar_month: Usage by Day: {selected_tool['brand']} {selected_tool['model']} {selected_tool['SN']}", divider = True)

query = (f"SELECT "
         f"date, "
         f"SUM(duration) as sum_duration, "
         f"SUM(energy) as sum_energy, "
         f"SUM(CASE WHEN avgCurrent >=0 THEN energy ELSE 0 END) AS energy_positive, "
         f"SUM(CASE WHEN avgCurrent <0 THEN energy ELSE 0 END) AS energy_negative, "
         f"FROM `{st.secrets['bigquery_project_id']}.{st.secrets['bigquery_dataset']}.events` "
         f"WHERE `datalogger` = \"{selected_tool['datalogger']}\" AND `date` >= \"{start_date}\" AND `date` <= \"{end_date}\" "
         f"GROUP BY `date` "
         f"ORDER BY `date` DESC "
         )
data_by_date = bigquery.df_from_query(query)
data_by_date['Total Time'] = data_by_date.apply(lambda row: functions.seconds_to_string(row['sum_duration']), axis = 1)

# data_by_date['date'] = pd.to_datetime(data_by_date['date'])
col_types = {
    'sum_duration': 'float64',
    'sum_energy': 'float64',
    'energy_positive': 'float64',
    'energy_negative': 'float64',
    'Total Time': 'str'
}
for k, v in col_types.items():
    data_by_date[k] = data_by_date[k].astype(v)


gob = st_aggrid.GridOptionsBuilder.from_dataframe(data_by_date)
gob.configure_selection('single', use_checkbox = False)
gob.configure_column(
    'date',
    headerCheckboxSelection = True, checkboxSelection = False,
    type = ["customDateTimeFormat"], custom_format_string='yyyy-MM-dd'
)
gob.configure_column('sum_duration', hide = True)


usage_by_day_cols = st.columns([0.6, 0.4])

with usage_by_day_cols[0]:
    data_by_date_selection = st_aggrid.AgGrid(
        data_by_date,
        gridOptions = gob.build(),
        update_on = ('modelUpdated', 500),
        key = 'data_by_date'
    )

with usage_by_day_cols[1]:
    usage_by_day_bar = px.bar(
        data_by_date,
        x = 'date',
        y = 'sum_duration'
    )
    usage_by_day_bar.update_xaxes(type = 'category')
    st.plotly_chart(
        usage_by_day_bar
    )

if data_by_date_selection.selected_data is None:
    st.stop()

selected_date = data_by_date_selection.selected_data.iloc[0]['date'] # literal "2025-03-13T00:00:00.000"
selected_date = datetime.datetime.strptime(selected_date, "%Y-%m-%dT%H:%M:%S.%f").date()


# -----
st.header(f':material/event_note: Events with {selected_tool['brand']} {selected_tool['model']} {selected_tool['SN']} on {selected_date}', divider = True)

events_data = event.events_df_by_id_and_date_range(selected_tool['datalogger'], start_date = selected_date, end_date = selected_date)
# st.write(events_data)
# combine adjacent events into "sessions"
events_data = event.combine_events(events_data, timeout_s = 60)

gob = st_aggrid.GridOptionsBuilder.from_dataframe(events_data)
gob.configure_selection('single', use_checkbox = False)
gob.configure_column(
    'Time (UTC)',
    headerCheckboxSelection = True, checkboxSelection = False,
    # type = ["customDateTimeFormat"], custom_format_string='yyyy-MM-dd'
)
columns_to_hide = ['date', 'filenames']
for col in columns_to_hide:
    gob.configure_column(col, hide = True)

events_data_cols = st.columns(2)

with events_data_cols[0]:
    events_data_selection = st_aggrid.AgGrid(
        events_data,
        gridOptions = gob.build(),
        update_on = ('modelUpdated', 500),
        key = 'events_data'
    )


if events_data_selection.selected_data is None:
    st.stop()

selected_event = events_data_selection.selected_data.iloc[0]
# st.write(selected_event)

filenames = ast.literal_eval(events_data_selection.selected_data['filenames'].iloc[0])
local_filenames = [data_from_cloud.server_to_local_filename(f) for f in filenames]

with events_data_cols[1]:
    with st.spinner('Downloading files...', show_time = True):
        for server_filename in filenames:
            local_filename = data_from_cloud.download_blob_by_name(server_filename)

    with st.spinner('Processing data...', show_time = True):
        dfs = list()
        for filename in local_filenames:
            df = pd.read_csv(filename)
            df = event.harmonize_columns(df)
            timestamp = event.timestamp_from_filename(filename)
            df['t (s)'] = df['t (s)'] + timestamp
            dfs.append(df)

        df = pd.concat(dfs)
        df = event.fix_energy_values(df)

    with st.spinner('Plotting data...', show_time = True):
        y_vars = st.multiselect(
            'Plot variables',
            options = df.columns.tolist(),
            default = 'current (A)'
        )
        resample_rate = st.segmented_control(
            'Resample rate',
            options = ['Original', 0.1, 0.5, 1, 5, 10],
            default = 0.5,
            selection_mode = 'single'
        )
        #if not y_vars:
            #y_vars = ['current (A)', 'voltage (V)']
        x_var = 't (s)'

        if resample_rate == 'Original':
            df_resample = df
        else:
            while True:
                df_resample = df.copy()
                df_resample['time'] = pd.to_timedelta(df['t (s)'], unit='s')
                df_resample = df_resample.resample(f'{float(resample_rate)}s', on = 'time').max()

                # avoid huge plots that freeze the client
                if df_resample[x_var].size > 1e5:
                    resample_rate += 1
                    continue
                break

        st.write(f'Original {df[x_var].size} datapoints')
        st.write(f'Resampled {df_resample[x_var].size} datapoints')
        st.plotly_chart(
            px.line(
                df_resample,
                x = x_var,
                y = y_vars,
            )
        )

        for y_var in y_vars:
            bin_width = 10
            nbins = math.ceil((df[y_var].max() - df[y_var].min()) / bin_width)
            hist = px.histogram(
                df,
                y = 'dt (s)',
                x = y_var,
                marginal = 'box',
                nbins = nbins
            )
            st.plotly_chart(hist, key = f'hist-{y_var}')

with events_data_cols[0]:
    st.download_button(
        label = "Download CSV",
        data = df.to_csv(index = False).encode('utf-8'),
        file_name = f"{selected_event['Time (UTC)']} {selected_tool['datalogger']} {selected_tool['brand']} {selected_tool['model']} {selected_tool['SN']}",
        mime = "text/csv",
        key = 'download-csv'
    )
    st.dataframe(df)