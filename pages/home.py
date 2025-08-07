import plotly.figure_factory
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
         f"ROUND(SUM(CASE WHEN avgCurrent >=0 THEN energy ELSE 0 END), 0) AS `Discharged Wh`, "
         f"ROUND(SUM(CASE WHEN avgCurrent <0 THEN energy ELSE 0 END), 0) AS `Charged Wh`, "
         f"MAX(date) as `Last Active` "
         f"FROM `{st.secrets['bigquery_project_id']}.{st.secrets['bigquery_dataset']}.events` "
         f"WHERE `date` >= \"{start_date}\" AND `date` <= \"{end_date}\" "
         f"GROUP BY `datalogger` "
         f"ORDER BY `Last Active` DESC "
         )
active_tools = bigquery.df_from_query(query)
active_tools['Total Time'] = active_tools.apply(lambda row: functions.seconds_to_string(row['sum_duration']), axis = 1)

# add info from tools in mongodb
tools_data = db.df(db.tools.find())
tools_data = tools_data.drop(columns = ['_id', 'history',])
active_tools = pd.merge(active_tools, tools_data, on = 'datalogger', how = 'left')
active_tools['Tool Name'] = active_tools['model'] + ' ' + active_tools['SN']

# add info from users in mongodb
users_data = db.df(db.users.find())
users_data = users_data[['_id', 'name']]
active_tools = pd.merge(active_tools, users_data, left_on = 'user', right_on = '_id', how = 'left')
active_tools = active_tools.drop(columns = ['user'])

gob = st_aggrid.GridOptionsBuilder.from_dataframe(active_tools)
gob.configure_default_column(
    filter = True,
    groupable = True,
    enableCellTextSelection = True,
)
gob.configure_selection('single', use_checkbox = False)
gob.configure_column(
    'datalogger',
    headerCheckboxSelection = True, checkboxSelection = False
)
gob.configure_column("Last Active", type=["customDateTimeFormat"], custom_format_string='yyyy-MM-dd')
columns_to_hide = [
    'sum_duration', '_id', 'history',
    'gps', 'gnss',
    'shape_height_in', 'shape_width_in',
    'Tool Name', 'schedule',
    'timezone', 'hotspot'
]
for c in columns_to_hide:
    gob.configure_column(c, hide = True)

col_types = {
    'datalogger': 'str',
    'Last Active': 'str',
    'Total Time': 'str',
    'brand': 'str',
    'model': 'str',
    'description': 'str',
    'SN': 'str',
    'timezone': 'str',
    'hotspot': 'str',
    'name': 'str'
}
for k, v in col_types.items():
    active_tools[k] = active_tools[k].astype(v)

active_tools = active_tools.replace({None: '', 'nan': ''})

active_tools_cols = st.columns([0.6, 0.4])
with active_tools_cols[0]:
    active_tools_selection = st_aggrid.AgGrid(
        active_tools,
        gridOptions = gob.build(),
        update_on = ('modelUpdated', 500),
        key = 'active_tools',
        fit_columns_on_grid_load = True
    )

with active_tools_cols[1]:
    st.plotly_chart(
        px.bar(
            active_tools.sort_values(by = 'sum_duration', ascending = True),
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
         f"SUM(CASE WHEN avgCurrent >=0 THEN energy ELSE 0 END) AS `Energy Wh Discharged`, "
         f"SUM(CASE WHEN avgCurrent <0 THEN energy ELSE 0 END) AS `Energy Wh Charged`, "
         f"FROM `{st.secrets['bigquery_project_id']}.{st.secrets['bigquery_dataset']}.events` "
         f"WHERE `datalogger` = \"{selected_tool['datalogger']}\" AND `date` >= \"{start_date}\" AND `date` <= \"{end_date}\" "
         f"GROUP BY `date` "
         f"ORDER BY `date` ASC "
         )
data_by_date = bigquery.df_from_query(query)
data_by_date['Total Time'] = data_by_date.apply(lambda row: functions.seconds_to_string(row['sum_duration']), axis = 1)
data_by_date['date'] = pd.to_datetime(data_by_date['date'])

col_types = {
    'sum_duration': 'float64',
    'Energy Wh Discharged': 'float64',
    'Energy Wh Charged': 'float64',
    'Total Time': 'str'
}
for k, v in col_types.items():
    data_by_date[k] = data_by_date[k].astype(v)

data_by_date['Energy Wh Discharged'] = data_by_date['Energy Wh Discharged'].round(2)
data_by_date['Energy Wh Charged'] = data_by_date['Energy Wh Charged'].round(2)

gob = st_aggrid.GridOptionsBuilder.from_dataframe(data_by_date)
gob.configure_default_column(
    filter = True,
    groupable = True,
    enableCellTextSelection = True,
)
gob.configure_selection('single', use_checkbox = False)
gob.configure_column(
    'date',
    headerCheckboxSelection = True, checkboxSelection = False,
    type = ["customDateTimeFormat"], custom_format_string='MM-dd-yyyy'
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
        data_by_date.sort_values(by = 'date', ascending = True),
        x = 'date',
        y = ['Energy Wh Discharged', 'Energy Wh Charged'],
        barmode = 'group'
    )
    # usage_by_day_bar.update_xaxes(type = 'category')
    usage_by_day_bar.update_xaxes(
        dtick = "D1",  # Set tick interval to 1 day
        tickformat = "%Y-%m-%d"  # Format tick labels as YYYY-MM-DD
    )
    st.plotly_chart(
        usage_by_day_bar
    )

if data_by_date_selection.selected_data is None:
    st.stop()

selected_date = data_by_date_selection.selected_data.iloc[0]['date'] # literal "2025-03-13T00:00:00.000"
selected_date = datetime.datetime.strptime(selected_date, "%Y-%m-%dT%H:%M:%S").date()


# -----
st.header(f':material/event_note: Events with {selected_tool['brand']} {selected_tool['model']} {selected_tool['SN']} on {selected_date}', divider = True)

events_data = event.events_df_by_id_and_date_range(selected_tool['datalogger'], start_date = selected_date, end_date = selected_date)

# combine adjacent events into "sessions"
dfs_by_session = event.combine_adjacent_events(events_data, timeout_s = 120)
if len(dfs_by_session) == 1 and dfs_by_session[0].empty:
    st.warning('Empty dataframe')
    st.write('events_data')
    st.write(events_data)
    st.write('dfs_by_session')
    st.write(dfs_by_session)
    st.stop()
dfs_by_session_and_charging = event.split_events_by_charging(dfs_by_session)
if not selected_tool['timezone'] or not isinstance(selected_tool['timezone'], str) or selected_tool['timezone'] == 'nan':
    selected_tool['timezone'] = 'UTC'
events_data = event.events_list_summarized(dfs_by_session_and_charging, selected_tool['timezone'])

gob = st_aggrid.GridOptionsBuilder.from_dataframe(events_data)
gob.configure_selection(
    'multiple',
    use_checkbox = True,
    header_checkbox = True,
    suppressRowClickSelection = False
)
gob.configure_column(
    'Time (local)',
    # headerCheckboxSelection = True, checkboxSelection = False,
    type = ["customDateTimeFormat"], custom_format_string='yyyy-MM-dd hh:mm:ss a (z)'
)
columns_to_hide = ['date', 'filenames', 'Duration (s)'] #  'timestamp',
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

with events_data_cols[1]:
    events_data_plot_df = events_data #[events_data['Duration (s)'] > 120]
    events_data_plot_df['bar_width'] = events_data_plot_df['Duration (s)'].astype(float) * 1000
    events_data_plot = px.bar(
        events_data_plot_df,
        x = 'Time (local)',
        y = 'Energy (Wh)'
    )

    #events_data_plot.update_xaxes(
        #dtick = "H1",
        #tickformat = "%H:%M:%S"
    #)
    events_data_plot.update_traces(width = events_data_plot_df['bar_width'])
    events_data_plot.update_xaxes(
        #type = 'category',
        tickformat = "%I:%M:%S %p"
    )

    st.plotly_chart(events_data_plot)


if events_data_selection.selected_data is None:
    st.stop()

events_plot_cols = st.columns([.5, .5])

with events_plot_cols[0]:
    st.subheader('Plotting selected events')
    # st.write(events_data_selection.selected_data[['Time (local)', 'Duration', 'Energy (Wh)']])

    with st.spinner('Downloading files...', show_time = True):
        dfs = list()
        for index, ev in events_data_selection.selected_data.iterrows():
            ev['filenames'] = ast.literal_eval(ev['filenames'])
            for server_filename in ev['filenames']:
                local_filename = data_from_cloud.download_blob_by_name(server_filename)

                try:
                    df = pd.read_csv(local_filename)
                except pd.errors.EmptyDataError as e:
                    print(local_filename, 'error', e)
                    continue
                df = event.harmonize_columns(df)
                timestamp = event.timestamp_from_filename(local_filename)
                df['t (s)'] = df['t (s)'] + timestamp
                df['time'] = pd.to_datetime(df['t (s)'], unit = 's', utc = True)
                df['time'] = df['time'].dt.tz_convert(selected_tool['timezone'])
                df = event.fix_energy_values(df)
                df = functions.remove_outliers(df, ['current (A)'], 6) # remove extreme outliers
                dfs.append(df)

        if len(dfs) == 0:
            st.stop()

        df = pd.concat(dfs)
        df = event.fix_energy_values(df)


    plot_vars = st.multiselect(
        'Plot variables',
        options = df.columns.tolist(),
        default = 'current (A)'
    )
    plot_color_cols = st.columns(2)
    with plot_color_cols[0]:
        plot_color = st.selectbox(
            'Color by variable',
            options = [None] + df.columns.tolist(),
        )
    with plot_color_cols[1]:
        color_scale = st.selectbox(
            'Color scale',
            options = px.colors.named_colorscales(),
            index = px.colors.named_colorscales().index('temps')
        )
    resample_rate = st.segmented_control(
        'Resample rate',
        options = ['Original', 0.1, 0.5, 1, 5, 10],
        default = 'Original',
        selection_mode = 'single'
    )

    x_var = 't (s)'

with events_plot_cols[1]:
    with st.spinner('Plotting data...', show_time = True):
        if resample_rate == 'Original':
            df_resample = df
        else:
            df_resample = df.copy()
            df_resample['timedelta'] = pd.to_timedelta(df['t (s)'], unit = 's')
            df_resample = df_resample.resample(f'{float(resample_rate)}s', on = 'timedelta').max()

        # avoid huge plots that freeze the client
        max_num_points = 1.5e5
        resample_rate_increment = 0.1
        resample_rate_override = resample_rate
        while df_resample[x_var].size * len(plot_vars) > max_num_points:
            if not isinstance(resample_rate_override, float):
                resample_rate_override = 0.0
            resample_rate_override = round(float(resample_rate_override) + resample_rate_increment, 2)
            df_resample = df.copy()
            df_resample['timedelta'] = pd.to_timedelta(df['t (s)'], unit='s')
            df_resample = df_resample.resample(f'{float(resample_rate_override)}s', on = 'timedelta').max()

        if resample_rate_override != resample_rate:
            st.write(f'Resample rate increased to {resample_rate_override}s to maintain plotting speed')

        line_plot = px.scatter(
            df_resample,
            x = 'time',
            y = plot_vars,
            color = plot_color,
            color_continuous_scale = color_scale
        )
        line_plot.update_xaxes(tickformat = '%I:%M:%S %p')
        st.plotly_chart(
            line_plot
        )

        hist_vars = st.multiselect(
            'Histogram variables',
            options = df.columns.tolist(),
            default = 'current (A)'
        )

        for y_var in hist_vars:
            bin_width = 5
            nbins = math.ceil((df[y_var].max() - df[y_var].min()) / bin_width)
            hist = px.histogram(
                df,
                y = 'dt (s)',
                x = y_var,
                marginal = 'box',
                nbins = nbins
            )
            st.plotly_chart(hist, key = f'hist-{y_var}')

with events_plot_cols[0]:
    st.subheader('Download CSV of selected events')
    selected_event = events_data_selection.selected_data.iloc[0]

    st.download_button(
        label = "Download CSV",
        data = df.to_csv(index = False).encode('utf-8'),
        file_name = f"{selected_event['Time (local)']} {selected_tool['datalogger']} {selected_tool['brand']} {selected_tool['model']} {selected_tool['SN']}.csv",
        mime = "text/csv",
        key = 'download-csv'
    )
    st.dataframe(df)