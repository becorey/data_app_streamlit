import streamlit as st
import datetime
import event

st.title("Events")

if 'db' not in st.session_state:
    st.write('Loading database...')
    st.stop()
db = st.session_state["db"]

today = datetime.datetime.now()
earlier_date = today - datetime.timedelta(days = 7)
date_range = st.date_input(
    label = 'Date Range',
    value = (earlier_date, today)
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    st.stop()

if 'selected_tool' in st.session_state and st.session_state['selected_tool'] is not None:
    disp_tool_info = st.session_state['selected_tool']
    disp_tool_info = disp_tool_info.drop(['_id', 'history', 'gps', 'gnss'])
    st.dataframe(
        disp_tool_info
    )
    events_data = event.events_df_by_id_and_date_range(st.session_state['selected_tool']['datalogger'], start_date, end_date)
else:
    events_data = event.events_df_recent(200, start_date, end_date)
    # st.warning('Please select a tool to view events')

st.dataframe(
    events_data,
    column_config = {
        '_index': None,
        '_id': None,
        'filename': None,
        'timestamp': None,
        'timestamp_inserted': None,
        'data': None,
        'history': None,
        'migrated_to_bigquery': None,
        'minVoltage': None,
        'maxVoltage': None,
        'maxCurrent': None,
        'maxPower': None,
        'minTemperature': None,
        'maxTemperature': None
    }
)