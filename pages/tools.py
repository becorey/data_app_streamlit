import streamlit as st

st.title("Tools")

if 'db' not in st.session_state:
    st.write('Loading database...')
    st.stop()

db = st.session_state["db"]
tools_data = db.df(db.tools.find())
tools_data['_id'] = tools_data['_id'].astype(str)


def tools_table_clicked():
    print('tools_table_clicked', st.session_state.tools_dataframe)
    selected_rows = st.session_state.tools_dataframe.selection.rows
    if len(selected_rows) == 0:
        return

    selected_index = selected_rows[0]
    selected_tool = tools_data.iloc[selected_index]
    st.session_state['selected_tool'] = selected_tool
    print(st.session_state['selected_tool'])
    return


event = st.dataframe(
    tools_data,
    key = 'tools_dataframe',
    on_select = tools_table_clicked,
    selection_mode = 'single-row',
    column_config = {
        '_index': None,
        '_id': None,
        'history': None,
        'gps': None,
        'gnss': None,
        'shape_height_in': None,
        'shape_width_in': None
    }
)

print(event)
if not event or len(event['selection']['rows']) == 0:
    st.session_state['selected_tool'] = None

if st.session_state['selected_tool'] is not None:
    st.switch_page("pages/events.py")
