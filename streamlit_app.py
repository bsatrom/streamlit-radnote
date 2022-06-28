import streamlit as st
import json
import pandas as pd
import snowflake.connector
import matplotlib

"""
# Blues Radnote Demo!

Select one or more devices from the list below to view readings and device location data.

"""

# Initialize connection.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

# Perform query.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

device_rows = run_query(f'select DISTINCT device, IFNULL(serial_number,device) as Name from session_vw;')
device_table = pd.DataFrame(device_rows, columns=("Device", "Serial Number"))

# env_data = pd.DataFrame(env_rows, columns=("Device", "When", "Loc", "Lat", "Lon", "Location", "Location Type", "Timezone", "Country", "Temp", "Humidity", "Pressure", "Altitude"))

"""
### Radnote Data
"""

st.metric("# of Devices", device_table.shape[0])

devices = st.selectbox("Select a device to view data",device_table[device_table.columns[::-1]])

if devices:
    """
    #### Options
    """
    show_charts = st.checkbox('Show charts?', True)
    show_map = st.checkbox('Show map?', True)
    show_table_data = st.checkbox('Show table data?', False)

    device_list = device_table.loc[device_table['Serial Number'].isin([devices])]

    for index, row in device_list.iterrows():
        tracker_rows = run_query(f'SELECT * from tracker_vw WHERE device = \'{row["Device"]}\' ORDER BY created desc;')
        tracker_data = pd.DataFrame(tracker_rows, columns=("ID", "Device", "When", "Loc", "lat", "lon", "Location", "Location Type", "Timezone", "Country", "Temp", "Voltage", "CPM", "CPM_SECS", "HDOP", "Sensor", "Status", "USV", "Time", "Serial Number", "Velocity", "Motion", "Seconds", "JCount", "Journey", "Distance", "Bearing"))

        air_rows = run_query(f'SELECT * from airinfo_vw WHERE device = \'{row["Device"]}\' ORDER BY created desc;')
        air_data = pd.DataFrame(air_rows, columns=("ID", "Device", "When", "Loc", "Lat", "Lon", "Location", "Location Type", "Timezone", "Country", "Temp", "Voltage", "CPM", "CSecs", "Sensor", "USV", "Serial Number"))

    if show_charts:
        """
        ### Environment Charts
        """

        rad_group = tracker_data[["CPM", "CPM_SECS"]]
        st.line_chart(rad_group)

        if air_data.shape[0] > 0:
            air_group = air_data[["Temp","Voltage"]]
            st.line_chart(air_group)

    if show_map:
        """
        ### Tracker Map
        """
        if tracker_data.shape[0] == 0:
            st.write("No Location Data Avaialble")
        else:
            tracker_locations = tracker_data[["lat", "lon"]]
            st.map(tracker_locations)

    if show_table_data:
        """
        ### Tracker Events
        """
        if tracker_data.shape[0] > 0:
            tracker_data[tracker_data.columns[::-1]]

        if air_data.shape[0] > 0:
            air_data[air_data.columns[::-1]]