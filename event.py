import os
import streamlit as st

import pandas as pd
import numpy as np
import datetime
from zoneinfo import ZoneInfo

import functions
from functions import split_path, seconds_to_string
import data_from_cloud
import bigquery


def rebase_time_mp_to_unix(mp_timestamp):
	# the timestamp from micropython corresponds to 0 at Jan-1-2000
	mp_timebase = int(datetime.datetime.timestamp(datetime.datetime.strptime("01/01/2000", "%d/%m/%Y")))
	mp_timestamp = int(mp_timestamp)
	unix_timestamp = mp_timestamp + mp_timebase
	return unix_timestamp


def timestamp_from_filename(filename):
	"""
	filename is like D4-D4-DA-BD-9A-58/events/747511440.csv
	the base is the micropython timestamp
	"""
	directory, base, ext = split_path(filename)
	timestamp = rebase_time_mp_to_unix(base)
	return timestamp


@st.cache_data
def events_df_by_id_and_date_range(
		datalogger_id,
		start_date, end_date, timezone = 'US/Central',
		select = None):
	if select is None:
		select = ['*']

	# TODO: use timezone and search on timestamp, not date, for more correct results
	rows = bigquery.find(
		select,
		where = [
			('datalogger', '=', datalogger_id),
			('date', '>=', start_date),
			('date', '<=', end_date)
		],
		order = [('timestamp', 'asc')],
		table = 'events'
	)

	return rows.to_dataframe()


@st.cache_data
def events_df_recent(limit = 100, start_date = None, end_date = None, select = None):
	if select is None:
		select = ['*']

	where = list()
	if start_date:
		where.append(('date', '>=', start_date))
	if end_date:
		where.append(('date', '<=', end_date))
	rows = bigquery.find(
		select,
		where = where,
		order = [('timestamp', 'desc')],
		table = 'events',
		limit = limit
	)

	return rows.to_dataframe()


def combine_events(df, timeout_s = 60):
	"""
	:param df: from events_df_by_id_and_date_range
	:return:
	"""
	df['end_timestamp'] = df['timestamp'] + df['duration']
	df['time_to_next'] = df['timestamp'].shift(-1) - df['end_timestamp']
	df['time_to_next'] = df['time_to_next'].fillna(60 * 60 * 24)

	df['charging'] = (df['avgCurrent'] < 0)

	# find indexes to split sessions
	# based on exceeding the session_timeout
	idx = np.append([0], np.where(df['time_to_next'] > timeout_s))

	# split df to list of dfs based on session indexes
	# https://stackoverflow.com/a/53395439/2666454
	dfs_by_session = [df.iloc[idx[n] + 1:idx[n + 1] + 1] for n in range(len(idx) - 1)]

	df_return = pd.DataFrame()
	for dfi in dfs_by_session:
		if dfi.empty:
			continue
		duration_sum = dfi['duration'].sum()
		start_timestamp = dfi['timestamp'].iloc[0]
		row = pd.DataFrame([{
			'Time (UTC)': functions.timestamp_to_str(start_timestamp, 'UTC'),
			#'timestamp': dfi['timestamp'].iloc[0],
			'Duration': functions.seconds_to_string(duration_sum),
			'Duration (s)': duration_sum,
			'Energy (Wh)': dfi['energy'].sum(),
			'date': dfi['date'].iloc[0],
			'filenames': dfi['filename'].tolist()
		}])
		df_return = pd.concat([df_return, row], ignore_index = True)

	df_return['Duration (s)'] = df_return['Duration (s)'].astype('float64')
	df_return['Energy (Wh)'] = df_return['Energy (Wh)'].astype('float64')
	return df_return

def event_by_filename(cloud_filename, timezone):
	rows = bigquery.find(
		['*'],
		[('filename', '=', cloud_filename)]
	)
	for row in rows:
		return Event(row, timezone)
	return


def harmonize_columns(df):
	""" harmonize column names across different datalogger versions """
	df
	if all([x in df.columns for x in ['dt (us)', 'current (A)', 'voltage (V)']]):
		# V5
		df['t (s)'] = df['dt (us)'].cumsum() / 1e6
		df['dt (s)'] = df['dt (us)'] / 1e6
		df['power (W)'] = df['voltage (V)'] * df['current (A)']
		df['delta energy (J)'] = df['power (W)'] * df['dt (us)'] * 1e-6
		df['energy (J)'] = df['delta energy (J)'].cumsum()

	elif all([x in df.columns for x in ['t (s)', 'VBUS (V)', 'CURRENT (A)', 'DIETEMP (deg C)', 'ENERGY (J)']]):
		# V6
		df = df.rename(columns = {
			'VBUS (V)': 'voltage (V)',
			'CURRENT (A)': 'current (A)',
			'DIETEMP (deg C)': 'temperature (deg C)',
			'ENERGY (J)': 'energy (J)'
		})
		df['delta energy (J)'] = df['energy (J)'].diff().fillna(0).round(6)
		df['power (W)'] = (df['voltage (V)'] * df['current (A)']).round(6)
		df['dt (s)'] = df['t (s)'].diff().fillna(0)
		df.loc[df['dt (s)'] < 0, 'dt (s)'] = 0
	else:
		return None

	return df


def fix_energy_values(df):
	# correct energy for negative current values
	df['delta energy (J)'] = df['delta energy (J)'].abs()
	sign_current = np.sign(df['current (A)'])
	df['delta energy (J)'] = df['delta energy (J)'] * sign_current
	df['energy (J)'] = df['delta energy (J)'].cumsum()

	# add energy unit Wh from J
	df['energy (Wh)'] = (df['energy (J)'] / 3600).round(6)
	df['delta energy (Wh)'] = df['energy (Wh)'].diff().fillna(0).round(6)

	return df

class Event:
	def __init__(self, row, timezone):
		"""
		init
		download
		post_process
		row: {
			datalogger -> str
			filename -> str
			timestamp -> int
			duration -> numeric
			avgVoltage, minVoltage, maxVoltage
			avgCurrent, maxCurrent
			maxPower
			energy
			avgTemperature, minTemperature, maxTemperature
			time -> TIME
			date -> DATE
			timestamp_inserted -> TIMESTAMP
		}
		"""
		self.row = row # row from bigquery
		self.local_filename = None
		self.timezone = timezone
		self.df = None # csv file read into a df
		self.timestamp = None

		return

	def download(self):
		self.local_filename = data_from_cloud.download_blob_by_name(self.row['filename'])
		return self.local_filename

	def post_process(self):
		if self.df is not None: return
		self.df = pd.read_csv(self.local_filename)
		self.harmonize_columns()
		self.add_local_time()
		return

	def add_local_time(self):
		self.timestamp = timestamp_from_filename(self.local_filename)
		self.df['t (s)'] = self.df['t (s)'] + self.timestamp
		return


	def start_time(self):
		utc_time = datetime.datetime.fromtimestamp(self.timestamp)
		local_time = utc_time.astimezone(ZoneInfo(self.timezone))
		return local_time.strftime('%Y-%m-%d %I:%M:%S %p')

	def __str__(self):
		return self.start_time() + ' ' + seconds_to_string(self.row['duration']) + ' ' + str(self.row['energy']) + ' Wh'


if __name__ == '__main__':
	datalogger_id = 'E8-6B-EA-33-6A-DC'
	start_date = '2024-06-09'
	end_date = '2024-06-10'
	timezone = 'US/Central'

	ev = event_by_filename("10-06-1C-30-10-14/events/770498144.csv", timezone)
	ev.download()
	ev.post_process()
	print(ev)

	quit()


	df = events_df_by_id_and_date_range(
		datalogger_id,
		start_date, end_date)

	for index, row in df.iterrows():
		ev = Event(row, timezone)
		# ev.
