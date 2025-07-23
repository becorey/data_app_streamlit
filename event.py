import os

import pandas as pd
import numpy as np
import datetime
from zoneinfo import ZoneInfo

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


def event_by_filename(cloud_filename, timezone):
	rows = bigquery.find(
		['*'],
		[('filename', '=', cloud_filename)]
	)
	for row in rows:
		return Event(row, timezone)
	return


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

	def harmonize_columns(self):
		""" harmonize column names across different datalogger versions """
		df = self.df
		if all([x in df.columns for x in ['dt (us)', 'current (A)', 'voltage (V)']]):
			# V5
			df['t (s)'] = df['dt (us)'].cumsum() / 1e6
			df['dt (s)'] = df['dt (us)'] / 1e6
			df['power (W)'] = df['voltage (V)'] * df['current (A)']
			df['delta energy (J)'] = df['power (W)'] * df['dt (us)'] * 1e-6
			df['energy (J)'] = df['delta energy (J)'].cumsum()

		elif all([x in df.columns for x in ['t (s)', 'VBUS (V)', 'CURRENT (A)', 'DIETEMP (deg C)', 'ENERGY (J)']]):
			# V6
			df = df.rename(columns = {'VBUS (V)': 'voltage (V)', 'CURRENT (A)': 'current (A)',
									  'DIETEMP (deg C)': 'temperature (deg C)', 'ENERGY (J)': 'energy (J)'})
			df['delta energy (J)'] = df['energy (J)'].diff().fillna(0).round(6)
			df['power (W)'] = (df['voltage (V)'] * df['current (A)']).round(6)
			df['dt (s)'] = df['t (s)'].diff().fillna(0)
			df.loc[df['dt (s)'] < 0, 'dt (s)'] = 0

		else:
			return None

		# correct energy for negative current values
		df['energy (J)'] = df['energy (J)'].abs()
		sign_current = np.sign(df['current (A)'])
		df['delta energy (J)'] = df['delta energy (J)'] * sign_current
		df['energy (J)'] = df['delta energy (J)'].cumsum()

		# add energy unit Wh from J
		df['energy (Wh)'] = (df['energy (J)'] / 3600).round(6)
		df['delta energy (Wh)'] = df['energy (Wh)'].diff().fillna(0).round(6)

		self.df = df
		return df

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
