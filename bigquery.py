from google.cloud import bigquery
from google.oauth2 import service_account
import time
import copy
from memoization import cached
import streamlit as st

#from db_mongo import DB_Handler

table = "events"
dataset = st.secrets['bigquery_dataset']
project_id = st.secrets['bigquery_project_id']
table_id = ".".join([project_id, dataset, table])
gcp_credentials = service_account.Credentials.from_service_account_info(
	st.secrets["gcp_service_account"]
)

client = bigquery.Client(
	project = project_id,
	credentials = gcp_credentials
)


def query(statement):
	""" general query, call with direct SQL statement string """
	request = client.query(statement)
	return request.result()


def update(table, statement):
	q = (
		f"UPDATE {project_id}.{dataset}.{table} "
		f"{statement}"
	)
	return query(q)


@cached(ttl=60*5)
def list_dataloggers(date_range = None):
	qu = (
			'SELECT `datalogger` '
			'FROM `' + table_id + '` '
	)
	if date_range:
		qu += (
			f"WHERE `date` BETWEEN '{date_range[0].strftime('%Y-%m-%d')}' AND '{date_range[1].strftime('%Y-%m-%d')}' "
		)
	qu += (
			'GROUP BY 1 '
			'ORDER BY `datalogger` '
	)
	print('query ', qu)
	rows = query(qu)
	ids = [r['datalogger'] for r in rows]
	return ids


def insert_bigquery(row_to_insert, table = 'events'):
	"""
	Insert a single row to bigquery
	"""
	table_id = ".".join([project_id, dataset, table])

	errors = client.insert_rows_json(
		table_id, [row_to_insert]
		#, row_ids = [None] * len(rows_to_insert)
	)
	if not errors:
		return True
	else:
		print("Encountered errors while inserting rows: {}".format(errors))
		return False
	return


def find(select, where = None, order = None, table = 'events', limit = None):
	schema = {
		'date': 'DATE'
	}
	select_text = ', '.join(['`' + str(v) + '`' for v in select])
	select_text = select_text.replace('`*`', '*')

	table_id = ".".join([project_id, dataset, table])

	query = f"SELECT {select_text} FROM `{table_id}` "
	if where:
		where_txt = " AND ".join(['`' + str(k) + '` ' + str(c) + ' "' + str(v) + '"' for k, c, v in where])
		query += f'WHERE {where_txt} '
	if order:
		order_text = ', '.join(['`' + str(v) + '` ' + str(d) for v, d in order])
		query += f'ORDER BY {order_text} '
	if limit:
		query += f'LIMIT {int(limit)}'

	print('query ', query)
	query_job = client.query(query)  # API request
	rows = query_job.result()
	return rows


@st.cache_data
def df_from_query(q):
	query_job = client.query(q)  # API request
	rows = query_job.result()
	return rows.to_dataframe()


def migrate_mongodb_to_bigquery(datalogger_id, limit = 1):
	print('migrate_mongodb_to_bigquery', datalogger_id)
	db = DB_Handler()
	query = {'datalogger': datalogger_id, 'migrated_to_bigquery': {'$ne': True}}
	fields = {'_id': 1, 'data': 0, 'history': 0, 'migrated_to_bigquery': 0}

	count = db.events.count_documents(query)
	print(count, 'events found')
	i = 0
	while count > 0:
		events = db.events.find(query, fields).limit(limit).sort('date', 1)

		events = list(events)
		if len(events) <= 0:
			return False

		for e in events:
			e.update((k, int(v)) for k, v in e.items() if k == 'timestamp')
			e.update({'time': e['date'].strftime("%H:%M:%S")})
			e.update({'date': e['date'].strftime("%Y-%m-%d")})

		for e in events:
			e_without_id = copy.deepcopy(e)
			del e_without_id['_id']
			i += 1
			print(i, 'insert_bigquery', e)

			result_bq = insert_bigquery(e_without_id)
			if result_bq:
				# db.update('events', str(e['_id']), {'migrated_to_bigquery': True})
				db.db['events'].update_one({'_id': e['_id']}, {'$set': {'migrated_to_bigquery': True}})

		count = db.events.count_documents(query)

	return


if __name__ == '__main__':
	#db = DB_Handler()
	#db.db['events'].update_many({'migrated_to_bigquery': True}, {'$set': {'migrated_to_bigquery': False}})

	#datalogger_ids = list_dataloggers()
	#for datalogger_id in datalogger_ids:
		#migrate_mongodb_to_bigquery(datalogger_id, limit = 10)
		#break

	#result = find(['filename'], [('filename', '=', '24-6F-28-D1-F6-50/events/732192944.csv')])
	#print(list(result))

	#print(list_dataloggers())
	#print(list_dataloggers.cache_info())

	update('tools', (
		'SET `description` = "aha" '
		'WHERE `SN` = "1234"'
	))

	pass