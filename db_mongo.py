from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import urllib.parse
import datetime
from bson.objectid import ObjectId
import pandas as pd
import time
import certifi
import decimal
import streamlit as st


def updated_fields(old_dict, new_dict, exclude = ('history',)):
	for ex in exclude:
		if ex in old_dict:
			del old_dict[ex]
		if ex in new_dict:
			del new_dict[ex]

	# this fails for a list in the dict
	try:
		result = dict(set(new_dict.items()) - set(old_dict.items()))
	except TypeError as e:
		print(e)
		print(old_dict, new_dict)
		result = dict()

	return result


def list_dataloggers_from_db(db):
	tools = db.tools.find({})
	datalogger_ids = [t['datalogger'] for t in tools]
	datalogger_ids = sorted(list(set(datalogger_ids))) # unique
	return datalogger_ids


class DB_Handler():
	def __init__(self, db_name = 'datalogger'):
		self.client = self.get_client()
		self.db = self.client[db_name]
		self.t_last_op = 0

		self.events = None
		self.tools = None
		self.users = None
		self.sessions = None
		self.studies = None
		self.gnss = None
		# shorten references
		for col in self.db.list_collection_names():
			setattr(self, col, self.db[col])

		return

	def get_client(self) -> MongoClient:
		DB_USERNAME = urllib.parse.quote_plus(st,secrets['mongo_username'])
		DB_PASSWORD = urllib.parse.quote_plus(st.secrets['mongo_password'])
		DB_CLUSTER = urllib.parse.quote_plus(st.secrets['mongo_cluster'])
		DB_APPNAME = urllib.parse.quote_plus(st.secrets['mongo_appname'])
		uri = (
			f'mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@{DB_CLUSTER}.afsghfr.mongodb.net/'
			f'?retryWrites=true'
			f'&w=majority'
			f'&appName={DB_APPNAME}'
			#f'&tls=true'
			#f'&ssl=true'
			#f'&ssl_cert_reqs=CERT_NONE'
		)

		uri = (
			f"mongodb://{DB_USERNAME}:{DB_PASSWORD}@ac-721shxu-shard-00-00.afsghfr.mongodb.net:27017,ac-721shxu-shard-00-01.afsghfr.mongodb.net:27017,ac-721shxu-shard-00-02.afsghfr.mongodb.net:27017/"
			f"?ssl=true"
			f"&replicaSet=atlas-djcin5-shard-0"
			f"&authSource=admin"
			f"&retryWrites=true"
			f"&w=majority"
			f"&appName={DB_APPNAME}"
		)

		# https://stackoverflow.com/a/68266787/2666454
		ca = certifi.where()
		# If you're getting SSL handshake failed
		# ensure your IP address is whitelisted in
		# Mongo Atlas -> Security -> Network Access
		client = MongoClient(
			uri,
			server_api = ServerApi('1'),
			tlsCAFile = ca,
			connect = False
		)
		return client

	def insert(self, collection, data):
		data.update({'history': []})
		for k, v in data.items():
			if isinstance(v, decimal.Decimal):
				data[k] = float(v)

		new_doc = self.db[collection].insert_one(data)
		self.t_last_op = time.time()
		return new_doc.inserted_id

	def update(self, collection, _id, data):
		if type(_id) not in [bytes, str, ObjectId]:
			print('warning type(_id)', type(_id), _id)
			_id = str(_id)
		if _id == 'nan':
			print('warning _id', _id)
			return

		# old_doc = self.db[collection].find_one({'_id': ObjectId(_id)})

		if 'history' in data:
			raise ValueError('Cannot directly update history')

		if len(data) <= 0:
			return

		for k, v in data.items():
			if isinstance(v, decimal.Decimal):
				data[k] = float(v)

		hist = {
			'what': data,
			'when': datetime.datetime.now()
		}
		self.db[collection].update_one({'_id': ObjectId(_id)}, {'$set': data, '$push': {'history': hist}})
		self.t_last_op = time.time()
		return

	def df(self, cursor):
		return pd.DataFrame(list(cursor))

if __name__ == '__main__':
	# Create a new client and connect to the server
	dbh = DB_Handler()
	# Send a ping to confirm a successful connection
	try:
		dbh.client.admin.command('ping')
		print("Pinged your deployment. You successfully connected to MongoDB!")
	except Exception as e:
		print(e)

