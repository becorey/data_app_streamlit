import os
import time

import google.api_core.exceptions
from google.cloud import storage
import itertools


projectID = 'i-azimuth-215919'
bucketName = 'bucket-i-azimuth-215919'
storage_client = storage.Client(projectID)
bucket = storage_client.get_bucket(bucketName)

def list_cloud_files(prefix = ''):
	blobs = bucket.list_blobs(prefix = prefix)
	return blobs


def list_dataloggers():
	"""
	list dataloggers based on all files in cloud storage (slow)
	"""
	print('list_dataloggers')
	blobs = bucket.list_blobs()
	ids = []
	for blob in blobs:
		id = blob.name.split('/')[0]

		# every datalogger ID is 6 bytes (12 chars) plus 5 - separators = 17
		if len(id) != 17:
			continue
		if id in ids:
			continue

		ids.append(id)
	return ids


def server_to_local_filename(server_file_name):
	return os.path.join('bucket', server_file_name.replace('/', os.sep))


def local_to_server_filename(local_filename):
	p = local_filename.replace(os.sep, '/')
	p = p.replace('bucket/', '')
	return p


def download_blob_by_name(blob_name):
	blob = bucket.blob(blob_name)
	return download_blob(blob)


def download_blob(blob, overwrite_existing = False):
	if blob.name.endswith('/'): # skip folders
		return False

	local_filename = server_to_local_filename(blob.name)

	if os.path.isfile(local_filename) and not overwrite_existing:
		# print(local_filename + ' already exists')
		return local_filename

	print('downloading ' + blob.name + ' to ' + local_filename)
	os.makedirs(os.path.dirname(local_filename), exist_ok = True) # create dirs if missing
	try:
		print(local_filename)
		blob.download_to_filename(local_filename)
	except google.api_core.exceptions.NotFound as e:
		print(time.time(), 'download_blob failed, file not found', blob.name, e)
		return False

	return local_filename


def move_blob(blob_name, destination_blob_name):
	""" https://cloud.google.com/storage/docs/copying-renaming-moving-objects#storage-move-object-python """
	source_blob = bucket.blob(blob_name)
	blob_copy = bucket.copy_blob(
		source_blob,
		bucket,
		destination_blob_name,
		if_generation_match = 0,
	)
	if blob_copy.name == destination_blob_name:
		bucket.delete_blob(blob_name)
	else:
		print('move_blob error blob_copy.name', blob_copy.name, '!= destination_blob_name', destination_blob_name)
	return blob_copy.name

def download_all_files():
	ids = list_dataloggers()
	print(ids)
	for id in ids:
		download_files_by_datalogger_id(id)
	return


def download_files_by_datalogger_id(datalogger_id, dirs = ('/events/', '/processed/'), filetypes = None):
	print('download_files_by_datalogger_id', datalogger_id, dirs)
	cfs = itertools.chain(
		*[list_cloud_files(datalogger_id + d) for d in dirs]
	)

	local_filenames = list()
	for cf in cfs:
		if filetypes and isinstance(filetypes, list):
			ext = cf.name.rsplit('.', 1)[-1]
			if ext not in filetypes:
				continue
		print(cf.name)
		local_filename = download_blob(cf)
		local_filenames.append(local_filename)
	return local_filenames


def upload_google_cloud_storage(source_file_name, destination_blob_name = ''):
	"""Uploads a file to the bucket."""
	# The ID of your GCS bucket
	# bucket_name = "your-bucket-name"
	# The path to your file to upload
	# source_file_name = "local/path/to/file"
	# The ID of your GCS object
	# destination_blob_name = "storage-object-name"
	global bucketName
	storage_client = storage.Client()
	bucket = storage_client.bucket(bucketName)
	if destination_blob_name == '':
		destination_blob_name = source_file_name

	blob = bucket.blob(destination_blob_name)
	blob.upload_from_filename(source_file_name)
	print(destination_blob_name, "uploaded to", bucketName)


if __name__ == "__main__":
	print("Run data_from_cloud.py")

	id = '10-06-1C-30-10-14'
	#download_files_by_datalogger_id(id)


	blobs = storage_client.list_blobs(
		bucketName,
		prefix = "",  # <- you need the trailing slash
		delimiter = "/",
		max_results = 1,
	)
	next(blobs, None)  # Force blobs to load.
	print(list(blobs.prefixes))

	print(list_dataloggers())