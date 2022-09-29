import json
import os
import csv
import glob
import requests
import gzip
import time
import hashlib
import ijson
import sys
import logging
from schema import SCHEMA

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_output_dir(output_dir, overwrite):
	if os.path.exists(output_dir):
		if overwrite:
			for file in glob.glob(f'{output_dir}/*'):
				os.remove(file)
	else:
		os.mkdir(output_dir)


def hashdict(data_dict):
	"""Get the hash of a dict (sort, convert to bytes, then hash)
	"""
	sorted_dict = dict(sorted(data_dict.items()))
	dict_as_bytes = json.dumps(sorted_dict).encode('utf-8')
	dict_hash = hashlib.md5(dict_as_bytes).hexdigest()
	return dict_hash


def write_dict_to_file(output_dir, filename, data):
	"""Write a dictionary to a CSV file as a row, 
	where the schema is given by the filename.
	"""
	file_loc = f'{output_dir}/{filename}.csv'

	fieldnames = SCHEMA[filename]

	if not os.path.exists(file_loc):
		with open(file_loc, 'w') as f:
			writer = csv.DictWriter(f, fieldnames = fieldnames)
			writer.writeheader()
			writer.writerow(data)
			return
	
	with open(file_loc, 'a') as f:
		writer = csv.DictWriter(f, fieldnames = fieldnames)
		writer.writerow(data)


def flatten_dict_to_file(obj, output_dir, prefix = '', **hash_ids):
	"""Takes an object, turns it into a dict, and 
	writes it to file.

	We have to track the hash_ids. This requires us to loop
	through the dict once to take out the plain values,
	then loop through it again to take care of the nested 
	dicts, while passing the hash_ids down as a param.
	"""

	data = {}

	for key, value in obj.items():
		key_id = f'{prefix}.{key}' if prefix else key

		if type(value) in [str, int, float]:
			data[key_id] = value

		elif type(value) == list and len(value) == 0:
			# Can set to false to not keep empty lists
			data[key_id] = value

		elif type(value) == list:
			if type(value[0]) in [str, int, float]:
				data[key_id] = value
		
	hash_ids[f'{prefix}_hash_id'] = hashdict(data)

	for key, value in hash_ids.items():
		data[key] = value

	for key, value in obj.items():
		key_id = f'{prefix}.{key}' if prefix else key
		
		if type(value) == list and value:
			if type(value[0])  == dict:
				for subvalue in value:
					flatten_dict_to_file(subvalue, output_dir, key_id, **hash_ids)
		
		elif type(value) == dict:            
			flatten_dict_to_file(value, output_dir, key_id, **hash_ids)

	write_dict_to_file(output_dir, prefix, data)

def get_mrfs_from_index(index_file_url):
	in_network_file_urls = []

	with requests.get(index_file_url, stream = True) as r:
		LOG.info(f"Began streaming file: {index_file_url}")
		url_size = round(int(r.headers['Content-length'])/1_000_000, 3)
		LOG.info(f"Size of file: {url_size} MB")

		f = r.content

		objs = ijson.items(f, 'reporting_structure.item.in_network_files')
		for obj in objs:
			for in_network_file_obj in obj:
				in_network_file_urls.append(in_network_file_obj['location'])
	
	LOG.info(f"Found: {len(in_network_file_urls)} in-network files.")
	return in_network_file_urls


def parse_to_file(url, output_dir, billing_code_list = []):
	"""This streams through a file, flattens it, and writes it to 
	file. It streams the zipped files, avoiding saving them to disk.

	MRFs are structured, schematically like
	{
		file_metadata (top matter),
		provider_references (always one line),
		[in_network_items] (multiple lines),
	}

	But the in_network_items are linked to provider references. The
	problem we have to solve is: how do we collect only the codes
	and provider references we want, while reading the file once?

	The answer is: cache the provider references during streaming,
	then filter the in_network_items. Once you know which provider
	references to keep, you can filter the cached object.

	The steps we take are:
	1. Check to see if there are matching codes. If so, write them
	2. Write the top matter to file
	3. Write the provider references to file
	"""
	s = time.time()

	with requests.get(url, stream = True) as r:
		LOG.info(f"Began streaming file: {url}")
		url_size = round(int(r.headers['Content-length'])/1_000_000, 3)
		LOG.info(f"Size of file: {url_size} MB")

		if url.endswith('json.gz'):
			f = gzip.GzipFile(fileobj = r.raw)
			LOG.info(f"Unzipping streaming file.")
		elif url.endswith('.json'):
			f = r.raw
		
		# Create a parser and loop through the top matter
		# Break as soon as we get to provider references
		parser = ijson.parse(f, use_float = True)

		root_data = {'url': url}
		for prefix, event, value in parser:
			if event in ['string', 'number']:
				root_data[f'{prefix}'] = value
				prefix, event, value = next(parser)
			if (prefix, event, value) == ('', 'map_key', 'provider_references'):
				break
		LOG.info(f"Successfully parsed top matter.")

		root_data['root_hash_id'] = hashdict(root_data)
		hash_ids = {'root_hash_id': root_data['root_hash_id']}
		
		# There will always be one or less provider references item
		# Cache this in a variable for later use
		objs = ijson.items(parser, 'provider_references', use_float = True)
		provider_refs_exist = False
		try:
			provider_references = next(obs)
			provider_refs_exist = True
			p_ref_size = round(sys.getsizeof(provider_references)/1_000_000, 3)
			LOG.info(f"Cached provider references. Size: {p_ref_size} MB.")
			provider_references_list = []
		except StopIteration:
			pass
		
		objs = ijson.items(parser, 'in_network.item', use_float = True)
		matching_codes_exist = False
		
		if billing_code_list:
			in_network_items = (o for o in objs if o['billing_code'] in billing_code_list)
		else:
			in_network_items = objs

		LOG.info(f"Started looping through in_network_items...")
		# First, search for matching codes and write them to file,
		# or end execution if not found
		for item in in_network_items:
			LOG.debug(f"Found billing code: {item['billing_code_type']}: {item['billing_code']}")
			matching_in_network_items = True
			flatten_dict_to_file(item, output_dir, prefix = 'in_network', **hash_ids)
			if provider_refs_exist:
				for negotiated_rate in item['negotiated_rates']:
					for provider_reference in negotiated_rate['provider_references']:
						provider_references_list.append(provider_reference)

		LOG.info(f"Finished looping through in_network_items.")
		if not matching_codes_exist:
			return

		# If we've found codes, write the top matter to file
		LOG.info(f"Wrote top matter to file.")
		write_dict_to_file(output_dir, 'root', root_data)

		# Finally, write the matching provider references to file
		if provider_refs_exist: 
			for provider_reference in provider_references:
				if provider_reference['provider_group_id'] in provider_references_list:
					flatten_dict_to_file(provider_reference, output_dir, prefix = 'provider_references', **hash_ids)
			LOG.info(f"Wrote provider references to file.")

		td = time.time() - s
		LOG.info(f'Total time taken: {round(td/60, 3)} min.')
