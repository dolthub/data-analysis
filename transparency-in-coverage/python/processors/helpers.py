import json
import os
import csv
import glob
import hashlib
import ijson
from schema import SCHEMA


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


def parse_top_matter(parser):

	prefix, event, value = next(parser)

	while event != 'start_array':

		builder = ijson.ObjectBuilder()
		builder.event(event, value)

		for nprefix, event, value in parser:
			if event == 'start_array':	
				return builder.value, (nprefix, event, value)

			builder.event(event, value)

		prefix, event, value = next(parser)


def parse_provider_refs(init_row, parser):

	prefix, event, value = init_row

	while event != 'start_array':
		prefix, event, value = next(parser)

	builder = ijson.ObjectBuilder()
	builder.event(event, value)

	for nprefix, event, value in parser:

		if (nprefix, event) == (prefix, 'end_array'):
			provider_references = builder.value
			return provider_references, (nprefix, event, value)

		builder.event(event, value)


def parse_in_network(init_row, parser, billing_code_filter = []):

	prefix, event, value = init_row

	while (prefix, event) != ('in_network.item', 'start_map'):
		
		if (prefix, event) == ('in_network', 'end_array'):
			return None, (prefix, event, value)

		prefix, event, value = next(parser)

	builder = ijson.ObjectBuilder()
	builder.event(event, value)

	for nprefix, event, value in parser:
		if (nprefix, event) == (prefix, 'end_map'):
			in_network = builder.value
			return in_network, (nprefix, event, value)

		elif (nprefix, event) == ('in_network.item.billing_code', 'string'):
			if billing_code_filter:
				if value not in billing_code_filter:					
					return None, (nprefix, event, value)

		builder.event(event, value)


