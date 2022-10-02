import json
import os
import csv
import glob
import hashlib
import ijson
import requests
from urllib.parse import urlparse
from schema import SCHEMA


def create_output_dir(output_dir, overwrite):
	if os.path.exists(output_dir):
		if overwrite:
			for file in glob.glob(f'{output_dir}/*'):
				os.remove(file)
	else:
		os.mkdir(output_dir)


def clean_url(url):
	parsed_url = urlparse(input_url)
	cleaned_url = (parsed_url[1] + parsed_url[2]).strip()
	return cleaned_url


def hashdict(data_dict):
	"""Get the hash of a dict (sort, convert to bytes, then hash)
	"""
	sorted_dict = dict(sorted(data_dict.items()))
	dict_as_bytes = json.dumps(sorted_dict).encode('utf-8')
	dict_hash = hashlib.md5(dict_as_bytes).hexdigest()
	return dict_hash


def dict_to_csv(data, output_dir, filename):
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


def flatten_obj(obj, output_dir, prefix = '', **hash_ids):
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
					flatten_obj(subvalue, output_dir, key_id, **hash_ids)
		
		elif type(value) == dict:            
			flatten_obj(value, output_dir, key_id, **hash_ids)

	dict_to_csv(data, output_dir, prefix)


def parse_root(parser):

	prefix, event, value = next(parser)

	while event != 'start_array':

		builder = ijson.ObjectBuilder()
		builder.event(event, value)

		for nprefix, event, value in parser:
			if event == 'start_array':	
				return builder.value, (nprefix, event, value)

			builder.event(event, value)

		prefix, event, value = next(parser)


def parse_provrefs(init_row, parser):

	prefix, event, value = init_row

	while event != 'start_array':
		prefix, event, value = next(parser)

	builder = ijson.ObjectBuilder()
	builder.event(event, value)

	for nprefix, event, value in parser:

		if (nprefix, event) == (prefix, 'end_array'):
			provrefs = builder.value
			return provrefs, (nprefix, event, value)

		builder.event(event, value)


def parse_innetwork(init_row, parser, code_filter = []):

	prefix, event, value = init_row

	while (prefix, event) != ('in_network.item', 'start_map'):

		if (prefix, event) == ('in_network', 'end_array'):
			raise ValueError('Done parsing in-network items.', (prefix, event, value))

		prefix, event, value = next(parser)

	builder = ijson.ObjectBuilder()
	builder.event(event, value)

	for nprefix, event, value in parser:
		if (nprefix, event) == (prefix, 'end_map'):
			innetwork = builder.value
			return innetwork, (nprefix, event, value)

		elif (nprefix, event) == ('in_network.item.billing_code', 'string'):
			if code_filter:
				if value not in code_filter:	
					raise ValueError(f'Code found ({value}) but not in code list.', (prefix, event, value))

		builder.event(event, value)


def fetch_remoteprovrefs(provrefs):
	new_provrefs = []
	for provref in provrefs:
		new_provref = provref.copy()
		if (loc := provref.get('location')):
			try:
				r = requests.get(loc)			
				new_provref['provider_groups'] = r.json()['provider_groups']
			except:
				loc = 'https://raw.githubusercontent.com/CMSgov/price-transparency-guide/master/examples/provider-reference/provider-reference.json'
				r = requests.get(loc)			
				new_provref['provider_groups'] = r.json()['provider_groups']
			new_provref.pop('location')
		new_provrefs.append(new_provref)
	return new_provrefs


def filter_provrefs(provref, npi_filter = [1111111111]):
    new_provrefs = []
    
    for provref in provref:        
        new_provref = provref.copy()
        new_provref_groups = []
        
        provref_groups = provref['provider_groups']
        
        for group in provref_groups:
            new_group = group.copy()
            npis = group['npi']
            new_npi = list(set(npis) & set(npi_filter))
            if new_npi:
                new_group['npi'] = new_npi
                new_provref_groups.append(new_group)
                
        if new_provref_groups:
            new_provref['provider_groups'] = new_provref_groups
            new_provrefs.append(new_provref)
            
    return new_provrefs


def process_innetwork(innetwork, provrefs, npi_filter = []):
	# new_neg_rates = []
	# for neg_rate in innetwork['negotiated_rates']:
	# 	new_neg_rate = neg_rate.copy()
	# 	for provref_id in neg_rate['provider_references']:
	# 		new_neg_rate['provider_groups'].extend(provrefs[provref_id])
	# 	new_neg_rates.append(new_neg_rate)
	# innetwork['negotiated_rates'] = new_neg_rates
	return innetwork

