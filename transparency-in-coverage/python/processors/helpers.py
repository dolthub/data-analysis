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


def read_billing_codes_from_csv(filename):

	with open(filename, 'r') as f:
		reader = csv.DictReader(f)
		codes = []
		for row in reader:
			codes.append((row['billing_code_type'], row['billing_code']))
	return codes


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
			data[key_id] = None

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
			provrefs = normalize_provrefs(provrefs)
			return provrefs, (nprefix, event, value)

		builder.event(event, value)


def normalize_provrefs(provrefs):
	"""Some provider_references have the key 'provider_group'
	instead of 'provider_groups' with an array. Normalize
	them to that they all match.
	"""
	for i, provref in enumerate(provrefs):
		if provref.get('provider_group'):
			provref['provider_groups'] = provref['provider_group']
			provref.pop('provider_group')
	return provrefs


def normalize_innetwork(innetwork, provrefs, provref_id_map):
	"""Optional (small size increase) but put all the 
	provider references inside the innetwork tables.
	"""
	for i, neg_rate in enumerate(innetwork['negotiated_rates']):
		if neg_rate.get('provider_references'):
			if neg_rate.get('provider_references'):
				provref_ids = neg_rate['provider_references']
				provref_idxs = [provref_id_map[j] for j in provref_ids]
				provref_info_arr = [provrefs[j] for j in provref_idxs]
				new_provider_groups = []
				for provref_info in provref_info_arr:
					for provgroup in provref_info['provider_groups']:
						new_provider_groups.append(provgroup)
		innetwork['negotiated_rates'][i]['provider_groups'] = new_provider_groups
		innetwork['negotiated_rates'][i].pop('provider_references')
	return innetwork


def parse_innetwork(init_row, parser, code_filter = []):

	prefix, event, value = init_row

	while (prefix, event) != ('in_network.item', 'start_map'):

		if (prefix, event) == ('in_network', 'end_array'):
			msg = 'Done parsing in-network items.'
			raise ValueError(msg, (prefix, event, value))

		prefix, event, value = next(parser)

	builder = ijson.ObjectBuilder()
	builder.event(event, value)

	for nprefix, event, value in parser:

		if (nprefix, event) == (prefix, 'end_map'):
			innetwork = builder.value
			return innetwork, (nprefix, event, value)

		elif (nprefix) == ('in_network.item.negotiated_rates'):
			if code_filter:
				billing_code_type = builder.value['billing_code_type']
				billing_code = str(builder.value['billing_code'])
				code_to_check = (billing_code_type, billing_code)

				if code_to_check not in code_filter:

					msg = f'Code found ({billing_code_type}: {billing_code}) but not in code_filter.'
					raise ValueError(msg, (prefix, event, value))

		builder.event(event, value)


def fetch_remoteprovrefs(provrefs):
	new_provrefs = []
	for provref in provrefs:
		new_provref = provref.copy()
		if (loc := provref.get('location')):
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
