"""
Open questions:

1. How to make this async?
2. Check for bugs
"""

import ijson
import json
import os
import csv
import glob
import requests
import gzip
import time
import hashlib
from tqdm import tqdm
import io


SCHEMA = {
    'root':[
        'root_hash_id',
        'reporting_entity_name',
        'reporting_entity_type',
        'last_updated_on',  
        'version',
        'url',],

    'in_network':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiation_arrangement',
        'in_network.name',
        'in_network.billing_code_type',
        'in_network.billing_code_type_version',
        'in_network.billing_code',
        'in_network.description',],

    'in_network.negotiated_rates':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.provider_references',],

    'in_network.negotiated_rates.negotiated_prices':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.negotiated_prices_hash_id',
        'in_network.negotiated_rates.negotiated_prices.negotiated_type',
        'in_network.negotiated_rates.negotiated_prices.negotiated_rate',
        'in_network.negotiated_rates.negotiated_prices.expiration_date',
        'in_network.negotiated_rates.negotiated_prices.service_code',
        'in_network.negotiated_rates.negotiated_prices.billing_class',
        'in_network.negotiated_rates.negotiated_prices.additional_information',
        'in_network.negotiated_rates.negotiated_prices.billing_code_modifier',],

    'in_network.negotiated_rates.provider_groups':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.provider_groups_hash_id',
        'in_network.negotiated_rates.provider_groups.npi',],

    'in_network.negotiated_rates.provider_groups.tin':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.provider_groups_hash_id',
        'in_network.negotiated_rates.provider_groups.tin_hash_id',
        'in_network.negotiated_rates.provider_groups.tin.type',
        'in_network.negotiated_rates.provider_groups.tin.value',],

    'provider_references':[
        'root_hash_id',
        'provider_references_hash_id',
        'provider_references.provider_group_id',],

    'provider_references.provider_groups':[
        'root_hash_id',
        'provider_references_hash_id',
        'provider_references.provider_groups_hash_id',
        'provider_references.provider_groups.npi',],

    'provider_references.provider_groups.tin':[
        'root_hash_id',
        'provider_references_hash_id',
        'provider_references.provider_groups_hash_id',
        'provider_references.provider_groups.tin_hash_id',
        'provider_references.provider_groups.tin.type',
        'provider_references.provider_groups.tin.value',]
}


def write_dict_to_file(output_dir, filename, data):
    """Write dictionary to one of the files
    defined in the schema
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
        return


def flatten_to_file(obj, output_dir, prefix = '', **hash_ids):
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

        plain_value = False

        if type(value) in [str, int, float]:
            plain_value = True

        elif type(value) == list and len(value) == 0:
            plain_value = True

        elif type(value) == list:
            if type(value[0]) in [str, int, float]:
                plain_value = True
        
        if plain_value:
            data[key_id] = value

    hash_ids[f'{prefix}_hash_id'] = hashdict(data)

    for key, value in hash_ids.items():
        data[key] = value

    for key, value in obj.items():

        key_id = f'{prefix}.{key}' if prefix else key

        dict_value = False

        if type(value) == list and value:
            if type(value[0]) in [dict]:
                dict_value = True

        if dict_value:
            for subvalue in value:
                flatten_to_file(subvalue, output_dir, key_id, **hash_ids)
                   
    write_dict_to_file(output_dir, prefix, data)


def hashdict(data_dict):
    """Get the hash of a dict (sort, convert to bytes, then hash)
    """
    sorted_dict = dict(sorted(data_dict.items()))
    return hashlib.md5(json.dumps(sorted_dict).encode('utf-8')).hexdigest()


def parse_to_file(url, billing_code_list, output_dir, overwrite = False):
    
    if os.path.exists(output_dir):
        if overwrite:
            for file in glob.glob(f'{output_dir}/*'):
                os.remove(file)
    else:
        os.mkdir(output_dir)

    with requests.get(url, stream = True) as r:

        f = gzip.GzipFile(fileobj = r.raw)

        data = {}
        hash_ids = {}

        parser = ijson.parse(f, use_float = True)
        
        for prefix, event, value in parser:
            if event in ['string', 'number']:
                data[f'{prefix}'] = value

            if (prefix, event, value) == ('', 'map_key', 'provider_references'):
                break

        data['url'] = url
        data['root_hash_id'] = hashdict(data)

        hash_ids['root_hash_id'] = data['root_hash_id']

        provider_data = next(ijson.items(parser, 'provider_references', use_float = True))
            
        objs = ijson.items(parser, 'in_network.item', use_float = True)
        
        codes_found = False
        provider_references_list = []

        for obj in objs:

            # Loop through objects
            if obj['billing_code'] in billing_code_list:
                codes_found = True

                # Write the object
                flatten_to_file(obj, output_dir, prefix = 'in_network', **hash_ids)

                for negotiated_rate in obj['negotiated_rates']:
                    for provider_reference in negotiated_rate['provider_references']:
                        provider_references_list.append(provider_reference)
                        
        if not codes_found:
            return

        write_dict_to_file(output_dir, 'root', data)
        
        for obj in provider_data:
            if obj['provider_group_id'] in provider_references_list:
                pass
                flatten_to_file(obj, output_dir, prefix = 'provider_references', **hash_ids)

# EXAMPLE usage

import polars as pl
from tqdm.notebook import tqdm
df = pl.read_csv('uhc-in-network-files.csv')
urls = df.filter(pl.col('url').str.contains('in-network')).sort('size')['url'][:1000].to_list()

my_code_list = ['85004', '85007', '85008', '85009', '85013', '85014', '85018', '85032', '85041', '85048', '85049']
my_output_dir = 'flatten'

if os.path.exists(my_output_dir):
    for file in glob.glob(f'{output_dir}/*'):
        os.remove(file)

my_code_list = ['85004', '85007', '85008', '85009', '85013', '85014', '85018', '85032', '85041', '85048', ''85049']
my_output_dir = 'flatten'

for url in tqdm(urls):
    try:
        parse_to_file(url, billing_code_list = my_code_list, output_dir = my_output_dir, overwrite = False)
    except ijson.common.IncompleteJSONError:
        continue

