"""
TODO:
1. Handle bundled codes
2. in_network.negotiated_rates.provider_references are stored as lists -- is this correct?
3. provider_references.provider_groups.npi is sometimes blank -- is this correct?
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
        'provider_references.provider_groups.npi',
        ],

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


def compress_data(data):
    json_data = json.dumps(data)
    encoded = json_data.encode('utf-8')
    compressed = gzip.compress(encoded)


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
                    flatten_to_file(subvalue, output_dir, key_id, **hash_ids)
        
        elif type(value) == dict:            
            flatten_to_file(value, output_dir, key_id, **hash_ids)

    write_dict_to_file(output_dir, prefix, data)


def hashdict(data_dict):
    """Get the hash of a dict (sort, convert to bytes, then hash)
    """
    sorted_dict = dict(sorted(data_dict.items()))
    return hashlib.md5(json.dumps(sorted_dict).encode('utf-8')).hexdigest()


def create_output_dir(output_dir, overwrite):
    if os.path.exists(output_dir):
        if overwrite:
            for file in glob.glob(f'{output_dir}/*'):
                os.remove(file)
    else:
        os.mkdir(output_dir)


def parse_to_file(url, billing_code_list, output_dir, overwrite = False, logging = False):
    """This streams through a file, flattens it, and writes it to 
    file. It streams the zipped files.

    MRFs are structured, schematically, like:

    {
        file_metadata,
        provider_references,
        in_network_items,
    }

    In order to filter out the metadata/codes/provider references, 
    we do the following.

    1. Stream the gzipped file
    2. Create a parser with ijson
    3. Manually parse through the front matter until we get to
    provider references
    4. Continue to provider_references, caching them in a variable
    5. Continue to in_network_items, writing matching codes 
    and saving provider_references
    6. Filter through the cached provider_references and save the 
    matching references
    """

    create_output_dir(output_dir, overwrite)

    with requests.get(url, stream = True) as r:
        f = gzip.GzipFile(fileobj = r.raw)

        parser = ijson.parse(f, use_float = True)

        root_data = {'url': url}
        for prefix, event, value in parser:
            if event in ['string', 'number']:
                root_data[f'{prefix}'] = value
                prefix, event, value = next(parser)
            if (prefix, event, value) == ('', 'map_key', 'provider_references'):
                break

        print("Got front matter")
        root_data['root_hash_id'] = hashdict(root_data)
        
        # There will always be exactly one item
        provider_references = next(ijson.items(parser, 'provider_references', use_float = True))
        
        in_network_items = ijson.items(parser, 'in_network.item', use_float = True)

        # Generator expression is possible here
        # in_network_items = (i for i in in_network_items if item['billing_code'] in billing_code_list)

        matching_in_network_items = False
        provider_references_list = []
        hash_ids = {'root_hash_id': root_data['root_hash_id']}

        print("Looking for matching codes")
        for item in in_network_items:
            if item['billing_code'] in billing_code_list:
                matching_in_network_items = True
                flatten_to_file(item, output_dir, prefix = 'in_network', **hash_ids)
            for negotiated_rate in item['negotiated_rates']:
                for provider_reference in negotiated_rate['provider_references']:
                    provider_references_list.append(provider_reference)

        if not matching_in_network_items:
            return

        print("Matching codes found.")
        write_dict_to_file(output_dir, 'root', root_data)

        for provider_reference in provider_references:
            if provider_reference['provider_group_id'] in provider_references_list:
                flatten_to_file(provider_reference, output_dir, prefix = 'provider_references', **hash_ids)
        
