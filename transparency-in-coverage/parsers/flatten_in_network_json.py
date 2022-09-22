"""
JSON Parser for the payor in-network files
TODOs:
1. Decide whether to use UUIDs or hashes. If hashes, what/how to hash?
2. Finalize columns for flattened files. As of writing, we only have 
this done for the negotiated rates file.
3. Finalize column names
4. Run tests and time
"""

import ijson
import csv
import uuid
import glob
import os

in_network_file = "INSERT_FILE_HERE.json"


def pop(string):
    """
    Convenience function for making strings easier
    to read.

    "pop"s out the 'item' parts from a string. So that
    "rate.item.price.item" becomes "rate.price"
    """
    return '.'.join([s for s in string.split('.') if s != 'item'])


def write_data(output_dir, filename, data):
    
    file_loc = f"{output_dir}/{filename}.csv"
    
    fieldnames = [pop(k) for k in data.keys()]
    
    # TODO
    # Fill this out for the rest of the tables
    if filename == 'in_network.negotiated_rates.negotiated_prices':
        fieldnames = ['root_uuid',
                         'in_network_uuid',
                         'in_network.negotiated_rates_uuid',
                         'in_network.negotiated_rates.negotiated_prices_uuid',
                         'in_network.negotiated_rates.negotiated_prices.negotiated_type',
                         'in_network.negotiated_rates.negotiated_prices.negotiated_rate',
                         'in_network.negotiated_rates.negotiated_prices.expiration_date',
                         'in_network.negotiated_rates.negotiated_prices.service_code',
                         'in_network.negotiated_rates.negotiated_prices.billing_class',
                         'in_network.negotiated_rates.negotiated_prices.billing_code_modifier']
    
    if not os.path.exists(file_loc):
        with open(file_loc, "w") as f:
            writer = csv.DictWriter(f, fieldnames = fieldnames)
            writer.writeheader()
            writer.writerow(data)
            return
    
    with open(file_loc, "a") as f:
        writer = csv.DictWriter(f, fieldnames = fieldnames)
        writer.writerow(data)
        return


def walk(prefix, parser, output_dir, **uuids):
    """
    Walk the JSON rows and write the chunks to file.

    The ijson parser produces rows that are prefixed
    by their location in the JSON. For example you might
    see a prefix like

    parent.item.child.item.grandchild

    Anything at the same level gets written to the same
    file. To make the files readable I've stripped off
    everything that comes before the last dot.

    Similarly, a prefix of '' (corresponding to the
    highest level in the JSON) doesn't play nicely
    with writing to file, so I've changed it to "root"
    when needed.
    
    The initial event is always 'start_map'
    """
    
    if prefix == '':
        prefix = 'root'
    
    data = {}
    
    # Pass parent UUIDs to child            
    uuids[f'{pop(prefix)}_uuid'] = uuid.uuid4()
    for key, value in uuids.items():
        data[key] = value

    new_prefix, new_event, new_value = next(parser)
    
    while new_event != 'end_map':
        
        if new_prefix == '':
            new_prefix = 'root'
            
        if new_event in ['string', 'number']:
            data[pop(new_prefix)] = new_value
            new_prefix, new_event, new_value = next(parser)
            continue
            
        if new_event == 'start_array':
            new_prefix, new_event, new_value = next(parser)

            if new_event in ['string', 'number']:
                data[pop(new_prefix)] = []
                while new_event != 'end_array':
                    data[pop(new_prefix)].append(new_value)
                    new_prefix, new_event, new_value = next(parser)
                    
        if new_event == 'start_map':
            walk(new_prefix, parser, output_dir, **uuids)
                    
        new_prefix, new_event, new_value = next(parser)
                        
    # Once we've reached "end map" and the prefix
    # matches, we've captured everything at this level
    # in the JSON. Write it to file.
    write_data(output_dir, pop(prefix), data)


def tableize_file(filename, output_dir = './flatten'):
    
    if not os.path.exists(output_dir):

        os.mkdir(output_dir)
        
    else:
        for file in glob.glob(f'{output_dir}/*'):
            os.remove(file)

    with open(in_network_file, "r") as f:
    
        parser = ijson.parse(f)
        prefix, event, value = next(parser)

        walk(prefix, parser, output_dir)

        
tableize_file(in_network_file)