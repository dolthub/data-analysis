"""
JSON PARSER FOR IN-NETWORK MRFS

This script parses and saves in-network MRF files in a specific
relational schema. It works with inflated, gzipped, and remote 
gzipped JSON. That means you can plug in a URL and stream the JSON
while writing it to file, without having to inflate it.

ISSUES:

1. On my (@spacelove/Alec Stein) M1 Macbook Pro, processing a 1.3GB
JSON file takes about 11 minutes. I thought that writing CSV files
line-by-line might be the bottleneck, but when I didn't write 
_anything_, it still took 6 minutes just to stream the whole JSON
 file.

This is unacceptably long. At this rate, it would take CPU years just
to process all the JSON MRFs.

We need this to be about 100x faster. 

2. My test showed that the flat CSV files total to about the same size
as the original JSON file. There's no free size reduction just for
making them relational. 

Out of curiosity I decided to check what was taking up most of the
space.

Without the UUIDs, there is a size reduction of 25% in the largest
file(prices).

With the UUIDs, parquet format reduces the size about 85%. 

Without the UUIDs and in parquet, the size reduction is about 96%.
(Interestingly, this is still 2x the size of the compressed JSON --
apparently quite a light format.)

This is all a problem for us at DoltHub since Dolt will only work with
data on the order of ~X00GB, and we need some kind of linking number
ID (a hash, or a UUID) and we won't get the compression that you get
with parquet.

Since the data is at least 100TB we have to reduce what we store by a
factor of at least 99.9%. There are some options:

    1. limit the procedures we collect (there are at least 20,000).
    BUT: streaming the files is still too slow. 2. limit the NPIs
    that we collect. Same issue as above. 3. find a more efficient
    storage format/schema

3. UUIDs were supposed to solve the problem of distributed collection.
Basically, if two people use incrementing IDs as linking numbers,
they'll end up overwriting each other's  data. (If person A gets the
same linking numbers 1, 2, 3... as person B, then when person B goes
to insert a row with primary key (1), they'll overwrite person A's
primary key (1).

With UUIDs this can't happen and collisions are negligibly rare. On
the other hand, it's very easy to get duplicate rows with UUIDs,
which is its own problem. Hashing the rows somehow and using that as
a PK might be the way to go -- but how, and what, to hash?

4. We can probably come up with better column names than the ones
given here. We also need to check that these columns actually fit the
data. The SCHEMA below does not understand bundled codes, for
example -- we need to add that flexibilty.

5. There is one more TODO here. When provider references are given as
a URL, an async function needs to fetch the provider information from
that URL.
"""

# The fastest backed for ijson. You'll need
# YALJ installed. See http://lloyd.github.io/yajl/
import ijson.backends.yajl2_c as ijson

# If you have trouble installing, do
# import ijson
# but this is much slower for streaming

import ijson
import os
import csv
import uuid
import glob
import requests
import gzip
import time

# PRELIMINARY SCHEMA
# Missing bundled codes, among other things
SCHEMA = {
    'root':[
        'root_uuid',
        'reporting_entity_name',
        'reporting_entity_type',
        'last_updated_on',
        'version',],

    'in_network':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiation_arrangement',
        'in_network.name',
        'in_network.billing_code_type',
        'in_network.billing_code_type_version',
        'in_network.billing_code',
        'in_network.description',],

    'in_network.negotiated_rates':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.provider_references',],

    'in_network.negotiated_rates.negotiated_prices':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.negotiated_prices_uuid',
        'in_network.negotiated_rates.negotiated_prices.negotiated_type',
        'in_network.negotiated_rates.negotiated_prices.negotiated_rate',
        'in_network.negotiated_rates.negotiated_prices.expiration_date',
        'in_network.negotiated_rates.negotiated_prices.service_code',
        'in_network.negotiated_rates.negotiated_prices.billing_class',
        'in_network.negotiated_rates.negotiated_prices.additional_information',
        'in_network.negotiated_rates.negotiated_prices.billing_code_modifier',],

    'in_network.negotiated_rates.provider_groups':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.provider_groups_uuid',
        'in_network.negotiated_rates.provider_groups.npi',],

    'in_network.negotiated_rates.provider_groups.tin':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.provider_groups_uuid',
        'in_network.negotiated_rates.provider_groups.tin_uuid',
        'in_network.negotiated_rates.provider_groups.tin.type',
        'in_network.negotiated_rates.provider_groups.tin.value',],

    'provider_references':[
        'root_uuid',
        'provider_references_uuid',
        'provider_references.provider_group_id',],

    'provider_references.provider_groups':[
        'root_uuid',
        'provider_references_uuid',
        'provider_references.provider_groups_uuid',
        'provider_references.provider_groups.npi',],

    'provider_references.provider_groups.tin':[
        'root_uuid',
        'provider_references_uuid',
        'provider_references.provider_groups_uuid',
        'provider_references.provider_groups.tin_uuid',
        'provider_references.provider_groups.tin.type',
        'provider_references.provider_groups.tin.value',]
}


def cull(prefix):
    """
    Convenience function for making prefix
    strings less cluttered.

    culls out the 'item' parts from a prefix. So that
    "rate.item.price.item" becomes "rate.price"

    The blank prefix '' doesn't play nicely with
    csv writing so I've made it 'root' by default
    """
    if prefix == '':
        return 'root'

    return prefix.replace('.item', '')


def write_to_csv(output_dir, filename, data):
    
    file_loc = f'{output_dir}/{filename}.csv'
    
    fieldnames = SCHEMA[filename]
    # fieldnames = data.keys()
    
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


def direct_stream(in_network_file):
    """Profiling tool"""
    s = time.time()

    if in_network_file.endswith('.json'):

        file_size_mb = os.path.getsize(in_network_file)//1_000_000
        print(f'Testing stream speed of inflated file: {in_network_file} ({file_size_mb} MB)')
    
        with open(in_network_file, 'r') as f:
            
            parser = ijson.parse(f, use_float = True)
            for row in parser:
                pass
            
    elif in_network_file.endswith('.json.gz'):

        file_size_mb = os.path.getsize(in_network_file)//1_000_000
        print(f'Testing stream speed of gzipped file: {in_network_file} ({file_size_mb} MB)')

        with open(in_network_file, 'rb') as g:
            
            f = gzip.GzipFile(fileobj = g)
            parser = ijson.parse(f, use_float = True)
            for row in parser:
                pass

    td = round(time.time() - s, 2)
    print(f'Time taken to stream without walking: {td} s\n')


def walk(prefix, parser, output_dir, write_data = True, **uuids):
    """
    Walk the JSON rows and write the chunks to file.

    The ijson parser produces rows that are prefixed
    by their location in the JSON. For example you might
    see a prefix like

    'parent.item.child.grandchild'

    equivalent to the "culled" prefix

    cull('parent.item.child.grandchild') = 'parent.child.grandchild'

    Anything at the same level gets written to the same
    file.
    """
    
    data = {}
    
    # Pass parent UUIDs to child            
    uuids[f'{cull(prefix)}_uuid'] = uuid.uuid4()
    for key, value in uuids.items():
        data[key] = value

    prefix, event, value = next(parser)
    
    while event != 'end_map':
        
        if event in ['string', 'number']:
            data[cull(prefix)] = value
            prefix, event, value = next(parser)
            continue
            
        if event == 'start_array':
            prefix, event, value = next(parser)

            if event in ['string', 'number']:
                data[cull(prefix)] = []

                while event != 'end_array':
                    data[cull(prefix)].append(value)
                    prefix, event, value = next(parser)
                    
        if event == 'start_map':
            walk(prefix, parser, output_dir, write_data, **uuids)
                    
        prefix, event, value = next(parser)
                        
    # Once we've reached "end map" and the prefix
    # matches, we've captured everything at this level
    # in the JSON. Write it to file.
    if write_data:
        write_to_csv(output_dir = output_dir, filename = cull(prefix), data = data)


def parse_json(in_network_file, output_dir = './flatten', remote = False, write_data = True):
    """
    Can parse inflated, gzipped, or remote gzipped JSON files.

    Usage:
        1. parse_json('in_network_file.json')
        2. parse_json('in_network_file.json.gz')
        3. parse_json('https://www.uhc.com/in_network_file.json.gz')

    Thanks to @jakesnipes for working out the details on GZip and
    streaming!
    """
    s = time.time()
    print(f'Using ijson backend: {ijson.backend}')
    if write_data == False:
        print('Debugging: not writing to file')
    else:
        print(f'Writing to output_dir = {output_dir}')

    if os.path.exists(output_dir):
        for file in glob.glob(f'{output_dir}/*'):
            os.remove(file)
    
    else: os.mkdir(output_dir)
    
    if in_network_file.startswith('http'):
        
        print(f'Streaming from: {in_network_file}')

        with requests.get(in_network_file, stream = True) as r:

            # use_float may give a slight performance boost
            f = gzip.GzipFile(fileobj = r.raw)
            parser = ijson.parse(f, use_float = True)
            prefix, event, value = next(parser)
            walk(prefix = prefix, parser = parser, output_dir = output_dir, write_data = write_data)
    
    elif in_network_file.endswith('.json'):

        file_size_mb = os.path.getsize(in_network_file)//1_000_000
        print(f'Streaming inflated file: {in_network_file} ({file_size_mb} MB)')
    
        with open(in_network_file, 'r') as f:
            
            parser = ijson.parse(f, use_float = True)
            prefix, event, value = next(parser)
            walk(prefix = prefix, parser = parser, output_dir = output_dir, write_data = write_data)
            
    elif in_network_file.endswith('.json.gz'):

        file_size_mb = os.path.getsize(in_network_file)//1_000_000
        print(f'Streaming gzipped file: {in_network_file} ({file_size_mb} MB)')

        with open(in_network_file, 'rb') as g:
            
            f = gzip.GzipFile(fileobj = g)
            parser = ijson.parse(f, use_float = True)
            prefix, event, value = next(parser)
            walk(prefix = prefix, parser = parser, output_dir = output_dir, write_data = write_data)

    td = round(time.time() - s, 2)
    print(f'Time taken to parse: {td} s\n')

direct_stream(in_network_file = './TEST_FILE.json.gz')

parse_json(in_network_file = './TEST_FILE.json.gz', output_dir = './flatten', write_data = False)

parse_json(in_network_file = './TEST_FILE.json.gz', output_dir = './flatten')
