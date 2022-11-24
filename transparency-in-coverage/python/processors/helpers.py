import json
import os
import csv
import glob
import hashlib
import ijson
import requests
import logging
import gzip
from urllib.parse import urlparse
from pathlib import Path
from schema import SCHEMA

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)

file_handler = logging.FileHandler('log.txt', 'a')
file_handler.setLevel(logging.DEBUG)

log.addHandler(file_handler)


def import_billing_codes(filename):
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        codes = []
        for row in reader:
            codes.append((row['billing_code_type'], row['billing_code']))
    return codes


def import_set(filename, ints=True):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        items = set()
        for row in reader:
            item = row[0]
            if ints:
                items.add(int(item))
            else:
                items.add(str(item))
    return items


def hashdict(data_dict):
    if not data_dict:
        raise ValueError

    sorted_tups = sorted(data_dict.items())
    dict_as_bytes = json.dumps(sorted_tups).encode('utf-8')
    dict_hash = hashlib.sha256(dict_as_bytes).hexdigest()[:16]
    return dict_hash


def innetwork_to_rows(obj, root_hash_key):
    rows = []

    in_network_vals = {
        'negotiation_arrangement': obj['negotiation_arrangement'],
        'name': obj['name'],
        'billing_code_type': obj['billing_code_type'],
        'billing_code_type_version': obj['billing_code_type_version'],
        'billing_code': obj['billing_code'],
        'description': obj['description'],
        'root_hash_key': root_hash_key,
    }

    in_network_hash_key = hashdict(in_network_vals)
    in_network_vals['in_network_hash_key'] = in_network_hash_key

    rows.append(('in_network', in_network_vals))

    for neg_rate in obj.get('negotiated_rates', []):
        neg_rates_hash_key = hashdict(neg_rate)

        for provgroup in neg_rate['provider_groups']:
            provgroup_vals = {
                'npi_numbers': provgroup['npi'],
                'negotiated_rates_hash_key': neg_rates_hash_key,
                'in_network_hash_key': in_network_hash_key,
                'root_hash_key': root_hash_key,
            }
            rows.append(('provider_groups', provgroup_vals))

        for neg_price in neg_rate['negotiated_prices']:
            neg_price_vals = {
                'billing_class': neg_price['billing_class'],
                'negotiated_type': neg_price['negotiated_type'],
                'service_code': sc if (sc := neg_price.get('service_code', None)) else None,
                'expiration_date': neg_price['expiration_date'],
                'additional_information': neg_price.get('additional_information', None),
                'billing_code_modifier': bcm if (bcm := neg_price.get('billing_code_modifier', None)) else None,
                'negotiated_rate': neg_price['negotiated_rate'],
                'root_hash_key': root_hash_key,
                'in_network_hash_key': in_network_hash_key,
                'negotiated_rates_hash_key': neg_rates_hash_key,
            }
            rows.append(('negotiated_prices', neg_price_vals))

    for bundle in obj.get('bundled_codes', []):
        bundle_vals = {
            'billing_code_type': bundle['billing_code_type'],
            'billing_code_type_version': bundle['billing_code_type_version'],
            'billing_code': bundle['billing_code'],
            'description': bundle['description'],
            'root_hash_key': root_hash_key,
            'in_network_hash_key': in_network_hash_key,
        }
        rows.append(('bundled_codes', bundle_vals))

    return rows


class MRFOpen:

    def __init__(self, loc):

        self.loc = loc
        self.parsed_url = urlparse(self.loc)

        # Figure out if the file is local or a URL
        if self.parsed_url.scheme in ('http', 'https'):
            self.is_remote = True
        else:
            self.is_remote = False

        self.suffix = ''.join(Path(self.parsed_url.path).suffixes)

        if self.suffix not in ('.json.gz', '.json'):
            log.critical(f'Not JSON: {self.loc}')
            raise Exception

        self.r = None
        self.f = None

    def __enter__(self):

        if self.is_remote:
            self.r = requests.get(self.loc, stream = True)

        if self.suffix == '.json.gz':

            if self.is_remote:
                self.f = gzip.GzipFile(fileobj = self.r.raw)
            else:
                self.f = gzip.open(self.loc, 'r')

            try:
                self.f.read(1)
            except Exception as e:
                log.critical(e)

        elif self.suffix == '.json':
            if self.is_remote:
                self.f = self.r.raw
            else:
                self.f = open(self.loc, 'r')

        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.r:
            self.r.close()

        if self.f:
            self.f.close()


class BlockFlattener:

    def __init__(self, code_set = None, npi_set = None):

        self.npi_set = npi_set
        self.code_set = code_set

        self.provider_references = None
        self.provider_reference_map = None

        self.root_written = False
        self.in_network_item = None


    def init_parser(self, f):
        self.parser = ijson.parse(f, use_float = True)


    def ffwd(self, to_row):
        while self.current_row != to_row:
            self.current_row = next(self.parser)


    def build_root(self):
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:
            self.current_row = (prefix, event, value)

            if (
                (prefix, event, value) == ('', 'map_key', 'provider_references')
                or (prefix, event, value) == ('', 'map_key', 'in_network')
            ):

                root_dict = builder.value

                self.root_hash_key = hashdict(root_dict)

                root_dict['root_hash_key'] = self.root_hash_key
                self.root_dict = root_dict

                return

            builder.event(event, value)


    def build_provider_references(self):
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:
            self.current_row = (prefix, event, value)

            if (prefix, event, value) == ('provider_references', 'end_array', None):

                provider_references = builder.value
                provider_reference_map = {
                    pref['provider_group_id']: pref['provider_groups'] for pref in provider_references
                }

                self.provider_references = provider_references
                self.provider_reference_map = provider_reference_map

                return

            # Filter out unwanted NPI numbers
            if prefix.endswith('npi.item'):
                if (
                    self.npi_set 
                    and value not in self.npi_set
                ):
                    continue

            if (
                prefix.endswith('provider_groups.item') 
                and event == 'end_map'
            ):
                if not builder.value[-1].get('provider_groups')[-1]['npi']:
                    builder.value[-1]['provider_groups'].pop()

            if (
                prefix.endswith('provider_references.item') 
                and event == 'end_map'
            ):
                if not builder.value[-1].get('provider_groups'):
                    builder.value.pop()

            builder.event(event, value)


    def build_remote_provider_references(self):
        new_provider_references = []

        for pref in new_provider_references:

            loc = pref.get('location')

            if not loc:
                new_provider_references.append(pref)
                continue

            with MRFOpen(loc) as f:

                builder = ijson.ObjectBuilder()

                parser = ijson.parse(f, use_float = True)
                for prefix, event, value in parser:

                    if prefix.endswith('npi.item'):
                        if value not in npi_set:
                            continue

                    elif (
                        prefix.endswith('provider_groups.item') 
                        and event == 'end_map'
                    ):
                        if not builder.value['provider_groups'][-1]['npi']:
                            builder.value['provider_groups'].pop()

                    builder.event(event, value)

                if builder.value.get('provider_groups'):
                    pref['provider_groups'] = builder.value['provider_groups']
                    pref.pop('location')
                    new_provider_references.append(pref)

        return new_provider_references


    def build_next_in_network_item(self):
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:
            self.current_row = (prefix, event, value)

            if (prefix, event, value) == ('in_network', 'end_array', None):
                return

            elif (prefix, event, value) == ('in_network.item', 'end_map', None):
                self.in_network_item = builder.value
                log.info(f'Found: {billing_code_type} {billing_code}')
                return

            elif (
                prefix.endswith('negotiated_rates') 
                and event == 'start_array'
            ):
                if self.code_set:
                    billing_code_type = builder.value['billing_code_type']
                    billing_code = str(builder.value['billing_code'])
                    if (billing_code_type, billing_code) not in self.code_set:
                        log.debug(f'Skipping code: {billing_code_type} {billing_code}')
                        self.ffwd(('in_network.item', 'end_map', None))
                        return

            # If no negotiated rates that match the criteria, return nothing
            elif (
                prefix.endswith('negotiated_rates') 
                and event == 'end_array'
            ):
                if not builder.value['negotiated_rates']:
                    log.info(f'Found but no rates for: {billing_code_type} {billing_code}')
                    self.ffwd(('in_network.item', 'end_map', None))
                    return

            elif (
                prefix.endswith('negotiated_rates.item') 
                and event == 'start_map'
            ):
                provider_groups = []

            # Add the groups in the provider_reference to the existing provgroups
            elif prefix.endswith('provider_references.item'):
                if (
                    self.provider_reference_map 
                    and (grps := self.provider_reference_map.get(value))
                ):
                    provider_groups.extend(grps)

            # Merge the provgroups array if the existing provider_groups
            # if either exist
            elif (
                prefix.endswith('negotiated_rates.item') 
                and event == 'end_map'
            ):

                if builder.value['negotiated_rates'][-1].get('provider_references'):
                    builder.value['negotiated_rates'][-1].pop('provider_references')

                builder.value['negotiated_rates'][-1].setdefault('provider_groups', [])
                builder.value['negotiated_rates'][-1]['provider_groups'].extend(provider_groups)

                if not builder.value['negotiated_rates'][-1].get('provider_groups'):
                    builder.value['negotiated_rates'].pop()

            elif (
                prefix.endswith('provider_groups.item') 
                and event == 'end_map'
            ):
                if not builder.value['negotiated_rates'][-1]['provider_groups'][-1]['npi']:
                    builder.value['negotiated_rates'][-1]['provider_groups'].pop()

            # Skip NPI numbers not in the list
            elif prefix.endswith('npi.item'):
                if (
                    npi_list 
                    and value not in npi_list
                ):
                    continue

            # Make sure service codes are integers
            elif prefix.endswith('service_code.item'):
                builder.event(event, int(value))
                continue


            builder.event(event, value)
            # print(builder.value)


    def rows_to_file(self, rows, out_dir):

        if type(rows) != list:
            rows = [rows]

        for row in rows:
            filename, row_data = row
            fieldnames = SCHEMA[filename]
            file_loc = f'{out_dir}/{filename}.csv'

            if not os.path.exists(file_loc):
                with open(file_loc, 'w') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

            with open(file_loc, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(row_data)

                
    def write_item(self, out_dir):

        if not os.path.exists(out_dir):
            os.mkdir(self.out_dir)

        if not self.root_written:
            self.rows_to_file(('root', self.root_dict), out_dir)
            self.root_written = True

        if self.in_network_item:
            rows = innetwork_to_rows(self.in_network_item, self.root_hash_key)
            self.rows_to_file(rows, out_dir)
            log.info(f'Writing to file...')
            self.in_network_item = None

