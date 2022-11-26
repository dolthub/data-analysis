import os
import csv
import glob
import hashlib
import json
import ijson
import requests
import gzip
import urllib
import pathlib
import logging
from .schema import SCHEMA
from collections import namedtuple


log = logging.getLogger()
logging.basicConfig(level=logging.INFO)

file_handler = logging.FileHandler('log.txt', 'a')
file_handler.setLevel(logging.DEBUG)

log.addHandler(file_handler)

Row = namedtuple('Row', ['filename', 'data'])


def data_import(filename):
    """
    Convenience function for importing codes and NPIs
    """

    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        objs = set()

        for row in reader:
            objs.add(tuple(row[f] for f in fieldnames))

        return objs

class MRFOpen:
    """
    Context for cleanly opening and handling JSON MRFs.
    Will open remote and local files alike.
    """

    def __init__(self, loc):

        self.r = None
        self.f = None
        self.loc = loc

        self.parsed_url = urllib.parse.urlparse(self.loc)

        if self.parsed_url.scheme in ('http', 'https'):
            self.is_remote = True
        else:
            self.is_remote = False

        self.suffix = ''.join(pathlib.Path(self.parsed_url.path).suffixes)

        if self.suffix not in ('.json.gz', '.json'):
            log.critical(f'Not JSON: {self.loc}')
            raise Exception

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
                self.r.raw.decode_content = True
                self.f = self.r.raw
            else:
                self.f = open(self.loc, 'r')

        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.r:
            self.r.close()

        if self.f:
            self.f.close()


class Flattener:
    """
    Bundled methods for handling the flattening
    of streamed JSON.
    """

    def __init__(
        self, 
        code_set = None, 
        npi_set = None
    ):
        """

        """
        self.npi_set = {} if npi_set is None else npi_set
        self.code_set = {} if code_set is None else code_set

        self.local_provider_references = None
        self.remote_provider_references = []

        self.provider_reference_map = None
        self.root_written = False
        self.in_network_item = None


    def init_parser(self, f):
        self.parser = ijson.parse(f, use_float = True)


    def ffwd(self, to_row):
        """Jump to later in the parsing stream"""
        while self.current_row != to_row:
            self.current_row = next(self.parser)


    def build_root(self):
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:
            self.current_row = (prefix, event, value)

            if (
                (prefix, event, value) in (
                    ('', 'map_key', 'provider_references'),
                    ('', 'map_key', 'in_network'))
            ):

                root_dict = builder.value

                self.root_hash_key = self.hashdict(root_dict)
                root_dict['root_hash_key'] = self.root_hash_key

                self.root_dict = root_dict

                return

            builder.event(event, value)


    def build_provider_references(self):
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:
            self.current_row = (prefix, event, value)

            if (
                (prefix, event, value) == ('provider_references', 'end_array', None)
            ):
                self.local_provider_references = builder.value
                return

            elif (
                prefix.endswith('npi.item')
                and self.npi_set
                and value not in self.npi_set
            ):
                continue

            elif (
                prefix.endswith('provider_groups.item')
                and event == 'end_map'
            ):
                if not builder.value[-1].get('provider_groups')[-1]['npi']:
                    builder.value[-1]['provider_groups'].pop()

            elif (
                prefix.endswith('provider_references.item') 
                and event == 'end_map'
            ):
                if builder.value and builder.value[-1].get('location'):
                    self.remote_provider_references.append(builder.value.pop())

                elif not builder.value[-1].get('provider_groups'):
                    builder.value.pop()

            builder.event(event, value)


    def build_remote_provider_references(self):
        for pref in self.remote_provider_references:

            loc = pref.get('location')

            with MRFOpen(loc) as f:
                builder = ijson.ObjectBuilder()

                parser = ijson.parse(f, use_float = True)
                for prefix, event, value in parser:

                    if (
                        prefix.endswith('npi.item') 
                        and self.npi_set
                        and value not in self.npi_set
                    ):
                        continue

                    elif (
                        prefix.endswith('provider_groups.item') 
                        and event == 'end_map'
                    ):
                        if not builder.value['provider_groups'][-1]['npi']:
                            builder.value['provider_groups'].pop()

                    builder.event(event, value)

                builder.value['provider_group_id'] = pref['provider_group_id']
                self.local_provider_references.append(builder.value)

        self.remote_provider_references = None


    def make_provider_ref_map(self):
        self.provider_reference_map = {
            pref['provider_group_id']: pref['provider_groups'] for pref in self.local_provider_references
        }


    def build_next_in_network_item(self):
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:
            self.current_row = (prefix, event, value)

            if (prefix, event, value) == ('in_network', 'end_array', None):
                return

            elif (prefix, event, value) == ('in_network.item', 'end_map', None):
                log.info(f"Found: {billing_code_tup}")
                self.in_network_item = builder.value
                return

            elif (
                prefix.endswith('negotiated_rates') 
                and event == 'start_array'
            ):
                billing_code_type = builder.value['billing_code_type']
                billing_code = str(builder.value['billing_code'])
                billing_code_tup = billing_code_type, billing_code

                if (
                    self.code_set
                    and billing_code_tup not in self.code_set
                ):
                    log.debug(f'Skipping: {billing_code_tup}')
                    self.ffwd(('in_network.item', 'end_map', None))

            elif (
                prefix.endswith('negotiated_rates') 
                and event == 'end_array'
                and self.code_set
                and not builder.value['negotiated_rates']
            ):
                log.info(f"No rates for {billing_code_tup}")
                self.ffwd(('in_network.item', 'end_map', None))
                return

            elif (
                prefix.endswith('negotiated_rates.item') 
                and event == 'start_map'
            ):
                provider_groups = []

            elif (
                self.provider_reference_map 
                and prefix.endswith('provider_references.item')
                and (grps := self.provider_reference_map.get(value))
            ):
                provider_groups.extend(grps)

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
                and not builder.value['negotiated_rates'][-1]['provider_groups'][-1]['npi']
            ):
                builder.value['negotiated_rates'][-1]['provider_groups'].pop()

            elif prefix.endswith('npi.item'):
                if (
                    self.npi_set 
                    and value not in self.npi_set
                ):
                    continue

            elif prefix.endswith('service_code.item'):
                try:
                    value = int(value)
                except ValueError:
                    pass

            builder.event(event, value)


    def hashdict(self, data_dict):

        if not data_dict:
            raise ValueError

        sorted_tups = sorted(data_dict.items())
        dict_as_bytes = json.dumps(sorted_tups).encode('utf-8')
        dict_hash = hashlib.sha256(dict_as_bytes).hexdigest()[:16]

        return dict_hash


    def in_network_item_to_rows(self):

        rows = []

        in_network_vals = {
            'negotiation_arrangement':   self.in_network_item['negotiation_arrangement'],
            'name':                      self.in_network_item['name'],
            'billing_code_type':         self.in_network_item['billing_code_type'],
            'billing_code_type_version': self.in_network_item['billing_code_type_version'],
            'billing_code':              self.in_network_item['billing_code'],
            'description':               self.in_network_item['description'],
            'root_hash_key':             self.root_hash_key,
        }

        in_network_hash_key = self.hashdict(in_network_vals)
        in_network_vals['in_network_hash_key'] = in_network_hash_key

        rows.append(Row('in_network', in_network_vals))

        for neg_rate in self.in_network_item.get('negotiated_rates', []):
            neg_rates_hash_key = self.hashdict(neg_rate)

            for provider_group in neg_rate['provider_groups']:
                provider_group_vals = {
                    'npi_numbers':               provider_group['npi'],
                    'tin_type':                  provider_group['tin']['type'],
                    'tin_value':                 provider_group['tin']['value'],
                    'negotiated_rates_hash_key': neg_rates_hash_key,
                    'in_network_hash_key':       in_network_hash_key,
                    'root_hash_key':             self.root_hash_key,
                }

                rows.append(Row('provider_groups', provider_group_vals))

            for neg_price in neg_rate['negotiated_prices']:

                neg_price_vals = {
                    'billing_class':             neg_price['billing_class'],
                    'negotiated_type':           neg_price['negotiated_type'],
                    'expiration_date':           neg_price['expiration_date'],
                    'negotiated_rate':           neg_price['negotiated_rate'],
                    'in_network_hash_key':       in_network_hash_key,
                    'negotiated_rates_hash_key': neg_rates_hash_key,
                    'service_code':              None if not (v := neg_price.get('service_code')) else v,
                    'additional_information':    neg_price.get('additional_information'),
                    'billing_code_modifier':     None if not (v := neg_price.get('billing_code_modifier')) else v,
                    'root_hash_key':             self.root_hash_key,
                }

                rows.append(Row('negotiated_prices', neg_price_vals))

        for bundle in self.in_network_item.get('bundled_codes', []):

            bundle_vals = {
                'billing_code_type':         bundle['billing_code_type'],
                'billing_code_type_version': bundle['billing_code_type_version'],
                'billing_code':              bundle['billing_code'],
                'description':               bundle['description'],
                'in_network_hash_key':       in_network_hash_key,
                'root_hash_key':             self.root_hash_key,
            }

            rows.append(Row('bundled_codes', bundle_vals))

        return rows


    def write_in_network_item(self, out_dir):

        if not os.path.exists(out_dir):
            os.mkdir(self.out_dir)

        if self.in_network_item:
            rows = self.in_network_item_to_rows()

            if not self.root_written:
                rows.append(Row('root', self.root_dict))

            for row in rows:
                filename = row.filename
                data = row.data

                fieldnames = SCHEMA[filename]
                file_loc = f'{out_dir}/{filename}.csv'
                file_exists = os.path.exists(file_loc)

                # TODO: this opens a file for each row
                with open(file_loc, 'a') as f:

                    writer = csv.DictWriter(f, fieldnames = fieldnames)

                    if not file_exists:
                        writer.writeheader()

                    writer.writerow(data)

            self.in_network_item = None
            self.root_written = True
