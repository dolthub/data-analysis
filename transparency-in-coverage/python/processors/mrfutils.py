import os
import csv
import glob
import hashlib
import json
import ijson
import requests
import gzip
import logging
from urllib.parse import urlparse
from pathlib import Path
from schema import SCHEMA

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)

file_handler = logging.FileHandler('log.txt', 'a')
file_handler.setLevel(logging.DEBUG)

log.addHandler(file_handler)


class InvalidMRF(Exception):
    pass


class MRFOpen:

    def __init__(self, loc):
        self.loc = loc
        self.f = None
        self.r = None
        self.suffix = ''.join(Path(urlparse(self.loc).path).suffixes)

        if self.suffix not in ('.json.gz', '.json'):
            log.critical(f'Not JSON: {self.loc}')
            raise InvalidMRF

    def __enter__(self):
        is_remote = urlparse(self.loc).scheme in ('http', 'https')

        if is_remote:
            self.r = requests.get(self.loc, stream = True)

        if self.suffix == '.json.gz':
            if is_remote:
                self.f = gzip.GzipFile(fileobj = self.r.raw)
            else:
                self.f = gzip.open(self.loc, 'r')
            try:
                self.f.read(1)
            except Exception as e:
                log.critical(e)
                raise InvalidMRF
        else:
            if is_remote:
                self.r.raw.decode_content = True
                self.f = self.r.raw
            else:
                self.f = open(self.loc, 'r')

        log.info(f'Succesfully opened file: {self.loc}')
        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.r:
            self.r.close()

        if self.f:
            self.f.close()


class MRFObjectBuilder:


    def __init__(self, f):
        self.parser = ijson.parse(f, use_float = True)

    # parser
    def ffwd(self, to_row):
        for current_row in self.parser:
            if current_row == to_row:
                break

    # parser
    def build_root(self):
        builder = ijson.ObjectBuilder()

        for (prefix, event, value) in self.parser:
            row = (prefix, event, value)
            if (row in [
                    ('', 'map_key', 'provider_references'),
                    ('', 'map_key', 'in_network')]):
                return builder.value, row
            builder.event(event, value)
        else:
            log.critical('Invalid MRF')
            raise InvalidMRF

    # parser
    def innet_items(
        self, 
        npi_set, 
        code_set,
        root_data,
        p_refs_map,
    ):
        builder = ijson.ObjectBuilder()
        root_hash_key = hashdict(root_data)

        for prefix, event, value in self.parser:

            if (prefix, event, value) == ('in_network', 'end_array', None):
                return

            elif (prefix, event, value) == ('in_network.item', 'end_map', None):
                log.info(f"Found: {billing_code_tup}")
                in_network_item = builder.value.pop()
                in_network_item['root_hash_key'] = root_hash_key
                yield in_network_item
                
            elif (
                (prefix, event) == ('in_network.item.negotiated_rates', 'start_array')
            ):
                billing_code_type = builder.value[-1]['billing_code_type']
                billing_code = str(builder.value[-1]['billing_code'])
                billing_code_tup = billing_code_type, billing_code

                if (
                    code_set
                    and billing_code_tup not in code_set
                ):
                    log.debug(f'Skipping: {billing_code_tup}')                    
                    self.ffwd(('in_network.item', 'end_map', None))
                    builder.value.pop()
                    builder.containers.pop()
                    continue

            elif (
                (prefix, event) == ('in_network.item.negotiated_rates', 'end_array')
                and not builder.value[-1]['negotiated_rates']
            ):
                log.info(f"No rates for {billing_code_tup}")
                self.ffwd(('in_network.item', 'end_map', None))
                builder.value.pop()
                builder.containers.pop()
                builder.containers.pop()
                continue

            elif (
                prefix.endswith('negotiated_rates.item') 
                and event == 'start_map'
            ):
                provider_groups = []

            elif (
                p_refs_map 
                and prefix.endswith('provider_references.item')
                and (grps := p_refs_map.get(value))
            ):
                provider_groups.extend(grps)

            elif (
                prefix.endswith('negotiated_rates.item') 
                and event == 'end_map'
            ):

                if builder.value[-1]['negotiated_rates'][-1].get('provider_references'):
                    builder.value[-1]['negotiated_rates'][-1].pop('provider_references')

                builder.value[-1]['negotiated_rates'][-1].setdefault('provider_groups', [])
                builder.value[-1]['negotiated_rates'][-1]['provider_groups'].extend(provider_groups)

                if not builder.value[-1]['negotiated_rates'][-1].get('provider_groups'):
                    builder.value[-1]['negotiated_rates'].pop()

            elif (
                prefix.endswith('provider_groups.item') 
                and event == 'end_map'
                and not builder.value[-1]['negotiated_rates'][-1]['provider_groups'][-1]['npi']
            ):
                builder.value[-1]['negotiated_rates'][-1]['provider_groups'].pop()

            elif prefix.endswith('npi.item'):
                if (
                    npi_set
                    and value not in npi_set
                ):
                    continue

            elif prefix.endswith('service_code.item'):
                try:
                    value = int(value)
                except ValueError:
                    pass

            
            builder.event(event, value)

    # parser
    def build_p_refs(self, npi_set):

        local_p_refs, remote_p_refs = self._build_local_p_refs(npi_set)
        new_p_refs = self._build_remote_p_refs(remote_p_refs, npi_set)

        if new_p_refs:
            local_p_refs.extend(new_p_refs)        

        return {
            pref['provider_group_id']: pref['provider_groups'] for pref in local_p_refs
        }

    # parser
    def _build_local_p_refs(self, npi_set):
        remote_p_refs = []
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:

            if (
                (prefix, event, value) == ('provider_references', 'end_array', None)
            ):
                local_p_refs = builder.value
                return builder.value, remote_p_refs

            elif (
                prefix.endswith('npi.item')
                and npi_set
                and not value in npi_set
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
                    remote_p_refs.append(builder.value.pop())

                elif not builder.value[-1].get('provider_groups'):
                    builder.value.pop()


            builder.event(event, value)

    # parser
    def _build_remote_reference(self, loc, npi_set):

        with MRFOpen(loc) as f:
            builder = ijson.ObjectBuilder()

            parser = ijson.parse(f, use_float = True)
            for prefix, event, value in parser:

                if (
                    prefix.endswith('npi.item') 
                    and npi_set
                    and not value in npi_set
                ):
                    continue

                elif (
                    prefix.endswith('provider_groups.item') 
                    and event == 'end_map'
                ):
                    if not builder.value['provider_groups'][-1]['npi']:
                        builder.value['provider_groups'].pop()

                builder.event(event, value)

        return builder.value

    # parser
    def _build_remote_p_refs(
        self, 
        remote_p_refs, 
        npi_set
    ):

        new_p_refs = []

        for pref in remote_p_refs:
            loc = pref.get('location')
            try:
                remote_reference = self._build_remote_reference(loc, npi_set)
                remote_reference['provider_group_id'] = pref['provider_group_id']
                new_p_refs.append(remote_reference)
            except Exception as e:
                log.warn('Error retrieving remote provider references')
                log.warn(loc)
                log.warn(e)
                pass


class MRFWriter:

    def __init__(self, out_dir):
        self.out_dir = out_dir
        self._make_dir()

    # out_dir
    def _make_dir(self):
        if not os.path.exists(self.out_dir):
            os.mkdir(self.out_dir)

    # out_dir
    def _write_rows(self, rows, filename):
        fieldnames = SCHEMA[filename]
        file_loc = f'{self.out_dir}/{filename}.csv'
        file_exists = os.path.exists(file_loc)

        with open(file_loc, 'a') as f:
            writer = csv.DictWriter(f, fieldnames = fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

    # out_dir
    def write_root(self, root_data):
        root_hash_key = hashdict(root_data)
        root_data['root_hash_key'] = root_hash_key
        self._write_rows([root_data], 'root')

    # out_dir
    def write_innet(self, item):

        in_network_vals = {
            'negotiation_arrangement':   item['negotiation_arrangement'],
            'name':                      item['name'],
            'billing_code_type':         item['billing_code_type'],
            'billing_code_type_version': item['billing_code_type_version'],
            'billing_code':              item['billing_code'],
            'description':               item['description'],
            'root_hash_key':             item['root_hash_key'],
        }

        in_network_hash_key = hashdict(in_network_vals)
        in_network_vals['in_network_hash_key'] = in_network_hash_key
        self._write_rows([in_network_vals], 'in_network')

        for neg_rate in item.get('negotiated_rates', []):

            neg_rates_hash_key = hashdict(neg_rate)
            provider_group_rows = []
            neg_price_rows = []

            for provider_group in neg_rate['provider_groups']:
                provider_group_vals = {
                    'npi_numbers':               provider_group['npi'],
                    'tin_type':                  provider_group['tin']['type'],
                    'tin_value':                 provider_group['tin']['value'],
                    'negotiated_rates_hash_key': neg_rates_hash_key,
                    'in_network_hash_key':       in_network_hash_key,
                    'root_hash_key':             item['root_hash_key'],
                }

                provider_group_rows.append(provider_group_vals)
                self._write_rows(provider_group_rows, 'provider_groups')

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
                    'root_hash_key':             item['root_hash_key'],
                }

                neg_price_rows.append(neg_price_vals)
                self._write_rows(neg_price_rows, 'negotiated_prices')



def data_import(filename):
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        objs = set()
        for row in reader:
            objs.add(tuple(row[f] for f in fieldnames))
        return objs


def hashdict(data: dict):
    if not data:
        raise ValueError
    sorted_tups = sorted(data.items())
    data_as_bytes = json.dumps(sorted_tups).encode('utf-8')
    data_hash = hashlib.sha256(data_as_bytes).hexdigest()[:16]
    return data_hash


def flatten_mrf(loc, npi_set, code_set, out_dir):
    
    with MRFOpen(loc) as f:
        m = MRFObjectBuilder(f)
        root_data, cur_row = m.build_root()
        writer = MRFWriter(out_dir)

        if cur_row == ('', 'map_key', 'provider_references'):
            p_ref_map = m.build_p_refs(npi_set)

            m.ffwd(('', 'map_key', 'in_network'))
            for item in m.innet_items(npi_set, code_set, root_data, p_ref_map):
                writer.write_innet(item)
            writer.write_root(root_data)
            return

        elif cur_row == ('', 'map_key', 'in_network'):
            m.ffwd(('', 'map_key', 'provider_references'))
            p_ref_map = m.build_p_refs(npi_set)

    with MRFOpen(loc) as f:
        m = MRFObjectBuilder(f)
        m.ffwd(('', 'map_key', 'in_network'))
        for item in m.innet_items(npi_set, code_set, root_data, p_ref_map):
            writer.write_innet(item)
        writer.write_root(root_data)