import os
import csv
import hashlib
import base64
import json
import ijson
import requests
import gzip
import logging
from urllib.parse import urlparse
from pathlib import Path
from schema import SCHEMA

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('log.txt', 'a')
file_handler.setLevel(logging.WARNING)
log.addHandler(file_handler)

class Parser:
    """
    Wrapper for default ijson parser that allows
    access to the current value
    """
    def __init__(self, f):
        self.__p = ijson.parse(f, use_float = True)

    def __iter__(self):
        return self

    def __next__(self):
        self.value = next(self.__p)
        return self.value


class InvalidMRF(Exception):
    def __init__(self, value):
        self.value = value


class MRFOpen:
    "Context manager for opening JSON(.gz) MRFs"
    def __init__(self, loc):
        self.loc = loc
        self.f = None
        self.r = None

        parsed_url = urlparse(self.loc)
        self.suffix = ''.join(Path(parsed_url.path).suffixes)

        if self.suffix not in ('.json.gz', '.json'):
            raise InvalidMRF(f'Not JSON: {self.loc}')

        self.is_remote = parsed_url.scheme in ('http', 'https')

    def __enter__(self):
        log.info(f'Opening {self.loc}')
        if (
            self.is_remote 
            and self.suffix == '.json.gz'
        ):
            self.r = requests.get(self.loc, stream = True)
            self.f = gzip.GzipFile(fileobj = self.r.raw)

        elif (
            self.is_remote 
            and self.suffix == '.json'
        ):            
            self.r = requests.get(self.loc, stream = True)
            self.r.raw.decode_content = True
            self.f = self.r.raw

        elif self.suffix == '.json.gz':
            self.f = gzip.open(self.loc, 'r')

        else:
            self.f = open(self.loc, 'r')

        log.debug(f'Opened file: {self.loc}')
        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.info(f'Closing {self.loc}')
        if self.is_remote:
            self.r.close()

        self.f.close()


class MRFObjectBuilder:
    """
    Takes a parser and returns necessary objects
    for parsing and flattening MRFs
    """
    def __init__(self, f):
        self.parser = Parser(f)

    def __iter__(self):
        return self

    def __next__(self):
        self.row = next(self.__p)
        return self.row

    def ffwd(self, to_row):
        """
        @param to_row: the row to fast-forward to
        """
        for current_row in self.parser:
            if current_row == to_row:
                break
        else:
            raise Exception('Fast-forward failed to find row')

    def collect_root(self):
        builder = ijson.ObjectBuilder()
        for (prefix, event, value) in self.parser:
            row = (prefix, event, value)
            if (row in [
                    ('', 'map_key', 'provider_references'),
                    ('', 'map_key', 'in_network')]):
                return builder.value
            builder.event(event, value)
        else:
            raise InvalidMRF("Read to EOF without finding root data")

    def collect_p_refs(self, npi_set):
        """
        Collects the provider references into a map. This replaces
        "provider_group_id" with provider groups
        @param npi_set: set
        @return: dict
        """

        p_refs, remote_p_refs = self._collect_p_refs(npi_set)
        new_p_refs = self._collect_remote_p_refs(remote_p_refs, npi_set)

        if new_p_refs:
            p_refs.extend(new_p_refs)

        return {
            p_ref['provider_group_id']: p_ref['provider_groups'] for p_ref in p_refs
        }

    def _collect_p_refs(self, npi_set):
        remote_p_refs = []
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:

            if (
                (prefix, event, value) == ('provider_references', 'end_array', None)
            ):
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

    def _collect_remote_reference(self, loc, npi_set):

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

    def _collect_remote_p_refs(
        self, 
        remote_p_refs, 
        npi_set
    ):

        new_p_refs = []

        for pref in remote_p_refs:
            loc = pref.get('location')
            try:
                remote_reference = self._collect_remote_reference(loc, npi_set)
                remote_reference['provider_group_id'] = pref['provider_group_id']
                new_p_refs.append(remote_reference)
            except Exception as e:
                log.warning(f'Error building remote provider references from {loc}')
                log.warning(e)

    def gen_innet_items(
            self,
            npi_set,
            code_set,
            root_data,
            p_refs_map,
    ):
        """
        Generator that returns a fully-constructed in-network item.
        @param npi_set: set
        @param code_set: set
        @param root_data: dict
        @param p_refs_map: dict
        @return: dict
        """
        builder = ijson.ObjectBuilder()
        root_hash_key = hashdict(root_data)

        for prefix, event, value in self.parser:

            if (prefix, event, value) == ('in_network', 'end_array', None):
                return

            elif (prefix, event, value) == ('in_network.item', 'end_map', None):
                log.info(f"Rates found for {billing_code_type} {billing_code}")
                in_network_item = builder.value.pop()
                in_network_item['root_hash_key'] = root_hash_key
                yield in_network_item

            elif (
                    (prefix, event) == (
            'in_network.item.negotiated_rates', 'start_array')
            ):
                billing_code_type = builder.value[-1]['billing_code_type']
                billing_code = str(builder.value[-1]['billing_code'])
                billing_code_tup = billing_code_type, billing_code

                if (
                        code_set
                        and billing_code_tup not in code_set
                ):
                    log.debug(f"Skipping {billing_code_type} {billing_code}: not in list")
                    self.ffwd(('in_network.item', 'end_map', None))
                    builder.value.pop()
                    builder.containers.pop()
                    continue

            elif (
                    (prefix, event) == (
            'in_network.item.negotiated_rates', 'end_array')
                    and not builder.value[-1]['negotiated_rates']
            ):
                log.debug(f"Skipping {billing_code_type} {billing_code}: no providers")
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

            # elif (
            #     not p_refs_map
            #     and prefix.endswith('provider_references.item')
            # ):
            #     raise Exception

            elif (
                    prefix.endswith('negotiated_rates.item')
                    and event == 'end_map'
            ):

                if builder.value[-1]['negotiated_rates'][-1].get(
                        'provider_references'):
                    builder.value[-1]['negotiated_rates'][-1].pop(
                        'provider_references')

                builder.value[-1]['negotiated_rates'][-1].setdefault(
                    'provider_groups', [])
                builder.value[-1]['negotiated_rates'][-1][
                    'provider_groups'].extend(provider_groups)

                if not builder.value[-1]['negotiated_rates'][-1].get(
                        'provider_groups'):
                    builder.value[-1]['negotiated_rates'].pop()

            elif (
                    prefix.endswith('provider_groups.item')
                    and event == 'end_map'
                    and not builder.value[-1]['negotiated_rates'][-1][
                'provider_groups'][-1]['npi']
            ):
                builder.value[-1]['negotiated_rates'][-1][
                    'provider_groups'].pop()

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


class MRFWriter:
    """Class for writing the MRF data to the appropriate
    files in the specified schema"""
    def __init__(self, out_dir, schema):
        self.out_dir = out_dir
        self._make_dir()
        self.schema = schema

    def _make_dir(self):
        if not os.path.exists(self.out_dir):
            os.mkdir(self.out_dir)

    def _write_rows(self, rows, filename):
        fieldnames = self.schema[filename]
        file_loc = f'{self.out_dir}/{filename}.csv'
        file_exists = os.path.exists(file_loc)

        with open(file_loc, 'a') as f:
            writer = csv.DictWriter(f, fieldnames = fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

    def write_root(self, root_data):
        root_hash_key = hashdict(root_data)
        root_data['root_hash_key'] = root_hash_key
        self._write_rows([root_data], 'root')

    def write_innet_item(self, item):

        in_network_vals = {
            'negotiation_arrangement':   item['negotiation_arrangement'],
            # 'name':                      item['name'],
            'billing_code_type':         item['billing_code_type'],
            'billing_code_type_version': item['billing_code_type_version'],
            'billing_code':              item['billing_code'],
            # 'description':               item['description'],
        }

        in_network_hash_key = hashdict(in_network_vals)
        in_network_vals['in_network_hash_key'] = in_network_hash_key
        in_network_vals['root_hash_key'] = item['root_hash_key']
        self._write_rows([in_network_vals], 'in_network')

        for neg_rate in item.get('negotiated_rates', []):

            # print(json.dumps(neg_rate, indent = 1))
            neg_rates_hash_key = hashdict(neg_rate)
            provider_group_rows = []
            neg_price_rows = []

            for provider_group in neg_rate['provider_groups']:
                provider_group_vals = {
                    'npi_numbers':               sorted(provider_group['npi']),
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
                    'service_code':              None if not (v := neg_price.get('service_code')) else sorted(v),
                    'additional_information':    neg_price.get('additional_information'),
                    'billing_code_modifier':     None if not (v := neg_price.get('billing_code_modifier')) else sorted(v),
                    'root_hash_key':             item['root_hash_key'],
                }

                neg_price_rows.append(neg_price_vals)
            self._write_rows(neg_price_rows, 'negotiated_prices')

            break


def data_import(filename):
    """
    Imports data as tuples from a given file.
    Iterates over rows
    @param filename: filename
    @return:
    """
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        objs = set()
        for row in reader:
            objs.add(tuple(row[f] for f in fieldnames))
        return objs


def hashdict(data, n_bytes = 6):
    """
    We use a SHA256 256-bit hash.

    A little math:
        * the hexadecimal representation is 64 chars long
        * each hexadecimal (2 chars) is a byte of information

    A good starting point is to use 40 bits for each hash, which is 5 bytes,
    or 10 hexadecimal chars.

    @param data: dict
    @param length: number of bits
    @return:
    """
    if not data:
        raise ValueError

    sorted_tups = sorted(data.items())
    data_utf8 = json.dumps(sorted_tups).encode()
    hash_s = hashlib.sha256(data_utf8).hexdigest()[:2*n_bytes]
    hash_b = bytes.fromhex(hash_s)
    hash_b64 = base64.b64encode(hash_b).decode('utf-8')
    return hash_b64


def flatten_mrf(loc, npi_set, code_set, out_dir):
    """
    Main function for flattening MRFs.

    There are three cases to consider:

    1. The MRF has its provider references at the top
    2. The MRF has its provider references at the bottom
    3. The MRF doesn't have provider references

    @param loc: remote or local file location
    @param npi_set: set of NPI numbers
    @param code_set: set of (CODE_TYPE, CODE) tuples (str, str)
    @param out_dir: output directry
    @return: returns nothing
    """
    with MRFOpen(loc) as f:

        m = MRFObjectBuilder(f)
        writer = MRFWriter(out_dir, SCHEMA)

        # Get root data from top of file
        root_data = m.collect_root()

        root_data['url'] = loc
        writer.write_root(root_data)

        # Case 1. The MRF has its provider references at the top
        if m.parser.value == ('', 'map_key', 'provider_references'):
            p_refs_map = m.collect_p_refs(npi_set)
            m.ffwd(('', 'map_key', 'in_network'))
            for item_data in m.gen_innet_items(npi_set, code_set, root_data, p_refs_map):
                writer.write_innet_item(item_data)
            return

        # Case 2/3. The MRF has its provider references either at the bottom,
        # or not at all.
        # We try to find them by fast-forwarding to the end and collecting
        # the provider references. If we do find them, we make a map.
        # Then read the file again.
        elif m.parser.value == ('', 'map_key', 'in_network'):
            log.info('No provider references found\n'
                     'Checking at end of file')
            try:
                m.ffwd(('', 'map_key', 'provider_references'))
                p_refs_map = m.collect_p_refs(npi_set)
            except:
                p_refs_map = None

    with MRFOpen(loc) as f:
        m = MRFObjectBuilder(f)
        m.ffwd(('', 'map_key', 'in_network'))
        for item_data in m.gen_innet_items(npi_set, code_set, root_data, p_refs_map):
            writer.write_innet_item(item_data)