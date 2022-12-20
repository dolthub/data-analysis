import os
import csv
import hashlib
import json
import ijson
import requests
import gzip
import logging
from urllib.parse import urlparse
from pathlib      import Path
from schema       import SCHEMA

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('log.txt', 'a')
file_handler.setLevel(logging.WARNING)
log.addHandler(file_handler)


class InvalidMRF(Exception):
    """Returned when we hit an invalid MRF."""
    def __init__(self, value):
        self.value = value


class Parser:
    """
    Wrapper for the default ijson parser.
    Preserves the current value in a variable "current".
    """
    def __init__(self, f):
        self.__p = ijson.parse(f, use_float = True)

    def __iter__(self):
        return self

    def __next__(self):
        self.current = next(self.__p)
        return self.current


class MRFOpen:
    """
    Context manager for opening JSON(.gz) MRFs.
    Handles local and remote gzipped and unzipped
    JSON files.
    """
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
            self.f = gzip.open(self.loc, 'rb')

        else:
            self.f = open(self.loc, 'rb')

        log.info(f'Successfully opened file: {self.loc}')
        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_remote:
            self.r.close()

        self.f.close()


def _fetch_remote_p_ref(loc, npi_set):
    """
    Given a remote reference location 'loc', collect
    the provider references at that location, filtering
    them so that it only collects those with NPI numbers
    in npi_set.
    """
    with MRFOpen(loc) as f:
        builder = ijson.ObjectBuilder()
        parser = Parser(f)

        for prefix, event, value in parser:

            if (
                prefix.endswith('npi.item')
                and npi_set
                and value not in npi_set
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


def _collect_remote_p_refs(remote_p_refs, npi_set):
    """
    Given a list of remote provider references, fetch
    them individually and build them. Return them as local
    provider references.
    """
    p_refs = []

    for p_ref in remote_p_refs:
        loc = p_ref.get('location')
        provider_group_id = p_ref['provider_group_id']
        remote_p_ref = _fetch_remote_p_ref(loc, npi_set)
        remote_p_ref['provider_group_id'] = provider_group_id
        p_refs.append(remote_p_ref)

    return p_refs


class MRFObjectBuilder:
    """
    Takes a parser and returns necessary objects
    for parsing and flattening MRFs.
    """
    def __init__(self, f):
        self.parser = Parser(f)

    def ffwd(self, to_row):
        """
        :param to_row: the row to fast-forward to
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
            raise InvalidMRF('Read to EOF without finding root data')

    def _prepare_provider_refs(self, npi_set):
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
                and value not in npi_set
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

    def collect_p_refs(self, npi_set):
        """
        Collects the provider references into a map. This replaces
        "provider_group_id" with provider groups
        :param npi_set: set
        :return: dict
        """
        local_p_refs, remote_p_refs = self._prepare_provider_refs(npi_set)

        local_p_refs.extend(
            _collect_remote_p_refs(remote_p_refs, npi_set)
        )

        return {
            p['provider_group_id']: p['provider_groups'] for p in local_p_refs
        }

    def in_network_items(
            self,
            npi_set,
            code_set,
            p_refs_map,
    ):
        """
        Generator that returns a fully-constructed in-network item.

        Note: if there's a bug in this program -- it's probably in
        this part.

        :param npi_set: set
        :param code_set: set
        :param p_refs_map: dict
        :return: dict
        """
        builder = ijson.ObjectBuilder()

        for prefix, event, value in self.parser:

            if (prefix, event, value) == ('in_network', 'end_array', None):
                return

            elif (prefix, event, value) == ('in_network.item', 'end_map', None):
                log.info(f"Rates found for {billing_code_type} {billing_code}")
                in_network_item = builder.value.pop()
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
                    log.debug(f"Skipping {billing_code_type} {billing_code}: not in list")
                    self.ffwd(('in_network.item', 'end_map', None))
                    builder.value.pop()
                    builder.containers.pop()
                    continue

            elif (
                    (prefix, event) == ('in_network.item.negotiated_rates', 'end_array')
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
                    and (groups := p_refs_map.get(value))
            ):
                provider_groups.extend(groups)

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

    def write_table(self, rows, filename):
        fieldnames = self.schema[filename]
        file_loc = f'{self.out_dir}/{filename}.csv'
        file_exists = os.path.exists(file_loc)

        with open(file_loc, 'a') as f:
            writer = csv.DictWriter(f, fieldnames = fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

    def write_in_network_item(self, item, root_hash):

        code_row = {
            'negotiation_arrangement': item['negotiation_arrangement'],
            'billing_code_type': item['billing_code_type'],
            'billing_code_type_version': item['billing_code_type_version'],
            'billing_code': item['billing_code'],
        }
        code_hash = hashdict(code_row)
        code_row['code_hash'] = code_hash
        self.write_table([code_row], 'codes')

        for rate in item.get('negotiated_rates', []):

            price_rows = []
            for price in rate['negotiated_prices']:

                if sc := price.get('service_code'):
                    price['service_code'] = json.dumps(sorted(sc))
                else:
                    price['service_code'] = None

                if bcm := price.get('billing_code_modifier'):
                    price['billing_code_modifier'] = json.dumps(sorted(bcm))
                else:
                    price['billing_code_modifier'] = None

                price_row = {
                    'billing_class': price['billing_class'],
                    'negotiated_type': price['negotiated_type'],
                    'expiration_date': price['expiration_date'],
                    'negotiated_rate': price['negotiated_rate'],
                    'service_code': price['service_code'],
                    'additional_information': price.get('additional_information'),
                    'billing_code_modifier': price['billing_code_modifier'],
                    'code_hash': code_hash,
                    'root_hash': root_hash,
                }
                price_row['negotiated_price_hash'] = hashdict(price_row)
                price_rows.append(price_row)
                self.write_table(price_rows, 'negotiated_prices')

            group_rows = []
            for group in rate['provider_groups']:
                group_row = {
                    'npi_numbers': json.dumps(sorted(group['npi'])),
                    'tin_type': group['tin']['type'],
                    'tin_value': group['tin']['value'],
                }
                group_row['provider_group_hash'] = hashdict(group_row)
                group_rows.append(group_row)
            self.write_table(group_rows, 'provider_groups')

            links = []
            for price in price_rows:
                for group in group_rows:
                    link = {
                        'provider_group_hash': group['provider_group_hash'],
                        'negotiated_price_hash': price['negotiated_price_hash'],
                    }
                    links.append(link)

            self.write_table(links, 'provider_groups_negotiated_prices_link')


def data_import(filename):
    """
    Imports data as tuples from a given file.
    Iterates over rows
    :param filename: filename
    :return:
    """
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        objs = set()
        for row in reader:
            objs.add(tuple([x.strip() for x in row]))
        return objs


def hashdict(data, n_bytes = 8):
    if not data:
        raise Exception("Hashed dictionary can't be empty")

    # Old way
    # data = sorted(data.items())
    # data = json.dumps(data).encode()

    # New way
    data = json.dumps(data, sort_keys = True).encode('utf-8')
    hash_s = hashlib.sha256(data).digest()[:n_bytes]
    hash_i = int.from_bytes(hash_s, 'little')
    return hash_i


def flatten_mrf(
        loc: str,
        npi_set: set,
        code_set: set,
        out_dir: str,
        url: str = None,
    ):
    """
    Main function for flattening MRFs.

    There are three cases to consider:

    1. The MRF has its provider references at the top
    2. The MRF has its provider references at the bottom
    3. The MRF doesn't have provider references

    :param loc: remote or local file location
    :param npi_set: set of NPI numbers
    :param code_set: set of (CODE_TYPE, CODE) tuples (str, str)
    :param out_dir: output directory
    :param url: complete, clickable file remote URL. Assumed to be loc unless
    specified
    :return: returns nothing
    """
    with MRFOpen(loc) as f:

        m = MRFObjectBuilder(f)
        writer = MRFWriter(out_dir, SCHEMA)

        # Get root data from top of file
        root_data = m.collect_root()
        root_data['filename'] = Path(loc).stem.split('.')[0]
        root_hash = hashdict(root_data)
        root_data['root_hash'] = root_hash

        # Importantly, URL stays outside the hash
        if url:
            root_data['url'] = url
        else:
            root_data['url'] = loc

        writer.write_table([root_data], 'root')

        # Case 1. The MRF has its provider references at the top
        if m.parser.current == ('', 'map_key', 'provider_references'):
            p_refs_map = m.collect_p_refs(npi_set)
            m.ffwd(('', 'map_key', 'in_network'))
            for item in m.in_network_items(npi_set, code_set, p_refs_map):
                writer.write_in_network_item(item, root_hash)
            return

        # Case 2/3. The MRF has its provider references either at the bottom,
        # or not at all.
        # We try to find them by fast-forwarding to the end and collecting
        # the provider references. If we do find them, we make a map.
        # Then read the file again.
        elif m.parser.current == ('', 'map_key', 'in_network'):
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
        for item in m.in_network_items(npi_set, code_set, p_refs_map):
            writer.write_in_network_item(item, root_hash)

