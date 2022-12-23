import os
import csv
import hashlib
import json
import ijson
import asyncio
import aiohttp
import itertools
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


def import_csv_to_set(filename):
	"""
	Imports data as tuples from a given file.
	Iterates over rows
	:param filename: filename
	:return:
	"""
	items = set()
	with open(filename, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			row = [col.strip() for col in row]
			if len(row) > 1:
				items.add(tuple(row))
			else:
				items.add(row.pop())
		return items


def dicthasher(data, n_bytes=8):
	if not data:
		raise Exception("Hashed dictionary can't be empty")
	data = json.dumps(data, sort_keys=True).encode('utf-8')
	hash_s = hashlib.sha256(data).digest()[:n_bytes]
	hash_i = int.from_bytes(hash_s, 'little')
	return hash_i


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
		self.__p = ijson.parse(f, use_float=True)
		self.current = None

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
			self.r = requests.get(self.loc, stream=True)
			self.f = gzip.GzipFile(fileobj=self.r.raw)

		elif (
			self.is_remote
			and self.suffix == '.json'
		):
			self.r = requests.get(self.loc, stream=True)
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


def process_remote_provider_reference(
	data,
	npi_filter
):
	for group in data['provider_groups']:
		group['npi'] = [str(npi) for npi in group['npi']]

		if npi_filter:
			group['npi'] = [npi for npi in group['npi'] if npi in npi_filter]

	data['provider_groups'] = [
		group for group in data['provider_groups']
		if group['npi']]

	if not data['provider_groups']:
		return

	return data


async def fetch_remote_provider_reference(
	session,
	provider_group_id,
	provider_reference_loc,
	npi_filter = None,
):
	async with session.get(provider_reference_loc) as response:
		log.info(f'Opened remote provider reference url:{provider_reference_loc}')
		assert response.status == 200
		f = await response.read()
		unprocessed_data = json.loads(f)
		unprocessed_data['provider_group_id'] = provider_group_id
		processed_remote_provider_reference = process_remote_provider_reference(
			unprocessed_data,
			npi_filter,
		)
		return processed_remote_provider_reference


async def fetch_remote_provider_references(
	unfetched_provider_references,
	npi_filter,
):
	"""
	:param unfetched_provider_references: list 
	:param npi_filter: set 
	:return: 
	"""
	tasks = []
	async with aiohttp.client.ClientSession() as session:
		for unfetched_provider_reference in unfetched_provider_references:
			provider_group_id = unfetched_provider_reference['provider_group_id']
			provider_reference_loc = unfetched_provider_reference['location']

			task = asyncio.wait_for(
				fetch_remote_provider_reference(session,
				                                provider_group_id,
				                                provider_reference_loc,
				                                npi_filter,),
				timeout = 5,
			)

			tasks.append(task)

		fetched_remote_provider_references = await asyncio.gather(*tasks)
		fetched_remote_provider_references = list(filter(lambda item: item, fetched_remote_provider_references))
		return fetched_remote_provider_references


class MRFObjectBuilder:
	"""
	Takes a parser and returns necessary objects
	for parsing and flattening MRFs.
	"""

	def __init__(self, f):
		self.parser = Parser(f)

	def _ffwd(self, to_row):
		"""
		:param to_row: the row to fast-forward to
		"""
		if self.parser.current == to_row:
			return
		for current_row in self.parser:
			if current_row == to_row:
				break
		else:
			raise StopIteration('Fast-forward failed to find row')

	def _process_root(self):
		builder = ijson.ObjectBuilder()
		for (prefix, event, value) in self.parser:
			row = (prefix, event, value)
			if (row in [
				('', 'map_key', 'provider_references'),
				('', 'map_key', 'in_network')]):
				root_data = builder.value
				return root_data
			builder.event(event, value)
		else:
			raise InvalidMRF('Read to EOF without finding root data')

	def _process_provider_references(self, npi_filter):
		unfetched_remote_provider_references = []
		builder = ijson.ObjectBuilder()

		for prefix, event, value in self.parser:

			if (prefix, event) == ('provider_references', 'end_array'):
				local_provider_references = builder.value
				return local_provider_references, unfetched_remote_provider_references

			elif (
				prefix.endswith('provider_groups.item')
				and event == 'end_map'
			):
				latest_value = builder.value[-1]
				latest_provider_group = latest_value['provider_groups'][-1]
				latest_npis = latest_provider_group['npi']

				if not latest_npis:
					latest_value['provider_groups'].pop()

			elif (
				prefix.endswith('provider_references.item')
				and event == 'end_map'
			):
				latest_value = builder.value[-1]

				if latest_value.get('location'):
					provider_reference = builder.value.pop()
					unfetched_remote_provider_references.append(provider_reference)

				elif not latest_value.get('provider_groups'):
					builder.value.pop()

			elif prefix.endswith('npi.item'):
				value = str(value)
				if (
					npi_filter and
					value not in npi_filter
				):
					continue

			builder.event(event, value)

	def _make_provider_reference_map(self, npi_filter):
		"""
		Collects the provider references into a map. This replaces
		"provider_group_id" with provider groups
		:param npi_filter: set
		:return: dict
		"""
		local_provider_references, unfetched_remote_provider_references = \
			self._process_provider_references(npi_filter)

		loop = asyncio.get_event_loop()
		fetched_remote_provider_references = loop.run_until_complete(
			fetch_remote_provider_references(
				unfetched_remote_provider_references,
				npi_filter)
		)

		local_provider_references.extend(
			fetched_remote_provider_references
		)

		provider_reference_map = {
			ref['provider_group_id']: ref['provider_groups']
			for ref in local_provider_references
		}

		return provider_reference_map

	def gen_in_network(
		self,
		npi_filter,
		code_filter,
		provider_reference_map,
	):
		"""
		Generator that returns a fully-constructed in-network item.

		Note: if there's a bug in this program -- it's probably in
		this part.

		:param npi_filter: set
		:param code_filter: set
		:param provider_reference_map: dict
		:return: dict
		"""
		builder = ijson.ObjectBuilder()

		for prefix, event, value in self.parser:

			if (prefix, event) == ('in_network', 'end_array'):
				return

			elif (prefix, event) == ('in_network.item', 'end_map'):
				log.info(f"Rates found for {code_type} {code}")
				in_network_item = builder.value.pop()

				yield in_network_item

				del code_type, code

			elif (
				(prefix, event) == ('in_network.item.negotiated_rates', 'start_array')
			):
				code_type = builder.value[-1]['billing_code_type']
				code = str(builder.value[-1]['billing_code'])

				if (
					code_filter
					and (code_type, code) not in code_filter
				):
					log.debug(f"Skipping {code_type} {code}: not in list")

					builder.value.pop()
					builder.containers.pop()

					self._ffwd(('in_network.item', 'end_map', None))
					continue

			elif (
				(prefix, event) == ('in_network.item.negotiated_rates', 'end_array')
				and not builder.value[-1]['negotiated_rates']
			):
				log.debug(f"Skipping {code_type} {code}: no providers")

				builder.value.pop()
				builder.containers.pop()
				builder.containers.pop()

				self._ffwd(('in_network.item', 'end_map', None))
				continue

			elif (
				prefix.endswith('negotiated_rates.item')
				and event == 'start_map'
			):
				provider_groups = []

			elif (
				provider_reference_map
				and prefix.endswith('provider_references.item')

			):
				groups = provider_reference_map.get(value)
				if groups:
					provider_groups.extend(groups)

			elif (
				prefix.endswith('negotiated_rates.item')
				and event == 'end_map'
			):
				latest_value = builder.value[-1]
				latest_rates = latest_value['negotiated_rates'][-1]

				latest_rates.pop('provider_references', None)
				latest_rates.setdefault('provider_groups', [])

				latest_rates['provider_groups'].extend(provider_groups)

				if not latest_rates['provider_groups']:
					latest_value['negotiated_rates'].pop()

			elif (
				prefix.endswith('provider_groups.item')
				and event == 'end_map'
			):
				latest_value = builder.value[-1]
				latest_rates = latest_value['negotiated_rates'][-1]
				latest_groups = latest_rates['provider_groups'][-1]
				latest_npis = latest_groups['npi']

				if not latest_npis:
					latest_rates['provider_groups'].pop()

			elif prefix.endswith('npi.item'):
				value = str(value)
				if (
					npi_filter and
					value not in npi_filter
				):
					continue

			elif prefix.endswith('service_code.item'):
				try:
					value = int(value)
				except ValueError:
					pass

			builder.event(event, value)

	def prepare_root_row(self, loc, url = None):
		root_row = self._process_root()
		root_row['filename'] = Path(loc).stem.split('.')[0]
		root_hash = dicthasher(root_row)
		root_row['root_hash'] = root_hash

		if url:
			root_row['url'] = url
		else:
			root_row['url'] = loc

		return root_row

	def prepare_provider_references(self, npi_filter):
		try:
			self._ffwd(('', 'map_key', 'provider_references'))
			provider_reference_map = self._make_provider_reference_map(npi_filter)
		except StopIteration:
			provider_reference_map = None
		return provider_reference_map

	def seek_to_in_network_items(self):
		self._ffwd(('', 'map_key', 'in_network'))

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

	def _write_table(self, rows, filename):
		fieldnames = self.schema[filename]
		file_loc = f'{self.out_dir}/{filename}.csv'
		file_exists = os.path.exists(file_loc)

		# newline = '' is to prevent Windows
		# from addiing \r\n\n to the end of each line
		with open(file_loc, 'a', newline = '') as f:
			writer = csv.DictWriter(f, fieldnames = fieldnames)
			if not file_exists:
				writer.writeheader()
			writer.writerows(rows)

	def _write_code(self, item):
		code_row = {
			'negotiation_arrangement':   item['negotiation_arrangement'],
			'billing_code_type':         item['billing_code_type'],
			'billing_code_type_version': item['billing_code_type_version'],
			'billing_code':              item['billing_code'],
		}

		code_hash = dicthasher(code_row)
		code_row['code_hash'] = code_hash
		self._write_table([code_row], 'codes')
		return code_row

	def _write_prices(self, prices, code_hash, root_hash):
		price_rows = []
		for price in prices:
			if sc := price.get('service_code'):
				price['service_code'] = json.dumps(sorted(sc))
			else:
				price['service_code'] = None

			if bcm := price.get('billing_code_modifier'):
				price['billing_code_modifier'] = json.dumps(sorted(bcm))
			else:
				price['billing_code_modifier'] = None

			price_row = {
				'billing_class':          price['billing_class'],
				'negotiated_type':        price['negotiated_type'],
				'expiration_date':        price['expiration_date'],
				'negotiated_rate':        price['negotiated_rate'],
				'service_code':           price['service_code'],
				'additional_information': price.get('additional_information'),
				'billing_code_modifier':  price['billing_code_modifier'],
				'code_hash':              code_hash,
				'root_hash':              root_hash,
			}
			price_row['negotiated_price_hash'] = dicthasher(price_row)
			price_rows.append(price_row)
		self._write_table(price_rows, 'negotiated_prices')
		return price_rows

	def _write_groups(self, provider_groups):
		group_rows = []
		for group in provider_groups:
			group_row = {
				'npi_numbers': json.dumps(sorted(group['npi'])),
				'tin_type':    group['tin']['type'],
				'tin_value':   group['tin']['value'],
			}
			group_row['provider_group_hash'] = dicthasher(group_row)
			group_rows.append(group_row)
		self._write_table(group_rows, 'provider_groups')
		return group_rows

	def _write_link(self, prices, provider_groups):
		links = []
		for price, group in itertools.product(prices, provider_groups):
			link = {
				'provider_group_hash':   group['provider_group_hash'],
				'negotiated_price_hash': price['negotiated_price_hash'],
			}
			links.append(link)
		self._write_table(links, 'provider_groups_negotiated_prices_link')
		return links

	def _write_rate(self, rate, code_hash, root_hash):
		prices = rate['negotiated_prices']
		price_rows = self._write_prices(prices, code_hash, root_hash)

		provider_groups = rate['provider_groups']
		provider_group_rows = self._write_groups(provider_groups)

		self._write_link(price_rows, provider_group_rows)

	def write_root(self, root_row):
		self._write_table([root_row], 'root')
		return root_row

	def write_in_network_item(self, item, root_hash):

		code_row = self._write_code(item)
		code_hash = code_row['code_hash']

		for rate in item.get('negotiated_rates'):
			self._write_rate(rate, code_hash, root_hash)

def flatten_mrf(
	loc: str,
	npi_filter: set,
	code_filter: set,
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
	:param npi_filter: set of NPI numbers
	:param code_filter: set of (CODE_TYPE, CODE) tuples (str, str)
	:param out_dir: output directory
	:param url: complete, clickable file remote URL. Assumed to be loc unless
	specified
	:return: returns nothing
	"""
	with MRFOpen(loc) as f:

		builder = MRFObjectBuilder(f)
		writer = MRFWriter(out_dir, SCHEMA)

		root_row = builder.prepare_root_row(loc, url)
		root_hash = root_row['root_hash']

		provider_reference_map = builder.prepare_provider_references(npi_filter)

		if not provider_reference_map:
			log.info("No provider references in this file")

		try:
			builder.seek_to_in_network_items()
			for item in builder.gen_in_network(npi_filter, code_filter, provider_reference_map):
				writer.write_in_network_item(item, root_hash)
			writer.write_root(root_row)
			return
		except StopIteration:
			log.info("Didn't find in-network items on first pass. Re-opening file")

	with MRFOpen(loc) as f:
		builder = MRFObjectBuilder(f)

		try:
			builder.seek_to_in_network_items()
		except StopIteration:
			raise InvalidMRF('No in-network items in this file')

		for item in builder.gen_in_network(npi_filter, code_filter, provider_reference_map):
			writer.write_in_network_item(item, root_hash)
		writer.write_root(root_row)
