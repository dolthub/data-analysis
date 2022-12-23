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


def _process_remote_provider_reference(
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


async def _fetch_remote_provider_reference(
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
		processed_remote_provider_reference = _process_remote_provider_reference(
			unprocessed_data,
			npi_filter,
		)
		return processed_remote_provider_reference


async def _fetch_remote_provider_references(
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
				_fetch_remote_provider_reference(session,
				                                 provider_group_id,
				                                 provider_reference_loc,
				                                 npi_filter, ),
				timeout = 5,
			)

			tasks.append(task)

		fetched_remote_provider_references = await asyncio.gather(*tasks)
		fetched_remote_provider_references = list(filter(lambda item: item, fetched_remote_provider_references))
		return fetched_remote_provider_references


class MRFProcessor:
	"""
	Takes a parser and returns necessary objects
	for parsing and flattening MRFs.
	"""

	def __init__(self, f):
		self.parser = Parser(f)
		self.in_network_parser = None
		self.provider_references_parser = None

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

			if (
				row == ('', 'map_key', 'provider_references') or
				row == ('', 'map_key', 'in_network')
			):
				return builder.value

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
			_fetch_remote_provider_references(
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

	def prepare_file_row_plan_row(self, loc, url = None):
		root_data = self._process_root()

		plan_row = {
			'reporting_entity_name': root_data['reporting_entity_name'],
			'reporting_entity_type': root_data['reporting_entity_type'],
			'plan_name': root_data.get('plan_name'),
			'plan_id': root_data.get('plan_id'),
			'plan_id_type': root_data.get('plan_id_type'),
			'plan_market_type': root_data.get('plan_market_type'),
			'last_updated_on': root_data['last_updated_on'],
			'version': root_data['version']
		}

		plan_hash = dicthasher(plan_row)
		plan_row['plan_hash'] = plan_hash

		file_row = {
			'filename': Path(loc).stem.split('.')[0]
		}

		filename_hash = dicthasher(file_row)
		file_row['filename_hash'] = filename_hash

		return file_row, plan_row

	def prepare_provider_references(self, npi_filter):
		try:
			self._ffwd(('', 'map_key', 'provider_references'))
			provider_reference_map = self._make_provider_reference_map(npi_filter)
		except StopIteration:
			provider_reference_map = None
		return provider_reference_map

	def jump_to_in_network(self):
		self._ffwd(('', 'map_key', 'in_network'))

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

	def _write_table(self, rows, tablename):
		fieldnames = self.schema[tablename]
		file_loc = f'{self.out_dir}/{tablename}.csv'
		file_exists = os.path.exists(file_loc)

		# newline = '' is to prevent Windows
		# from addiing \r\n\n to the end of each line
		with open(file_loc, 'a', newline = '') as f:
			writer = csv.DictWriter(f, fieldnames = fieldnames)
			if not file_exists:
				writer.writeheader()
			writer.writerows(rows)

	def _write_plan(self, root_data):

		plan_row = {
			'reporting_entity_name': root_data['reporting_entity_name'],
			'reporting_entity_type': root_data['reporting_entity_type'],
			'plan_name': root_data['plan_name'],
			'plan_id': root_data['plan_id'],
			'plan_id_type': root_data['plan_id_type'],
			'plan_market_type': root_data['plan_market_type'],
			'last_updated_on': root_data['last_updated_on'],
			'version': root_data['version']
		}
		plan_hash = dicthasher(plan_row)
		plan_row['plan_hash'] = plan_hash
		self._write_table([plan_row], 'plan')
		return plan_row

	def _write_file(self, root_data):

		file_row = {
			'filename': root_data['filename']
		}
		filename_hash = dicthasher(file_row)
		file_row['filename_hash'] = filename_hash
		file_row['url'] = root_data['url']
		self._write_table([file_row], 'file')
		return file_row

	def _write_plan_file(self, plan_row, file_row):

		linking_row = {
			'plan_hash': plan_row['plan_hash'],
			'filename_hash': file_row['filename_hash']
		}

		self._write_table([linking_row], 'plans_files')
		return linking_row

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

	def _write_prices(self, prices, code_hash, filename_hash):
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
				'filename_hash':          filename_hash,
			}
			price_row['price_hash'] = dicthasher(price_row)
			price_rows.append(price_row)
		self._write_table(price_rows, 'prices')
		return price_rows

	def _write_provider_groups(self, provider_groups):
		provider_group_rows = []
		for group in provider_groups:
			group_row = {
				'npi_numbers': json.dumps(sorted(group['npi'])),
				'tin_type':    group['tin']['type'],
				'tin_value':   group['tin']['value'],
			}
			group_row['provider_group_hash'] = dicthasher(group_row)
			provider_group_rows.append(group_row)
		self._write_table(provider_group_rows, 'provider_groups')
		return provider_group_rows

	def _write_prices_provider_groups(self, price_rows, provider_group_rows):
		linking_hashes = []
		for price_row, provider_group_row in itertools.product(price_rows, provider_group_rows):
			link = {
				'provider_group_hash':   provider_group_row['provider_group_hash'],
				'price_hash':            price_row['price_hash'],
			}
			linking_hashes.append(link)
		self._write_table(linking_hashes, 'prices_provider_groups')
		return linking_hashes

	def write_file_and_plan(self, file_row, plan_row):

		self._write_table([file_row], 'files')
		self._write_table([plan_row], 'plans')

		self._write_plan_file(plan_row, file_row)

	def write_in_network_item(self, item, filename_hash):

		code_row = self._write_code(item)
		code_hash = code_row['code_hash']

		for rate in item.get('negotiated_rates'):
			prices = rate['negotiated_prices']
			price_rows = self._write_prices(prices, code_hash, filename_hash)

			provider_groups = rate['provider_groups']
			provider_group_rows = self._write_provider_groups(provider_groups)

			self._write_prices_provider_groups(price_rows, provider_group_rows)


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

		processor = MRFProcessor(f)
		writer = MRFWriter(out_dir, SCHEMA)

		file_row, plan_row = processor.prepare_file_row_plan_row(loc, url)
		filename_hash = file_row['filename_hash']

		provider_reference_map = processor.prepare_provider_references(npi_filter)

		if not provider_reference_map:
			log.info("No provider references in this file")

		try:
			processor.jump_to_in_network()
			for item in processor.gen_in_network(npi_filter, code_filter, provider_reference_map):
				writer.write_in_network_item(item, filename_hash)
			writer.write_file_and_plan(file_row, plan_row)
			return
		except StopIteration:
			log.info("Didn't find in-network items on first pass. Re-opening file")

	with MRFOpen(loc) as f:
		processor = MRFProcessor(f)

		try:
			processor.jump_to_in_network()
		except StopIteration:
			raise InvalidMRF('No in-network items in this file')

		for item in processor.gen_in_network(npi_filter, code_filter, provider_reference_map):
			writer.write_in_network_item(item, filename_hash)
		writer.write_file_and_plan(file_row, plan_row)
