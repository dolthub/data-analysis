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

# You can remove this if necessary, but be warned
try:
	assert ijson.backend == 'yajl2_c'
except AssertionError:
	raise Exception('Extremely slow without the yajl2_c backend')

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
	npi_filter=None,
):
	async with session.get(provider_reference_loc) as response:
		log.info(
			f'Opened remote provider reference url:{provider_reference_loc}')
		assert response.status == 200
		f = await response.read()
		unprocessed_data = json.loads(f)
		unprocessed_data['provider_group_id'] = provider_group_id
		processed_remote_provider_reference = _process_remote_provider_reference(
			data=unprocessed_data,
			npi_filter=npi_filter,
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
			provider_group_id = unfetched_provider_reference[
				'provider_group_id']
			provider_reference_loc = unfetched_provider_reference['location']

			task = asyncio.wait_for(
				_fetch_remote_provider_reference(
					session=session,
					provider_group_id=provider_group_id,
					provider_reference_loc=provider_reference_loc,
					npi_filter=npi_filter),
				timeout=5,
			)

			tasks.append(task)

		fetched_remote_provider_references = await asyncio.gather(*tasks)
		fetched_remote_provider_references = list(
			filter(lambda item: item, fetched_remote_provider_references))
		return fetched_remote_provider_references


def process_provider_reference(item, npi_filter = None):
	result = {}
	result['provider_group_id'] = item['provider_group_id']
	result['provider_groups'] = []

	if item.get('location'):
		return item

	for provider_group in item['provider_groups']:
		npi = [str(n) for n in provider_group['npi']]
		if npi_filter:
			npi = [n for n in npi if n in npi_filter]
		tin = provider_group['tin']
		if npi:
			result['provider_groups'].append({'npi': npi, 'tin': tin})

	if not result['provider_groups']:
		return

	return result


def process_rate(item, provider_reference_map):
	provider_groups = item.get('provider_groups', [])
	provider_references = item.get('provider_references', [])

	for provider_group_id in provider_references:
		addl_provider_groups = provider_reference_map.get(provider_group_id)
		if addl_provider_groups:
			provider_groups.extend(addl_provider_groups)

	if not provider_groups:
		return

	item.pop('provider_references', None)
	item['provider_groups'] = provider_groups
	return item


def process_in_network_item(item, provider_reference_map, code_filter = None):

	if code_filter and item['billing_code'] not in code_filter:
		return

	rates = []
	for unprocessed_rate in item['negotiated_rates']:
		rate = process_rate(unprocessed_rate, provider_reference_map)
		if rate:
			rates.append(rate)

	if not rates:
		return

	item['negotiated_rates'] = rates
	return item


def make_provider_reference_map(provider_references):
	return {
		p['provider_group_id']: p['provider_groups']
		for p in provider_references
	}


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
		with open(file_loc, 'a', newline='') as f:
			writer = csv.DictWriter(f, fieldnames=fieldnames)

			if not file_exists:
				writer.writeheader()

			if type(rows) == list:
				writer.writerows(rows)

			if type(rows) == dict:
				row = rows
				writer.writerow(row)

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
		self._write_table(plan_row, 'plan')
		return plan_row

	def _write_file(self, root_data):

		file_row = {
			'filename': root_data['filename']
		}
		filename_hash = dicthasher(file_row)
		file_row['filename_hash'] = filename_hash
		file_row['url'] = root_data['url']
		self._write_table(file_row, 'file')
		return file_row

	def _write_plan_file(self, plan_row, file_row):

		linking_row = {
			'plan_hash': plan_row['plan_hash'],
			'filename_hash': file_row['filename_hash']
		}

		self._write_table(linking_row, 'plans_files')
		return linking_row

	def _write_code(self, item):

		code_row = {
			# 'negotiation_arrangement':   item['negotiation_arrangement'],
			'billing_code_type': item['billing_code_type'],
			'billing_code_type_version': item['billing_code_type_version'],
			'billing_code': item['billing_code'],
		}

		code_hash = dicthasher(code_row)
		code_row['code_hash'] = code_hash
		self._write_table(code_row, 'codes')
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
				'billing_class': price['billing_class'],
				'negotiated_type': price['negotiated_type'],
				'expiration_date': price['expiration_date'],
				'negotiated_rate': price['negotiated_rate'],
				'service_code': price['service_code'],
				'additional_information': price.get('additional_information'),
				'billing_code_modifier': price['billing_code_modifier'],
				'code_hash': code_hash,
				'filename_hash': filename_hash,
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
				'tin_type': group['tin']['type'],
				'tin_value': group['tin']['value'],
			}
			group_row['provider_group_hash'] = dicthasher(group_row)
			provider_group_rows.append(group_row)
		self._write_table(provider_group_rows, 'provider_groups')
		return provider_group_rows

	def _write_prices_provider_groups(self, price_rows, provider_group_rows):

		linking_row = []
		for price_row, provider_group_row in itertools.product(price_rows,
		                                                       provider_group_rows):
			link = {
				'provider_group_hash': provider_group_row[
					'provider_group_hash'],
				'price_hash': price_row['price_hash'],
			}
			linking_row.append(link)
		self._write_table(linking_row, 'prices_provider_groups')
		return linking_row

	def write_file_and_plan(self, file_row, plan_row):

		self._write_table(file_row, 'files')
		self._write_table(plan_row, 'plans')

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


class MRFFlattener:

	def __init__(self, loc, npi_filter, code_filter):
		self.loc = loc
		self.npi_filter = npi_filter
		self.code_filter = code_filter
		self.root = None
		self.provider_reference_map = None

		# setup filename hash
		filename = Path(loc).stem.split('.')[0]
		self.file_row = {'filename': filename}
		self.filename_hash = dicthasher(self.file_row)
		self.file_row['filename_hash'] = self.filename_hash
		self.file_row['url'] = loc

	def _set_plan_row(self):

		plan_row = {}
		for key in [
			'reporting_entity_name',
			'reporting_entity_type',
			'plan_name',
			'plan_id',
			'plan_id_type',
			'plan_market_type',
			'last_updated_on',
			'version',
		]:
			plan_row[key] = self.root.value.get(key)
		plan_hash = dicthasher(plan_row)
		plan_row['plan_hash'] = plan_hash
		self.plan_row = plan_row

	def make_first_pass(self, f, writer):

		parser = ijson.parse(f, use_float = True)

		root = ijson.ObjectBuilder()
		provider_references = ijson.ObjectBuilder()
		in_network_items = ijson.ObjectBuilder()

		unfetched_provider_references = []
		provider_reference_map = None

		for prefix, event, value in parser:

			if prefix.startswith('provider_references'):
				provider_references.event(event, value)

				if (prefix, event) == ('provider_references.item', 'end_map'):
					unprocessed_reference = provider_references.value.pop()

					if unprocessed_reference.get('location'):
						unfetched_provider_references.append(unprocessed_reference)
						continue

					provider_reference = process_provider_reference(
						unprocessed_reference
					)

					if provider_reference:
						provider_references.value.insert(1, provider_reference)

			elif prefix.startswith('in_network'):
				in_network_items.event(event, value)

				if not hasattr(provider_references, 'value'):
					continue

				if not provider_references.value:
					continue

				elif provider_reference_map == None:
					provider_reference_map = make_provider_reference_map(
						provider_references.value)

				if (prefix, event) == ('in_network.item', 'end_map'):

					unprocessed_item = in_network_items.value.pop()

					in_network_item = process_in_network_item(
						item = unprocessed_item,
						code_filter = self.code_filter,
						provider_reference_map = provider_reference_map
					)

					if in_network_item:
						writer.write_in_network_item(in_network_item, self.filename_hash)

			else:
				root.event(event, value)

		self.provider_reference_map = provider_reference_map

		if not root.value.get('reporting_entity_name'):
			raise InvalidMRF('This is not an MRF')

		self.root = root
		self._set_plan_row()
		writer.write_file_and_plan(self.file_row, self.plan_row)

	def make_second_pass(self, f, writer):

		parser = ijson.parse(f, use_float = True)

		in_network_items = ijson.ObjectBuilder()

		for prefix, event, value in parser:

			if prefix.startswith('in_network'):
				in_network_items.event(event, value)

				if (prefix, event) == ('in_network.item', 'end_map'):

					unprocessed_item = in_network_items.value.pop()

					in_network_item = process_in_network_item(
						item = unprocessed_item,
						code_filter = self.code_filter,
						provider_reference_map = self.provider_reference_map
					)

					if in_network_item:
						writer.write_in_network_item(in_network_item, self.filename_hash)

