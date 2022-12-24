"""
How this works, at a high level.

It needs to connect each in-network item to a provider reference.

1. We can save memory by saving the provider references to a
temporary SQLite database after filtering them. We'll take a function
that takes the provider references, hashes them, and saves them.
2.
"""
import os
import csv
import hashlib
import json
import ijson
import asyncio
import aiohttp
import requests
import gzip
import logging
import functools
from urllib.parse import urlparse
from pathlib import Path
from schema import SCHEMA
from contextlib import ContextDecorator

# You can remove this if necessary, but be warned
try:
	assert ijson.backend == 'yajl2_c'
except AssertionError:
	raise Exception('Extremely slow without the yajl2_c backend')


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class InvalidMRF(Exception):
	pass


class MRFOpen(ContextDecorator):
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


def make_dir(out_dir):
	if not os.path.exists(out_dir):
		os.mkdir(out_dir)


def dicthasher(data, n_bytes = 8):
	if not data:
		raise Exception("Hashed dictionary can't be empty")
	data = json.dumps(data, sort_keys=True).encode('utf-8')
	hash_s = hashlib.sha256(data).digest()[:n_bytes]
	hash_i = int.from_bytes(hash_s, 'little')
	return hash_i


def append_hash(item, name):
	hash_ = dicthasher(item)
	item[name] = hash_
	return item


def _filename_hash(loc):
	filename = Path(loc).stem.split('.')[0]
	filename_hash = dicthasher(filename)
	return filename_hash


def _write_table(rows, tablename, out_dir):
	fieldnames = SCHEMA[tablename]
	file_loc = f'{out_dir}/{tablename}.csv'
	file_exists = os.path.exists(file_loc)

	# newline = '' is to prevent Windows
	# from adding \r\n\n to the end of each line
	with open(file_loc, 'a', newline='') as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)

		if not file_exists:
			writer.writeheader()

		if type(rows) == list:
			writer.writerows(rows)

		if type(rows) == dict:
			row = rows
			writer.writerow(row)


def _make_plan_row(plan: dict):

	keys = [
		'reporting_entity_name',
		'reporting_entity_type',
		'plan_name',
		'plan_id',
		'plan_id_type',
		'plan_market_type',
		'last_updated_on',
		'version',
	]

	plan_row = {key: plan.get(key) for key in keys}
	plan_row = append_hash(plan_row, 'plan_hash')
	return plan_row


def _make_file_row(loc, url):

	filename = Path(loc).stem.split('.')[0]
	file_row = {'filename': filename}
	file_row = append_hash(file_row, 'filename_hash')
	file_row['url'] = url
	return file_row


def _make_plan_file_row(plan_row, file_row):

	plan_file_row = {
		'plan_hash': plan_row['plan_hash'],
		'filename_hash': file_row['filename_hash']
	}

	return plan_file_row


def _make_code_row(code: dict):

	keys = [
		'billing_code_type',
		'billing_code_type_version',
		'billing_code',
	]

	code_row = {key : code[key] for key in keys}
	code_row = append_hash(code_row, 'code_hash')

	return code_row


def _make_price_row(price: dict, code_hash, filename_hash):

	keys = [
		'billing_class',
		'negotiated_type',
		'expiration_date',
		'negotiated_rate',
		'service_code',
		'additional_information',
	]

	price_row = {key : price.get(key) for key in keys}

	optional_json_keys = [
		'service_code',
		'billing_code_modifier',
	]

	for key in optional_json_keys:
		if price.get(key):
			sorted_value = sorted(price[key])
			price_row[key] = json.dumps(sorted_value)

	hashes = {
		'code_hash': code_hash,
		'filename_hash': filename_hash
	}

	price_row.update(hashes)
	price_row = append_hash(price_row, 'price_hash')

	return price_row


def _make_price_rows(prices, code_hash, filename_hash):

	price_rows = []
	for price in prices:
		price_row = _make_price_row(price, code_hash, filename_hash)
		price_rows.append(price_row)

	return price_rows


def _make_provider_group_row(provider_group: dict):

	provider_group_row = {
		'npi_numbers': json.dumps(sorted(provider_group['npi'])),
		'tin_type':    provider_group['tin']['type'],
		'tin_value':   provider_group['tin']['value'],
	}

	provider_group_row = append_hash(provider_group_row, 'provider_group_hash')

	return provider_group_row


def _make_provider_group_rows(provider_groups):

	provider_group_rows = []
	for provider_group in provider_groups:
		provider_group_row = _make_provider_group_row(provider_group)
		provider_group_rows.append(provider_group_row)

	return provider_group_rows


def _make_prices_provider_groups_rows(price_rows, provider_group_rows):

	prices_provider_groups_rows = []
	for price_row in price_rows:
		for provider_group_row in provider_group_rows:

			prices_provider_groups_row = {
				'provider_group_hash': provider_group_row['provider_group_hash'],
				'price_hash': price_row['price_hash'],
			}

			prices_provider_groups_rows.append(prices_provider_groups_row)

	return prices_provider_groups_rows


def _write_in_network_item(in_network_item: dict, filename_hash, out_dir):

	code_row = _make_code_row(in_network_item)
	_write_table(code_row, 'codes', out_dir)

	code_hash = code_row['code_hash']

	for rate in in_network_item['negotiated_rates']:
		prices = rate['negotiated_prices']
		provider_groups = rate['provider_groups']

		price_rows = _make_price_rows(prices, code_hash, filename_hash)
		_write_table(price_rows, 'prices', out_dir)

		provider_group_rows = _make_provider_group_rows(provider_groups)
		_write_table(provider_group_rows, 'provider_groups', out_dir)

		prices_provider_group_rows = _make_prices_provider_groups_rows(price_rows, provider_group_rows)
		_write_table(prices_provider_group_rows, 'prices_provider_groups', out_dir)


async def _fetch_remote_provider_reference(
	session,
	provider_group_id,
	provider_reference_loc,
	npi_filter,
):
	async with session.get(provider_reference_loc) as response:

		log.info(f'Opened remote provider reference url:{provider_reference_loc}')
		assert response.status == 200

		f = await response.read()

		unprocessed_data = json.loads(f)
		unprocessed_data['provider_group_id'] = provider_group_id

		processed_remote_provider_reference = _process_provider_reference(
			item = unprocessed_data,
			npi_filter = npi_filter,
		)
		return processed_remote_provider_reference


async def _fetch_remote_provider_references(
	unfetched_provider_references,
	npi_filter,
):
	tasks = []
	async with aiohttp.client.ClientSession() as session:

		for unfetched_reference in unfetched_provider_references:

			provider_group_id = unfetched_reference['provider_group_id']
			provider_reference_loc = unfetched_reference['location']

			task = asyncio.wait_for(
				_fetch_remote_provider_reference(
					session = session,
					provider_group_id = provider_group_id,
					provider_reference_loc = provider_reference_loc,
					npi_filter = npi_filter,
				),
				timeout=5,
			)

			tasks.append(task)

		fetched_remote_provider_references = await asyncio.gather(*tasks)
		fetched_remote_provider_references = list(
			filter(lambda item: item, fetched_remote_provider_references))

		return fetched_remote_provider_references


def _process_provider_reference(
	item,
	npi_filter,
):
	result = {
		'provider_group_id' : item['provider_group_id'],
		'provider_groups'   : []
	}

	if item.get('location'):
		return item

	for provider_group in item['provider_groups']:
		npi = [str(n) for n in provider_group['npi']]
		if npi_filter:
			npi = [n for n in npi if n in npi_filter]
		if not npi:
			continue
		tin = provider_group['tin']
		provider_group = {'npi': npi, 'tin': tin}
		result['provider_groups'].append(provider_group)

	if not result['provider_groups']:
		return

	return result


def make_provider_reference_map(
	provider_references,
	unfetched_provider_references,
	npi_filter,
):

	loop = asyncio.get_event_loop()
	fetched_provider_references = loop.run_until_complete(
		_fetch_remote_provider_references(
			unfetched_provider_references = unfetched_provider_references,
			npi_filter = npi_filter,
		)
	)

	provider_references.extend(
		fetched_provider_references
	)

	provider_reference_map = {
		p['provider_group_id']: p['provider_groups']
		for p in provider_references
		if p is not None
	}

	return provider_reference_map


def _process_rate(
	item,
	provider_reference_map,
):

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


def _process_in_network_item(
	item,
	provider_reference_map,
	# code_filter = None
):

	# the point optimization takes care of this
	# if code_filter:
	# 	billing_code_type = item['billing_code_type']
	# 	billing_code = str(item['billing_code'])
	# 	if (billing_code_type, billing_code) not in code_filter:
	# 		log.debug(f"skipped {item['billing_code']} not in list")
	# 		return
	#
	# if item['negotiation_arrangement'] != 'ffs':
	# 	log.debug('wrong type')
	# 	return

	rates = []
	for unprocessed_rate in item['negotiated_rates']:
		rate = _process_rate(unprocessed_rate, provider_reference_map)
		if rate:
			rates.append(rate)

	if not rates:
		return

	item['negotiated_rates'] = rates
	return item


def flattener(
	fileobj,
	npi_filter,
	code_filter,
	provider_reference_map,
	filename_hash,
	out_dir,
) -> tuple:

	provider_reference_order = None
	# None, 'top', or 'bottom'

	parser = ijson.parse(fileobj, use_float = True)

	plan = ijson.ObjectBuilder()
	provider_references = ijson.ObjectBuilder()
	in_network_items = ijson.ObjectBuilder()

	unfetched_provider_references = []

	for prefix, event, value in parser:

		if prefix.startswith('provider_references') and not hasattr(flattener, 'provider_reference_map'):
			provider_references.event(event, value)
			provider_reference_order = 'bottom'

			if (prefix, event) == ('provider_references.item', 'end_map'):
				unprocessed_reference = provider_references.value.pop()

				if unprocessed_reference.get('location'):
					unfetched_provider_references.append(unprocessed_reference)
					continue

				provider_reference = _process_provider_reference(
					item = unprocessed_reference,
					npi_filter = npi_filter
				)

				if provider_reference:
					provider_references.value.insert(1, provider_reference)

		elif prefix.startswith('in_network'):
			in_network_items.event(event, value)

			# If we've passed through the provider_references
			# block, provider_references.value will exist,
			# but if we've just entered this block, we won't
			# have fetched the remote provider refs yet.
			if (
				hasattr(provider_references, 'value')
				and provider_reference_map is None
			):
				# Going through this path means the file is
				# in the right order
				provider_reference_order = 'top'
				provider_reference_map = make_provider_reference_map(
					provider_references = provider_references.value,
					unfetched_provider_references = unfetched_provider_references,
					npi_filter = npi_filter,
				)

				# We don't need this anymore
				provider_references.value.clear()

			# Point optimization #1
			# The following code chunk can be commented out
			# entirely. It adds some complexity but basically,
			# we pass momentarily to the parser to get some
			# extra control over the objects we build
			if hasattr(in_network_items, 'value') and in_network_items.value:

				item = in_network_items.value[-1]
				code_type = item.get('billing_code_type')
				code = item.get('billing_code')
				arrangement = item.get('negotiation_arrangement')

				# This stops us from having to build in-network
				# objects (which are large) when their billing codes
				# don't fit the filter
				if code and code_type and code_filter:
					if (code_type, str(code)) not in code_filter:
						log.debug(f'Skipping {code_type} {code}: filtered out')
						while True:
							prefix, event, _ = next(parser)
							if (prefix, event) == ('in_network.item', 'end_map'):
								break
						in_network_items.value.pop()
						in_network_items.containers.pop()
						continue

				# If the code has the wrong arrangement, skip
				if arrangement and arrangement != 'ffs':
					log.debug(f"Skipping item: arrangement: {arrangement} not 'ffs'")
					while True:
						prefix, event, _ = next(parser)
						if (prefix, event) == ('in_network.item', 'end_map'):
							break
					in_network_items.value.pop()
					in_network_items.containers.pop()
					continue

			if (prefix, event) == ('in_network.item', 'end_map'):

				unprocessed_item = in_network_items.value.pop()

				in_network_item = _process_in_network_item(
					item = unprocessed_item,
					provider_reference_map = provider_reference_map
				# 	code_filter = self.code_filter,
				)

				if in_network_item:
					code_type = in_network_item['billing_code_type']
					code = in_network_item['billing_code']
					log.debug(f'Writing {code_type} {code}')
					_write_in_network_item(in_network_item, filename_hash, out_dir)

		else:
			plan.event(event, value)

	if not plan.value.get('reporting_entity_name'):
		raise InvalidMRF

	return provider_reference_order, provider_reference_map, plan.value

def flatten(
	loc,
	npi_filter,
	code_filter,
	out_dir,
	url,
):
	filename_hash = _filename_hash(loc)
	with MRFOpen(loc) as f:
		result = flattener(
			fileobj = f,
			npi_filter = npi_filter,
			code_filter = code_filter,
			provider_reference_map  = None,
			filename_hash = filename_hash,
			out_dir = out_dir,
		)

	provider_reference_order, provider_reference_map, plan = result

	if provider_reference_order == 'bottom':

		log.debug('Found provider references at the bottom. Running again')

		with MRFOpen(loc) as f:
			flattener(
				fileobj = f,
				npi_filter = npi_filter,
				code_filter = code_filter,
				provider_reference_map = provider_reference_map,
				filename_hash = filename_hash,
				out_dir = out_dir,
			)

	if not url:
		url = loc

	file_row = _make_file_row(loc, url)
	_write_table(file_row, 'files', out_dir)

	plan_row = _make_plan_row(plan)
	_write_table(plan_row, 'plans', out_dir)

	plan_file_row = _make_plan_file_row(plan_row, file_row)
	_write_table(plan_file_row, 'plans_files', out_dir)