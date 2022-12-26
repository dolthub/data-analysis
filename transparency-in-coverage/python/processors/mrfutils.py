"""
The function

def flatten_mrf()...

starts grabbing objects from the MRF. No matter what the order
the file is in, on the first pass it always grabs the plan data
and the provider references.

If the flattener encounters the in-network items after the provider
references, it will flatten those on the first pass. Otherwise, it
will open the file again and re-run, recycling the provider
references that it got from the first pass.

The plan data is only written after the flattener has successfully run.
"""
import asyncio
import csv
import gzip
import hashlib
import io
import json
import logging
import os
import functools
from typing import Generator
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import ijson
import requests

from schema import SCHEMA

# You can remove this if necessary, but be warned
try:
	assert ijson.backend == 'yajl2_c'
except AssertionError:
	raise Exception('Extremely slow without the yajl2_c backend')

SENTINEL = 'SENTINEL'

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class InvalidMRF(Exception):
	pass


class JSONOpen:
	"""
	Context manager for opening JSON(.gz) MRFs.
	Handles local and remote gzipped and unzipped
	JSON files.
	"""

	def __init__(self, input):
		if type(input) == bytes:
			self.data = input
			self.loc = None
		else:
			self.loc = input
			self.data = None

		self.f = None
		self.r = None
		self.is_remote = None

		if self.loc:
			print('location!!')
			parsed_url = urlparse(self.loc)
			self.suffix = ''.join(Path(parsed_url.path).suffixes)

			if self.suffix not in ('.json.gz', '.json'):
				raise InvalidMRF(f'Suffix not JSON: {self.loc}')

			self.is_remote = parsed_url.scheme in ('http', 'https')

	def __enter__(self):
		if self.data:
			self.f = io.BytesIO(self.data)
			return self.f
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
		if self.r and self.is_remote:
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


def _make_code_row(in_network_item: dict):

	keys = [
		'billing_code_type',
		'billing_code_type_version',
		'billing_code',
	]

	code_row = {key : in_network_item[key] for key in keys}
	code_row = append_hash(code_row, 'code_hash')

	return code_row


def _make_price_row(
	price: dict,
	code_hash,
	filename_hash
):

	keys = [
		'billing_class',
		'negotiated_type',
		'expiration_date',
		'negotiated_rate',
		'additional_information',
	]

	price_row = {key : price.get(key) for key in keys}

	optional_json_keys = [
		'service_code',
		'billing_code_modifier',
	]

	for key in optional_json_keys:
		if price.get(key):
			price.get(key)
			sorted_value = sorted(price[key])
			price_row[key] = json.dumps(sorted_value)

	hashes = {
		'code_hash': code_hash,
		'filename_hash': filename_hash
	}

	price_row.update(hashes)
	price_row = append_hash(price_row, 'price_hash')

	return price_row


def _make_price_rows(
	prices: list[dict],
	code_hash,
	filename_hash,
):

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


def _make_provider_group_rows(provider_groups: list[dict]):

	provider_group_rows = []
	for provider_group in provider_groups:
		provider_group_row = _make_provider_group_row(provider_group)
		provider_group_rows.append(provider_group_row)

	return provider_group_rows


def _make_prices_provider_groups_rows(
	price_rows: list[dict],
	provider_group_rows: list[dict]
):

	prices_provider_groups_rows = []
	for price_row in price_rows:
		for provider_group_row in provider_group_rows:

			prices_provider_groups_row = {
				'provider_group_hash': provider_group_row['provider_group_hash'],
				'price_hash': price_row['price_hash'],
			}

			prices_provider_groups_rows.append(prices_provider_groups_row)

	return prices_provider_groups_rows


def _write_in_network_item(
	in_network_item: dict,
	filename_hash,
	out_dir
):

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

	code_type = in_network_item['billing_code_type']
	code = in_network_item['billing_code']
	log.debug(f'Wrote {code_type} {code}')


def _write_plan(
	plan: dict,
	loc,
	url,
	out_dir
):

	file_row = _make_file_row(loc, url)
	_write_table(file_row, 'files', out_dir)

	plan_row = _make_plan_row(plan)
	_write_table(plan_row, 'plans', out_dir)

	plan_file_row = _make_plan_file_row(plan_row, file_row)
	_write_table(plan_file_row, 'plans_files', out_dir)


async def _fetch_remote_provider_reference(
	session,
	provider_group_id,
	provider_reference_loc: str,
	npi_filter: set,
) -> dict:
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
	unfetched_provider_references: list[dict],
	npi_filter: set,
) -> list[dict]:
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

		fetched_references = await asyncio.gather(*tasks)
		fetched_references = [item for item in fetched_references if item]

		return fetched_references


def _process_provider_group(
	provider_group: dict,
	npi_filter: set,
) -> dict:
	npi = [str(n) for n in provider_group['npi']]

	if npi_filter:
		npi = [n for n in npi if n in npi_filter]

	if not npi:
		return

	tin = provider_group['tin']

	provider_group = {'npi': npi, 'tin': tin}
	return provider_group


def _process_provider_reference(
	item: dict,
	npi_filter: set,
) -> dict:
	processed_provider_groups = []
	for provider_group in item['provider_groups']:
		provider_group = _process_provider_group(provider_group, npi_filter)
		if provider_group:
			processed_provider_groups.append(provider_group)

	if not processed_provider_groups:
		return

	result = {
		'provider_group_id' : item['provider_group_id'],
		'provider_groups'   : processed_provider_groups
	}

	return result


def _combine_local_remote_provider_references(
	provider_references: list[dict],
	unfetched_provider_references: list[dict],
	npi_filter: set,
) -> list[dict]:

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

	return provider_references


def _process_rate(
	rate: dict,
	provider_reference_map: dict,
	npi_filter,
) -> dict:

	provider_groups = rate.get('provider_groups', [])
	if provider_reference_map and rate.get('provider_references'):
		for provider_group_id in rate['provider_references']:
			addl_provider_groups = provider_reference_map.get(provider_group_id)
			if addl_provider_groups:
				provider_groups.extend(addl_provider_groups)
		rate.pop('provider_references')

	processed_provider_groups = []
	for provider_group in provider_groups:
		processed_provider_group = _process_provider_group(provider_group, npi_filter)
		if processed_provider_group:
			processed_provider_groups.append(processed_provider_group)

	if not processed_provider_groups:
		return

	rate['provider_groups'] = processed_provider_groups

	return rate


def _process_in_network_item(
	in_network_item: dict,
	provider_reference_map: dict,
	npi_filter: set,
	# code_filter = None
) -> dict:

	# the local optimization takes care of this
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
	for unprocessed_rate in in_network_item['negotiated_rates']:
		rate = _process_rate(unprocessed_rate, provider_reference_map, npi_filter)
		if rate:
			rates.append(rate)

	if not rates:
		return

	in_network_item['negotiated_rates'] = rates

	return in_network_item


def _ffwd(parser, to_prefix, to_event):
	for prefix, event, _ in parser:
		if (prefix, event) == (to_prefix, to_event):
			return


_ffwd_in_network_item = functools.partial(_ffwd, to_prefix = 'in_network.item', to_event = 'end_map')
_ffwd_in_network_array = functools.partial(_ffwd, to_prefix = 'in_network', to_event = 'end_array')


def _local_optimization(in_network_items, parser, code_filter):
	"""
	This stops us from having to build in-network objects (which are large)
	when their billing codes or arrangement don't fit the filter. It's kind of a
	hack which is why I bundled these changes into one function.
	"""
	item = in_network_items.value[-1]
	code_type = item.get('billing_code_type')
	code = item.get('billing_code')

	if code and code_type and code_filter:
		if (code_type, str(code)) not in code_filter:
			log.debug(f'Skipping {code_type} {code}: filtered out')
			_ffwd_in_network_item(parser)
			in_network_items.value.pop()
			in_network_items.containers.pop()
			return

	arrangement = item.get('negotiation_arrangement')
	if arrangement and arrangement != 'ffs':
		log.debug(f"Skipping item: arrangement: {arrangement} not 'ffs'")
		_ffwd_in_network_item(parser)
		in_network_items.value.pop()
		in_network_items.containers.pop()
		return


def _make_provider_reference_map(
	parser,
	npi_filter: set,
) -> dict:

	unfetched_provider_references = []

	provider_references = ijson.ObjectBuilder()
	provider_references.event('start_array', None)

	for prefix, event, value in parser:
		provider_references.event(event, value)
		if (prefix, event) == ('provider_references.item', 'end_map'):
			unprocessed_reference = provider_references.value.pop()
			if unprocessed_reference.get('location'):
				unfetched_provider_references.append(unprocessed_reference)
				continue

			provider_reference = _process_provider_reference(
				item = unprocessed_reference,
				npi_filter = npi_filter)

			if provider_reference:
				provider_references.value.insert(1, provider_reference)

		elif (prefix, event) == ('provider_references', 'end_array'):
			combined_provider_references = _combine_local_remote_provider_references(
				provider_references = provider_references.value,
				unfetched_provider_references = unfetched_provider_references,
				npi_filter = npi_filter,)

			provider_reference_map = {
				p['provider_group_id']: p['provider_groups']
				for p in combined_provider_references
				if p is not None
			}

			return provider_reference_map


def _in_network_items(
	parser,
	code_filter: set,
) -> Generator[dict, None, None]:

	in_network_items = ijson.ObjectBuilder()
	in_network_items.event('start_array', None)

	for prefix, event, value in parser:
		in_network_items.event(event, value)

		# This line can be commented out! but it's faster with it in
		if hasattr(in_network_items, 'value') and len(in_network_items.value) > 0:
			_local_optimization(in_network_items, parser, code_filter)

		if (prefix, event) == ('in_network.item', 'end_map'):
			yield in_network_items.value.pop()

		elif (prefix, event) == ('in_network', 'end_array'):
			return


def _processed_in_network_items(
	unprocessed_items: Generator[dict, None, None],
	provider_reference_map: dict,
	npi_filter: set,
) -> Generator[dict, None, None]:

	for unprocessed_item in unprocessed_items:
		processed_item = _process_in_network_item(
			in_network_item = unprocessed_item,
			npi_filter = npi_filter,
			provider_reference_map = provider_reference_map)
		if processed_item:
			yield processed_item


def _gen_mrf_contents(
	file,
	npi_filter: set,
	code_filter: set,
) -> Generator[dict, None, None]:

	provider_reference_map = None
	plan = ijson.ObjectBuilder()

	parser = ijson.parse(file, use_float = True)
	for prefix, event, value in parser:

		if prefix.startswith('provider_references'):
			provider_reference_map = _make_provider_reference_map(
				parser = parser,
				npi_filter = npi_filter,)

		elif prefix.startswith('in_network'):
			if provider_reference_map is None:
				_ffwd_in_network_array(parser)
				continue

			unprocessed_items = _in_network_items(parser, code_filter)
			processed_items = _processed_in_network_items(
				unprocessed_items = unprocessed_items,
				provider_reference_map = provider_reference_map,
				npi_filter = npi_filter)
			yield from processed_items

		else:
			plan.event(event, value)

	yield SENTINEL
	yield plan.value
	yield provider_reference_map # can be None


def _gen_mrf_in_network_contents(
	file,
	code_filter: set,
	npi_filter: set,
	provider_reference_map: dict,
) -> Generator[dict, None, None]:

	parser = ijson.parse(file, use_float = True)
	_ffwd(parser, 'in_network', 'start_array')

	unprocessed_items = _in_network_items(parser, code_filter)
	processed_items = _processed_in_network_items(
		unprocessed_items = unprocessed_items,
		provider_reference_map = provider_reference_map,
		npi_filter = npi_filter,)
	yield from processed_items


def _gen_ordered_mrf_contents(
	loc,
	npi_filter: set = None,
	code_filter: set = None,
) -> Generator[dict, None, None]:

	"""Returns in_network items, followed by SENTINEL, followed
	by plan."""
	with JSONOpen(loc) as f:
		contents = _gen_mrf_contents(
			file = f,
			npi_filter = npi_filter,
			code_filter = code_filter,)

		first_value = next(contents)
		if first_value != SENTINEL:
			yield first_value
			yield from contents
			return

	plan = next(contents)
	provider_reference_map = next(contents)

	if not plan.get('reporting_entity_name'):
		raise InvalidMRF

	with JSONOpen(loc) as f:
		in_network_items = _gen_mrf_in_network_contents(
			file = f,
			npi_filter = npi_filter,
			code_filter = code_filter,
			provider_reference_map = provider_reference_map)
		yield from in_network_items
		yield SENTINEL
		yield plan


def flatten_mrf(
	loc: str,
	npi_filter: set,
	code_filter: set,
	out_dir: str,
	url: str,
) -> None:

	make_dir(out_dir)
	filename_hash = _filename_hash(loc)
	url = url if url else loc

	ordered_contents = _gen_ordered_mrf_contents(
		npi_filter,
		code_filter,
		loc)

	for in_network_item in ordered_contents:

		if in_network_item == SENTINEL:
			break

		_write_in_network_item(
				in_network_item,
				filename_hash,
				out_dir)

	plan = next(ordered_contents)
	_write_plan(plan, loc, url, out_dir)

