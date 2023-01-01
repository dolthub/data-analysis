"""
The function

>>> def json_mrf_to_csv()...

flattens the data from a JSON MRF into a CSV.

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
import json
import logging
import os
from typing import Generator
from pathlib import Path
from urllib.parse import urlparse
from functools import partial

import aiohttp
import ijson
import requests

from schema import SCHEMA

# You can remove this if necessary, but be warned
try:
	assert ijson.backend == 'yajl2_c'
except AssertionError:
	raise Exception('Extremely slow without the yajl2_c backend')


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

	def __init__(self, filename):
		self.filename = filename
		self.f = None
		self.r = None
		self.is_remote = None

		parsed_url = urlparse(self.filename)
		self.suffix = ''.join(Path(parsed_url.path).suffixes)

		if self.suffix not in ('.json.gz', '.json'):
			raise InvalidMRF(f'Suffix not JSON: {self.filename}')

		self.is_remote = parsed_url.scheme in ('http', 'https')

	def __enter__(self):
		if (
			self.is_remote
			and self.suffix == '.json.gz'
		):
			self.r = requests.get(self.filename, stream=True)
			self.f = gzip.GzipFile(fileobj=self.r.raw)

		elif (
			self.is_remote
			and self.suffix == '.json'
		):
			self.r = requests.get(self.filename, stream=True)
			self.r.raw.decode_content = True
			self.f = self.r.raw

		elif self.suffix == '.json.gz':
			self.f = gzip.open(self.filename, 'rb')

		else:
			self.f = open(self.filename, 'rb')

		log.info(f'Opened file: {self.filename}')
		return self.f

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.r and self.is_remote:
			self.r.close()

		self.f.close()


def import_csv_to_set(filename: str):
	"""Imports data as tuples from a given file."""
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


def filename_hasher(filename):

	# retrieve/only/this_part_of_the_file.json(.gz)
	filename = Path(filename).stem.split('.')[0]
	file_row = {'filename': filename}
	filename_hash = dicthasher(file_row)

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


def _make_file_row(filename, filename_hash, url):

	filename = Path(filename).stem.split('.')[0]
	file_row = {
		'filename': filename,
		'filename_hash': filename_hash,
		'url': url
	}

	return file_row


def _make_plan_file_row(plan_row, filename_hash):

	plan_file_row = {
		'plan_hash': plan_row['plan_hash'],
		'filename_hash': filename_hash,
	}

	return plan_file_row


def write_plan(
	plan: dict,
	filename_hash,
	filename,
	url,
	out_dir
):

	file_row = _make_file_row(filename, filename_hash, url)
	_write_table(file_row, 'files', out_dir)

	plan_row = _make_plan_row(plan)
	_write_table(plan_row, 'plans', out_dir)

	plan_file_row = _make_plan_file_row(plan_row, filename_hash)
	_write_table(plan_file_row, 'plans_files', out_dir)


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

	# TODO billing code modifier can have empty strings
	# in its JSON -- remove these before sorting
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


def write_in_network_item(
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


def process_provider_group(
	provider_group: dict,
	npi_filter: set,
) -> dict:
	npi = [str(n) for n in provider_group['npi']]

	if npi_filter:
		npi = [n for n in npi if n in npi_filter]

	if npi:
		tin = provider_group['tin']
		provider_group = {'npi': npi, 'tin': tin}
		return provider_group


def process_provider_reference(
	provider_reference: dict,
	npi_filter: set,
) -> dict:

	provider_groups = process_provider_groups(
		provider_reference['provider_groups'],
		npi_filter
	)

	if provider_groups:
		provider_reference = {
			'provider_group_id' : provider_reference['provider_group_id'],
			'provider_groups'   : provider_groups
		}
		return provider_reference


def replace_provider_references_in_rates(
	rates: list[dict],
	provider_reference_map: dict,
):
	for rate in rates:
		provider_groups = rate.get('provider_groups', [])
		if provider_reference_map and rate.get('provider_references'):
			for provider_group_id in rate['provider_references']:
				addl_provider_groups = provider_reference_map.get(provider_group_id, [])
				provider_groups.extend(addl_provider_groups)
			rate.pop('provider_references')
		rate['provider_groups'] = provider_groups
	return rates


def replace_provider_references(
	in_network_items: Generator,
	provider_reference_map: dict,
):
	for in_network_item in in_network_items:
		in_network_item['negotiated_rates'] = replace_provider_references_in_rates(
			in_network_item['negotiated_rates'],
			provider_reference_map)
		yield in_network_item


def npi_filter_in_network(
	in_network_items: Generator,
	npi_filter: set,
):
	for in_network_item in in_network_items:
		rates = filter_npis_from_rates(in_network_item['negotiated_rates'], npi_filter)
		if rates:
			in_network_item['negotiated_rates'] = rates
			yield in_network_item


def filter_npis_from_rate(
	rate: dict,
	npi_filter: set,
) -> dict:

	provider_groups = rate['provider_groups']
	processed_provider_groups = process_provider_groups(provider_groups, npi_filter)

	if processed_provider_groups:
		rate['provider_groups'] = processed_provider_groups
		return rate


def process_arr(func, arr, *args, **kwargs):
	processed_arr = []
	for item in arr:
		if processed_item := func(item, *args, **kwargs):
			processed_arr.append(processed_item)
	return processed_arr


# process_provider_references = partial(process_arr, process_provider_reference)
process_provider_groups     = partial(process_arr, process_provider_group)
filter_npis_from_rates      = partial(process_arr, filter_npis_from_rate)
# process_in_network_items    = partial(process_arr, process_in_network_item)


def ffwd(parser, to_prefix, to_event):
	for prefix, event, _ in parser:
		# Short circuit evaluation is better than tuple
		# comparison
		if prefix == to_prefix and event == to_event:
			return
	else:
		raise StopIteration


def _skip_filtered_in_network_items(
	parser,
	builder: ijson.ObjectBuilder,
	code_filter: dict,
):
	"""
	This stops us from having to build in-network objects (which are large)
	when their billing codes or arrangement don't fit the filter. It's kind of a
	hack which is why I bundled these changes into one function.
	"""
	item = builder.value[-1]
	code_type = item.get('billing_code_type')
	code = item.get('billing_code')

	if code and code_type and code_filter:
		if (code_type, str(code)) not in code_filter:
			log.debug(f'Skipping {code_type} {code}: filtered out')
			ffwd(parser, 'in_network.item', 'end_map')
			builder.value.pop()
			builder.containers.pop()
			return

	arrangement = item.get('negotiation_arrangement')
	if arrangement and arrangement != 'ffs':
		log.debug(f"Skipping item: arrangement: {arrangement} not 'ffs'")
		ffwd(parser, 'in_network.item', 'end_map')
		builder.value.pop()
		builder.containers.pop()
		return


def gen_provider_references(parser) -> Generator:

	builder = ijson.ObjectBuilder()
	builder.event('start_array', None)

	for prefix, event, value in parser:
		builder.event(event, value)
		if (prefix, event) == ('provider_references.item', 'end_map'):
			provider_reference = builder.value.pop()
			yield provider_reference

		elif (prefix, event) == ('provider_references', 'end_array'):
			return


async def worker(
	queue: asyncio.Queue,
	provider_references: list,
	npi_filter: set,
):
	while True:
		# Get a "work item" out of the queue.
		try:
			session, url, provider_group_id = await queue.get()
			response  = await session.get(url)
			assert response.status == 200
			data      = await response.read()
			json_data = json.loads(data)
			log.debug(f'Opened remote provider reference: {url}')
			json_data['provider_group_id'] = provider_group_id
			provider_reference = process_provider_reference(json_data, npi_filter)
			if provider_reference:
				provider_references.append(provider_reference)
		except AssertionError as err:
			# Response status was 404 or something
			log.debug(f'Encountered bad response status: {response.status}')
			pass
		except Exception as err:
			raise
			# provider_references.append(err)
		finally:
			# Notify the queue that the "work item" has been processed.
			queue.task_done()


async def make_provider_references(items: Generator, npi_filter: set):
	# Create a queue that we will use to store our "workload".
	queue = asyncio.Queue()
	tasks = []
	provider_references = []

	for i in range(10_000):
		task = asyncio.create_task(worker(queue, provider_references, npi_filter))
		tasks.append(task)

	async with aiohttp.client.ClientSession() as session:
		for item in items:
			if url := item.get('location'):
				provider_group_id = item['provider_group_id']
				queue.put_nowait((session, url, provider_group_id))
				continue
			processed_item = process_provider_reference(item, npi_filter)

			if processed_item:
				provider_references.append(processed_item)

		await queue.join()

	# Cancel our worker tasks
	for task in tasks:
		task.cancel()

	# Wait until all worker tasks are cancelled.
	await asyncio.gather(*tasks, return_exceptions=True)

	ref_map = {
		p['provider_group_id']:p['provider_groups']
		for p in provider_references
		# if p is not None
	}

	return ref_map


def code_filter_in_network(
	parser,
	code_filter: set,
) -> Generator:

	builder = ijson.ObjectBuilder()
	builder.event('start_array', None)
	for prefix, event, value in parser:

		builder.event(event, value)

		# This line can be commented out! but it's faster with it in
		if hasattr(builder, 'value') and len(builder.value) > 0:
			_skip_filtered_in_network_items(parser, builder, code_filter)

		if (prefix, event) == ('in_network.item', 'end_map'):
			item = builder.value.pop()
			yield item


# Basic pipeline
# MRF -> read header -> filter provider refs -> read in-network items
# read in-network items: filter items -> process items
class MRFContent:
	"""
	Bucket for MRF data. Assumes that the MRF is in one of the
	following three orders:

	1. plan -> provider_references -> in_network (most common)
	2. plan -> in_network (second-most common)
	3. plan -> in_network -> provider_references (least common)

	As far as I can tell, no one puts their plan data at the
	bottom, although that's possible. If that turns out to be
	the case this function will have to be modified a little bit
	(but that won't be a big deal.)

	Usage:
	>>> content = MRFContent(filename, npi_filter, code_filter)
	>>> content.start_conn() # fetches plan info and opens file
	>>> content.plan # access plan information
	>>> content.in_network_items() # generates items as file is read

	From there you can write the items and plan information as you
	read them in.
	"""

	def __init__(self, filename, npi_filter = None, code_filter = None):
		self.filename = filename
		self.code_filter = code_filter
		self.npi_filter = npi_filter

	def start_conn(self):
		self.parser = self.reset_parser()
		self.set_plan()

	def reset_parser(self) -> Generator:
		with JSONOpen(self.filename) as f:
			yield from ijson.parse(f, use_float = True)

	def set_plan(self) -> dict:
		builder = ijson.ObjectBuilder()
		for prefix, event, value in self.parser:
			builder.event(event, value)
			if value in ('provider_references', 'in_network'):
				self.plan = builder.value
				break
		else:
			raise InvalidMRF

	async def prepare_in_network_state(self) -> None:
		# Normally ordered case
		provider_reference_items = gen_provider_references(self.parser)
		if next(self.parser) == ('provider_references', 'start_array', None):
			self.ref_map = await make_provider_references(provider_reference_items, self.npi_filter)
			ffwd(self.parser, 'in_network', 'start_array')
			return
		try:
			# Check for provider references at bottom
			ffwd(self.parser, 'provider_references', 'start_array')
		except StopIteration:
			# StopIteration -> they don't exist
			self.ref_map = None
		else:
			# Collect them
			self.ref_map = await make_provider_references(provider_reference_items, self.npi_filter)
		finally:
			self.parser = self.reset_parser()
			ffwd(self.parser, 'in_network', 'start_array')

	def in_network_items(self) -> Generator:
		asyncio.run(self.prepare_in_network_state())
		code_filtered_items = code_filter_in_network(self.parser, self.code_filter)
		replaced_items      = replace_provider_references(code_filtered_items, self.ref_map)
		npi_filtered_items  = npi_filter_in_network(replaced_items, self.npi_filter)
		return npi_filtered_items


def json_mrf_to_csv(
	filename: str,
	npi_filter: set,
	code_filter: set,
	out_dir: str,
	url: str = None,
) -> None:
	"""
	Writes MRF content to a flat file CSV in a specific schema.
	The url parameter is optional -- if you pass only a loc,
	we assume that the file is remote.

	If you pass a filename and a URL, the file is read from filename but the
	URL that you input is used. This is just saved for bookkeeping.

	!Importantly! you have to make sure that whatever file you saved
	matches the filename that the URL returns. We index the files by
	name, so if the name is different, the index will be wrong.
	"""

	#TODO warn user if filename is URL

	make_dir(out_dir)
	url = url if url else filename

	# Explicitly make this variable up-front since both sets of tables
	# are linked by it (in_network and plan tables)
	filename_hash = filename_hasher(filename)

	content = MRFContent(filename, npi_filter, code_filter)
	content.start_conn()

	for in_network_item in content.in_network_items():
		write_in_network_item(in_network_item, filename_hash, out_dir)

	write_plan(content.plan, filename_hash, filename, url, out_dir)