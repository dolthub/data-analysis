"""
All you really need to know
###########################

The function

>>> def json_mrf_to_csv()...

flattens the data from a JSON MRF into a CSV.

If the flattener encounters the in-network items after the provider
references, it will flatten those on the first pass. Otherwise, it will open
the file again and re-run, recycling the provider references that it got from
the first pass.

The plan data is only written after the flattener has successfully run.

Naming conventions
##################

MRS contain a lot of objects with long names. I use these names as vars just
because typing out the full-length names gives me a headache.

* top-level information --> plan
* provider_references --> references
* provider_groups --> groups (there are no other groups)
* provider_group_id --> group_id
* in-network --> in_network
* negotiated_rates --> rates

For the time being I set the tab width to be 8 spaces to force myself to be
more concise with my functions.

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


def write_table(rows, tablename, out_dir):

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


def plan_row_from_dict(plan: dict):

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


def file_row_from_filename(filename, filename_hash, url):

	filename = Path(filename).stem.split('.')[0]
	file_row = {
		'filename': filename,
		'filename_hash': filename_hash,
		'url': url
	}

	return file_row


def plan_file_row_from_row(plan_row, filename_hash):

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

	file_row = file_row_from_filename(filename, filename_hash, url)
	write_table(file_row, 'files', out_dir)

	plan_row = plan_row_from_dict(plan)
	write_table(plan_row, 'plans', out_dir)

	plan_file_row = plan_file_row_from_row(plan_row, filename_hash)
	write_table(plan_file_row, 'plans_files', out_dir)


def code_row_from_dict(in_network_item: dict):

	keys = [
		'billing_code_type',
		'billing_code_type_version',
		'billing_code',
	]

	code_row = {key : in_network_item[key].strip() for key in keys}
	code_row = append_hash(code_row, 'code_hash')

	return code_row


def price_row_from_dict(
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
			sorted_value = [value.strip() for value in sorted(price[key])]
			price_row[key] = json.dumps(sorted_value)

	hashes = {
		'code_hash': code_hash,
		'filename_hash': filename_hash
	}

	price_row.update(hashes)
	price_row = append_hash(price_row, 'price_hash')

	return price_row


def price_rows_from_dicts(
	prices: list[dict],
	code_hash,
	filename_hash,
):

	price_rows = []
	for price in prices:
		price_row = price_row_from_dict(price, code_hash, filename_hash)
		price_rows.append(price_row)

	return price_rows


def group_row_from_dict(group: dict):

	group_row = {
		'npi_numbers': json.dumps(sorted(group['npi'])),
		'tin_type':    group['tin']['type'],
		'tin_value':   group['tin']['value'],
	}

	group_row = append_hash(group_row, 'provider_group_hash')

	return group_row


def group_rows_from_dicts(groups: list[dict]):

	provider_group_rows = []
	for provider_group in groups:
		provider_group_row = group_row_from_dict(provider_group)
		provider_group_rows.append(provider_group_row)

	return provider_group_rows


def prices_groups_rows_from_dicts(
	price_rows: list[dict],
	group_rows: list[dict]
):

	prices_groups_rows = []
	for price_row in price_rows:
		for group_row in group_rows:

			prices_groups_row = {
				'provider_group_hash': group_row['provider_group_hash'],
				'price_hash': price_row['price_hash'],
			}

			prices_groups_rows.append(prices_groups_row)

	return prices_groups_rows


def write_in_network_item(
	in_network_item: dict,
	filename_hash,
	out_dir
):

	code_row = code_row_from_dict(in_network_item)
	write_table(code_row, 'codes', out_dir)

	code_hash = code_row['code_hash']

	for rate in in_network_item['negotiated_rates']:
		prices = rate['negotiated_prices']
		provider_groups = rate['provider_groups']

		price_rows = price_rows_from_dicts(prices, code_hash, filename_hash)
		write_table(price_rows, 'prices', out_dir)

		group_rows = group_rows_from_dicts(provider_groups)
		write_table(group_rows, 'provider_groups', out_dir)

		prices_groups_rows = prices_groups_rows_from_dicts(price_rows, group_rows)
		write_table(prices_groups_rows, 'prices_provider_groups', out_dir)

	code_type = in_network_item['billing_code_type']
	code = in_network_item['billing_code']
	log.debug(f'Wrote {code_type} {code}')


def process_group(
	group: dict,
	npi_filter: set,
) -> dict:

	group['npi'] = [str(n) for n in group['npi']]

	if not npi_filter:
		return group

	group['npi'] = [n for n in group['npi'] if n in npi_filter]

	if group['npi']:
		return group


def process_reference(
	reference: dict,
	npi_filter: set,
) -> dict:

	groups = reference['provider_groups']
	groups = process_groups(groups, npi_filter)

	if groups:
		reference = {
			'provider_group_id' : reference['provider_group_id'],
			'provider_groups'   : groups
		}
		return reference


def replace_rates(
	rates: list[dict],
	reference_map: dict,
):
	if not reference_map:
		# TODO look into this
		# If there's no map, and there's only a reference and if there
		# are no provider groups we can forget about the whole rate
		for rate in rates:
			rate.pop('provider_references', None)
		rates = [rate for rate in rates if rate.get('provider_groups')]
		return rates

	for rate in rates:
		if group_ids := rate.get('provider_references'):
			groups = rate.get('provider_groups', [])
			for group_id in group_ids:
				addl_groups = reference_map.get(group_id, [])
				groups.extend(addl_groups)
			rate.pop('provider_references')
			rate['provider_groups'] = groups
	return rates


def replace_in_network_rates(
	in_network_items: Generator,
	reference_map: dict,
):
	for in_network_item in in_network_items:
		rates = in_network_item['negotiated_rates']
		rates = replace_rates(rates, reference_map)
		in_network_item['negotiated_rates'] = rates
		yield in_network_item


def process_in_network(
	in_network_items: Generator,
	npi_filter: set,
):
	for in_network_item in in_network_items:
		rates = in_network_item['negotiated_rates']
		rates = process_rates(rates, npi_filter)
		if rates:
			in_network_item['negotiated_rates'] = rates
			yield in_network_item


def process_rate(
	rate: dict,
	npi_filter: set,
) -> dict:

	groups = rate['provider_groups']
	groups = process_groups(groups, npi_filter)

	if groups:
		rate['provider_groups'] = groups
		return rate


def process_arr(func, arr, *args, **kwargs):
	processed_arr = []
	for item in arr:
		if processed_item := func(item, *args, **kwargs):
			processed_arr.append(processed_item)
	return processed_arr


# process_provider_references = partial(process_arr, process_provider_reference)
process_groups     = partial(process_arr, process_group)
process_rates      = partial(process_arr, process_rate)
# process_in_network_items    = partial(process_arr, process_in_network_item)


def ffwd(parser: Generator, to_prefix: str, to_event: str):
	for prefix, event, _ in parser:
		if prefix == to_prefix and event == to_event:
			break
	else:
		raise StopIteration


def skip_item_by_code(
	parser: Generator,
	builder: ijson.ObjectBuilder,
	code_filter: set,
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


def gen_references(parser: Generator) -> Generator:

	builder = ijson.ObjectBuilder()
	builder.event('start_array', None)

	for prefix, event, value in parser:
		builder.event(event, value)
		if (prefix, event) == ('provider_references.item', 'end_map'):
			reference = builder.value.pop()
			yield reference

		elif (prefix, event) == ('provider_references', 'end_array'):
			return


async def fetch_remote_reference(
	session: aiohttp.client.ClientSession,
	url: str,
):
	response = await session.get(url)
	assert response.status == 200

	log.debug(f'Opened remote provider reference: {url}')

	data = await response.read()
	reference = json.loads(data)
	return reference


# TODO I hate this function name
# and think this needs to be broken down into
# smaller funcs
async def append_processed_remote_reference(
	queue: asyncio.Queue,
	processed_references: list,
	npi_filter: set,
):
	while True:
		try:
			# Get a "work item" out of the queue.
			session, url, group_id = await queue.get()

			reference = await fetch_remote_reference(session, url)
			reference['provider_group_id'] = group_id
			reference = process_reference(reference, npi_filter)

			if reference:
				processed_references.append(reference)

		except AssertionError:
			# Response status was 404 or something
			log.debug(f'Encountered bad response status')
		finally:
			# Notify the queue that the "work item" has been processed.
			queue.task_done()


# TODO I don't like that we process the refs separately
async def make_reference_map(
	references: Generator,
	npi_filter: set
):
	"""
	Processes all provider references and return a map like:
	{
		1: [group1, group2, ...],
		2: [group1, group2, ...],
	}
	where each provider group has been filtered to only contain
	the NPIs contained in `npi_filter`.
	"""
	# Create a queue that we will use to store our "workload".
	queue = asyncio.Queue()

	# Tasks hold the consumers. reference is appended to
	# by consumers and by the main loop of this function
	tasks = []
	processed_references = []

	for i in range(200):
		coro = append_processed_remote_reference(queue, processed_references, npi_filter)
		task = asyncio.create_task(coro)
		tasks.append(task)

	async with aiohttp.client.ClientSession() as session:
		for reference in references:
			if url := reference.get('location'):
				group_id = reference['provider_group_id']
				queue.put_nowait((session, url, group_id))
				continue

			reference = process_reference(reference, npi_filter)
			if reference:
				processed_references.append(reference)

		# Block until all items in the queue have been received and processed
		await queue.join()

	# To understand why this sleep is here, see:
	# https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
	await asyncio.sleep(.250)

	# Cancel our worker tasks
	for task in tasks:
		task.cancel()

	# Wait until all worker tasks are cancelled.
	await asyncio.gather(*tasks, return_exceptions=True)

	reference_map = {
		reference['provider_group_id']: reference['provider_groups']
		for reference in processed_references
	}

	return reference_map


def gen_in_network_items(
	parser: Generator,
	code_filter: set,
) -> Generator:

	builder = ijson.ObjectBuilder()
	builder.event('start_array', None)

	for prefix, event, value in parser:
		builder.event(event, value)

		# This line can be commented out! but it's faster with it in
		if hasattr(builder, 'value') and len(builder.value) > 0:
			skip_item_by_code(parser, builder, code_filter)

		if (prefix, event) == ('in_network.item', 'end_map'):
			in_network_item = builder.value.pop()
			yield in_network_item

		elif (prefix, event) == ('in_network', 'end_array'):
			return


# Basic pipeline
# MRF -> read header -> filter provider refs -> read in-network items
# read in-network items: filter items -> process items
class MRFContent:
	"""
	Bucket for MRF data. Assumes that the MRF is in one of the following
	three orders:

	1. plan -> provider_references -> in_network (most common)
	2. plan -> in_network (second-most common)
	3. plan -> in_network -> provider_references (least common)

	As far as I can tell, no one puts their plan data at the bottom,
	although that's possible. If that turns out to be the case this
	function will have to be modified a little bit (but that won't be a
	big deal.)

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
		self.parser = self.start_parser()
		self.set_plan()

	def start_parser(self) -> Generator:
		with JSONOpen(self.filename) as f:
			yield from ijson.parse(f, use_float = True)

	def set_plan(self):
		builder = ijson.ObjectBuilder()
		for prefix, event, value in self.parser:
			builder.event(event, value)
			if value in ('provider_references', 'in_network'):
				self.plan = builder.value
				break
		else:
			raise InvalidMRF

	async def set_references(self) -> None:
		"""
		Probably looks pretty WTF. All this does is collect the
		provider references (and reset the parsing stream if need be.)
		The problem is they can either be located in one of three
		places:

		1. {
			...
			'provider_references': <-- here (most common)
			'in_network': ...
		}
		2. {
			...
			'in_network': ...
			'provider_references': <-- here (rarely)
		}
		3. {
			...
			'in_network': ...
		} (aka nowhere, semi-common)

		This function checks in order (1, 2, 3). First we look for
		case (1). If we don't find it, we try case (2), and if we hit a
		StopIteration, we know we have case (3).

		In cases (2) and (3) we have to restart the parser since the
		in-network items are now above us.
		"""
		# Case (1)
		references = gen_references(self.parser)
		if next(self.parser) == ('provider_references', 'start_array', None):
			self.reference_map = await make_reference_map(references, self.npi_filter)
			return
		try:
			# Check for case (2)
			ffwd(self.parser, 'provider_references', 'start_array')
		except StopIteration:
			# StopIteration -> they don't exist (3)
			self.reference_map = None
		else:
			# Collect them
			self.reference_map = await make_reference_map(references, self.npi_filter)
		finally:
			self.parser = self.start_parser()

	def in_network_items(self) -> Generator:
		"""
		Generator pipeline for returning processed in-network items.

		gen_in_network_items -> Generator of filtered items
		replace_in_network_rates -> Generator of items w/ rates subbed
		process_in_network -> Generator of items with npis removed
		"""
		asyncio.run(self.set_references())
		ffwd(self.parser, 'in_network', 'start_array')

		in_network_items = gen_in_network_items(self.parser, self.code_filter)
		replaced_items   = replace_in_network_rates(in_network_items, self.reference_map)
		return process_in_network(replaced_items, self.npi_filter)


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