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

MRFs contain a lot of objects with long names. I use these names as vars just
because typing out the full-length names gives me a headache.

* top-level information --> plan
* provider_references --> references
* provider_groups --> groups (there are no other groups)
* provider_group_id --> group_id
* in-network --> in_network
* negotiated_rates --> rates

For the time being I set the tab width to be 8 spaces to force myself to be
more concise with my functions.

NOTES: There's more than one way to go about this.
* You don't have to swap the provider references in the rates. You can keep them
as they are and write them later. But this way if you have a custom code -- NPI
mapping you can optionally delete NPI numbers contingent on which billing code
you're looking at.
* Possible room for optimization: basic_parse instead of parse. You'd probably
need to write a +1/-1 tracker every time you hit a start_map/end_map event, so
that you can track your depth in the JSON tree.
"""
from __future__ import annotations

import asyncio
from functools import partial
from itertools import product
from typing import Generator

import aiohttp
import ijson

from helpers import *
from schema import SCHEMA

# You can remove this if necessary, but be warned
# Right now this only works with python 3.9
assert ijson.backend == 'yajl2_c'

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# To distinguish data from rows
Row = dict

def write_table(
	rows: list[Row] | Row,
	tablename: str,
	out_dir: str,
) -> None:

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


def plan_row_from_dict(plan: dict) -> Row:

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


def file_row_from_filename(
	filename: str,
	filename_hash: int,
	url: str
) -> Row:

	filename = Path(filename).stem.split('.')[0]
	file_row = {
		'filename': filename,
		'filename_hash': filename_hash,
		'url': url
	}

	return file_row


def plan_file_row_from_row(
	plan_row: Row,
	filename_hash: int
) -> Row:

	plan_file_row = {
		'plan_hash': plan_row['plan_hash'],
		'filename_hash': filename_hash,
	}

	return plan_file_row


def write_plan(
	plan: dict,
	filename_hash: int,
	filename: str,
	url: str,
	out_dir: str,
) -> None:

	file_row = file_row_from_filename(filename, filename_hash, url)
	write_table(file_row, 'files', out_dir)

	plan_row = plan_row_from_dict(plan)
	write_table(plan_row, 'plans', out_dir)

	plan_file_row = plan_file_row_from_row(plan_row, filename_hash)
	write_table(plan_file_row, 'plans_files', out_dir)


def code_row_from_dict(in_network_item: dict) -> Row:

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
	code_hash: str,
	filename_hash: str,
) -> Row:

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
	code_hash: str,
	filename_hash: str,
) -> list[Row]:

	price_rows = []
	for price in prices:
		price_row = price_row_from_dict(price, code_hash, filename_hash)
		price_rows.append(price_row)

	return price_rows


def group_row_from_dict(group: dict) -> Row:

	group_row = {
		'npi_numbers': json.dumps(sorted(group['npi'])),
		'tin_type':    group['tin']['type'],
		'tin_value':   group['tin']['value'],
	}

	group_row = append_hash(group_row, 'provider_group_hash')

	return group_row


def group_rows_from_dicts(groups: list[dict]) -> list[Row]:

	provider_group_rows = []
	for provider_group in groups:
		provider_group_row = group_row_from_dict(provider_group)
		provider_group_rows.append(provider_group_row)

	return provider_group_rows


def prices_groups_rows_from_dicts(
	price_rows: list[Row],
	group_rows: list[Row]
) -> list[Row]:

	prices_groups_rows = []
	for price_row, group_row in product(price_rows, group_rows):

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
) -> None:

	code_row = code_row_from_dict(in_network_item)
	write_table(code_row, 'codes', out_dir)

	code_hash = code_row['code_hash']

	for rate in in_network_item['negotiated_rates']:
		prices = rate['negotiated_prices']
		groups = rate.get('provider_groups', [])

		price_rows = price_rows_from_dicts(prices, code_hash, filename_hash)
		write_table(price_rows, 'prices', out_dir)

		group_rows = group_rows_from_dicts(groups)
		write_table(group_rows, 'provider_groups', out_dir)

		prices_groups_rows = prices_groups_rows_from_dicts(price_rows, group_rows)
		write_table(prices_groups_rows, 'prices_provider_groups', out_dir)

	code_type = in_network_item['billing_code_type']
	code = in_network_item['billing_code']
	log.debug(f'Wrote {code_type} {code}')


def process_arr(func, arr, *args, **kwargs):
	processed_arr = []
	for item in arr:
		if processed_item := func(item, *args, **kwargs):
			processed_arr.append(processed_item)
	return processed_arr


def process_group(
	group: dict,
	npi_filter: set,
) -> dict | None:

	group['npi'] = [str(n) for n in group['npi']]
	if not npi_filter:
		return group

	group['npi'] = [n for n in group['npi'] if n in npi_filter]
	if group['npi']:
		return group
	return None

process_groups = partial(process_arr, process_group)


def process_reference(
	reference: dict,
	npi_filter: set,
) -> dict | None:

	groups = reference['provider_groups']
	groups = process_groups(groups, npi_filter)

	if groups:
		reference = {
			'provider_group_id' : reference['provider_group_id'],
			'provider_groups'   : groups
		}
		return reference
	return None


def process_in_network(
	in_network_items: Generator,
	npi_filter: set,
) -> Generator:
	for in_network_item in in_network_items:
		rates = in_network_item['negotiated_rates']
		rates = process_rates(rates, npi_filter)
		if rates:
			in_network_item['negotiated_rates'] = rates
			yield in_network_item


def process_rate(
	rate: dict,
	npi_filter: set,
) -> dict | None:

	if groups := rate.get('provider_groups'):
		rate['provider_groups'] = process_groups(groups, npi_filter)

	# if rate.get('provider_groups') or rate.get('provider_references'):
	if rate.get('provider_groups'):
		return rate
	return None

process_rates = partial(process_arr, process_rate)


# TODO simplify
def ffwd(
	parser: Generator,
	to_prefix: str | None = None,
	to_event:  str | None = None,
	to_value:  str | None = None,
) -> None:
	if to_value is None and to_prefix is not None and to_event is not None:
		for prefix, event, _ in parser:
			if prefix == to_prefix and event == to_event:
				break
		else: raise StopIteration
	elif to_prefix is None and to_event is not None and to_value is not None:
		for _, event, value in parser:
			if event == to_event and value == to_value:
				break
		else: raise StopIteration
	elif to_event is None and to_prefix is not None and to_value is not None:
		for prefix, _, value in parser:
			if prefix == to_prefix and value == to_value:
				break
		else: raise StopIteration
	else:
		raise NotImplementedError


def gen_in_network_items(
	parser: Generator,
	code_filter: set,
) -> Generator:
	builder = ijson.ObjectBuilder()
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


def gen_references(parser: Generator) -> Generator:

	builder = ijson.ObjectBuilder()
	# builder.event('start_array', None)

	for prefix, event, value in parser:
		builder.event(event, value)
		if (prefix, event) == ('provider_references.item', 'end_map'):
			reference = builder.value.pop()
			yield reference

		elif (prefix, event) == ('provider_references', 'end_array'):
			return


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
			ffwd(parser, to_prefix='in_network.item', to_event='end_map')
			builder.value.pop()
			builder.containers.pop()
			return

	arrangement = item.get('negotiation_arrangement')
	if arrangement and arrangement != 'ffs':
		log.debug(f"Skipping item: arrangement: {arrangement} not 'ffs'")
		ffwd(parser, to_prefix='in_network.item', to_event='end_map')
		builder.value.pop()
		builder.containers.pop()
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
	processed_references: list[dict],
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


# TODO simplify
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
	queue: asyncio.Queue = asyncio.Queue()

	# Tasks hold the consumers. reference is appended to
	# by consumers and by the main loop of this function
	tasks = []
	processed_references: list[dict] = []

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


async def _get_reference_map(parser, npi_filter) -> dict | None:
	"""Possible file structures.
	1. {    ...
		'provider_references': <-- here (most common)
		'in_network': ...
	}
	2. {    ...
		'in_network': ...
		'provider_references': <-- here (rarely)
	}
	3. {    ...
		'in_network': ...
	} (aka nowhere, semi-common)

	This function checks the MRF in order (1, 2, 3). First we look for
	case (1). If we don't find it, we try case (2), and if we hit a
	StopIteration, we know we have case (3).
	"""
	# Case (1)`
	next_, parser = peek(parser)
	if next_ == ('provider_references', 'start_array', None):
		references = gen_references(parser)
		return await make_reference_map(references, npi_filter)
	try:
		# Case (2)
		ffwd(parser, to_prefix='', to_value='provider_references')
	except StopIteration:
		# StopIteration -> they don't exist (3)
		return None
	else:
		# Collect them (ends on ('', 'end_map', None))
		references = gen_references(parser)
		return await make_reference_map(references, npi_filter)

def get_reference_map(parser, npi_filter):
	"""Wrapper to turn _get_reference_map into a sync function"""
	return asyncio.run(_get_reference_map(parser, npi_filter))


def swap_references(
	in_network_items: Generator,
	reference_map: dict,
) -> Generator:
	"""Takes the provider reference ID in the rate
	and replaces it with the corresponding provider
	groups from reference_map"""

	if not reference_map:
		yield from in_network_items
		return

	for item in in_network_items:
		rates = item['negotiated_rates']
		for rate in rates:
			references = rate.get('provider_references')
			if not references:
				continue
			groups = rate.get('provider_groups', [])
			for reference in references:
				addl_groups = reference_map.get(reference, [])
				groups.extend(addl_groups)
			rate.pop('provider_references')
			rate['provider_groups'] = groups

		item['negotiated_rates'] = [rate for rate in rates if rate.get('provider_groups')]

		if item['negotiated_rates']:
			yield item


def start_parser(filename) -> Generator:
	with JSONOpen(filename) as f:
		yield from ijson.parse(f, use_float = True)


def get_plan(parser) -> dict:
	builder = ijson.ObjectBuilder()
	for prefix, event, value in parser:
		builder.event(event, value)
		if value in ('provider_references', 'in_network'):
			return builder.value
	else:
		raise InvalidMRF


class Content:

	def __init__(self, file, code_filter, npi_filter):
		self.file        = file
		self.npi_filter  = npi_filter
		self.code_filter = code_filter

	def start_conn(self):
		self.parser  = start_parser(self.file)
		self.plan    = get_plan(self.parser)
		self.ref_map = get_reference_map(self.parser, self.npi_filter)

	def prepare_in_network(self):
		next_, parser = peek(self.parser)
		if next_ in [('', 'end_map', None), None]:
			self.parser = start_parser(self.file)
			ffwd(self.parser, to_prefix='', to_value='in_network')

	def in_network_items(self) -> Generator:
		self.prepare_in_network()
		filtered_items = gen_in_network_items(self.parser, self.code_filter)
		swapped_items  = swap_references(filtered_items, self.ref_map)
		return process_in_network(swapped_items, self.npi_filter)


def json_mrf_to_csv(
	url: str,
	out_dir: str,
	file:        str | None = None,
	code_filter: set | None = None,
	npi_filter:  set | None = None,
) -> None:
	"""
	Writes MRF content to a flat file CSV in a specific schema.
	The filename parameter is optional. If you only pass a URL we assume
	that it's a remote file. If you pass a filename, you must also pass a URL.

	As of 1/2/2023 the filename is extracted from the URL, so this
	isn't an optional parameter.
	"""

	if file is None:
		file = url

	# Explicitly make this variable up-front since both sets of tables
	# are linked by it (in_network and plan tables)
	filename_hash = filename_hasher(file)

	content = Content(file, code_filter, npi_filter)
	content.start_conn()
	plan = content.plan
	processed_items = content.in_network_items()

	make_dir(out_dir)

	for item in processed_items:
		write_in_network_item(item, filename_hash, out_dir)

	write_plan(plan, filename_hash, file, url, out_dir)