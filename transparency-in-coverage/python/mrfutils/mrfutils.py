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

* top-level information --> insurer_metadata
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
import itertools
from typing import Generator

import aiohttp
import ijson

from helpers import *
from schema.schema import SCHEMA

# You can remove this if necessary, but be warned
# Right now this only works with python 3.9/3.10
# install on Mac with
# brew install yajl
# or comment out this line!
assert ijson.backend in ('yajl2_c', 'yajl2_cffi')

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s')
log.setLevel(logging.DEBUG)

# To distinguish data from rows
Row = dict

# TODO handle npi_set and code_set in a custom data class

def extract_filename_from_url(url: str) -> str:
	return Path(url).stem.split('.')[0]

def write_table(
	row_data: list[Row] | Row,
	table_name: str,
	out_dir: str,
) -> None:

	fieldnames = SCHEMA[table_name]
	file_loc = f'{out_dir}/{table_name}.csv'
	file_exists = os.path.exists(file_loc)

	# newline = '' is to prevent Windows
	# from adding \r\n\n to the end of each line
	with open(file_loc, 'a', newline = '') as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)

		if not file_exists:
			writer.writeheader()

		if isinstance(row_data, list):
			writer.writerows(row_data)

		elif isinstance(row_data, dict):
			writer.writerow(row_data)


def file_row_from_mixed(
	plan_data: dict,
	url: str
) -> Row:

	filename = extract_filename_from_url(url)

	file_row = dict(
		filename = filename,
		last_updated_on = plan_data['last_updated_on']
	)

	file_row = append_hash(file_row, 'id')

	# URL is excluded from the hash
	file_row['url'] = url

	return file_row


def write_file(
	plan: dict,
	url: str,
	out_dir: str,
) -> None:

	file_row = file_row_from_mixed(plan, url)
	write_table(file_row, 'file', out_dir)


def insurer_row_from_dict(plan_data: dict) -> Row:

	keys = [
		'reporting_entity_name',
		'reporting_entity_type',
	]

	insurer_row = {key : plan_data[key] for key in keys}
	insurer_row = append_hash(insurer_row, 'id')

	return insurer_row


def write_insurer(
	plan_metadata: dict,
	out_dir: str,
) -> None:

	insurer_row = insurer_row_from_dict(plan_metadata)
	write_table(insurer_row, 'insurer', out_dir)


def code_row_from_dict(in_network_item: dict) -> Row:

	keys = [
		'billing_code_type',
		'billing_code_type_version',
		'billing_code',
	]

	# We use .get instead of [] because sometimes
	# the insurance companies use improperly formatted files!
	# ideally, because these fields aren't optional, we should do
	# in_network_item[key]
	code_row = {key : in_network_item.get(key) for key in keys}
	code_row = append_hash(code_row, 'id')

	return code_row


def price_metadata_price_tuple_from_dict(
	price_item: dict,
) -> tuple[Row, float] | None:

	keys = [
		'billing_class',
		'negotiated_type',
		'expiration_date',
	]

	price_metadata_row = {key: price_item[key] for key in keys}

	# Optional key
	price_metadata_row['additional_information'] = price_item.get('additional_information')

	optional_json_keys = [
		'service_code',
		'billing_code_modifier',
	]

	for key in optional_json_keys:
		if price_item.get(key):
			price_item[key] = [value for value in price_item[key] if value is not None]
			sorted_value = sorted(price_item[key])
			if not sorted_value:
				# [] should resove to None in the database
				price_metadata_row[key] = None
			else:
				price_metadata_row[key] = json.dumps(sorted_value)

	price_metadata_row = append_hash(price_metadata_row, 'id')
	negotiated_rate = price_item['negotiated_rate']

	return price_metadata_row, negotiated_rate


def price_metadata_combined_rows_from_dict(rate: dict) -> list[tuple[Row, float]]:

	price_metadata_combined_rows = []
	for price in rate['negotiated_prices']:
		price_metadata_combined_row = price_metadata_price_tuple_from_dict(price)
		price_metadata_combined_rows.append(price_metadata_combined_row)

	return price_metadata_combined_rows


def tin_rate_file_rows_from_mixed(
	tin_rows: list[Row],
	rate_rows: list[Row],
	file_id: str,
) -> list[Row]:

	rate_ids = [row['id'] for row in rate_rows]
	tin_ids = [row['id'] for row in tin_rows]

	tin_rate_file_rows = []

	for rate_id, tin_id in itertools.product(rate_ids, tin_ids):
		tin_rate_file_row = Row(
			tin_id = tin_id,
			rate_id = rate_id,
			file_id = file_id,
		)

		tin_rate_file_rows.append(tin_rate_file_row)

	return tin_rate_file_rows


def rate_rows_from_mixed(
	insurer_row: Row,
	code_row: Row,
	price_metadata_combined_rows: list[tuple[Row, float]],
) -> list[Row]:

	rate_rows = []

	for price_metadata_row, negotiated_rate in price_metadata_combined_rows:
		rate_row = Row(
			insurer_id = insurer_row['id'],
			code_id = code_row['id'],
			price_metadata_id = price_metadata_row['id'],
			negotiated_rate = negotiated_rate
		)

		rate_row = append_hash(rate_row, 'id')
		rate_rows.append(rate_row)

	return rate_rows


def tin_rows_and_npi_tin_rows_from_dict(
	groups: dict,
) -> tuple(list[Row], list[Row]):

	tin_rows = []
	npi_tin_rows = []

	for group in groups:
		tin_row = Row(
			tin_type = group['tin']['type'],
			tin_value = group['tin']['value']
		)
		tin_row = append_hash(tin_row, 'id')
		tin_rows.append(tin_row)

		for npi in group['npi']:
			npi_tin_row = Row(
				tin_id = tin_row['id'],
				npi = npi,
			)
			npi_tin_rows.append(npi_tin_row)

	return tin_rows, npi_tin_rows


def write_in_network_item(
	plan_metadata: dict,
	file_id: str,
	in_network_item: dict,
	out_dir
) -> None:

	code_row = code_row_from_dict(in_network_item)
	write_table(code_row, 'code', out_dir)

	insurer_row = insurer_row_from_dict(plan_metadata)

	for rate in in_network_item['negotiated_rates']:

		price_metadata_combined_rows = price_metadata_combined_rows_from_dict(rate)
		price_metadata_rows = [a[0] for a in price_metadata_combined_rows]
		write_table(price_metadata_rows, 'price_metadata', out_dir)

		rate_rows = rate_rows_from_mixed(
			insurer_row = insurer_row,
			code_row = code_row,
			price_metadata_combined_rows = price_metadata_combined_rows,
		)
		write_table(rate_rows, 'rate', out_dir)

		groups = rate['provider_groups']

		tin_rows, npi_tin_rows = tin_rows_and_npi_tin_rows_from_dict(groups)
		write_table(tin_rows, 'tin', out_dir)
		write_table(npi_tin_rows, 'npi_tin', out_dir)

		tin_rate_file_rows = tin_rate_file_rows_from_mixed(
			rate_rows = rate_rows,
			tin_rows = tin_rows,
			file_id = file_id
		)
		write_table(tin_rate_file_rows, 'tin_rate_file', out_dir)

	code_type = in_network_item['billing_code_type']
	code = in_network_item['billing_code']
	log.debug(f'Wrote {code_type} {code}')


def process_arr(func, arr, *args, **kwargs):
	processed_arr = []
	for item in arr:
		if processed_item := func(item, *args, **kwargs):
			processed_arr.append(processed_item)
	return processed_arr


def process_group(group: dict, npi_filter: set) -> dict | None:
	group['npi'] = [str(n) for n in group['npi']]
	if not npi_filter:
		return group

	group['npi'] = [n for n in group['npi'] if n in npi_filter]
	if not group['npi']:
		return

	return group


def process_groups(groups: list[dict], npi_filter: set) -> list[dict] | None:
	processed_arr = []
	for group in groups:
		if processed_item := process_group(group, npi_filter):
			processed_arr.append(processed_item)
	return processed_arr


def process_reference(reference: dict, npi_filter: set) -> dict | None:
	groups = process_groups(reference['provider_groups'], npi_filter)
	if groups:
		reference['provider_groups'] = groups
		return reference


def process_in_network(in_network_items: Generator, npi_filter: set) -> Generator:
	for in_network_item in in_network_items:
		rates = process_rates(in_network_item['negotiated_rates'], npi_filter)
		if rates:
			in_network_item['negotiated_rates'] = rates
			yield in_network_item


def process_rate(rate: dict, npi_filter: set) -> dict | None:
	# Will not work if references haven't been swapped out yet
	assert rate.get('provider_references') is None
	groups = process_groups(rate['provider_groups'], npi_filter)

	if not groups:
		return

	rate['provider_groups'] = groups

	prices = []
	for price in rate['negotiated_prices']:
		if price['negotiated_type'] in ('negotiated', 'fee schedule'):
			prices.append(price)

	if not prices:
		return

	rate['negotiated_prices'] = prices
	return rate


def process_rates(rates: list[dict], npi_filter: set) -> list[dict] | None:
	processed_arr = []
	for rate in rates:
		if processed_item := process_rate(rate, npi_filter):
			processed_arr.append(processed_item)
	return processed_arr


# TODO simplify
def ffwd(
	parser: Generator,
	to_prefix,
	to_event = None,
	to_value = None,
) -> None:
	# Must have at least one of these arguments selected
	assert not all(arg is None for arg in (to_event, to_value))

	if to_value is None:
		for prefix, event, _ in parser:
			if prefix == to_prefix and event == to_event:
				break
		else:
			raise StopIteration

	elif to_event is None:
		for prefix, _, value in parser:
			if prefix == to_prefix and value == to_value:
				break
		else:
			raise StopIteration
	else:
		raise NotImplementedError


def gen_in_network_items(
	parser: Generator,
	code_filter: set,
) -> Generator:
	builder = ijson.ObjectBuilder()
	for prefix, event, value in parser:

		if type(value) == str:
			value = value.strip()
			if value == '':
				value = None

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
			log.debug('Encountered bad response status')
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

	if reference_map is None:
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
		self.parser = start_parser(self.file)
		self.plan_metadata = get_plan(self.parser)
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
	assert url is not None

	if file is None:
		file = url

	# filename = extract_filename_from_url(url)

	# Explicitly make this variable up-front since both sets of tables
	# are linked by it (in_network and plan tables)

	content = Content(file, code_filter, npi_filter)
	content.start_conn()
	plan_metadata = content.plan_metadata

	file_row = file_row_from_mixed(plan_metadata, url)
	file_id  = file_row['id']

	make_dir(out_dir)

	processed_items = content.in_network_items()
	for item in processed_items:
		write_in_network_item(plan_metadata, file_id, item, out_dir)

	write_file(plan_metadata, url, out_dir)
	write_insurer(plan_metadata, out_dir)