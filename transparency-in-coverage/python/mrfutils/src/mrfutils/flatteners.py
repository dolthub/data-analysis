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

Anything tagged #HOTFIX is a quick fix for a broken implementation of an MRF
"""
from __future__ import annotations

import asyncio
import itertools
from typing import Generator

import aiohttp
import ijson

from mrfutils.helpers import *
from mrfutils.schema.schema import SCHEMA

# You can remove this if necessary, but be warned
# Right now this only works with python 3.9/3.10
# install on Mac with
# brew install yajl
# or comment out this line!
# assert ijson.backend in ('yajl2_c', 'yajl2_cffi')

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


def file_row_from_url(
	url: str
) -> Row:

	filename = extract_filename_from_url(url)

	file_row = dict(
		filename = filename,
	)

	file_row = append_hash(file_row, 'id')
	file_row['url'] = url

	return file_row


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


def rate_metadata_rate_tuple_from_dict(
	rate_item: dict,
) -> tuple[Row, float] | None:

	keys = [
		'billing_class',
		'negotiated_type',
		'expiration_date',
	]

	rate_metadata_row = {key: rate_item[key] for key in keys}

	# Optional key
	rate_metadata_row['additional_information'] = rate_item.get('additional_information')

	optional_json_keys = [
		'service_code',
		'billing_code_modifier',
	]

	for key in optional_json_keys:
		if rate_item.get(key):
			rate_item[key] = [value for value in rate_item[key] if value is not None]
			sorted_value = sorted(rate_item[key])
			if not sorted_value:
				# [] should resove to None in the database
				rate_metadata_row[key] = None
			else:
				rate_metadata_row[key] = json.dumps(sorted_value)

	rate_metadata_row = append_hash(rate_metadata_row, 'id')
	negotiated_rate = rate_item['negotiated_rate']

	return rate_metadata_row, negotiated_rate


def rate_metadata_combined_rows_from_dict(rate: dict) -> list[tuple[Row, float]]:

	rate_metadata_combined_rows = []
	for price in rate['negotiated_prices']:
		rate_metadata_combined_row = rate_metadata_rate_tuple_from_dict(price)
		rate_metadata_combined_rows.append(rate_metadata_combined_row)

	return rate_metadata_combined_rows


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
	code_row: Row,
	rate_metadata_combined_rows: list[tuple[Row, float]],
) -> list[Row]:

	rate_rows = []

	for rate_metadata_row, negotiated_rate in rate_metadata_combined_rows:
		rate_row = Row(
			code_id = code_row['id'],
			rate_metadata_id = rate_metadata_row['id'],
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
	file_id: str,
	in_network_item: dict,
	out_dir
) -> None:

	code_row = code_row_from_dict(in_network_item)
	write_table(code_row, 'code', out_dir)

	for rate in in_network_item['negotiated_rates']:

		rate_metadata_combined_rows = rate_metadata_combined_rows_from_dict(rate)
		rate_metadata_rows = [a[0] for a in rate_metadata_combined_rows]
		write_table(rate_metadata_rows, 'rate_metadata', out_dir)

		rate_rows = rate_rows_from_mixed(
			code_row = code_row,
			rate_metadata_combined_rows = rate_metadata_combined_rows,
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

# experimental mod
from array import array
def process_group(group: dict, npi_filter: set) -> dict | None:
	try:
		group['npi'] = [int(n) for n in group['npi']]
	except KeyError:
		# I was alerted that sometimes this key is capitalized
		# HOTFIX
		group['npi'] = [int(n) for n in group['NPI']]

	group['npi'] = array('L', group['npi'])

	if not npi_filter:
		return group

	group['npi'] = [n for n in group['npi'] if n in npi_filter]

	if not group['npi']:
		return

	group['npi'] = array('L', group['npi'])

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


async def _get_reference_map(parser, npi_filter) -> dict:
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
	# Case (1)
	next_, parser = peek(parser)
	if next_ == ('provider_references', 'start_array', None):
		references = gen_references(parser)
		return await make_reference_map(references, npi_filter)
	try:
		# Case (2)
		ffwd(parser, to_prefix='', to_value='provider_references')
	except StopIteration:
		# StopIteration -> they don't exist (3)
		return {}
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


def in_network_file_to_csv(
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
	if npi_filter:
		log.debug('Converting npi_filter to ints from strings')
		npi_filter = set(int(n) for n in list(npi_filter))

	assert url is not None
	assert validate_url(url)
	make_dir(out_dir)

	if file is None: file = url

	completed = False
	ref_map = None

	metadata = ijson.ObjectBuilder()
	parser = start_parser(file)

	file_row = file_row_from_url(url)
	file_row['url'] = url
	file_id = file_row['id']

	while True:
		# This loop runs as long as there's a parser.
		# We don't use
		# >>> While parser
		# since we occasionally create a new parser instance
		# when the file is out of order.

		# There are basically three cases we need to consider:
		# 1. We hit the provider_references
		# 2. We hit the in_network items
		# 3. Everything else
		try:
			prefix, event, value = next(parser)
		except StopIteration:
			if completed: break
			if ref_map is None: ref_map = {}
			parser = start_parser(file)
			ffwd(parser, to_prefix='', to_value='in_network')
			prefix, event, value = ('', 'map_key', 'in_network')
			prepend(('', 'map_key', 'in_network'), parser)

		if value == 'provider_references':
			ref_map = get_reference_map(parser, npi_filter)

		# There are four things that need to come before in_network
		# 1. reporting_entity_name
		# 2. reporting_entity_type
		# 3. provider_references
		# 4. last_updated_on
		elif value == 'in_network':
			if ref_map is None:
				ffwd(parser, to_prefix = 'in_network', to_event = 'end_array')
				continue

			filtered_items = gen_in_network_items(parser, code_filter)
			swapped_items = swap_references(filtered_items, ref_map)

			for item in process_in_network(swapped_items, npi_filter):
				write_in_network_item(file_id, item, out_dir)

			completed = True

		elif not completed:
			metadata.event(event, value)

	file_row.update(metadata.value)
	write_table(file_row, 'file', out_dir)

### TOOLS FOR PROCESSING INDEX FILES

def gen_plan_file(parser):
	plan_file = ijson.ObjectBuilder()
	for prefix, event, value in parser:
		plan_file.event(event, value)

		if (prefix, event) == ('reporting_structure.item', 'end_map'):
			yield plan_file.value
			plan_file = ijson.ObjectBuilder()

		elif (prefix, event, value) == ('reporting_structure', 'end_array', None):
			return

def write_plan_file(plan_file, toc_id, out_dir):

	if not plan_file.get('in_network_files'):
		return

	toc_plan_file_link = dicthasher(plan_file)
	plan_rows = []
	file_rows = []

	for plan in plan_file['reporting_plans']:

		plan_row = append_hash(plan, 'id')
		plan_row['toc_id'] = toc_id

		write_table(plan_row, 'toc_plan', out_dir)
		plan_rows.append(plan_row)

	for file in plan_file['in_network_files']:

		url = file['location']
		file_row = dict(
			filename = extract_filename_from_url(url)
		)
		file_row = append_hash(file_row, 'id')

		file_row['toc_id'] = toc_id
		file_row['url'] = url
		file_row['description'] = file['description']

		write_table(file_row, 'toc_file', out_dir)
		file_rows.append(file_row)

	print(len(plan_rows))
	print(len(file_rows))

	for plan_row in plan_rows:
		for file_row in file_rows:
			toc_plan_file_row = dict(
				link = toc_plan_file_link,
				toc_file_id = file_row['id'],
				toc_plan_id = plan_row['id'],
			)
			write_table(toc_plan_file_row, 'toc_plan_file', out_dir)

def toc_file_to_csv(
	url: str,
	out_dir: str,
	file:        str | None = None,
) -> None:
	assert url is not None
	assert validate_url(url)
	make_dir(out_dir)

	if file is None:
		file = url

	with JSONOpen(file) as f:

		parser = ijson.parse(f)
		toc_row = dict(
			filename = extract_filename_from_url(url)
		)
		toc_row = append_hash(toc_row, 'id')
		toc_row['url'] = url
		toc_id = toc_row['id']
		metadata = ijson.ObjectBuilder()
		for prefix, event, value in parser:
			if (prefix, event, value) == ('reporting_structure', 'start_array', None):
				for plan_file in gen_plan_file(parser):
					write_plan_file(plan_file, toc_id, out_dir)
			else:
				metadata.event(event, value)
	toc_row.update(metadata.value)
	write_table(toc_row, 'toc', out_dir)
