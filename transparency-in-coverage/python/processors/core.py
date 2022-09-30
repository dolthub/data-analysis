import time
import requests
import logging
import ijson
import gzip
import sys
from helpers import parse_in_network, parse_provider_refs, \
					parse_top_matter, flatten_dict_to_file, \
					hashdict, write_dict_to_file
from urllib.parse import urlparse

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_mrfs_from_index(index_file_url):
	"""The in-network files are references from index.json files
	on the payor websites. This will stream one of those files
	"""
	s = time.time()
	in_network_file_urls = []

	with requests.get(index_file_url, stream = True) as r:
		LOG.info(f"Began streaming file: {index_file_url}")
		try:
			url_size = round(int(r.headers['Content-length'])/1_000_000, 3)
			LOG.info(f"Size of file: {url_size} MB")
		except KeyError:
			LOG.info(f"Size of index file unknown.")

		if urlparse(index_file_url)[2].endswith('.json.gz'):
			f = gzip.GzipFile(fileobj = r.raw)
			LOG.info(f"Unzipping streaming file.")
		elif urlparse(index_file_url)[2].endswith('.json'):
			f = r.content
		else:
			LOG.info(f"File does not have an extension. Aborting.")
			return

		parser = ijson.parse(f, use_float = True)

		for prefix, event, value in parser:
			if (prefix, event) == ('reporting_structure.item.in_network_files.item.location', 'string'):
				LOG.debug(f"Found in-network file: {value}")
				in_network_file_urls.append(value)

	td = time.time() - s
	LOG.info(f"Found: {len(in_network_file_urls)} in-network files.")
	LOG.info(f"Time taken: {round(td/60, 3)} min.")
	return in_network_file_urls


def parse_to_file(input_url, output_dir, billing_code_filter = []):
	"""This streams through a file, flattens it, and writes it to 
	file. It streams the zipped files, avoiding saving them to disk.

	MRFs are structured, schematically like
	{
		file_metadata (top matter),
		provider_references (always one line, if exists)
		[in_network_items] (multiple lines),
	}

	But the in_network_items are linked to provider references. The
	problem we have to solve is: how do we collect only the codes
	and provider references we want, while reading the file once?

	The answer is: cache the provider references during streaming,
	then filter the in_network_items. Once you know which provider
	references to keep, you can filter the cached object.

	The steps we take are:
	1. Check to see if there are matching codes. If so, write them
	2. Write the top matter to file
	3. Write the provider references to file
	"""
	s = time.time()

	parsed_url = urlparse(input_url)
	url = (parsed_url[1] + parsed_url[2]).strip()
	LOG.info(f"Streaming file: {url}")

	with requests.get(input_url, stream = True) as r:

		url_size = round(int(r.headers['Content-length'])/1_000_000, 3)
		LOG.info(f"Size of file: {url_size} MB")

		if parsed_url[2].endswith('.json.gz'):
			f = gzip.GzipFile(fileobj = r.raw)
		elif parsed_url[2].endswith('.json'):
			f = r.content
		else:
			LOG.info(f"File does not have an extension. Aborting.")
			return
		
		parser = ijson.parse(f, use_float = True)


		## PARSE TOP MATTER
		LOG.info(f"Streaming top matter...")
		root_data, (prefix, event, value) = parse_top_matter(parser)
		LOG.info(f"Successfully parsed top matter.")

		# PARSE PROVIDER REFS
		if (prefix, event) == ('provider_references', 'start_array'):
			LOG.info(f"Streaming provider references...")
			provider_refs_exist = True
			provider_references_list = []
			provider_references, (prefix, event, value) = parse_provider_refs((prefix, event, value), parser)
			p_ref_size = round(sys.getsizeof(provider_references)/1_000_000, 4)
			LOG.info(f"Cached provider references. Size: {p_ref_size} MB.")

		root_data['url'] = url
		root_data['root_hash_id'] = hashdict(root_data)

		hash_ids = {'root_hash_id': root_data['root_hash_id']}

		LOG.info(f"Streaming in-network items (codes)...")

		# PARSE IN-NETWORK OBJECTS ONE AT A TIME
		codes_exist = False
		while (prefix, event) != ('in_network', 'end_array'):

			in_network_item, (prefix, event, value) = parse_in_network((prefix, event, value), parser, billing_code_filter)

			if in_network_item:
				codes_exist = True
				LOG.debug(f"Billing code in BILLING_CODE_LIST found: {in_network_item['billing_code']}")
				flatten_dict_to_file(in_network_item, output_dir, prefix = 'in_network', **hash_ids)
				if provider_refs_exist:
					for negotiated_rate in in_network_item['negotiated_rates']:
						for provider_reference in negotiated_rate['provider_references']:
							provider_references_list.append(provider_reference)
			else:
				LOG.debug(f"Code found ({value}) but not in BILLING_CODE_LIST. Continuing...")

		if not codes_exist:
			return

		LOG.debug(f'Wrote all in-network items to file')

		write_dict_to_file(output_dir, 'root', root_data)
		LOG.info(f"Wrote top matter to file.")

		if provider_refs_exist: 
			for provider_reference in provider_references:
				if provider_reference['provider_group_id'] in provider_references_list:
					flatten_dict_to_file(provider_reference, output_dir, prefix = 'provider_references', **hash_ids)
			LOG.info(f"Wrote provider references to file.")

		td = time.time() - s
		LOG.info(f'Total time taken: {round(td/60, 3)} min.')