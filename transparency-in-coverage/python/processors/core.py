import time
import requests
import logging
import ijson
import gzip
import sys
from urllib.parse import urlparse, urljoin
from helpers import parse_innetwork, parse_provrefs, \
					parse_root, flatten_obj, \
					hashdict, dict_to_csv, fetch_remoteprovrefs, \
					normalize_innetwork


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


def stream_json_to_csv(input_url, output_dir, code_filter = []):
	"""This streams through a JSON, flattens it, and writes it to 
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

	with requests.get(input_url, stream = True) as r:

		urlpath = urlparse(input_url).path
		url = urljoin(input_url, urlpath)
		LOG.info(f"Streaming file: {url}")

		url_size_mb = round(int(r.headers['Content-length'])/1_000_000, 3)
		LOG.info(f"Size of file: {url_size_mb} MB")

		if urlpath.endswith('.json.gz'):
			f = gzip.GzipFile(fileobj = r.raw)
		elif urlpath.endswith('.json'):
			f = r.content
		else:
			LOG.warning(f"Unknown extension. Stopping.")
			return
		
		parser = ijson.parse(f, use_float = True)

		## PARSE TOP MATTER
		root_data, row = parse_root(parser)
		LOG.info(f"Parsed root.")

		# PARSE PROVIDER REFS
		exist_provrefs = False
		LOG.info(f"Creating provider refs...")
		if row == ('provider_references', 'start_array', None):
			exist_provrefs = True
			provrefs, row = parse_provrefs(row, parser)
			provref_id_map = {r['provider_group_id']:i for i, r in enumerate(provrefs)}
			LOG.info(f"Parsed provider references.")

			provrefs_size = round(sys.getsizeof(provrefs)/1_000_000, 4)
			LOG.debug(f"Cached provider references. Size: {provrefs_size} MB.")

			provrefs = fetch_remoteprovrefs(provrefs)
			LOG.debug(f"Fetched remote provider references.")

		root_data['url'] = url
		root_data['root_hash_id'] = hashdict(root_data)

		hash_ids = {'root_hash_id': root_data['root_hash_id']}

		# PARSE IN-NETWORK OBJECTS ONE AT A TIME
		exist_codes = False
		provref_id_set = set()

		LOG.debug(f"Streaming in-network items (codes)...")
		while row != ('in_network', 'end_array', None):

			try:
				innetwork, row = parse_innetwork(row, parser, code_filter)
				innetwork = normalize_innetwork(innetwork, provrefs, provref_id_map)
			except ValueError as err:
				message, row = err.args
				LOG.debug(message)
				continue

			if not exist_provrefs:
				exist_codes = True
				flatten_obj(innetwork, output_dir, 'in_network', **hash_ids)
				continue


			for neg_rate in innetwork['negotiated_rates']:
				new_neg_rate = neg_rate.copy()
				for provref_id in neg_rate.get('provider_references', []):
					if provref_id in provref_id_set:
						continue

					provref_index = provref_id_map[provref_id]
					provref = provrefs[provref_index]
					provref_id_set.add(provref_id)
					flatten_obj(provref, output_dir, 'provider_references', **hash_ids)
					LOG.info(f"Wrote provider reference {provref_id} to file.")
			
			flatten_obj(innetwork, output_dir, 'in_network', **hash_ids)
			LOG.info(f"Wrote billing code {innetwork['billing_code']} to file.")
			exist_codes = True


		if not exist_codes:
			return

		dict_to_csv(root_data, output_dir, 'root')
		LOG.info(f"Wrote root to file.")

		td = time.time() - s
		LOG.info(f'Total time taken: {round(td/60, 3)} min.')