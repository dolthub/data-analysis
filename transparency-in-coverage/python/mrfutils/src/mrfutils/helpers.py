import csv
import gzip
import hashlib
import json
import logging
import os
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse

import requests

from mrfutils.exceptions import InvalidMRF

log = logging.getLogger('mrfutils')
log.setLevel(logging.INFO)


def prepend(value, iterator):
	"""Prepend a single value in front of an iterator
	>>>  prepend(1, [2, 3, 4])
	>>>  1 2 3 4
	"""
	return chain([value], iterator)


def peek(iterator):
	"""
	Usage:
	>>> next_, iter = peek(iter)
	allows you to peek at the next value of the iterator
	"""
	try: next_ = next(iterator)
	except StopIteration: return None, iterator
	return next_, prepend(next_, iterator)


class JSONOpen:
	"""
	Context manager for opening JSON(.gz) MRFs.
	Usage:
	>>> with JSONOpen('localfile.json') as f:
	or
	>>> with JSONOpen(some_json_url) as f:
	including both zipped and unzipped files.
	"""

	def __init__(self, filename):
		self.filename = filename
		self.f = None
		self.r = None
		self.is_remote = None

		parsed_url = urlparse(self.filename)
		self.suffix = ''.join(Path(parsed_url.path).suffixes)
		if not self.suffix:
			self.suffix = ''.join(Path(parsed_url.query).suffixes)

		if not (
			self.suffix.endswith('.json.gz') or
			self.suffix.endswith('.json')
		):
			raise InvalidMRF(f'Suffix not JSON: {self.filename=} {self.suffix=}')

		self.is_remote = parsed_url.scheme in ('http', 'https')

	def __enter__(self):
		if (
			self.is_remote
			# endswith is used to protect against the case
			# where the filename contains lots of dots
			# insurer.stuff.json.gz
			and self.suffix.endswith('.json.gz')
		):
			self.s = requests.Session()
			self.r = self.s.get(self.filename, stream=True)
			self.f = gzip.GzipFile(fileobj=self.r.raw)

		elif (
			self.is_remote
			and self.suffix.endswith('.json')
		):
			self.s = requests.Session()
			self.r = self.s.get(self.filename, stream=True)
			self.r.raw.decode_content = True
			self.f = self.r.raw

		elif self.suffix == '.json.gz':
			self.f = gzip.open(self.filename, 'rb')

		else:
			self.f = open(self.filename, 'rb')

		log.info(f'Opened file: {self.filename}')
		return self.f

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.is_remote:
			self.s.close()
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
				item = row.pop()
				items.add(item)
		return items


def make_dir(out_dir):

	if not os.path.exists(out_dir):
		os.mkdir(out_dir)


def dicthasher(data: dict, n_bytes = 8) -> int:

	if not data:
		raise Exception("Hashed dictionary can't be empty")

	data = json.dumps(data, sort_keys=True).encode('utf-8')
	hash_s = hashlib.sha256(data).digest()[:n_bytes]
	hash_i = int.from_bytes(hash_s, 'little')

	return hash_i


def append_hash(item: dict, name: str) -> dict:

	hash_ = dicthasher(item)
	item[name] = hash_

	return item


def filename_hasher(filename: str) -> int:

	# retrieve/only/this_part_of_the_file.json(.gz)
	filename = Path(filename).stem.split('.')[0]
	file_row = {'filename': filename}
	filename_hash = dicthasher(file_row)

	return filename_hash


def validate_url(test_url: str) -> bool:
	# https://stackoverflow.com/a/38020041
	try:
		result = urlparse(test_url)
		return all([result.scheme, result.netloc])
	except:
		return False
