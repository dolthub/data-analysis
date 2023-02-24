"""
Example for how to use the functions in mrfutils.py

>>> python3 example_cli.py --url <your_url> --npis <csvfile> --codes <csvfile>

If you plan on importing files with separate URLs, you can feed the URL
to the URL parameter of `json_mrf_to_csv`. Just add an additional command
line argument to do that.
"""
import argparse
import logging

from helpers import import_csv_to_set, import_comb_to_dict
from mrfutils import json_mrf_to_csv

logging.basicConfig()
log = logging.getLogger('mrfutils')
log.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out-dir', default = 'csv_output')
parser.add_argument('-c', '--code-file')
parser.add_argument('-n', '--npi-file')
parser.add_argument('--comb-file')

args = parser.parse_args()

url = args.url
out_dir = args.out_dir

if args.code_file:
    code_filter = import_csv_to_set(args.code_file)
else:
    code_filter = None

if args.npi_file:
    npi_filter = import_csv_to_set(args.npi_file)
else:
    npi_filter = None

if args.comb_file:
    log.debug('Loading combination file')
    import os
    import pickle
    if os.path.exists('comb.pickle'):
        with open('comb.pickle', 'rb') as f:
            comb_filter = pickle.load(f)
    else:
        comb_filter = import_comb_to_dict(args.comb_file)
        with open('comb.pickle', 'wb') as f:
            pickle.dump(comb_filter, f)

    log.debug('Combination file loaded')
else:
    comb_filter = None

json_mrf_to_csv(
    file = args.file,
    url = args.url,
    npi_filter = npi_filter,
    code_filter = code_filter,
    comb_filter = comb_filter,
    out_dir = out_dir
)