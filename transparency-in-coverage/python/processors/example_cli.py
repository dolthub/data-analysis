"""
Example for how to use the functions in mrfutils.py

>>> python3 example_cli.py --url <your_url> --npis <csvfile> --codes <csvfile>

If you plan on importing files with separate URLs, you can feed the URL
to the URL parameter of `json_mrf_to_csv`. Just add an additional command
line argument to do that.
"""
import argparse
import logging
from mrfutils import import_csv_to_set, json_mrf_to_csv

logging.basicConfig()
log = logging.getLogger('mrfutils')
log.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out', default = 'csv_output')
parser.add_argument('-c', '--codes')
parser.add_argument('-n', '--npis')

args = parser.parse_args()

url = args.url
out_dir = args.out

if args.codes:
    code_filter = import_csv_to_set(args.codes)
else:
    code_filter = None

if args.npis:
    npi_filter = import_csv_to_set(args.npis)
else:
    npi_filter = None

if args.file:
    file = args.file
else:
    file = None

json_mrf_to_csv(
    file = file,
    url = url,
    npi_filter = npi_filter,
    code_filter = code_filter,
    out_dir = out_dir
)