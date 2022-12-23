import argparse
import logging
from mrfutils import import_csv_to_set, flatten_mrf, InvalidMRF

logging.basicConfig()
log = logging.getLogger('mrfutils')
log.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out', default = 'out_dir')
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

try:
    flatten_mrf(
        loc = url,
        out_dir = out_dir,
        code_filter= code_filter,
        npi_filter= npi_filter,
        url = url # optional, see docstring
    )
except InvalidMRF as e:
    log.critical(e)