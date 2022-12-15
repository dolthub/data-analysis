from mrfutils import data_import, flatten_mrf, InvalidMRF
import argparse

import logging
logging.basicConfig()

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out')
parser.add_argument('-c', '--codes')
parser.add_argument('-n', '--npis')

args = parser.parse_args()

url = args.url
out_dir = args.out
code_set = data_import(args.codes)
npi_set = {int(x[0]) for x in data_import(args.npis)}

try:
    flatten_mrf(
        loc = url,
        out_dir = out_dir,
        code_set = code_set,
        npi_set = npi_set
    )
except InvalidMRF as e:
    log.critical(e)