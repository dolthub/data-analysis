from mrfutils import data_import, flatten_mrf, InvalidMRF
import argparse

import logging
logging.basicConfig()  # Add logging level here if you plan on using logging.info() instead of my_logger as below.

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out')

args = parser.parse_args()

code_set = data_import('test/codes.csv')
npi_set = {int(x[0]) for x in data_import('test/npis.csv')}

try:
    flatten_mrf(
        loc=args.url,
        out_dir=args.out,
        code_set=code_set,
        npi_set=npi_set
    )
except InvalidMRF as e:
    log.critical(e)