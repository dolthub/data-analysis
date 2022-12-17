from core import stream_json_to_csv
from helpers import create_output_dir, import_set
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--url")
parser.add_argument("-o", "--out")
parser.add_argument("-n", "--npi")
args = parser.parse_args()

logger = logging.getLogger("core")
logger.setLevel(level=logging.DEBUG)

output_dir = args.out
npi_set = import_set(args.npi)

# https://www.aapc.com/codes/cpt-codes-range/90935-90940/
dialysis = [
    ("CPT", "90935"),
    ("CPT", "90937"),
    ("CPT", "90940"),
    ]

create_output_dir(output_dir, overwrite=False)

try:
    stream_json_to_csv(
        args.url, output_dir=output_dir, code_list=dialysis, npi_list=npi_set
    )
except Exception as e:
    logger.warn(f'Failed for {args.url}')
    logger.warn(e)