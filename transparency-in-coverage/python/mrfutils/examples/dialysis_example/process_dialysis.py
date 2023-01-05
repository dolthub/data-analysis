# from core import stream_json_to_csv
# from helpers import create_output_dir, import_set
from mrfutils import json_mrf_to_csv
from helpers import import_csv_to_set
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--url")
parser.add_argument("-n", "--npi")
args = parser.parse_args()

logger = logging.getLogger("mrfutils")
logger.setLevel(level=logging.DEBUG)


file_handler = logging.FileHandler('dialysis.log', 'a')
file_handler.setLevel(logging.WARNING)

logger.addHandler(file_handler)

npi_filter = import_csv_to_set(args.npi)


# https://www.aapc.com/codes/cpt-codes-range/90935-90940/
dialysis_code_filter = {
    ("CPT", "90935"),
    ("CPT", "90937"),
    ("CPT", "90940"),
}

if __name__ == "__main__":
    try:
        json_mrf_to_csv(
            url = args.url,
            out_dir ='dialysis_csv_out',
            code_filter = dialysis_code_filter,
            npi_filter = npi_filter
        )
    except Exception as e:
        logger.warning(f'Failed for {args.url}')
        logger.warning(e)