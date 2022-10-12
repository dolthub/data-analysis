from core import stream_json_to_csv
from helpers import create_output_dir, import_set
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--url")
parser.add_argument("-o", "--out")
args = parser.parse_args()

logger = logging.getLogger("core")
logger.setLevel(level=logging.DEBUG)

output_dir = args.out

obgyn_npi_set = import_set("data/obgyn_npi.csv")
hospital_npi_set = import_set("data/hospital_npi.csv")
npi_set = obgyn_npi_set.union(hospital_npi_set)

# souce: https://www.bcbsok.com/pdf/obstetrical_billing_multiple_birth.pdf
c_sections = [
    ("CPT", "59510"),
    ("CPT", "59514"),
    ("CPT", "59515"),
    ("CPT", "59618"),
    ("CPT", "59620"),
    ("CPT", "59622"),
]

create_output_dir(output_dir, overwrite=False)
stream_json_to_csv(
    args.url, output_dir=output_dir, code_list=c_sections, npi_list=npi_set
)
