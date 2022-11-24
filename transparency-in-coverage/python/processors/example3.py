from core import flatten_json
from helpers import import_set
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out')
args = parser.parse_args()

obgyn_npi_set = import_set('data/obgyn_npi.csv')
hospital_npi_set = import_set('data/hospital_npi.csv')
npi_set = obgyn_npi_set.union(hospital_npi_set)

# source: https://www.bcbsok.com/pdf/obstetrical_billing_multiple_birth.pdf
c_sections = [
    ('CPT', '59510'),
    ('CPT', '59514'),
    ('CPT', '59515'),
    ('CPT', '59618'),
    ('CPT', '59620'),
    ('CPT', '59622'),
    ('MS-DRG', '788'),
]

flatten_json(
    args.url, out_dir = args.out, code_set = c_sections, npi_set = npi_set
)
