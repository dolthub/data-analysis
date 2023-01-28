from mrfutils import json_mrf_to_csv
from helpers import import_csv_to_set
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--url")
parser.add_argument("-n", "--npi")
parser.add_argument("-o", "--out")
args = parser.parse_args()

npi_filter = import_csv_to_set(args.npi)

if args.out:
    out_dir = args.out
else:
    out_dir = "dialysis_csv_out"

# https://www.aapc.com/codes/cpt-codes-range/90935-90940/
dialysis_code_filter = {
    ("CPT", "90935"),
    ("CPT", "90937"),
    ("CPT", "90940"),
}

if __name__ == "__main__":
    try:
        json_mrf_to_csv(
            url=args.url,
            out_dir=out_dir,
            code_filter=dialysis_code_filter,
            npi_filter=npi_filter,
        )
    except Exception as e:
        print(e)
