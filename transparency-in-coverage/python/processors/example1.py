from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir, read_billing_codes_from_csv, read_npi_from_csv
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("core")
logger.setLevel(level=logging.WARNING)

OUTPUT_DIR = "flatten"

BILLING_CODE_LIST = read_billing_codes_from_csv("example_billing_codes.csv")
NPI_LIST = read_npi_from_csv("example_npi.csv")
print(NPI_LIST)

URLS = [
    # "https://raw.githubusercontent.com/CMSgov/price-transparency-guide/c3ba257f41f4b289b574557e2fcf0833c36ef79f/examples/in-network-rates/in-network-rates-bundle-single-plan-sample.json",
    # "https://raw.githubusercontent.com/CMSgov/price-transparency-guide/c3ba257f41f4b289b574557e2fcf0833c36ef79f/examples/in-network-rates/in-network-rates-capitation-single-plan-sample.json",
    # "https://raw.gprovider_references.item.provider_groups.itemithubusercontent.com/CMSgov/price-transparency-guide/c3ba257f41f4b289b574557e2fcf0833c36ef79f/examples/in-network-rates/in-network-rates-fee-for-service-single-plan-sample.json",
    # "https://raw.githubusercontent.com/CMSgov/price-transparency-guide/master/examples/in-network-rates/in-network-rates-multiple-plans-sample.json",
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates.json.gz",
]

create_output_dir(OUTPUT_DIR, overwrite=True)

with logging_redirect_tqdm():
    for url in tqdm(URLS):
        stream_json_to_csv(
            url, output_dir=OUTPUT_DIR, code_list=BILLING_CODE_LIST, npi_list=NPI_LIST
        )
