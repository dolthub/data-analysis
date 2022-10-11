from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir, import_billing_codes, import_set
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("core")
logger.setLevel(level=logging.WARNING)

output_dir = "flatten"

code_list = import_billing_codes("data/example_billing_codes.csv")
npi_list = import_set("data/example_npi.csv")

urls = [
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates.json.gz",
]

create_output_dir(OUTPUT_DIR, overwrite=True)

with logging_redirect_tqdm():
    for url in tqdm(URLS):
        stream_json_to_csv(
            url, output_dir=OUTPUT_DIR, code_list=BILLING_CODE_LIST, npi_list=NPI_LIST
        )
