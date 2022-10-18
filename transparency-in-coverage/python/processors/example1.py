from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir, import_billing_codes, import_set
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("core")
logger.setLevel(level=logging.INFO)

output_dir = "flatten"

code_list = import_billing_codes("data/example_billing_codes.csv")
npi_list = import_set("data/example_npi.csv")

urls = [
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-10-01/2022-10-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Saint-Louis-University_GSP-901-MD47_in-network-rates.json.gz",
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates.json.gz",
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz",
]

create_output_dir(output_dir, overwrite=True)

with logging_redirect_tqdm():
    for url in tqdm(urls):
        stream_json_to_csv(url, output_dir=output_dir, code_list=code_list, npi_list=npi_list)
