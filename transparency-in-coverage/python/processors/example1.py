from core import flatten_json
from helpers import import_billing_codes, import_set
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("core")
logger.setLevel(level=logging.INFO)


code_set = import_billing_codes("data/example_billing_codes.csv")
npi_set = import_set("data/example_npi.csv")

urls = [
    # '/Users/alecstein/Downloads/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates 2.txt',
    '/Users/alecstein/Downloads/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates 2.json',
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-10-01/2022-10-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Saint-Louis-University_GSP-901-MD47_in-network-rates.json.gz",
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates.json.gz",
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz",
    "https://www.google.com/"
]

with logging_redirect_tqdm():
    for url in tqdm(urls):
        # try:
            flatten_json(loc = url, out_dir = 'debug', code_set=code_set, npi_set = npi_set)
        # except Exception as e:
            # print(e)
