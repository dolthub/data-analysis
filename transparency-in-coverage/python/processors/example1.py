from core import flatten_json
from helpers import data_import
from tqdm import tqdm


code_set = None
npi_set = None

# code_set = data_import("data/example_billing_codes.csv")
npi_set = set(int(n[0]) for n in data_import("data/example_npi.csv"))
npi_set.add(1043450828)
# code_set = {('MS-DRG', '0964'),('CPT', '27447')}

urls = [
    "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-10-01/2022-10-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Saint-Louis-University_GSP-901-MD47_in-network-rates.json.gz",
    "https://www.google.com/"
]

for url in tqdm(urls):
    flatten_json(
        loc = url, 
        out_dir = 'debug', 
        code_set=code_set, 
        npi_set = npi_set
    )
