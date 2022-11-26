from core import flatten_json
from helpers import data_import
from tqdm import tqdm

npi_set = set({1780763284, 1356891600})
code_set = {('MS-DRG', '0001'), ('MS-DRG', '0687')}

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
