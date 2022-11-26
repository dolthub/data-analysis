from core import flatten_json
from helpers import data_import
from tqdm import tqdm

npi_set = set({1508935891, 1356891600, 1111111111})
code_set = {('CPT', '27447'), ('MS-DRG', '0001'), ('MS-DRG', '0687'), ('HCPCS', 'U0005')}

urls = [
    'https://raw.githubusercontent.com/CMSgov/price-transparency-guide/master/examples/in-network-rates/in-network-rates-fee-for-service-single-plan-sample.json',
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
