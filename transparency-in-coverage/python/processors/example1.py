from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger('core')
logger.setLevel(level=logging.DEBUG)

# FIRST EXAMPLE
OUTPUT_DIR = 'flatten'

create_output_dir(OUTPUT_DIR, overwrite = True)

urls = [
	'https://raw.githubusercontent.com/CMSgov/price-transparency-guide/c3ba257f41f4b289b574557e2fcf0833c36ef79f/examples/in-network-rates/in-network-rates-bundle-single-plan-sample.json',
	'https://raw.githubusercontent.com/CMSgov/price-transparency-guide/c3ba257f41f4b289b574557e2fcf0833c36ef79f/examples/in-network-rates/in-network-rates-capitation-single-plan-sample.json',
	'https://raw.githubusercontent.com/CMSgov/price-transparency-guide/c3ba257f41f4b289b574557e2fcf0833c36ef79f/examples/in-network-rates/in-network-rates-fee-for-service-single-plan-sample.json',
	'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates.json.gz',
]

with logging_redirect_tqdm():
	for url in tqdm(urls):
		stream_json_to_csv(url, output_dir = OUTPUT_DIR, code_filter = [])