from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger('core')
logger.setLevel(level=logging.DEBUG)

OUTPUT_DIR = 'flatten'

BILLING_CODE_LIST = read_billing_codes_from_csv('example_billing_codes.csv')

INDEX_FILE_URL = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_CLEVELAND-CLINIC-FLORIDA-GROUP-BENEFIT-PLAN_index.json'

my_urls = get_mrfs_from_index(INDEX_FILE_URL)

with logging_redirect_tqdm():
	for url in tqdm(my_urls):
		stream_json_to_csv(url, output_dir = OUTPUT_DIR, code_filter = BILLING_CODE_LIST)


