from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger('core')
logger.setLevel(level=logging.DEBUG)

OUTPUT_DIR = 'flatten'

BILLING_CODE_LIST = [
	'36415',
	'80053',
	'85025',
	'80061',
	'84443',
	'83036',
	'82306',
	'81001',
	'82570',
	'87086',
	'84439',
	'82607',
	'87088',
	'82043',
	'80048',
	'84153',
	'85027',
	'86003',
	'83540',
	'82728',
	'83550',
	'U0003',
	'83735',
	'85652',
	'87186',
	'86140',
	'82746',
	'84550',
	'87077',
	'U0005',
	'85610',
	'83970',
	'82784',
	'86235',
	'84156',
	'84481',
	'84100',
	'82550',
	'84436',
	'G0103',
	'84403',
	'82248',
	'80069',
	'86038',
	'86769',
	'83521',
	'84165',
	'83880',
	'81003',
	'80307',]

INDEX_FILE_URL = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_CLEVELAND-CLINIC-FLORIDA-GROUP-BENEFIT-PLAN_index.json'

my_urls = get_mrfs_from_index(INDEX_FILE_URL)

with logging_redirect_tqdm():
	for url in tqdm(my_urls):
		stream_json_to_csv(url, output_dir = OUTPUT_DIR, code_filter = BILLING_CODE_LIST)


