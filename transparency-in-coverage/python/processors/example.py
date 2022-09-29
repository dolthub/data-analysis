from core import parse_to_file, get_mrfs_from_index
from helpers import create_output_dir
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger('helpers')
logger.setLevel(level=logging.DEBUG)

BILLING_CODE_LIST = [
	'0001A',
	'0001F',
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

OUTPUT_DIR = 'flatten'
INDEX_FILE_URL = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_CLEVELAND-CLINIC-FLORIDA-GROUP-BENEFIT-PLAN_index.json'

my_urls = get_mrfs_from_index(INDEX_FILE_URL)

create_output_dir(OUTPUT_DIR, overwrite = True)

with logging_redirect_tqdm():
	for url in tqdm(my_urls):
		parse_to_file(url, output_dir = OUTPUT_DIR, billing_code_filter = BILLING_CODE_LIST)


