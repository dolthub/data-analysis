from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("core")
logger.setLevel(level=logging.DEBUG)

output_dir = "flatten"

code_list = import_billing_codes("data/example_billing_codes.csv")
index_file_url = "https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_CLEVELAND-CLINIC-FLORIDA-GROUP-BENEFIT-PLAN_index.json"

urls = get_mrfs_from_index(index_file_url)

with logging_redirect_tqdm():
    for url in tqdm(urls):
        stream_json_to_csv(url, output_dir=output_dir, code_list=code_list)
