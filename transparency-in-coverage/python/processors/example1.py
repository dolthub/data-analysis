from mrfutils import import_csv_to_set, MRFWriter, MRFOpen, MRFFlattener, InvalidMRF
from schema import SCHEMA
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm
from pathlib import Path
import logging

log = logging.getLogger('mrfutils')

code_filter = import_csv_to_set('test/codes.csv')
npi_filter = import_csv_to_set('test/npis.csv')

p = Path(__file__).parent.absolute()

urls = [
    f'{p}/test/test_file_1.json',
    f'{p}/test/test_file_2.json', # provider references at end
    f'{p}/test/test_file_3.json.gz',
    f'{p}/test/test_file_4.json', # should fail
    f'{p}/test/test_file_5.json.gz',
]

for url in tqdm(urls):
    with logging_redirect_tqdm():
            with MRFOpen(url) as f:

                writer = MRFWriter(out_dir='simple', schema=SCHEMA)

                flattener = MRFFlattener(
                    loc=url,
                    code_filter=code_filter,
                    npi_filter=npi_filter
                )

                try:
                    flattener.make_first_pass(f, writer)

                except InvalidMRF as e:
                    log.critical(e)