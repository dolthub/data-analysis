from mrfutils import data_import, flatten_mrf, InvalidMRF
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm
from pathlib import Path
import logging

log = logging.getLogger('mrfutils')

code_set = data_import('test/codes.csv')
npi_set = {int(x[0]) for x in data_import('test/npis.csv')}

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
        try:
            flatten_mrf(
                loc = url, 
                out_dir = 'debug', 
                code_set=code_set, 
                npi_set = npi_set
            )
        except InvalidMRF as e:
            log.critical(e)
            pass



