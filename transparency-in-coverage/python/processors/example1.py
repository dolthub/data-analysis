from mrfutils import data_import, flatten_mrf, InvalidMRF
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm
from pathlib import Path
import logging

log = logging.getLogger('mrfutils')
logging.basicConfig(level = logging.DEBUG)

npi_set = {
    1508935891, 
    1356891600, 
    1111111111,
    1639520216,
    }

code_set = {
    ('CPT', '27447'), 
    ('MS-DRG', '0001'), 
    ('MS-DRG', '0687'), 
    ('HCPCS', 'U0005'), 
    ('MS-DRG', '945'),
    ('CPT', '00104'),
    }

p = Path(__file__).parent.absolute()

urls = [
    # 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-11-01/2022-11-01_ALL-SAVERS-INSURANCE-COMPANY_Insurer_PS1-50_C2_in-network-rates.json.gz',
    # 'http://www.google.com/',
    f'{p}/test/test_file_1.json',
    f'{p}/test/test_file_2.json',
    f'{p}/test/test_file_3.json.gz',
    f'{p}/test/test_file_4.json',
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



