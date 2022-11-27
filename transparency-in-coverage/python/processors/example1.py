from core import flatten_json
from helpers import data_import
from tqdm import tqdm
from pathlib import Path

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
    f'{p}/test/test_file_1.json',
    f'{p}/test/test_file_2.json',
    f'{p}/test/test_file_3.json.gz',
    f'{p}/test/test_file_4.json',
    f'{p}/test/test_file_5.json.gz',
]

for url in tqdm(urls):
    flatten_json(
        loc = url, 
        out_dir = 'debug', 
        code_set=code_set, 
        npi_set = npi_set
    )



