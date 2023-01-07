#!/usr/bin/python3

from mrfutils import import_csv_to_set, json_mrf_to_csv
from exceptions import InvalidMRF
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm
from pathlib import Path
import logging

def main():
    log = logging.getLogger('mrfutils')

    code_filter = import_csv_to_set('../quest/codes.csv')
    npi_filter = import_csv_to_set('../quest/npis.csv')

    p = Path(__file__).parent.absolute()

    urls = [
        f'{p}/../test/test_file_1.json',
        f'{p}/../test/test_file_2.json', # provider references at end
        f'{p}/../test/test_file_3.json.gz',
        f'{p}/../test/test_file_4.json', # should fail
        # f'{p}/test/test_file_5.json.gz',
    ]

    for url in tqdm(urls):
        with logging_redirect_tqdm():
            try:
                json_mrf_to_csv(
                    url = url,
                    npi_filter = npi_filter,
                    code_filter = code_filter,
                    out_dir ='../example_1')
            except InvalidMRF:
                log.warning('Not a valid MRF.')
            except Exception as e:
                log.error(e)

if __name__ == "__main__":
    main()

