#!/usr/bin/python3

from mrfutils import import_csv_to_set, json_mrf_to_csv
from exceptions import InvalidMRF
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm
import logging

def main():
    log = logging.getLogger('mrfutils')

    code_filter = import_csv_to_set('../quest/codes.csv')
    npi_filter = import_csv_to_set('../quest/npis.csv')

    in_f = open("urls.txt", "r")
    urls = in_f.read().strip().split("\n")
    in_f.close()

    for url in tqdm(urls):
        with logging_redirect_tqdm():
            tries_left = 3
            while tries_left > 0:
                try:
                    json_mrf_to_csv(
                        url = url,
                        npi_filter = npi_filter,
                        code_filter = code_filter,
                        out_dir ='../example_1')
                except InvalidMRF:
                    log.warning('Not a valid MRF.')
                    break
                except Exception as e:
                    log.error(e)
                    tries_left -= 1

if __name__ == "__main__":
    main()

