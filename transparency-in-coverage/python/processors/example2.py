#!/usr/bin/python3

import requests
import time
import ijson
from urllib.parse import urlparse
import sys

from mrfutils import MRFOpen

def mrfs_from_idx(idx_url):
    '''
    Gets in-network files from index.json files
    '''
    s = time.time()
    in_network_file_urls = []

    with MRFOpen(idx_url) as f:

        parser = ijson.parse(f, use_float=True)

        for prefix, event, value in parser:

            if (
                prefix.endswith('location')
                and event == 'string'
            ):
                print(f'Found in-network file: {value}')
                in_network_file_urls.append(value)

    td = time.time() - s

    print(f'Found: {len(in_network_file_urls)} in-network files.')
    print(f'Time taken: {round(td/60, 3)} min.')

    return in_network_file_urls

def main():
    if len(sys.argv) == 2:
        idx_url = sys.argv[1]
    else:
        idx_url = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'

    mrfs_from_idx(idx_url)

if __name__ == "__main__":
    main()
