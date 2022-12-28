#!/usr/bin/env python

# Purpose:
#   - Based on example2.py
#   - Process URL *or* a file of URLs to extract in_network URLs from an index file
# 
#
import argparse
import logging
import sys
import time

import ijson
from tqdm import tqdm

from mrfutils import InvalidMRF, JSONOpen


logging.basicConfig()
log = logging.getLogger('mrfutils')
log.setLevel(logging.INFO)


def save_urls(input_list, save_file='in_network_urls_full.txt'):
    """ Input a list of URLs and save them to simple txt file. """
    if not input_list:
        return
    with open(save_file, 'w') as f:
        for line in input_list:
            f.write(f"{line.strip()}\n")
    log.info(f"in-network file successfully saved to: {save_file}")
    return

def mrfs_from_idx(index_loc):
    """
    A generator that yields in_network URL's for use in your code
    :param index_loc:   A local/remote path to MRF resource
    """
    with JSONOpen(index_loc) as f:
        for prefix, event, value in ijson.parse(f, use_float=True):
            if (
                prefix.endswith('location')
                and event == 'string'
                and 'in-network' in value
            ):
                yield value


def main():
    parser = argparse.ArgumentParser('Fetch an index resource and extract in-network URLs')
    parser.add_argument('-u', '--url', help='URL pointing to a TOC index remote json file')
    parser.add_argument('-f', '--input-file', dest='input_file', 
                       help='File of Index URLs to process in bulk')
    args = parser.parse_args()

    targets_list = []
    fobj = open('in_network_urls.txt', 'w')
    if args.input_file:
        with open(args.input_file, 'r') as f:
            targets_list = [line.strip() for line in f]
        log.info("Targets list built, {} URLs to process".format(len(targets_list)))
    elif args.url:
        # url = args.url
        targets_list.append(args.url)
    else:
        log.info("Did not pass URL or input file, processing example data")
        idx_url = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'
        # anthem index url: 
        # idx_url = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-12-01_anthem_index.json.gz'
        targets_list.append(idx_url)
    
    rates_urls = []
    for url in targets_list:
        for result in mrfs_from_idx(url):
            fobj.write(f"{result.strip()}\n")
    fobj.close()
    log.info(f"Finished fetching in-network URLs from target file")
    return


if __name__ == '__main__':
    main()
