#!/usr/bin/env python
import argparse
import logging
import sys
import time

import ijson
import requests

from mrfutils import MRFOpen


logging.basicConfig()
log = logging.getLogger('mrfutils')



def save_urls(input_list, save_file='in_network_urls.txt'):
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
    Gets in-network files from index.json files
    :param index_loc:
    :return: list of in-network file URL's
    """
    in_network_file_urls = []
    with MRFOpen(index_loc) as f:
        parser = ijson.parse(f, use_float=True)
        for prefix, event, value in parser:
            if (
                prefix.endswith('location')
                and event == 'string'
                and 'in-network' in value
            ):
                in_network_file_urls.append(value)

    print(f'Found: {len(in_network_file_urls)} in-network files:')
    print(in_network_file_urls)
    return in_network_file_urls


def main():
    parser = argparse.ArgumentParser('Fetch an index resource and extract in-network URLs')
    parser.add_argument('-u', '--url', help='URL pointing to a TOC index remote json file')
    parser.add_argument('-f', '--input-file', dest='input_file', 
                       help='File of Index URLs to process in bulk')
    # parser.add_argument('-o', '--out', dest='out_dir', default='out_dir')
    args = parser.parse_args()

    targets_list = []
    if args.input_file:
        with open(args.input_file, 'r') as f:
            targets_list = [line.strip() for line in f]
        log.info("Targets list built, {} URLs to process".format(len(targets_list)))
    elif args.url:
        # url = args.url
        targets_list.append(args.url)
    else:
        # print("[!] You must specify either a url or an input file")
        # parser.print_help()
        # sys.exit(1)
        log.info("Did not pass URL or input file, processing example data")
        idx_url = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'
        targets_list.append(idx_url)
    
    rates_urls = []
    for url in targets_list:
        rates_urls.extend(mrfs_from_idx(url))

    # Once finished, we have a big list of in-network rates URLs
    print(f"[*] Finished fetching, collected {len(rates_urls)} in-network URLs in total")
    log.info(f"[*] Finished fetching, collected {len(rates_urls)} in-network URLs in total")
    save_urls(rates_urls)
    # save_urls(rates_urls, save_file="urls_file.txt")
    return


if __name__ == '__main__':
    # print("[*] Running example routine to extract in-network files from a test index/TOC URL")
    main()
