#!/usr/bin/env python

# Purpose:
#   - Based on example2.py
#   - Process URL *or* a file of URLs
#   - 
#
#
#
#




import argparse
import logging
import sys
import time

import ijson
from tqdm import tqdm

from mrfutils import InvalidMRF, MRFOpen, MRFObjectBuilder


logging.basicConfig()
log = logging.getLogger('mrfutils')



def save_urls(input_list, save_file='in_network_urls_full.txt'):
    """ Input a list of URLs and save them to simple txt file. """
    if not input_list:
        return
    with open(save_file, 'w') as f:
        for line in input_list:
            f.write(f"{line.strip()}\n")
    log.info(f"in-network file successfully saved to: {save_file}")
    return


# def yield_records(jsonobj, query=None):
#     if not query:
#         query = "reporting_structure.item.in_network_files"
#     for item in ijson.items(jsonobj, query, use_float=True):
#         yield item


def mrfs_from_idx(index_loc):
    """
    A generator that yields in_network URL's for use in your code
    :param index_loc:   A local/remote path to MRF resource
    """
    with MRFOpen(index_loc) as f:
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
    # parser.add_argument('-o', '--out', dest='out_dir', default='out_dir')
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
        # print("[!] You must specify either a url or an input file")
        # parser.print_help()
        # sys.exit(1)
        log.info("Did not pass URL or input file, processing example data")
        #idx_url = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'
        # anthem index url: 
        idx_url = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-12-01_anthem_index.json.gz'
        targets_list.append(idx_url)
    
    rates_urls = []
    for url in targets_list:
        for result in mrfs_from_idx(url):
            #rates_urls.append(result)
            fobj.write(f"{result.strip()}\n")
    fobj.close()
    # Once finished, we have a big list of in-network rates URLs
    #print(f"[*] Finished fetching, collected {len(rates_urls)} in-network URLs in total")
    #log.info(f"Finished fetching, collected {len(rates_urls)} in-network URLs in total")
    log.info(f"Finished fetching in-network URLs from target file")
    #save_urls(rates_urls)
    # save_urls(rates_urls, save_file="urls_file.txt")
    return


if __name__ == '__main__':
    # print("[*] Running example routine to extract in-network files from a test index/TOC URL")
    main()
