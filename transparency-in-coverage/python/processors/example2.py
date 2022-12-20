#!/usr/bin/env python
import argparse
import logging
import sys
import time

#!/usr/bin/env python
import argparse
import logging
import sys
import time

import ijson
"""
Examples:
    >>> python example2.py
    will run this script against a test index.json file.

    >>> python example2.py <index.json>
    will run this script against your choice of file.

    >>> python example2.py <index.json> <filename>
    will write the results to file.
"""

import logging
import sys
from idxutils import gen_in_network_links
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


logging.basicConfig()
log = logging.getLogger('idxutils')
log.setLevel(logging.DEBUG)

# @rl1987
if len(sys.argv) == 2:
    index_loc = sys.argv[1]
else:
    index_loc = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'

filename = 'extracted_links.csv'
with open(filename, 'a+') as f:
    for in_network_file in gen_in_network_links(index_loc):
        f.write(f'{in_network_file},\n')
        log.info(f'Wrote {in_network_file} to {filename}')