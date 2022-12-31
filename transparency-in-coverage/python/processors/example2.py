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

logging.basicConfig()
log = logging.getLogger("idxutils")
log.setLevel(logging.DEBUG)

# @rl1987
if len(sys.argv) >= 2:
    index_loc = sys.argv[1]
else:
    index_loc = (
        "https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json"
    )

filename = "extracted_links.txt"
if len(sys.argv) == 3:
    filename = sys.argv[2]

with open(filename, "a+") as f:
    for in_network_file in gen_in_network_links(index_loc):
        f.write(f"{in_network_file}\n")
        log.info(f"Wrote {in_network_file} to {filename}")
