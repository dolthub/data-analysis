import ijson
import logging
from idxutils import gen_in_network_links
from mrfutils import MRFOpen


logging.basicConfig()
log = logging.getLogger('idxutils')
log.setLevel(logging.DEBUG)

index_loc = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'
filename = 'extracted_links.csv'
with open(filename, 'a+') as f:
    for in_network_file in gen_in_network_links(index_loc):
        f.write(f'{in_network_file},\n')
        log.info(f'Wrote {in_network_file} to {filename}')