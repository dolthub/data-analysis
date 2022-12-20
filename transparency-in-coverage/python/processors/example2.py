import ijson
import logging
from mrfutils import MRFOpen

logging.basicConfig()
log = logging.getLogger('mrfutils')
log.setLevel(logging.DEBUG)

def index_in_network_files_gen(
        index_loc,):
    """
    Gets in-network files from index.json files
    :param index.json URL:
    :param write_to_file: bool
    :param filename: str
    :return: list of in-network file URLS
    """
    with MRFOpen(index_loc) as f:
        num_found = 0
        parser = ijson.parse(f, use_float=True)
        for prefix, event, value in parser:
            if (
                prefix.endswith('location')
                and event == 'string'
                and 'in-network' in value
            ):
                yield value
                num_found += 1

    print(f'Found: {num_found} in-network files.')

index_loc = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'
filename = 'extracted_links.csv'
with open(filename, 'a+') as f:
    for in_network_file in index_in_network_files_gen(index_loc):
        f.write(f'{in_network_file},\n')
        log.info(f'Wrote {in_network_file} to {filename}')