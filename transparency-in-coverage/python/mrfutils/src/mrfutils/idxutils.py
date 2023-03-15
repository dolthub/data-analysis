import ijson
import logging
from mrfutils.helpers import JSONOpen

log = logging.getLogger('mrfutils')
log.setLevel(logging.DEBUG)

def gen_in_network_links(index_loc,):
    """
    Gets in-network files from index.json files
    :param index.json URL:
    """
    with JSONOpen(index_loc) as f:
        count = 0
        parser = ijson.parse(f, use_float=True)
        for prefix, event, value in parser:
            if (
                prefix.endswith('location')
                and event == 'string'
                and 'in-network' in value
            ):
                log.debug(value)
                yield value
                count += 1

    log.debug(f'Found: {count} in-network files.')