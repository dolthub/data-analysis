#!/usr/bin/python3

import ijson
import logging
from mrfutils import JSONOpen

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

def get_unique_in_network_urls(toc_url, limit=None):
    seen_urls = dict()

    for url in gen_in_network_links(toc_url):
        seen_urls[url] = True

        if limit is not None and len(seen_urls) >= limit:
            break

    return list(seen_urls.keys())
