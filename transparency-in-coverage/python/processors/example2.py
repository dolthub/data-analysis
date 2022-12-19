import ijson
from mrfutils import MRFOpen

def mrfs_from_idx(index_loc):
    """
    Gets in-network files from index.json files
    :param idx_url:
    :return: list of in-network file URLS
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

    print(f'Found: {len(in_network_file_urls)} in-network files.')
    print(in_network_file_urls)
    return in_network_file_urls

index_loc = 'https://www.allegiancecosttransparency.com/2022-07-01_LOGAN_HEALTH_index.json'
mrfs_from_idx(index_loc)