from helpers import BlockFlattener, MRFOpen

def flatten_json(loc, out_dir, code_set = None, npi_set = None):

    with MRFOpen(loc) as f:

        flattener = BlockFlattener(code_set, npi_set)
        flattener.init_parser(f)

        flattener.build_root()

        flattener.ffwd(('', 'map_key', 'provider_references'))
        flattener.build_provider_references()
        flattener.gather_remote_provider_references()

        """
        Sometimes it happens that the MRF is out of order: 
        the provider references are at the bottom. Fast-forwarding
        to the in-network items will result in a StopIteration.

        In this case, re-open the file (start a new parser in a
        new context) and then FFWD to the in-network items,
        while holding onto the previously computed provider references
        and root data.
        """

        try:
            flattener.ffwd(('in_network', 'start_array', None))
            while flattener.current_row != ('in_network', 'end_array', None):
                flattener.build_next_in_network_item()
                flattener.write_in_network_item(out_dir)

            return
        except StopIteration:
            pass

    with MRFOpen(loc) as f:
        flattener.init_parser(f)

        flattener.ffwd(('in_network', 'start_array', None))
        while flattener.current_row != ('in_network', 'end_array', None):
            flattener.build_next_in_network_item()
            flattener.write_in_network_item(out_dir)

        return