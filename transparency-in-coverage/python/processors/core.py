from helpers import MRFFlattener, MRFOpen, MRFWriter, InvalidMRF

def flatten_json(loc, out_dir, code_set = None, npi_set = None):
    """
    Pattern for flattening JSON and filtering with a code_set/npi_set.
    Functions like a finite state machine.
    """

    with MRFOpen(loc) as f:

        flattener = MRFFlattener(code_set, npi_set)
        flattener.init_parser(f)

        # Build (but don't write) the root data
        root_data = flattener.build_root()

        # Jump (fast-forward) to provider references
        # Build (but don't write) the provider references
        flattener.build_provider_references()

        """
        Sometimes it happens that the MRF is out of order: 
        the provider references are at the bottom. Fast-forwarding
        to the in-network items will result in a StopIteration.

        In this case, re-open the file (start a new parser in a
        new context) and then FFWD to the in-network items,
        while holding onto the previously computed provider references
        and root data.
        """

        writer = MRFWriter(root_data)

        # Provider references above in-network items
        if flattener.provider_refs_at_top:
            for item in flattener.in_network_items():
                writer.write_in_network_item(item, out_dir)
            return

    # Close and re-open for when provider references are
    # below the in-network items. If there are still no 
    # in-network items, the file is marked as invalid
    with MRFOpen(loc) as f:

        flattener.init_parser(f)

        try:
            for item in flattener.in_network_items():
                writer.write_in_network_item(item, out_dir)
        except StopIteration:
            raise InvalidMRF
