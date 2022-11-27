from helpers import MRFItemBuilder, MRFWriter
from processors.helpers import open_mrf


def run(loc, npi_set, code_set, out_dir):

    with open_mrf(loc) as f:

        m = MRFItemBuilder(f)

        root_data, cur_row = m.build_root()
        writer = MRFWriter(root_data)

        if cur_row == ('', 'map_key', 'provider_references'):
            provider_references_map = m.build_provider_references(npi_set)

            m.ffwd(('', 'map_key', 'in_network'))
            for item in m.in_network_items(npi_set, code_set, provider_references_map):
                writer.write_in_network_item(item, out_dir)
            return

        elif cur_row == ('', 'map_key', 'in_network'):
            m.ffwd(('', 'map_key', 'provider_references'))
            provider_references_map = m.build_provider_references(npi_set)

    with open_mrf(loc) as f:

        m = MRFItemBuilder(f)

        m.ffwd(('', 'map_key', 'in_network'))
        for item in m.in_network_items(npi_set, code_set, provider_references_map):
            writer.write_in_network_item(item, out_dir)
