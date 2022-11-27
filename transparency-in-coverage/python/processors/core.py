from mrfutils import MRFOpen, MRFObjectBuilder, MRFWriter

def run(loc, npi_set, code_set, out_dir):

    with MRFOpen(loc) as f:

        m = MRFObjectBuilder(f)

        root_data, cur_row = m.build_root()
        writer = MRFWriter(root_data)

        if cur_row == ('', 'map_key', 'provider_references'):
            p_ref_map = m.build_provider_references(npi_set)

            m.ffwd(('', 'map_key', 'in_network'))
            for item in m.in_network_items(npi_set, code_set, p_ref_map):
                writer.write_in_network_item(item, root_data, out_dir)
            return

        elif cur_row == ('', 'map_key', 'in_network'):
            m.ffwd(('', 'map_key', 'provider_references'))
            p_ref_map = m.build_provider_references(npi_set)

    with MRFOpen(loc) as f:

        m = MRFObjectBuilder(f)

        m.ffwd(('', 'map_key', 'in_network'))
        for item in m.in_network_items(npi_set, code_set, p_ref_map):
            writer.write_in_network_item(item, root_data, out_dir)