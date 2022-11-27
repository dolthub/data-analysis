from mrfutils import MRFOpen, MRFObjectBuilder, MRFWriter

def run(loc, npi_set, code_set, out_dir):

    with MRFOpen(loc) as f:

        m = MRFObjectBuilder(f)

        root_data, cur_row = m.build_root()
        writer = MRFWriter(out_dir)

        if cur_row == ('', 'map_key', 'provider_references'):
            p_ref_map = m.build_provider_references(npi_set)

            m.ffwd(('', 'map_key', 'in_network'))
            for item in m.innet_items(npi_set, code_set, root_data, p_ref_map):
                writer.write_innet(item)
            writer.write_root(root_data)
            return

        elif cur_row == ('', 'map_key', 'in_network'):
            m.ffwd(('', 'map_key', 'provider_references'))
            p_ref_map = m.build_provider_references(npi_set)

    with MRFOpen(loc) as f:

        m = MRFObjectBuilder(f)

        m.ffwd(('', 'map_key', 'in_network'))
        for item in m.innet_items(npi_set, code_set, root_data, p_ref_map):
            writer.write_innet(item)
        writer.write_root(root_data)