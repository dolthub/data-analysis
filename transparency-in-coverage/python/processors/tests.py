import unittest
from pathlib import Path
from mrfutils import (MRFOpen,
                      MRFObjectBuilder,
                      hashdict,
                      _collect_remote_p_refs)


class TestMRFObjectBuilder(unittest.TestCase):

    npi_set = {'1111111111', '5555555555', '2020202020'}
    code_set = {('TS-TST', '0000')}
    loc = 'test/test.json'

    def test_remote_ref(self):
        with MRFOpen(self.loc) as f:
            m = MRFObjectBuilder(f)
            m.ffwd(('', 'map_key', 'provider_references'))
            _, remote_p_refs = m._prepare_provider_refs(self.npi_set)
            new_p_refs = _collect_remote_p_refs(remote_p_refs, self.npi_set)
            new_p_ref = new_p_refs[0]

        self.assertTrue(
            new_p_ref['provider_groups'][0]['npi'] == ['1111111111']
        )
        self.assertTrue(
            new_p_ref['provider_groups'][0]['tin']['value'] == '22-2222222'
        )

    def test_p_refs_map(self):
        with MRFOpen(self.loc) as f:
            m = MRFObjectBuilder(f)
            m.ffwd(('', 'map_key', 'provider_references'))
            p_refs_map = m.collect_p_refs(self.npi_set)

        self.assertTrue(
            p_refs_map[0][0]['npi'] == ['1111111111']
        )
        self.assertTrue(
            p_refs_map[1][0]['npi'] == ['2020202020', '1111111111']
        )

        # remote case
        self.assertTrue(p_refs_map[2][0]['npi'] == ['1111111111'])

    def test_npis(self):
        with MRFOpen(self.loc) as f:
            m = MRFObjectBuilder(f)
            root_data = m.collect_root()

            root_data['filename'] = Path(self.loc).stem.split('.')[0]
            root_hash_key = hashdict(root_data)
            root_data['root_hash_key'] = root_hash_key

            root_data['url'] = Path(self.loc).stem.split('.')[0]

            p_refs_map = m.collect_p_refs(self.npi_set)
            m.ffwd(('', 'map_key', 'in_network'))
            g = m.in_network_items(self.npi_set, self.code_set, p_refs_map)
            item_data = next(g)

        negotiated_rates = item_data['negotiated_rates']

        self.assertTrue(
            negotiated_rates[0]['provider_groups'][0]['npi'] == ['5555555555']
        )
        self.assertTrue(
            negotiated_rates[0]['provider_groups'][1]['npi'] == ['5555555555']
        )
        self.assertTrue(
            negotiated_rates[0]['provider_groups'][2]['npi'] == ['1111111111']
        )
        self.assertTrue(
            negotiated_rates[0]['provider_groups'][3]['npi'] == ['2020202020',
                                                                 '1111111111']
        )


if __name__ == '__main__':
    unittest.main()
