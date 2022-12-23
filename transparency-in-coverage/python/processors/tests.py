import unittest
import aiohttp
from pathlib import Path
from mrfutils import (
	MRFOpen,
	MRFProcessor,
	dicthasher,
	_fetch_remote_provider_reference
)

class TestSingleFetch(unittest.IsolatedAsyncioTestCase):
	loc = "https://raw.githubusercontent.com/CMSgov/price-transparency-guide/master/examples/provider-reference/provider-reference.json"
	p_ref_id = 2
	async def test_remote_ref(self):
		async with aiohttp.client.ClientSession() as session:
			data = await _fetch_remote_provider_reference(
				session,
				self.p_ref_id,
				self.loc,
			)

		group = data['provider_groups'][0]
		self.assertTrue(group['npi'] == ['1111111111'])
		self.assertTrue(group['tin']['value'] == '22-2222222')

class TestSingleFetch(unittest.IsolatedAsyncioTestCase):
	loc = "https://raw.githubusercontent.com/CMSgov/price-transparency-guide/master/examples/provider-reference/provider-reference.json"
	p_ref_id = 2
	npi_filter = {'6383593489'}
	async def test_remote_ref(self):
		async with aiohttp.client.ClientSession() as session:
			data = await _fetch_remote_provider_reference(
				session,
				self.p_ref_id,
				self.loc,
				self.npi_filter,
			)

		self.assertTrue(data == None)

# class TestTimeout(unittest.IsolatedAsyncioTestCase):
# 	loc = "http://www.google.com:81/"
# 	p_ref_id = 2
# 	async def test_remote_ref(self):
# 		async with aiohttp.client.ClientSession() as session:
# 			data = await fetch_remote_provider_reference(
# 				session,
# 				self.p_ref_id,
# 				self.loc,
# 			)
#
# 		group = data['provider_groups'][0]
# 		self.assertTrue(group['npi'] == ['1111111111'])
# 		self.assertTrue(group['tin']['value'] == '22-2222222')

class TestMRFObjectBuilder(unittest.TestCase):

	npi_filter = {'1111111111', '5555555555', '2020202020'}
	code_filter = {('TS-TST', '0000')}
	loc = 'test/test.json'

	def test_p_refs_map(self):
		with MRFOpen(self.loc) as f:
			m = MRFProcessor(f)
			m._ffwd(('', 'map_key', 'provider_references'))
			p_refs_map = m._make_provider_reference_map(self.npi_filter)

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
			m = MRFProcessor(f)
			root_data = m._process_root()

			root_data['filename'] = Path(self.loc).stem.split('.')[0]
			root_hash_key = dicthasher(root_data)
			root_data['root_hash_key'] = root_hash_key

			root_data['url'] = Path(self.loc).stem.split('.')[0]

			p_refs_map = m._make_provider_reference_map(self.npi_filter)
			m._ffwd(('', 'map_key', 'in_network'))
			g = m.gen_in_network(self.npi_filter, self.code_filter, p_refs_map)
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
