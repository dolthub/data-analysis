import unittest
import aiohttp
from mrfutils import (
	MRFOpen,
	MRFProcessor,
	_fetch_remote_provider_reference
)

class TestUnfilteredFetch(unittest.IsolatedAsyncioTestCase):

	provider_group_id = 2

	async def test_fetch_no_filter(self):
		loc = "https://raw.githubusercontent.com/CMSgov/price-transparency-guide/master/examples/provider-reference/provider-reference.json"
		async with aiohttp.client.ClientSession() as session:
			data = await _fetch_remote_provider_reference(
				session = session,
				provider_group_id = self.provider_group_id,
				provider_reference_loc = loc,
				npi_filter = None
			)

		group = data['provider_groups'][0]
		self.assertTrue(group['npi'] == ['1111111111'])
		self.assertTrue(group['tin']['value'] == '22-2222222')


class TestMRFProcessor(unittest.TestCase):

	npi_filter = {'1111111111', '5555555555', '2020202020'}
	code_filter = {('TS-TST', '0000')}
	loc = 'test/test.json'

	def test_provider_references_map(self):
		with MRFOpen(self.loc) as f:
			processor = MRFProcessor(f)
			provider_reference_map = processor.prepare_provider_references(self.npi_filter)

		self.assertTrue(
			provider_reference_map[0][0]['npi'] == ['1111111111']
		)
		self.assertTrue(
			provider_reference_map[1][0]['npi'] == ['2020202020', '1111111111']
		)
		# remote case
		self.assertTrue(provider_reference_map[2][0]['npi'] == ['1111111111'])

	def test_npis(self):
		with MRFOpen(self.loc) as f:
			processor = MRFProcessor(f)
			provider_reference_map = processor.prepare_provider_references(self.npi_filter)
			processor.jump_to_in_network()
			g = processor.gen_in_network(
				npi_filter = self.npi_filter,
				code_filter = self.code_filter,
				provider_reference_map = provider_reference_map
			)
			item_data = next(g)

		first_rate = item_data['negotiated_rates'][0]

		self.assertEqual(
			first_rate['provider_groups'][0]['npi'],['5555555555']
		)
		self.assertEqual(
			first_rate['provider_groups'][1]['npi'],['5555555555']
		)
		self.assertEqual(
			first_rate['provider_groups'][2]['npi'],['1111111111']
		)
		self.assertEqual(
			first_rate['provider_groups'][3]['npi'],['2020202020','1111111111']
		)


if __name__ == '__main__':
	unittest.main()
