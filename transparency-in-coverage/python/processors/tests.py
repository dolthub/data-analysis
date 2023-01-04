import unittest
import copy
from mrfutils import (
	Content,
	process_group,
	process_reference,
	process_rate,
)

files = [
	'test/test_file_ordered.json',
	'test/test_file_out_of_order.json'
]

sample_reference = {
	'provider_group_id': 0,
	'provider_groups': [
		{
			'npi': ['9889889881', 1234567890],
			'tin': {'type': 'ein', 'value': '9999999999'}
		}
	]
}
sample_group = sample_reference['provider_groups'][0]

sample_rate = {
	"negotiated_prices":
		[{
			"additional_information": "",
			"billing_class": "institutional",
			"billing_code_modifier": [],
			"expiration_date": "9999-12-31",
			"negotiated_rate": 9999999.99,
			"negotiated_type": "negotiated",
			"service_code": []
		}],
	"provider_groups":
		[{
			"npi": [4444444444, 5555555555] ,
			"tin": {"type": "ein", "value": "11-1111111"}
		}],
	"provider_references":[2]
}

sample_reference_map = {
	2:[{
		"npi":[2020202020, 3030303030, 1111111111],
		"tin":{"type": "ein", "value": "88-8888888"}
	}]
}

class Test(unittest.TestCase):

	def setUp(self) -> None:
		self.test_files = files

	def test_process_rate_single_npi(self):
		rate = copy.deepcopy(sample_rate)
		rate.pop('provider_references')
		rate = process_rate(rate, {'4444444444'})
		assert rate['provider_groups'][0]['npi'] == ['4444444444']

	def test_process_rate_multiple_npi(self):
		rate = copy.deepcopy(sample_rate)
		rate.pop('provider_references')
		rate = process_rate(rate, {'4444444444', 'notused'})
		assert rate['provider_groups'][0]['npi'] == ['4444444444']

	def test_process_rate_no_matches(self):
		rate = copy.deepcopy(sample_rate)
		rate.pop('provider_references')
		rate = process_rate(rate, {'no', 'definitelynot'})
		assert rate is None

	def test_process_rate_no_npi_list(self):
		rate = copy.deepcopy(sample_rate)
		rate.pop('provider_references')
		rate = process_rate(rate, None)
		assert rate['provider_groups'][0]['npi'] == ['4444444444', '5555555555']

	def test_process_rate_forget_pop_references(self):
		rate = copy.deepcopy(sample_rate)
		self.assertRaises(AssertionError, process_rate, rate, {'doesntmatter'})

	def test_no_references(self):
		npi_filter = None
		code_filter = None
		file = 'test/test_file_no_references.json'
		content = Content(file, code_filter, npi_filter)
		content.start_conn()
		plan = content.plan
		ref_map = content.ref_map
		assert ref_map is None
		assert plan['reporting_entity_name'] == 'TEST ENTITY'
		processed_items = content.in_network_items()
		first_item = next(processed_items)
		assert first_item['billing_code'] == '0000'
		second_item = next(processed_items)
		assert second_item['billing_code'] == '1111'

	def test_normal_ordering(self):
		npi_filter = None
		code_filter = None
		file = 'test/test_file_ordered.json'
		content = Content(file, code_filter, npi_filter)
		content.start_conn()

		plan = content.plan
		assert plan['reporting_entity_name'] == 'TEST ENTITY'

		processed_items = content.in_network_items()

		first_item = next(processed_items)
		assert first_item['billing_code'] == '0000'

		second_item = next(processed_items)
		assert second_item['billing_code'] == '1111'

	def test_reverse_ordering(self):
		npi_filter = None
		code_filter = None
		file = 'test/test_file_ordered.json'
		content = Content(file, code_filter, npi_filter)
		content.start_conn()

		plan = content.plan
		assert plan['reporting_entity_name'] == 'TEST ENTITY'

		processed_items = content.in_network_items()

		first_item = next(processed_items)
		assert first_item['billing_code'] == '0000'

		second_item = next(processed_items)
		assert second_item['billing_code'] == '1111'


	def test_process_group(self):
		group = sample_group.copy()
		group = process_group(group, {'9889889881'})
		assert group['npi'] == ['9889889881']
		assert group['tin'] == {'type': 'ein', 'value': '9999999999'}

	def test_process_reference(self):
		reference = sample_reference.copy()
		reference = process_reference(reference, {'9889889881'})
		groups = reference['provider_groups']
		npis = groups[0]['npi']

		assert len(npis) == 1

	def test_npi_filtering_ordered(self):
		npi_filter = {'9889889881'}
		code_filter = None
		file = 'test/test_file_ordered.json'

		content = Content(file, code_filter, npi_filter)
		content.start_conn()

		plan = content.plan
		assert plan['reporting_entity_name'] == 'TEST ENTITY'

		processed_items = content.in_network_items()

		items = list(processed_items)
		assert items[0]['billing_code'] == '0000'
		assert len(items) == 1

	def test_npi_filtering_out_of_order(self):
		npi_filter = {'9889889881'}
		code_filter = None
		file = 'test/test_file_out_of_order.json'
		content = Content(file, code_filter, npi_filter)
		content.start_conn()

		plan = content.plan
		assert plan['reporting_entity_name'] == 'TEST ENTITY'

		processed_items = content.in_network_items()

		items = list(processed_items)
		assert items[0]['billing_code'] == '0000'
		assert len(items) == 1

	def test_remote_npi_filtering(self):
		npi_filter = {'2222222222'}
		code_filter = None
		for file in self.test_files:
			content = Content(file, code_filter, npi_filter)
			content.start_conn()
			processed_items = content.in_network_items()

			first_item = next(processed_items)
			assert first_item['name'] == 'TEST NAME 2'
			rates = first_item['negotiated_rates']
			assert len(rates) == 1
			provider_groups = rates[0]['provider_groups']
			assert len(provider_groups) == 1
			npis = provider_groups[0]['npi']
			assert len(npis) == 1
			assert npis[0] == '2222222222'

	def test_multiple_npi_filtering(self):
		npi_filter = {'1234567890', '4444444444'}
		code_filter = None
		for file in self.test_files:
			content = Content(file, code_filter, npi_filter)
			content.start_conn()
			processed_items = content.in_network_items()
			first_item = next(processed_items)
			assert first_item['billing_code'] == '0000'
			rates = first_item['negotiated_rates']
			assert len(rates) == 1
			provider_groups = rates[0]['provider_groups']
			assert len(provider_groups) == 3

	def test_code_filtering(self):
		code_filter = {('TS-TST', '0000')}
		npi_filter = None
		for file in self.test_files:
			content = Content(file, code_filter, npi_filter)
			content.start_conn()
			processed_items = content.in_network_items()
			first_item = next(processed_items)
			assert first_item['billing_code'] == '0000'

	def test_combined_filtering(self):
		code_filter = {('TS-TST', '0000')}
		npi_filter = {'4444444444'}
		for file in self.test_files:
			content = Content(file, code_filter, npi_filter)
			content.start_conn()
			processed_items = content.in_network_items()
			first_item = next(processed_items)
			assert first_item['billing_code'] == '0000'

	def test_not_in_list(self):
		code_filter = {('TS-TST', '0000')}
		# code_filter = None
		npi_filter = {'NOTINLIST'}
		for file in self.test_files:
			content = Content(file, code_filter, npi_filter)
			content.start_conn()
			processed_items = content.in_network_items()
			assert len(list(processed_items)) == 0

	# def test_hashes_match(self):
	# 	Still need to write a test for this
	# 	for file in self.test_files:
	# 		content = MRFContent(file)
	# 		content.start()
	# 		in_network_items = content.in_network_items
	# 		first_item = next(in_network_items)
	# 		assert first_item['billing_code'] == '0000'


if __name__ == '__main__':
	unittest.main()
