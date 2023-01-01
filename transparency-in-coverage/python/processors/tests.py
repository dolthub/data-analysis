import unittest
from mrfutils import (
	MRFContent,
	process_group,
	process_reference,
	process_rate,
	replace_rates,
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

	def test_replace_rates_no_reference_map(self):
		rates = [sample_rate.copy()]
		rates = replace_rates(rates, {})
		assert len(rates) == 1
		groups = rates[0]['provider_groups']
		group = groups[0]
		assert group['npi'] == [4444444444, 5555555555]

	def test_replace_rates_reference_map(self):
		rates = [sample_rate.copy()]
		rates = replace_rates(rates, sample_reference_map)
		groups = rates[0]['provider_groups']
		assert len(groups) == 2
		assert len(groups[0]['npi']) == 2
		assert len(groups[1]['npi']) == 3

	def test_process_rate(self):
		# TBD

	def test_ordering(self):
		for idx, file in enumerate(self.test_files):
			content = MRFContent(file)
			content.start_conn()
			in_network_items = content.in_network_items()
			first_item = next(in_network_items)
			assert first_item['billing_code'] == '0000'
			second_item = next(in_network_items)
			assert second_item['billing_code'] == '1111'
			plan = content.plan
			assert plan['reporting_entity_name'] == 'TEST ENTITY'

	def test_npi_filtering(self):
		npi_filter = {'9889889881'}
		for idx, file in enumerate(self.test_files):
			content = MRFContent(file, npi_filter = npi_filter)
			content.start_conn()
			in_network_items = list(content.in_network_items())
			assert in_network_items[0]['billing_code'] == '0000'
			assert len(in_network_items) == 1
			plan = content.plan
			assert plan['reporting_entity_name'] == 'TEST ENTITY'

	def test_remote_npi_filtering(self):
		npi_filter = {'2222222222'}
		for file in self.test_files:
			content = MRFContent(file, npi_filter = npi_filter)
			content.start_conn()
			in_network_items = content.in_network_items()
			first_item = next(in_network_items)
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
		for file in self.test_files:
			content = MRFContent(file, npi_filter = npi_filter)
			content.start_conn()
			in_network_items = content.in_network_items()
			first_item = next(in_network_items)
			assert first_item['billing_code'] == '0000'
			rates = first_item['negotiated_rates']
			assert len(rates) == 1
			provider_groups = rates[0]['provider_groups']
			assert len(provider_groups) == 3

	def test_code_filtering(self):
		code_filter = {('TS-TST', '0000')}
		for file in self.test_files:
			content = MRFContent(file, code_filter = code_filter)
			content.start_conn()
			in_network_items = content.in_network_items()
			first_item = next(in_network_items)
			assert first_item['billing_code'] == '0000'

	def test_combined_filtering(self):
		code_filter = {('TS-TST', '0000')}
		npi_filter = {'4444444444'}
		for file in self.test_files:
			content = MRFContent(file, code_filter = code_filter, npi_filter = npi_filter)
			content.start_conn()
			in_network_items = content.in_network_items()
			first_item = next(in_network_items)
			assert first_item['billing_code'] == '0000'
	#
	# def test_not_in_list(self):
	# 	code_filter = {('TS-TST', '0000')}
	# 	# code_filter = None
	# 	npi_filter = {'NOTINLIST'}
	# 	for file in self.test_files:
	# 		content = MRFContent(file, code_filter = code_filter, npi_filter = npi_filter)
	# 		content.start_conn()
	# 		in_network_items = content.in_network_items()
	# 		assert len(list(in_network_items)) == 0

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
