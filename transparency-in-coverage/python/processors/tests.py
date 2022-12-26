import unittest
from itertools import permutations
from mrfutils import _gen_ordered_mrf_contents


def generate_test_files():
	"""
	Generates a list of all possible permutations of a test file
	"""
	with open('test/test_json_strs.txt') as f:
		lines = f.readlines()
	file_orders = permutations(lines)
	files = []
	for file_order in file_orders:
		filestring = '{' + ','.join(file_order) + '}'
		files.append(bytes(filestring.encode('utf-8')))
	return files


class Test(unittest.TestCase):

	def setUp(self) -> None:
		self.test_files = generate_test_files()

	def test_ordering(self):
		for file in self.test_files:
			contents = _gen_ordered_mrf_contents(file)
			first_item = next(contents)
			assert first_item['billing_code'] == '0000'
			second_item = next(contents)
			assert second_item['billing_code'] == '1111'
			third_item = next(contents)
			assert third_item == 'SENTINEL'
			fourth_item = next(contents)
			assert fourth_item['reporting_entity_name'] == 'TEST ENTITY'

	def test_npi_filtering(self):
		npi_filter = {'9889889881'}
		for file in self.test_files:
			contents = _gen_ordered_mrf_contents(file, npi_filter=npi_filter)
			first_item = next(contents)
			assert first_item['billing_code'] == '0000'
			rates = first_item['negotiated_rates']
			assert len(rates) == 1
			provider_groups = rates[0]['provider_groups']
			assert len(provider_groups) == 1
			npis = provider_groups[0]['npi']
			assert len(npis) == 1
			assert npis[0] == '9889889881'
			second_item = next(contents)
			assert second_item == 'SENTINEL'

	def test_remote_npi_filtering(self):
		npi_filter = {'2222222222'}
		for file in self.test_files:
			contents = _gen_ordered_mrf_contents(file, npi_filter=npi_filter)
			first_item = next(contents)
			assert first_item['name'] == 'TEST NAME 2'
			rates = first_item['negotiated_rates']
			assert len(rates) == 1
			provider_groups = rates[0]['provider_groups']
			assert len(provider_groups) == 1
			npis = provider_groups[0]['npi']
			assert len(npis) == 1
			assert npis[0] == '2222222222'
			second_item = next(contents)
			assert second_item == 'SENTINEL'

	def test_multiple_npi_filtering(self):
		npi_filter = {'1234567890', '4444444444'}
		for file in self.test_files:
			contents = _gen_ordered_mrf_contents(file, npi_filter=npi_filter)
			first_item = next(contents)
			assert first_item['billing_code'] == '0000'
			rates = first_item['negotiated_rates']
			assert len(rates) == 1
			provider_groups = rates[0]['provider_groups']
			assert len(provider_groups) == 3

	def test_code_filtering(self):
		code_filter = {('TS-TST', '0000')}
		for file in self.test_files:
			contents = _gen_ordered_mrf_contents(file, code_filter=code_filter)
			first_item = next(contents)
			assert first_item['billing_code'] == '0000'
			second_item = next(contents)
			assert second_item == 'SENTINEL'

	def test_combined_filtering(self):
		code_filter = {('TS-TST', '0000')}
		npi_filter = {'4444444444'}
		for file in self.test_files:
			contents = _gen_ordered_mrf_contents(file, code_filter=code_filter, npi_filter=npi_filter)
			first_item = next(contents)
			assert first_item['billing_code'] == '0000'
			second_item = next(contents)
			assert second_item == 'SENTINEL'


if __name__ == '__main__':
	unittest.main()
