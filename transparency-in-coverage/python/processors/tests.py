import unittest
from itertools import permutations
from mrfutils import MRFContent


def generate_test_files():
    """
    Generates a list of all possible permutations of a test file
    """
    with open("test/test_json_strs.txt") as f:
        lines = f.readlines()
    plan_line = lines[0]
    file_orders = [(plan_line, lines[2], lines[1]), (plan_line, lines[1], lines[2])]
    files = []
    for file_order in file_orders:
        filestring = "{" + ",".join(file_order) + "}"
        files.append(bytes(filestring.encode("utf-8")))
    return files


class Test(unittest.TestCase):
    def setUp(self) -> None:
        self.test_files = generate_test_files()

    def test_ordering(self):
        for idx, file in enumerate(self.test_files):
            content = MRFContent(file)
            content.start()
            in_network_items = content.in_network_items
            first_item = next(in_network_items)
            assert first_item["billing_code"] == "0000"
            second_item = next(in_network_items)
            assert second_item["billing_code"] == "1111"
            plan = content.plan
            assert plan["reporting_entity_name"] == "TEST ENTITY"

    def test_npi_filtering(self):
        npi_filter = {"9889889881"}
        for file in self.test_files:
            content = MRFContent(file, npi_filter=npi_filter)
            content.start()
            in_network_items = list(content.in_network_items)
            assert in_network_items[0]["billing_code"] == "0000"
            assert len(in_network_items) == 1
            plan = content.plan
            assert plan["reporting_entity_name"] == "TEST ENTITY"

    def test_remote_npi_filtering(self):
        npi_filter = {"2222222222"}
        for file in self.test_files:
            content = MRFContent(file, npi_filter=npi_filter)
            content.start()
            in_network_items = content.in_network_items
            first_item = next(in_network_items)
            assert first_item["name"] == "TEST NAME 2"
            rates = first_item["negotiated_rates"]
            assert len(rates) == 1
            provider_groups = rates[0]["provider_groups"]
            assert len(provider_groups) == 1
            npis = provider_groups[0]["npi"]
            assert len(npis) == 1
            assert npis[0] == "2222222222"

    def test_multiple_npi_filtering(self):
        npi_filter = {"1234567890", "4444444444"}
        for file in self.test_files:
            content = MRFContent(file, npi_filter=npi_filter)
            content.start()
            in_network_items = content.in_network_items
            first_item = next(in_network_items)
            assert first_item["billing_code"] == "0000"
            rates = first_item["negotiated_rates"]
            assert len(rates) == 1
            provider_groups = rates[0]["provider_groups"]
            assert len(provider_groups) == 3

    def test_code_filtering(self):
        code_filter = {("TS-TST", "0000")}
        for file in self.test_files:
            content = MRFContent(file, code_filter=code_filter)
            content.start()
            in_network_items = content.in_network_items
            first_item = next(in_network_items)
            assert first_item["billing_code"] == "0000"

    def test_combined_filtering(self):
        code_filter = {("TS-TST", "0000")}
        npi_filter = {"4444444444"}
        for file in self.test_files:
            content = MRFContent(file, code_filter=code_filter, npi_filter=npi_filter)
            content.start()
            in_network_items = content.in_network_items
            first_item = next(in_network_items)
            assert first_item["billing_code"] == "0000"

    # def test_hashes_match(self):
    # Still need to write a test for this
    # for file in self.test_files:
    # 	content = MRFContent(file)
    # 	content.start()
    # 	in_network_items = content.in_network_items
    # 	first_item = next(in_network_items)
    # 	assert first_item['billing_code'] == '0000'


if __name__ == "__main__":
    unittest.main()
