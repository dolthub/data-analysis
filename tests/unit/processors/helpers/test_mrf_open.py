import unittest


from processors.helpers import MRFOpen


class TestMRFOpen(unittest.TestCase):
    def test_when_loc_is_web_url_is_remote(self):
        opener = MRFOpen(loc="https://my.website.net/file.json")
        self.assertTrue(opener.is_remote)

    def test_when_loc_is_local_url_is_not_remote(self):
        opener = MRFOpen(loc="file://my/local/file.json")
        self.assertFalse(opener.is_remote)

    def test_when_file_not_json_type_raises_exception(self):
        with self.assertRaises(Exception):
            MRFOpen(loc="http://non.json.file")
