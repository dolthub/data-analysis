#!/usr/bin/python3

import unittest

from mrfutils import Content

class Test(unittest.TestCase):
    def test_content_init(self):
        file = "mrf.json.gz"
        npi_filter = ["1750368700", "1215914288", "1033196001"]
        code_filter = ["CPT,80053", "CPT,85025"]

        content = Content(file, code_filter, npi_filter)

        assert content is not None
        assert npi_filter == content.npi_filter
        assert code_filter == code_filter

if __name__ == "__main__":
    unittest.main()
