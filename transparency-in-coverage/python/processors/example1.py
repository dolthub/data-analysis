#!/usr/bin/python3

import logging
from pathlib import Path

from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm

from exporters import CSVExporter
from mrfutils import import_csv_to_set, json_mrf_to_csv, InvalidMRF

log = logging.getLogger("mrfutils")

code_filter = import_csv_to_set("test/codes.csv")
npi_filter = import_csv_to_set("test/npis.csv")

p = Path(__file__).parent.absolute()

urls = [
    f"{p}/test/test_file_1.json",
    f"{p}/test/test_file_2.json",  # provider references at end
    f"{p}/test/test_file_3.json.gz",
    f"{p}/test/test_file_4.json",  # should fail
]

exporter = CSVExporter("out_dir")
exporter.start()

for url in tqdm(urls):
    with logging_redirect_tqdm():
        try:
            json_mrf_to_csv(
                loc=url,
                url=url,
                npi_filter=npi_filter,
                code_filter=code_filter,
                exporter=exporter,
            )
        except InvalidMRF:
            log.warning("Not a valid MRF.")

exporter.finalise()
