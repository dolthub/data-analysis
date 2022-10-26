import time
import requests
import logging
import ijson
import gzip
from urllib.parse import urlparse
from helpers import (
    hashdict,
    build_root,
    build_provrefs,
    build_remote_refs,
    build_innetwork,
    innetwork_to_rows,
    rows_to_file,
    provrefs_to_idx,
)


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_mrfs_from_index(index_file_url):
    """
    Gets in-network files from index.json files
    """
    s = time.time()
    in_network_file_urls = []

    with requests.get(index_file_url, stream=True) as r:
        LOG.info(f"Began streaming file: {index_file_url}")
        try:
            url_size = round(int(r.headers["Content-length"]) / 1_000_000, 3)
            LOG.info(f"Size of file: {url_size} MB")
        except KeyError:
            LOG.info(f"Size of index file unknown.")

        if urlparse(index_file_url)[2].endswith(".json.gz"):
            f = gzip.GzipFile(fileobj=r.raw)
            LOG.info(f"Unzipping streaming file.")
        elif urlparse(index_file_url)[2].endswith(".json"):
            f = r.content
        else:
            LOG.info(f"File does not have an extension. Aborting.")
            return

        parser = ijson.parse(f, use_float=True)

        for prefix, event, value in parser:
            if (prefix, event) == (
                "reporting_structure.item.in_network_files.item.location",
                "string",
            ):
                LOG.debug(f"Found in-network file: {value}")
                in_network_file_urls.append(value)

    td = time.time() - s
    LOG.info(f"Found: {len(in_network_file_urls)} in-network files.")
    LOG.info(f"Time taken: {round(td/60, 3)} min.")
    return in_network_file_urls


def stream_json_to_csv(input_url, output_dir, code_list=None, npi_list=None):
    """
    This streams through JSON, flattens it, and writes it to
    file
    """
    with requests.get(input_url, stream=True) as r:

        urlpath = urlparse(input_url).path

        if urlpath.endswith(".json.gz"):
            f = gzip.GzipFile(fileobj=r.raw)
        elif urlpath.endswith(".json"):
            f = r.content

        parser = ijson.parse(f, use_float=True)

        root_vals, row = build_root(parser)
        root_hash_id = hashdict(root_vals)
        root_vals["root_hash_id"] = root_hash_id

        prefix, event, value = row

        LOG.info("Getting provider references")

        if (prefix, event) == ("provider_references", "start_array"):
            provrefs, row = build_provrefs(row, parser, npi_list)
            LOG.info("Getting remote references")
            provrefs = build_remote_refs(provrefs, npi_list)

            if provrefs:
                provref_idx = provrefs_to_idx(provrefs)
            else:
                provref_idx = None

        LOG.info("Building in-network array")

        root_written = False
        for prefix, event, value in parser:
            if (prefix, event) == ("in_network.item", "start_map"):
                row = prefix, event, value
                innetwork, row = build_innetwork(row, parser, code_list, npi_list, provref_idx)

                if innetwork:
                    innetwork_rows = innetwork_to_rows(innetwork, root_hash_id)
                    rows_to_file(innetwork_rows, output_dir)

                    if not root_written:
                        rows_to_file([("root", root_vals)], output_dir)
                        root_written = True
