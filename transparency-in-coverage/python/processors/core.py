import time
import requests
import logging
import ijson
import gzip
import sys
from urllib.parse import urlparse, urljoin
from helpers import (
    hashdict,
    build_root,
    build_provrefs,
    build_innetwork,
    innetwork_to_rows,
    rows_to_file,
    provrefs_to_idx,
)


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_mrfs_from_index(index_file_url):
    """The in-network files are references from index.json files
    on the payor websites. This will stream one of those files
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
    """This streams through a JSON, flattens it, and writes it to
    file. It streams the zipped files, avoiding saving them to disk.

    MRFs are structured, schematically like
    {
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    file_metadata (top matter),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    provider_references (always one line, if exists)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    [in_network_items] (multiple lines),
    }

    But the in_network_items are linked to provider references. The
    problem we have to solve is: how do we collect only the codes
    and provider references we want, while reading the file once?

    The answer is: cache the provider references during streaming,
    then filter the in_network_items. Once you know which provider
    references to keep, you can filter the cached object.

    The steps we take are:
    1. Check to see if there are matching codes. If so, write them
    2. Write the top matter to file
    3. Write the provider references to file
    """
    s = time.time()

    with requests.get(input_url, stream=True) as r:

        urlpath = urlparse(input_url).path
        url = urljoin(input_url, urlpath)

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

            if not provrefs:
                return

            provref_idx = provrefs_to_idx(provrefs)

        LOG.info("Building in-network array")

        root_written = False
        for prefix, event, value in parser:
            if (prefix, event) == ("in_network.item", "start_map"):
                row = prefix, event, value
                innetwork, row = build_innetwork(row, parser, code_list, provref_idx)

                if innetwork:
                    innetwork_rows = innetwork_to_rows(innetwork, root_hash_id)
                    rows_to_file(innetwork_rows, output_dir)
                if not root_written:
                    rows_to_file([("root", root_vals)], output_idr)
                    root_written = True

        td = time.time() - s
        LOG.info(f"Total time taken: {round(td/60, 3)} min.")
