"""
The function

>>> def json_mrf_to_csv()...

flattens the data from a JSON MRF into a CSV.

If the flattener encounters the in-network items after the provider
references, it will flatten those on the first pass. Otherwise, it
will open the file again and re-run, recycling the provider
references that it got from the first pass.

The plan data is only written after the flattener has successfully run.
"""
import asyncio
import csv
import gzip
import hashlib
import io
import json
import logging
import os
from typing import Generator
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import ijson
import requests

from exporters import AbstractExporter
from schema import SCHEMA

# You can remove this if necessary, but be warned
try:
    assert ijson.backend == "yajl2_c"
except AssertionError:
    raise Exception("Extremely slow without the yajl2_c backend")


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class InvalidMRF(Exception):
    pass


class JSONOpen(io.BufferedIOBase):
    """
    Context manager for opening JSON(.gz) MRFs.
    Handles local and remote gzipped and unzipped
    JSON files.
    """

    def __init__(self, input):
        if type(input) == bytes:
            self.data = input
            self.loc = None
        else:
            self.loc = input
            self.data = None

        self.f = None
        self.r = None
        self.is_remote = None

        if self.loc:
            parsed_url = urlparse(self.loc)
            self.suffix = "".join(Path(parsed_url.path).suffixes)

            if self.suffix not in (".json.gz", ".json"):
                raise InvalidMRF(f"Suffix not JSON: {self.loc}")

            self.is_remote = parsed_url.scheme in ("http", "https")

    def __enter__(self):
        if self.data:
            self.f = io.BytesIO(self.data)
            return self.f
        if self.is_remote and self.suffix == ".json.gz":
            self.r = requests.get(self.loc, stream=True)
            self.f = gzip.GzipFile(fileobj=self.r.raw)

        elif self.is_remote and self.suffix == ".json":
            self.r = requests.get(self.loc, stream=True)
            self.r.raw.decode_content = True
            self.f = self.r.raw

        elif self.suffix == ".json.gz":
            self.f = gzip.open(self.loc, "rb")

        else:
            self.f = open(self.loc, "rb")

        log.info(f"Successfully opened file: {self.loc}")
        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.r and self.is_remote:
            self.r.close()

        self.f.close()


def import_csv_to_set(filename: str):
    """Imports data as tuples from a given file."""
    items = set()

    with open(filename, "r") as f:
        reader = csv.reader(f)

        for row in reader:
            row = [col.strip() for col in row]

            if len(row) > 1:
                items.add(tuple(row))

            else:
                items.add(row.pop())

        return items


def dicthasher(data, n_bytes=8):

    if not data:
        raise Exception("Hashed dictionary can't be empty")

    data = json.dumps(data, sort_keys=True).encode("utf-8")
    hash_s = hashlib.sha256(data).digest()[:n_bytes]
    hash_i = int.from_bytes(hash_s, "little")

    return hash_i


def append_hash(item, name):

    hash_ = dicthasher(item)
    item[name] = hash_

    return item


def _filename_hash(loc):

    filename = Path(loc).stem.split(".")[0]
    file_row = {"filename": filename}
    filename_hash = dicthasher(file_row)

    return filename_hash


def _make_plan_row(plan: dict):

    keys = [
        "reporting_entity_name",
        "reporting_entity_type",
        "plan_name",
        "plan_id",
        "plan_id_type",
        "plan_market_type",
        "last_updated_on",
        "version",
    ]

    plan_row = {key: plan.get(key) for key in keys}
    plan_row = append_hash(plan_row, "plan_hash")

    return plan_row


def _make_file_row(loc, url):

    filename = Path(loc).stem.split(".")[0]
    file_row = {"filename": filename}
    file_row = append_hash(file_row, "filename_hash")
    file_row["url"] = url

    return file_row


def _make_plan_file_row(plan_row, file_row):

    plan_file_row = {
        "plan_hash": plan_row["plan_hash"],
        "filename_hash": file_row["filename_hash"],
    }

    return plan_file_row


def _make_code_row(in_network_item: dict):

    keys = [
        "billing_code_type",
        "billing_code_type_version",
        "billing_code",
    ]

    code_row = {key: in_network_item[key] for key in keys}
    code_row = append_hash(code_row, "code_hash")

    return code_row


def _make_price_row(price: dict, code_hash, filename_hash):

    keys = [
        "billing_class",
        "negotiated_type",
        "expiration_date",
        "negotiated_rate",
        "additional_information",
    ]

    price_row = {key: price.get(key) for key in keys}

    optional_json_keys = [
        "service_code",
        "billing_code_modifier",
    ]

    for key in optional_json_keys:
        if price.get(key):
            price.get(key)
            sorted_value = sorted(price[key])
            price_row[key] = json.dumps(sorted_value)

    hashes = {"code_hash": code_hash, "filename_hash": filename_hash}

    price_row.update(hashes)
    price_row = append_hash(price_row, "price_hash")

    return price_row


def _make_price_rows(
    prices: list[dict],
    code_hash,
    filename_hash,
):

    price_rows = []
    for price in prices:
        price_row = _make_price_row(price, code_hash, filename_hash)
        price_rows.append(price_row)

    return price_rows


def _make_provider_group_row(provider_group: dict):

    provider_group_row = {
        "npi_numbers": json.dumps(sorted(provider_group["npi"])),
        "tin_type": provider_group["tin"]["type"],
        "tin_value": provider_group["tin"]["value"],
    }

    provider_group_row = append_hash(provider_group_row, "provider_group_hash")

    return provider_group_row


def _make_provider_group_rows(provider_groups: list[dict]):

    provider_group_rows = []
    for provider_group in provider_groups:
        provider_group_row = _make_provider_group_row(provider_group)
        provider_group_rows.append(provider_group_row)

    return provider_group_rows


def _make_prices_provider_groups_rows(
    price_rows: list[dict], provider_group_rows: list[dict]
):

    prices_provider_groups_rows = []
    for price_row in price_rows:
        for provider_group_row in provider_group_rows:

            prices_provider_groups_row = {
                "provider_group_hash": provider_group_row["provider_group_hash"],
                "price_hash": price_row["price_hash"],
            }

            prices_provider_groups_rows.append(prices_provider_groups_row)

    return prices_provider_groups_rows


def write_in_network_item(in_network_item: dict, filename_hash, exporter):
    code_row = _make_code_row(in_network_item)
    exporter.write_row("codes", code_row)

    code_hash = code_row["code_hash"]

    for rate in in_network_item["negotiated_rates"]:
        provider_groups = rate["provider_groups"]
        provider_group_rows = _make_provider_group_rows(provider_groups)
        exporter.write_rows("provider_groups", provider_group_rows)

        prices = rate["negotiated_prices"]
        price_rows = _make_price_rows(prices, code_hash, filename_hash)
        exporter.write_rows("prices", price_rows)

        prices_provider_group_rows = _make_prices_provider_groups_rows(
            price_rows, provider_group_rows
        )
        exporter.write_rows("prices_provider_groups", prices_provider_group_rows)

    code_type = in_network_item["billing_code_type"]
    code = in_network_item["billing_code"]
    log.debug(f"Wrote {code_type} {code}")


def write_plan(plan: dict, loc, url, exporter):
    plan_row = _make_plan_row(plan)
    exporter.write_row("plans", plan_row)

    file_row = _make_file_row(loc, url)
    exporter.write_row("files", file_row)

    plan_file_row = _make_plan_file_row(plan_row, file_row)
    exporter.write_row("plans_files", plan_file_row)


async def _fetch_remote_provider_reference(
    session,
    provider_group_id,
    provider_reference_loc: str,
    npi_filter: set,
) -> dict:
    async with session.get(provider_reference_loc) as response:

        log.info(f"Opened remote provider reference url:{provider_reference_loc}")
        assert response.status == 200

        f = await response.read()

        unprocessed_data = json.loads(f)
        unprocessed_data["provider_group_id"] = provider_group_id

        processed_remote_provider_reference = _process_provider_reference(
            item=unprocessed_data,
            npi_filter=npi_filter,
        )
        return processed_remote_provider_reference


async def _fetch_remote_provider_references(
    unfetched_provider_references: list[dict],
    npi_filter: set,
) -> list[dict]:
    tasks = []
    async with aiohttp.client.ClientSession() as session:

        for unfetched_reference in unfetched_provider_references:

            provider_group_id = unfetched_reference["provider_group_id"]
            provider_reference_loc = unfetched_reference["location"]

            task = asyncio.wait_for(
                _fetch_remote_provider_reference(
                    session=session,
                    provider_group_id=provider_group_id,
                    provider_reference_loc=provider_reference_loc,
                    npi_filter=npi_filter,
                ),
                timeout=5,
            )

            tasks.append(task)

        fetched_references = await asyncio.gather(*tasks)
        fetched_references = [item for item in fetched_references if item]

        return fetched_references


def _process_provider_group(
    provider_group: dict,
    npi_filter: set,
) -> dict:
    npi = [str(n) for n in provider_group["npi"]]

    if npi_filter:
        npi = [n for n in npi if n in npi_filter]

    if not npi:
        return

    tin = provider_group["tin"]

    provider_group = {"npi": npi, "tin": tin}
    return provider_group


def _process_provider_reference(
    item: dict,
    npi_filter: set,
) -> dict:
    processed_provider_groups = []
    for provider_group in item["provider_groups"]:
        provider_group = _process_provider_group(provider_group, npi_filter)
        if provider_group:
            processed_provider_groups.append(provider_group)

    if not processed_provider_groups:
        return

    result = {
        "provider_group_id": item["provider_group_id"],
        "provider_groups": processed_provider_groups,
    }

    return result


def _combine_local_remote_provider_references(
    provider_references: list[dict],
    unfetched_provider_references: list[dict],
    npi_filter: set,
) -> list[dict]:

    loop = asyncio.get_event_loop()
    fetched_provider_references = loop.run_until_complete(
        _fetch_remote_provider_references(
            unfetched_provider_references=unfetched_provider_references,
            npi_filter=npi_filter,
        )
    )

    provider_references.extend(fetched_provider_references)

    return provider_references


def _process_rate(
    rate: dict,
    provider_reference_map: dict,
    npi_filter,
) -> dict:

    provider_groups = rate.get("provider_groups", [])
    if provider_reference_map and rate.get("provider_references"):
        for provider_group_id in rate["provider_references"]:
            addl_provider_groups = provider_reference_map.get(provider_group_id)
            if addl_provider_groups:
                provider_groups.extend(addl_provider_groups)
        rate.pop("provider_references")

    processed_provider_groups = []
    for provider_group in provider_groups:
        processed_provider_group = _process_provider_group(provider_group, npi_filter)
        if processed_provider_group:
            processed_provider_groups.append(processed_provider_group)

    if not processed_provider_groups:
        return

    rate["provider_groups"] = processed_provider_groups

    return rate


def _process_in_network_item(
    in_network_item: dict,
    provider_reference_map: dict,
    npi_filter: set,
    # code_filter = None
) -> dict:

    # the local optimization takes care of this
    # if code_filter:
    # 	billing_code_type = item['billing_code_type']
    # 	billing_code = str(item['billing_code'])
    # 	if (billing_code_type, billing_code) not in code_filter:
    # 		log.debug(f"skipped {item['billing_code']} not in list")
    # 		return
    #
    # if item['negotiation_arrangement'] != 'ffs':
    # 	log.debug('wrong type')
    # 	return

    rates = []
    for unprocessed_rate in in_network_item["negotiated_rates"]:
        rate = _process_rate(unprocessed_rate, provider_reference_map, npi_filter)
        if rate:
            rates.append(rate)

    if not rates:
        return

    in_network_item["negotiated_rates"] = rates

    return in_network_item


def _ffwd(parser, to_prefix, to_event):
    for prefix, event, _ in parser:
        if (prefix, event) == (to_prefix, to_event):
            return


def _local_optimization(in_network_items, parser, code_filter):
    """
    This stops us from having to build in-network objects (which are large)
    when their billing codes or arrangement don't fit the filter. It's kind of a
    hack which is why I bundled these changes into one function.
    """
    item = in_network_items.value[-1]
    code_type = item.get("billing_code_type")
    code = item.get("billing_code")

    if code and code_type and code_filter:
        if (code_type, str(code)) not in code_filter:
            log.debug(f"Skipping {code_type} {code}: filtered out")
            _ffwd(parser, "in_network.item", "end_map")
            in_network_items.value.pop()
            in_network_items.containers.pop()
            return

    arrangement = item.get("negotiation_arrangement")
    if arrangement and arrangement != "ffs":
        log.debug(f"Skipping item: arrangement: {arrangement} not 'ffs'")
        _ffwd(parser, "in_network.item", "end_map")
        in_network_items.value.pop()
        in_network_items.containers.pop()
        return


def _make_provider_reference_map(
    parser,
    npi_filter: set,
) -> dict:

    unfetched_provider_references = []

    provider_references = ijson.ObjectBuilder()
    provider_references.event("start_array", None)

    for prefix, event, value in parser:
        provider_references.event(event, value)
        if (prefix, event) == ("provider_references.item", "end_map"):
            unprocessed_reference = provider_references.value.pop()
            if unprocessed_reference.get("location"):
                unfetched_provider_references.append(unprocessed_reference)
                continue

            provider_reference = _process_provider_reference(
                item=unprocessed_reference, npi_filter=npi_filter
            )

            if provider_reference:
                provider_references.value.insert(1, provider_reference)

        elif (prefix, event) == ("provider_references", "end_array"):
            combined_provider_references = _combine_local_remote_provider_references(
                provider_references=provider_references.value,
                unfetched_provider_references=unfetched_provider_references,
                npi_filter=npi_filter,
            )

            provider_reference_map = {
                p["provider_group_id"]: p["provider_groups"]
                for p in combined_provider_references
                if p is not None
            }
            return provider_reference_map


def _plan(parser):
    plan = ijson.ObjectBuilder()
    for prefix, event, value in parser:
        plan.event(event, value)
        if value in ("provider_references", "in_network"):
            return plan.value
    else:
        raise InvalidMRF


def _in_network_items(
    parser,
    code_filter: set,
) -> Generator[dict, None, None]:

    in_network_items = ijson.ObjectBuilder()
    in_network_items.event("start_array", None)
    for prefix, event, value in parser:
        in_network_items.event(event, value)

        # This line can be commented out! but it's faster with it in
        if hasattr(in_network_items, "value") and len(in_network_items.value) > 0:
            _local_optimization(in_network_items, parser, code_filter)

        if (prefix, event) == ("in_network.item", "end_map"):
            yield in_network_items.value.pop()

        elif (prefix, event) == ("in_network", "end_array"):
            return


def _processed_in_network_items(
    unprocessed_items: Generator[dict, None, None],
    provider_reference_map: dict,
    npi_filter: set,
) -> Generator[dict, None, None]:

    for unprocessed_item in unprocessed_items:
        processed_item = _process_in_network_item(
            in_network_item=unprocessed_item,
            npi_filter=npi_filter,
            provider_reference_map=provider_reference_map,
        )
        if processed_item:
            yield processed_item


class MRFContent:
    """
    Bucket for MRF data. Assumes that the MRF is in one of the
    following three orders:

    1. plan -> provider_references -> in_network (most common)
    2. plan -> in_network (second-most common)
    3. plan -> in_network -> provider_references (least common)

    As far as I can tell, no one puts their plan data at the
    bottom, although that's possible. If that turns out to be
    the case this function will have to be modified a little bit
    (but that won't be a big deal.)

    Usage:
        >>> content = MRFContent(loc, npi_filter, code_filter)
    >>> content.start() # fetches plan info and opens file
    >>> content.plan # access plan information
    >>> content.in_network_items # generates items as file is read

    From there you can write the items and plan information as you
    read them in.
    """

    def __init__(self, loc, npi_filter=None, code_filter=None):
        self.loc = loc
        self.code_filter = code_filter
        self.npi_filter = npi_filter
        self.extractor = self._extractor()

    def start(self):
        """Opens the file, grabs the plan metadata"""
        self.plan = next(self.extractor)

    @property
    def in_network_items(self) -> Generator[dict, None, None]:
        yield from self.extractor

    def _extractor(self) -> Generator[dict, None, None]:
        """
        Generates MRF dictionaries in the order:
        1. plan
        2. all in-network items
        The first time you call this function's __next__() you'll
        get the plan information. This is what happens in start
        >>> content.start() # advance extractor one step
        The rest of the items are yielded afterwards
        """
        with JSONOpen(self.loc) as f:

            parser = ijson.parse(f, use_float=True)
            yield _plan(parser)

            prefix, _, _ = next(parser)
            if prefix == "provider_references":
                provider_reference_map = _make_provider_reference_map(
                    parser=parser,
                    npi_filter=self.npi_filter,
                )

                _ffwd(parser, "in_network", "start_array")
                unprocessed_items = _in_network_items(parser, self.code_filter)
                yield from _processed_in_network_items(
                    unprocessed_items=unprocessed_items,
                    provider_reference_map=provider_reference_map,
                    npi_filter=self.npi_filter,
                )
                return
            # Enter this block if we don't find provider references
            # Try to see if they're at the bottom
            try:
                _ffwd(parser, "provider_references", "start_array")
                provider_reference_map = _make_provider_reference_map(
                    parser=parser,
                    npi_filter=self.npi_filter,
                )
            except StopIteration:
                provider_reference_map = None
        # Enter this block when provider references are either
        # at the bottom or not found
        with JSONOpen(self.loc) as f:
            parser = ijson.parse(f, use_float=True)
            _ffwd(parser, "in_network", "start_array")
            unprocessed_items = _in_network_items(parser, self.code_filter)
            yield from _processed_in_network_items(
                unprocessed_items=unprocessed_items,
                provider_reference_map=provider_reference_map,
                npi_filter=self.npi_filter,
            )


def json_mrf_to_csv(
    loc: str,
    npi_filter: set,
    code_filter: set,
    exporter: AbstractExporter,
    url: str = None,
) -> None:
    """
    Writes MRF content to a flat file CSV in a specific schema.
    The url parameter is optional -- if you pass only a loc,
    we assume that the file is remote.

    If you pass a loc and a URL the file is read from loc but the
    URL that you input is used. This is just saved for bookkeeping.

    !Importantly! you have to make sure that whatever file you saved
    matches the filename that the URL returns. We index the files by
    name, so if the name is different, the index will be wrong.
    """

    filename_hash = _filename_hash(loc)

    content = MRFContent(loc, npi_filter, code_filter)
    content.start()

    write_plan(plan=content.plan, loc=loc, url=url if url else loc, exporter=exporter)

    for in_network_item in content.in_network_items:
        write_in_network_item(
            in_network_item=in_network_item,
            filename_hash=filename_hash,
            exporter=exporter,
        )
