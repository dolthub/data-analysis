import json
import os
import csv
import glob
import hashlib
import ijson
import requests
import logging
from urllib.parse import urlparse
from schema import SCHEMA


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_output_dir(output_dir, overwrite):
    if os.path.exists(output_dir):
        if overwrite:
            for file in glob.glob(f"{output_dir}/*"):
                os.remove(file)
    else:
        os.mkdir(output_dir)


def import_billing_codes(filename):
    with open(filename, "r") as f:
        reader = csv.DictReader(f)
        codes = []
        for row in reader:
            codes.append((row["billing_code_type"], row["billing_code"]))
    return codes


def import_set(filename, ints=True):
    with open(filename, "r") as f:
        reader = csv.reader(f)
        items = set()
        for row in reader:
            item = row[0]
            if ints:
                items.add(int(item))
            else:
                items.add(str(item))
    return items


def clean_url(input_url):
    parsed_url = urlparse(input_url)
    cleaned_url = (parsed_url[1] + parsed_url[2]).strip()
    return cleaned_url

def hashdict(data_dict):
    if not data_dict:
        raise ValueError

    sorted_tups = sorted(data_dict.items())
    dict_as_bytes = json.dumps(sorted_tups).encode("utf-8")
    dict_hash = hashlib.sha256(dict_as_bytes).hexdigest()[:16]
    return dict_hash

def rows_to_file(rows, output_dir):
    for row in rows:
        filename, row_data = row
        fieldnames = SCHEMA[filename]
        file_loc = f"{output_dir}/{filename}.csv"

        if not os.path.exists(file_loc):
            with open(file_loc, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

        with open(file_loc, "a") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row_data)


def innetwork_to_rows(obj, root_hash_id):
    rows = []

    in_network_vals = {
        "negotiation_arrangement": obj["negotiation_arrangement"],
        "name": obj["name"],
        "billing_code_type": obj["billing_code_type"],
        "billing_code_type_version": obj["billing_code_type_version"],
        "billing_code": obj["billing_code"],
        "description": obj["description"],
        "root_hash_id": root_hash_id,
    }

    in_network_hash_id = hashdict(in_network_vals)
    in_network_vals["in_network_hash_id"] = in_network_hash_id

    rows.append(("in_network", in_network_vals))

    for neg_rate in obj.get("negotiated_rates", []):
        neg_rates_hash_id = hashdict(neg_rate)

        for provgroup in neg_rate["provider_groups"]:
            provgroup_vals = {
                "npi_numbers": provgroup["npi"],
                # "tin_type": provgroup["tin"]["type"],
                # "tin_value": provgroup["tin"]["value"],
                "negotiated_rates_hash_id": neg_rates_hash_id,
                "in_network_hash_id": in_network_hash_id,
                "root_hash_id": root_hash_id,
            }
            rows.append(("provider_groups", provgroup_vals))

        for neg_price in neg_rate["negotiated_prices"]:
            neg_price_vals = {
                "billing_class": neg_price["billing_class"],
                "negotiated_type": neg_price["negotiated_type"],
                "service_code": sc if (sc := neg_price.get("service_code", None)) else None,
                "expiration_date": neg_price["expiration_date"],
                "additional_information": neg_price.get("additional_information", None),
                "billing_code_modifier": bcm if (bcm := neg_price.get("billing_code_modifier", None)) else None,
                "negotiated_rate": neg_price["negotiated_rate"],
                "root_hash_id": root_hash_id,
                "in_network_hash_id": in_network_hash_id,
                "negotiated_rates_hash_id": neg_rates_hash_id,
            }
            rows.append(("negotiated_prices", neg_price_vals))

    for bundle in obj.get("bundled_codes", []):
        bundle_vals = {
            "billing_code_type": bundle["billing_code_type"],
            "billing_code_type_version": bundle["billing_code_type_version"],
            "billing_code": bundle["billing_code"],
            "description": bundle["description"],
            "root_hash_id": root_hash_id,
            "in_network_hash_id": in_network_hash_id,
        }
        rows.append(("bundled_codes", bundle_vals))

    return rows


def build_root(parser):
    builder = ijson.ObjectBuilder()

    for prefix, event, value in parser:

        if event == "start_array":
            return builder.value, (prefix, event, value)

        builder.event(event, value)


def provrefs_to_idx(provrefs):
    provref_idx = {x["provider_group_id"]: x["provider_groups"] for x in provrefs}
    return provref_idx


def build_provrefs(init_row, parser, npi_list=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_array"):
            return builder.value, (nprefix, event, value)

        if nprefix.endswith("npi.item"):
            if value not in npi_list:
                continue

        if nprefix.endswith("provider_groups.item") and event == "end_map":
            if not builder.value[-1].get("provider_groups")[-1]["npi"]:
                builder.value[-1]["provider_groups"].pop()

        if nprefix.endswith("provider_references.item") and event == "end_map":
            if not builder.value[-1].get("provider_groups"):
                builder.value.pop()

        builder.event(event, value)


def build_innetwork(init_row, parser, code_list=None, npi_list=None, provref_idx=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_map"):
            LOG.info(f"Writing data for code: {billing_code_type} {billing_code}")
            return builder.value, (nprefix, event, value)

        elif nprefix.endswith("negotiated_rates") and event == "start_array":
            if code_list:
                billing_code_type = builder.value["billing_code_type"]
                billing_code = str(builder.value["billing_code"])
                if (billing_code_type, billing_code) not in code_list:
                    LOG.debug(f"Skipping code: {billing_code_type} {billing_code}")
                    return None, (nprefix, event, value)

        # If no negotiated rates that match the criteria, return nothing
        elif nprefix.endswith("negotiated_rates") and event == "end_array":
            if not builder.value["negotiated_rates"]:
                LOG.debug(f"No rates for: {billing_code_type} {billing_code}")
                return None, (nprefix, event, value)

        elif nprefix.endswith("negotiated_rates.item") and event == "start_map":
            provgroups = []

        # Add the groups in the provider_reference to the existing provgroups
        elif nprefix.endswith("provider_references.item"):
            if provref_idx and (new_provgroups := provref_idx.get(value)):
                provgroups.extend(new_provgroups)

        # Merge the provgroups array if the existing provider_groups
        # if either exist
        elif nprefix.endswith("negotiated_rates.item") and event == "end_map":

            if builder.value["negotiated_rates"][-1].get("provider_references"):
                builder.value["negotiated_rates"][-1].pop("provider_references")

            builder.value["negotiated_rates"][-1].setdefault("provider_groups", [])
            builder.value["negotiated_rates"][-1]["provider_groups"].extend(provgroups)

            if not builder.value["negotiated_rates"][-1].get("provider_groups"):
                builder.value["negotiated_rates"].pop()

        elif nprefix.endswith("provider_groups.item") and event == "end_map":
            if not builder.value["negotiated_rates"][-1]["provider_groups"][-1]["npi"]:
                builder.value["negotiated_rates"][-1]["provider_groups"].pop()

        # Skip NPI numbers not in the list
        elif nprefix.endswith("npi.item"):
            if npi_list and value not in npi_list:
                continue

        # Make sure service codes are integers
        elif nprefix.endswith("service_code.item"):
            builder.event(event, int(value))
            continue

        builder.event(event, value)


# This should become an async function eventually
def build_remote_refs(provrefs, npi_list=None):

    new_provrefs = []

    for provref in provrefs:
        if not provref.get("location"):
            new_provrefs.append(provref)
        else:
            loc = provref.get("location")
            r = requests.get(loc)
            f = r.content

            parser = ijson.parse(f, use_float=True)

            prefix, event, value = next(parser)

            builder = ijson.ObjectBuilder()
            builder.event(event, value)

            for prefix, event, value in parser:

                if prefix.endswith("npi.item"):
                    if value not in npi_list:
                        continue

                elif prefix.endswith("provider_groups.item") and event == "end_map":
                    if not builder.value["provider_groups"][-1]["npi"]:
                        builder.value["provider_groups"].pop()

                builder.event(event, value)

            if builder.value.get("provider_groups"):
                provref["provider_groups"] = builder.value["provider_groups"]
                provref.pop("location")
                new_provrefs.append(provref)

    return new_provrefs
