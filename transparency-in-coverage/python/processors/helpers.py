import json
import os
import csv
import glob
import hashlib
import ijson
import requests
from urllib.parse import urlparse
from schema import SCHEMA


def create_output_dir(output_dir, overwrite):
    if os.path.exists(output_dir):
        if overwrite:
            for file in glob.glob(f"{output_dir}/*"):
                os.remove(file)
    else:
        os.mkdir(output_dir)


def read_billing_codes_from_csv(filename):
    with open(filename, "r") as f:
        reader = csv.DictReader(f)
        codes = []
        for row in reader:
            codes.append((row["billing_code_type"], row["billing_code"]))
    return codes


def read_npi_from_csv(filename):
    with open(filename, "r") as f:
        reader = csv.DictReader(f)
        codes = []
        for row in reader:
            codes.append(row["npi"])
    return codes


def clean_url(url):
    parsed_url = urlparse(input_url)
    cleaned_url = (parsed_url[1] + parsed_url[2]).strip()
    return cleaned_url


def hashdict(data_dict):
    """Get the hash of a dict (sort, convert to bytes, then hash)"""
    if not data_dict:
        raise ValueError
    sorted_dict = dict(sorted(data_dict.items()))
    dict_as_bytes = json.dumps(sorted_dict).encode("utf-8")
    dict_hash = hashlib.md5(dict_as_bytes).hexdigest()
    return dict_hash


def rows_to_file(rows, output_dir):
    for row in rows:

        filename = row[0]
        row_data = row[1]
        fieldnames = SCHEMA[filename]
        file_loc = f"{output_dir}/{filename}.csv"

        if not os.path.exists(file_loc):
            with open(file_loc, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(row_data)

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
    }

    in_network_hash_id = hashdict(in_network_vals)
    in_network_vals["in_network_hash_id"] = in_network_hash_id

    rows.append(("in_network", in_network_vals))

    for neg_rate in obj.get("negotiated_rates", []):
        neg_rates_hash_id = hashdict(neg_rate)

        for provgroup in neg_rate["provider_groups"]:
            provgroup_vals = {
                "npi_numbers": provgroup["npi"],
                "tin_type": provgroup["tin"]["type"],
                "tin_value": provgroup["tin"]["value"],
                "negotiated_rates_hash_id": neg_rates_hash_id,
                "in_network_hash_id": in_network_hash_id,
                "root_hash_id": root_hash_id,
            }
            rows.append(("provider_groups", provgroup_vals))

        for neg_price in neg_rate["negotiated_prices"]:
            neg_price_vals = {
                "billing_class": neg_price["billing_class"],
                "negotiated_type": neg_price["negotiated_type"],
                "service_code": neg_price.get("service_code", None),
                "expiration_date": neg_price["expiration_date"],
                "additional_information": neg_price.get("additional_information", None),
                "billing_code_modifier": neg_price.get("billing_code_modifier", None),
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

    for provref in provrefs:
        for provgroup in provref["provider_groups"]:
            provgroup["npi"] = list(set(provgroup["npi"]) & set([1467915983]))

    provref["provider_groups"] = [x for x in provref["provider_groups"] if x["npi"]]

    provref_idx = {x["provider_group_id"]: x["provider_groups"] for x in provrefs}

    return provref_idx


def build_provrefs(init_row, parser, npi_list=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_array"):
            provrefs = builder.value
            return provrefs, (nprefix, event, value)

        if (nprefix, event) == ("provider_references.item", "start_map"):

            provref_builder = ijson.ObjectBuilder()
            provref_builder.event(event, value)

            for (nnprefix, event, value) in parser:

                provgroups = []

                if (nnprefix, event) == (nprefix, "end_map"):
                    if provref_builder.value["provider_groups"]:
                        builder.value.append(provref_builder.value)
                    break

                if (nnprefix, event, value) == (
                    "provider_references.item",
                    "map_key",
                    "provider_groups",
                ):

                    for nnnprefix, event, value in parser:

                        if (nnnprefix, event) == (
                            "provider_references.item",
                            "map_key",
                        ):
                            provref_builder.value["provider_groups"] = provgroups
                            break

                        if (nnnprefix, event) == (
                            "provider_references.item.provider_groups.item",
                            "start_map",
                        ):

                            row = (nnnprefix, event, value)
                            provgroup, row = build_prov_group(row, parser, npi_list)
                            if provgroup:
                                provgroups.append(provgroup)

                provref_builder.event(event, value)

        builder.event(event, value)


def build_prov_group(init_row, parser, npi_list=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_map"):
            prov_group = builder.value
            if not prov_group["npi"]:
                return None, (nprefix, event, value)
            return prov_group, (nprefix, event, value)

        if nprefix.endswith("provider_groups.item.npi.item"):
            if npi_list:
                if value not in npi_list:
                    continue

        builder.event(event, value)


def build_prov_group_arr(init_row, parser):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_array"):
            prov_group_arr = builder.value
            if not prov_group_arr:
                return None, (nprefix, event, value)
            return prov_group_arr, (nprefix, event, value)

        if (nprefix, event) == (
            "in_network.item.negotiated_rates.item.provider_groups.item",
            "start_map",
        ):
            row = (nprefix, event, value)
            prov_group_item, row = build_prov_group(row, parser)
            (nprefix, event, value) = row
            if prov_group_item:
                builder.value.append(prov_group_item)

        builder.event(event, value)


def build_neg_rate(init_row, parser, code_list, provref_idx=None):

    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_map"):
            builder.value.pop("provider_references")
            neg_rate_item = builder.value
            if not builder.value.get("provider_groups", None):
                return None, (nprefix, event, value)
            return neg_rate_item, (nprefix, event, value)

        if (nprefix) == (
            "in_network.item.negotiated_rates.item.provider_references.item"
        ):
            if builder.value.get("provicer_groups", None):
                builder.value["provider_groups"].extend(provref_idx[value])
            else:
                builder.value["provider_groups"] = provref_idx[value]

        if (nprefix, event) == (
            "in_network.item.negotiated_rates.item.provider_groups",
            "start_array",
        ):
            row = (nprefix, event, value)
            builder.event(event, value)
            prov_group_arr, row = build_prov_group_arr(
                row, parser, code_list, provref_idx
            )

            if prov_group_arr:
                builder.value.get("provider_groups", []).extend(prov_group_arr)
            (nprefix, event, value) = row

        builder.event(event, value)


def build_neg_rate_arr(init_row, parser, code_list, provref_idx=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_array"):
            neg_rate_arr = builder.value
            if not neg_rate_arr:
                return None, (nprefix, event, value)
            return neg_rate_arr, (nprefix, event, value)

        if (nprefix, event) == (
            "in_network.item.negotiated_rates.item",
            "start_map",
        ):
            row = (nprefix, event, value)
            neg_rate_item, row = build_neg_rate(row, parser, code_list, provref_idx)
            (nprefix, event, value) = row
            if neg_rate_item:
                builder.value.append(neg_rate_item)

        builder.event(event, value)


def build_innetwork(init_row, parser, code_list=None, provref_idx=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_map"):
            innetwork_item = builder.value
            return innetwork_item, (nprefix, event, value)

        if (nprefix, event) == (
            "in_network.item.negotiated_rates",
            "start_array",
        ) and code_list:
            billing_code_type = builder.value["billing_code_type"]
            billing_code = str(builder.value["billing_code"])

            if (billing_code_type, billing_code) not in code_list:
                return None, (nprefix, event, value)

        if (nprefix, event) == ("in_network.item.negotiated_rates", "start_array"):
            builder.event(event, value)
            row = (nprefix, event, value)
            neg_rate_arr, row = build_neg_rate_arr(row, parser, code_list, provref_idx)
            builder.value["negotiated_rates"] = neg_rate_arr
            (nprefix, event, value) = row

            if not neg_rate_arr:
                while (nprefix, event) != (prefix, "end_map"):
                    nprefix, event, value = next(parser)
                return None, (nprefix, event, value)

        builder.event(event, value)


def build_innetwork_arr(init_row, parser, code_list=None, provref_idx=None):
    prefix, event, value = init_row

    builder = ijson.ObjectBuilder()
    builder.event(event, value)

    for nprefix, event, value in parser:

        if (nprefix, event) == (prefix, "end_array"):
            innetwork_arr = builder.value
            return innetwork_arr, (nprefix, event, value)

        if (nprefix, event) == ("in_network.item", "start_map"):
            row = (nprefix, event, value)
            innetwork_item, row = build_innetwork(row, parser, code_list, provref_idx)
            if innetwork_item:
                builder.value.append(innetwork_item)


# def fetch_remoteprovrefs(provrefs):
#     new_provrefs = []
#     for provref in provrefs:
#         new_provref = provref.copy()
#         if loc := provref.get("location"):
#             r = requests.get(loc)
#             new_provref["provider_groups"] = r.json()["provider_groups"]
#             new_provref.pop("location")
#         new_provrefs.append(new_provref)
#     return new_provrefs
