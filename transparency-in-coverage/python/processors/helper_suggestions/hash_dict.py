import hashlib
import json


def hashdict(data_dict: dict):
    if not data_dict:
        raise ValueError

    sorted_tups = sorted(data_dict.items())
    dict_as_bytes = json.dumps(sorted_tups).encode("utf-8")
    dict_hash = hashlib.sha256(dict_as_bytes).hexdigest()[:16]

    return dict_hash
