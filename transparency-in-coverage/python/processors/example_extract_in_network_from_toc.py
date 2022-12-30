#!/usr/bin/python3

"""
This file shows how to capture (and put into a small table)
all of the in-network files in Anthem's gigantic 15GB ZIPPED
JSON file.
"""
import re
from urllib.parse import urlparse

from mrfutils import JSONOpen, _filename_hash

import mysql.connector as connector

def insert_file_url(cnx, url):
    """
    :param plan_name: str
    :param url: str
    """
    cur = cnx.cursor()

    o = urlparse(url)
    filename = o.path.split('/')[-1]
    filename_hash = _filename_hash(filename)
    filename = filename.replace('.gz', '').replace('.json', '')

    cur.execute("INSERT IGNORE files (filename_hash, filename, url) VALUES (%s, %s, %s)", (filename_hash, filename, url))

def main():
    anthem_toc_url ='https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-12-01_anthem_index.json.gz'
    cnx = connector.connect(user='rl', password='trustno1', host='127.0.0.1', database='quest')

    # Regular expression for capturing plan and location information
    # in the bytestrings returned by f.readlines()
    desc_loc_pat = "\"description\":\"(.+?)\",\"location\":\"(.+?)\""
    plan_pat = r'"plan_name":"(.+?)"'

    seen_urls = dict()

    with JSONOpen(anthem_toc_url) as f:
        for line in f:
            line = str(line)
            if 'in-network' in line:
                g = re.findall(desc_loc_pat, line)
                match = re.search(plan_pat, line)
                plan_name = match.group(1)
                for description, url in g:
                    if not url in seen_urls:
                        print(url)
                        insert_file_url(cnx, url)
                        cnx.commit()
                        seen_urls[url] = True

if __name__ == "__main__":
    main()

