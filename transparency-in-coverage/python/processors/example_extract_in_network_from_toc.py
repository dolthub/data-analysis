"""
This file shows how to capture (and put into a small table)
all of the in-network files in Anthem's gigantic 15GB ZIPPED
JSON file.
"""
import sqlite3
import re
from mrfutils import JSONOpen

anthem_toc_url = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-12-01_anthem_index.json.gz"
dbname = "anthem-in-network.db"

con = sqlite3.connect(dbname)
cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS urls (id INTEGER PRIMARY KEY, url UNIQUE)")
cur.execute(
    "CREATE TABLE IF NOT EXISTS plans (id INTEGER PRIMARY KEY, name, description)"
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS plan_url (
        plan_id,
        url_id,
        FOREIGN KEY (url_id) REFERENCES urls(id),
        FOREIGN KEY (plan_id) REFERENCES plans(id)
    )
    """
)

con.commit()


def insert_plan_url(plan_name, description, url):
    """
    :param plan_name: str
    :param url: str
    """
    cur.execute(
        "SELECT id FROM plans WHERE (name, description) = (?, ?)",
        (plan_name, description),
    )
    if (res := cur.fetchone()) is None:
        cur.execute(
            "INSERT INTO plans (name, description) VALUES (?, ?)",
            (plan_name, description),
        )
        plan_id = cur.lastrowid
    else:
        plan_id = res[0]

    cur.execute("SELECT id FROM urls WHERE url = ?", (url,))
    if (res := cur.fetchone()) is None:
        cur.execute("INSERT INTO urls (url) VALUES (?)", (url,))
        url_id = cur.lastrowid
    else:
        url_id = res[0]

    cur.execute(
        "INSERT OR IGNORE INTO plan_url (plan_id, url_id) VALUES (?, ?)",
        (plan_id, url_id),
    )


# Regular expression for capturing plan and location information
# in the bytestrings returned by f.readlines()
desc_loc_pat = '"description":"(.+?)","location":"(.+?)"'
plan_pat = r'"plan_name":"(.+?)"'

with JSONOpen(anthem_toc_url) as f:
    for line in f:
        line = str(line)
        if "in-network" in line:
            g = re.findall(desc_loc_pat, line)
            match = re.search(plan_pat, line)
            plan_name = match.group(1)
            for description, url in g:
                insert_plan_url(plan_name, description, url)
                con.commit()

con.close()
