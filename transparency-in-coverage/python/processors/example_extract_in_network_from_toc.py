"""
This file shows how to capture (and put into a small table)
all of the in-network files in Anthem's gigantic 15GB ZIPPED
JSON file.
"""
import sqlite3
import re
from mrfutils import MRFOpen
import time


TESTING = False

anthem_toc_url ='https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-12-01_anthem_index.json.gz'
dbname = "anthem-in-network.db"

con = sqlite3.connect(dbname)
cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS urls (id INTEGER PRIMARY KEY, url UNIQUE)")
cur.execute("CREATE TABLE IF NOT EXISTS plans (id INTEGER PRIMARY KEY, name, description)")
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS plan_url (
        plan_id,
        url_id,
        FOREIGN KEY (url_id) REFERENCES urls(id),
        FOREIGN KEY (plan_id) REFERENCES plans(id)
    )
    """)

con.commit()


def database_write(sql, values=None):
    if not TESTING:
        db_cur = con.cursor()
        attempt = 0
        error = False
        while True:
            attempt += 1
            try:
                db_cur.execute(sql, values)
                con.commit()
            except Exception as e:
                print("Trying again. ", attempt, ", ", e)
                error = True
                if 'UNIQUE' in str(e):
                    break
                else:
                    print("Database locked.")
                    time.sleep(1)
                continue
            else:
                break
        else:
            print('Couldn\'t access database.')
        return db_cur.lastrowid


def database_read(sql, values=None, records='All'):
    db_cur = con.cursor()
    attempt = 0
    rows = []
    while True:
        attempt += 1
        try:
            if values is not None:
                db_cur.execute(sql, values)
            else:
                db_cur.execute(sql)
            if records == 'All':
                rows = db_cur.fetchall()
            else:
                rows = db_cur.fetchone()
        except Exception as e:
            print("Trying again. ", attempt, ", ", e)
            time.sleep(1)
            if 'UNIQUE' in str(e):
                break
            continue
        else:
            break
    else:
        print('Couldn\'t access database.')
    return rows


def insert_plan_url(plan_name, description, url):
    """
    :param plan_name: str
    :param url: str
    """
    sql = 'SELECT id FROM plans WHERE (name, description) = (?, ?)'
    res = database_read(sql, values=[plan_name, description], database=dbname, records='1')
    if res is None:
        sql = 'INSERT INTO plans (name, description) VALUES (?, ?)'
        plan_id = database_write(sql, values=[plan_name, description], database=dbname)
    else:
        plan_id = res[0]

    sql = 'SELECT id FROM urls WHERE url = ?'
    res = database_read(sql, values=[url, ], database=dbname, records='1')
    if res is None:
        sql = 'INSERT INTO urls (url) VALUES (?)'
        url_id = database_write(sql, values=[url, ], database=dbname)
    else:
        url_id = res[0]

    cur.execute("INSERT OR IGNORE INTO plan_url (plan_id, url_id) VALUES (?, ?)", (plan_id, url_id))

# Regular expression for capturing plan and location information
# in the bytestrings returned by f.readlines()


desc_loc_pat = re.compile(r'"description":"(.+?)","location":"(.+?)"', re.S)
plan_pat = re.compile(r'"plan_name":"(.+?)"', re.S)
print("\n\n")
with MRFOpen(anthem_toc_url) as f:
    for line in f:
        line = str(line)
        if 'in-network' in line:
            g = desc_loc_pat.findall(line)
            match = plan_pat.search(line)
            plan_name = match.group(1)
            print(f'\r{plan_name}', end="")
            for description, url in g:
                insert_plan_url(plan_name, description, url)
                con.commit()

con.close()
