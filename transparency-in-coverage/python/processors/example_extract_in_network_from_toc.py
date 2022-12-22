import sqlite3
import re
from mrfutils import MRFOpen

dbname = "anthem-in-network.db"

con = sqlite3.connect(dbname)
cur = con.cursor()

cur.execute(
    """
    CREATE TABLE urls (
        url PRIMARY KEY
    )
    """
)
cur.execute(
    """
    CREATE TABLE plan_urls (
        plan,
        url,
        PRIMARY KEY (plan, url),
        FOREIGN KEY (url) REFERENCES urls(url)
    )
    """)

con.commit()

toc_url ='https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-12-01_anthem_index.json.gz'

with MRFOpen(toc_url) as f:
    for line in f:
        strline = str(line)
        if 'in-network' in strline:
            g = re.findall("\"description\":\"(.+?)\",\"location\":\"(.+?)\"", strline)
            for plan, url in g:
                try:
                    cur.execute("INSERT INTO urls (url) VALUES (?)", (url,))
                    row_id = cur.lastrowid
                except sqlite3.IntegrityError:
                    row_id = cur.execute("SELECT * FROM urls WHERE url = ?;", (url,)).lastrowid
                cur.execute("INSERT OR IGNORE INTO plan_urls (plan, url) VALUES (?,?)", (plan, row_id))
            con.commit()