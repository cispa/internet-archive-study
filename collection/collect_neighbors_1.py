import requests
import psycopg2
import argparse
from multiprocessing import Pool
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import traceback
import json
import time

from utils.database import get_conn
from config import PROCESSES

idx = 0

def collect_indicies(domain, date, ticks=25, days=30, attempt=0):
    global idx
    print(idx, domain, date)
    idx += 1
    if days >= 2000:
        # We will probably find no more entries
        return None

    from_date = (date - timedelta(days=days)).strftime("%Y%m%d")
    to_date = (date + timedelta(days=days)).strftime("%Y%m%d")
    url = f"https://web.archive.org/cdx/search/cdx?url={domain}&output=json&from={from_date}&to={to_date}"

    # print(url)
    try:
        res = requests.get(url)
        data = res.json()
    except json.decoder.JSONDecodeError:
        # This happens when too many reqeusts
        # no waiting
        if attempt < 0:
            time.sleep(5)
            return collect_indicies(domain, date, attempt=attempt+1)

        print("Too many req...")
        error = traceback.format_exc()
        error += "\n\n" + res.text
        out = (date, None, domain, None, None, error)
        return [out]
    except:
        error = traceback.format_exc()
        out = (date, None, domain, None, None, error)
        return [out]

    # parse
    indicies_all = dict()
    for d in data[1:]:
        cur_date = datetime.strptime(d[1], "%Y%m%d%H%M%S")
        indicies_all[cur_date] = d

    if len(indicies_all) == 0:
        # This should not be possible, but is somehow
        error = "This data was not found at all"
        out = (date, None, domain, None, None, error)
        return [out]

    dates = list(indicies_all.keys())
    dates.sort()
    closest_date = datetime(2000,1,1)

    for d in dates:
        if abs(date - d) < abs(closest_date - d):
            closest_date = d

    closest_idx = dates.index(closest_date)
    if closest_idx <= ticks or closest_idx + ticks >= len(dates) + 1:
        recursive_indicies = collect_indicies(domain, date, days=days*4)
        if recursive_indicies is not None:
            return recursive_indicies
        else:
            error = "Not enough ticks"
            out = (date, None, domain, None, None, error)
            return [out]

    indicies_out = list()
    for actual_date in dates[closest_idx - ticks : closest_idx + ticks + 1]:
        final_url = f"https://web.archive.org/web/{indicies_all[actual_date][1]}/{indicies_all[actual_date][2]}"
        status_code = indicies_all[actual_date][4]        
        indicies_out.append((date, actual_date, domain, final_url, status_code, ""))

    return indicies_out

def main():
    for i in range(100):
        limit = 1000
        offset = i * 1000
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(f"""
            SELECT a.url, a.date
            FROM (SELECT url, date, count(*) FROM responses WHERE archived_url IS NOT NULL AND arch = 'archiveorg' GROUP BY url, date) as a
            FULL JOIN (SELECT url, date, count(*) FROM archiveorg_indices WHERE error NOT LIKE '%Traceback%' GROUP BY url, date) as b
            ON a.url = b.url and a.date = b.date
            WHERE b.date is NULL ORDER BY a.url, b.date
            LIMIT {limit} OFFSET {offset};
        """)
        
        if cur.rowcount == 0:
            break

        data = cur.fetchall()
        cur.close()
        conn.close()


        with Pool(PROCESSES) as pool:
            res = pool.starmap(collect_indicies, data)

            res = [item for sub in res for item in sub]

            conn = get_conn()
            cur = conn.cursor()

            cur.executemany("INSERT INTO archiveorg_indices (date, actual_date, url, final_url, status, error) VALUES (%s, %s, %s, %s, %s, %s)", res)

            cur.close()
            conn.close()


if __name__ == "__main__":
    main()
