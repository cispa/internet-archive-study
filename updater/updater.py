import gzip
import json
import os

import multiprocessing
import urllib3

from utils.headers import classify_headers
from utils.database import get_conn

from config import PROCESSES, STORAGE

args = None

def set_valid(table, reset=False):
    conn = get_conn(True)
    cur = conn.cursor()

    if reset:
        cur.execute(f"""UPDATE {table} SET valid = False, actual_date = NULL WHERE arch!='c-crawl'""")
        print(f"Resetting {cur.rowcount}")

    cur.execute(f"""
    UPDATE {table} SET actual_date = 
    COALESCE(headers->>'memento-datetime', headers->>'Memento-Datetime')::timestamp 
    WHERE actual_date IS NULL AND (headers->>'memento-datetime' is not null or headers->>'Memento-Datetime' 
    is not null);
    """)
    print(f"Setting actual_date {cur.rowcount}")

    cur.execute(f"""
    UPDATE {table} SET valid = True 
    WHERE abs(EXTRACT(epoch FROM date - actual_date)) < 60*60*24 * 42 
    AND actual_date IS NOT NULL
    AND NOT valid;
    """)
    print(f"Timestamp within 4 weeks: {cur.rowcount}")

    cur.execute(f"""
    UPDATE {table} SET valid = False WHERE arch != 'c-crawl' AND actual_date IS NOT NULL 
    AND headers::text NOT ILIKE '%x-archive-orig%'
    """)
    print(f"Datetime, but no other headers: {cur.rowcount}")

    cur.execute(f"""
    UPDATE {table} SET archived_url =  substring(final_url FROM '(?<=[0-9]+(mp_)?/)http.*$') 
    WHERE actual_date IS NOT NULL AND archived_url IS NULL AND arch != 'c-crawl';
    """)
    print(f"archived_url: {cur.rowcount}")


def update_headers(table, mode):
    conn = get_conn(True)
    cur = conn.cursor()

    if mode == 'archive':
        qry = """
        SELECT id, headers, arch, archived_url FROM """ + table + """
        WHERE actual_date IS NOT NULL AND
        security_headers IS NULL
        """
        update_qry = "UPDATE " + table + " SET security_headers=%s WHERE id=%s"
    elif mode == 'live':
        qry = """
        SELECT id, headers, 'live', end_url FROM """ + table + """"
        WHERE status_code = 200 AND
        security_headers IS NULL
        """
        update_qry = "UPDATE " + table + " SET security_headers=%s WHERE id=%s"

    while True:
        cur.execute(qry)

        if cur.rowcount == 0:
            break

        process_data = []

        for resp_id, headers, arch, archived_url in cur.fetchall():
            for h in list(headers.keys()):
                if h.lower() != h:
                    headers[h.lower()] = headers[h]
            parsed_url = urllib3.util.parse_url(archived_url)
            origin = f"{parsed_url.scheme}://{parsed_url.host}"
            process_data.append((resp_id, arch, headers, origin))

        def worker(argv):
            resp_id, arch, headers, origin = argv
            if arch in ('c-crawl', 'live'):
                result = classify_headers(headers)
            else:
                new_headers = {h[len('x-archive-orig-'):]: v for h, v in headers.items() if
                               h.startswith('x-archive-orig-')}
                result = classify_headers(new_headers, origin)

            return resp_id, result

        print(f"Updating {len(process_data)} rows")
        with multiprocessing.Pool(PROCESSES) as pool:
            results = pool.map(worker, process_data)
            for resp_id, result in results:
                cur.execute(update_qry, (json.dumps(result), resp_id))
        break


def update_length(table):
    conn = get_conn(True)
    cur = conn.cursor()

    while True:
        cur.execute("""
        SELECT id, content_hash FROM """ + table + """  WHERE length IS NULL and status != -1 LIMIT 10000
        """)

        if cur.rowcount == 0:
            break

        print(f"Updating {cur.rowcount} rows")
        for r_id, content_hash in cur.fetchall():
            file_dir = os.path.join(STORAGE, content_hash[0], content_hash[1])
            file_path = os.path.join(file_dir, f"{content_hash}.gz")
            with gzip.open(file_path) as fh:
                length = len(fh.read())
                cur.execute("UPDATE " + table + "  SET length=%s WHERE id=%s", (length, r_id))


def update_trackers(table):
    conn = get_conn(True)
    cur = conn.cursor()

    with open("misc/disconnect_me_trackers.json", "r") as f:
        trackers_disconnect = set(json.load(f))

    while True:

        cur.execute("""
            SELECT id, script_info 
            FROM """ + table + """ 
            WHERE script_info IS NOT NULL
            AND trackers IS NULL
            LIMIT 5000
        """)

        if cur.rowcount == 0:
            break
        print(f"Updating {cur.rowcount}")
        for r_id, script_info in cur.fetchall():
            trackers = set(script_info['sites']) & trackers_disconnect
            cur.execute("""
            UPDATE """ + table + """  SET trackers=%s WHERE id=%s
            """, (list(trackers), r_id))

def main(table, mode='archive'):
    set_valid(table)
    update_length(table)
    update_headers(table, mode)
    update_trackers(table)

if __name__ == '__main__':
    main()
