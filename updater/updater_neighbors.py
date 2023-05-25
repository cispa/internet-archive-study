import json
import multiprocessing
import urllib3

from utils.headers import classify_headers
from utils.database import get_conn
from adblockparser import AdblockRules

from config import PROCESSES

def update_headers():
    conn = get_conn(True)
    cur = conn.cursor()

    qry = """
    SELECT id, headers, arch, archived_url FROM responses_neighbors
    WHERE error IS NULL
    AND security_headers IS NULL
    LIMIT 10000
    """
    update_qry = "UPDATE responses_neighbors SET security_headers=%s WHERE id=%s"

    while True:
        cur.execute(qry)

        if cur.rowcount == 0:
            break

        process_data = []

        for resp_id, headers, arch, archived_url in cur.fetchall():
            if not headers:
                print(resp_id, headers, arch, archived_url)
            for h in list(headers.keys()):
                if h.lower() != h:
                    headers[h.lower()] = headers[h]
            parsed_url = urllib3.util.parse_url(archived_url)
            origin = f"{parsed_url.scheme}://{parsed_url.host}"
            process_data.append((resp_id, arch, headers, origin))

        def worker(argv):
            resp_id, arch, headers, origin = argv
            new_headers = {h[len('x-archive-orig-'):]: v for h, v in headers.items() if
                            h.startswith('x-archive-orig-')}
            result = classify_headers(new_headers, origin)

            return resp_id, result

        print(f"Updating {len(process_data)} rows")
        with multiprocessing.Pool(PROCESSES) as pool:
            results = pool.map(worker, process_data)
            for resp_id, result in results:
                pass
                cur.execute(update_qry, (json.dumps(result), resp_id))

def update_trackers():
    conn = get_conn(True)
    cur = conn.cursor()
    
    with open("misc/disconnect_me_trackers.json", "r") as f:
        trackers_disconnect = set(json.load(f))

    while True:

        cur.execute("""
            SELECT id, src_inclusion_info 
            FROM responses_neighbors
            WHERE trackers_src_inclusion IS NULL
            AND error IS NULL
            LIMIT 100000
        """)

        if cur.rowcount == 0:
            break
        print(f"Updating {cur.rowcount}")
        for r_id, script_info in cur.fetchall():
            trackers = set()
            for element in script_info:
                trackers = trackers.union(set(script_info[element]['sites']) & trackers_disconnect)
            cur.execute("""
            UPDATE responses_neighbors SET trackers_src_inclusion=%s WHERE id=%s
            """, (list(trackers), r_id))

def update_trackers_easyprivacy():
    conn = get_conn(True)
    cur = conn.cursor()

    with open("misc/easyprivacy_trackingservers.txt", "r") as f:
        data = [d.strip() for d in f.readlines()]
        trackers_easyprivacy = AdblockRules(data)

    while True:

        cur.execute("""
            SELECT id, src_inclusion_info 
            FROM responses_neighbors
            WHERE trackers_easyprivacy IS NULL
            AND error IS NULL
            LIMIT 100000
        """)

        if cur.rowcount == 0:
            break
        print(f"Updating {cur.rowcount}")
        for r_id, script_info in cur.fetchall():
            trackers = set()
            for element in script_info:
                for site in script_info[element]['sites']:
                    if trackers_easyprivacy.should_block(site):
                        trackers.add(site)
            cur.execute("""
            UPDATE responses_neighbors SET trackers_easyprivacy=%s WHERE id=%s
            """, (list(trackers), r_id))

def main():
    update_headers()
    update_trackers()
    update_trackers_easyprivacy()

if __name__ == '__main__':
    main()
