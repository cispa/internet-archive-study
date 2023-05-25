import json
from utils.database import get_conn
from adblockparser import AdblockRules

def update_trackers(year):
    conn = get_conn(True)
    cur = conn.cursor()
    
    with open("misc/disconnect_me_trackers.json", "r") as f:
        trackers_disconnect = set(json.load(f))

    with open("misc/easyprivacy_trackingservers.txt", "r") as f:
        data = [d.strip() for d in f.readlines()]
        trackers_easyprivacy = AdblockRules(data)

    while True:
        # I *** up serial, so we need to use url and req_url
        cur.execute(f"""
            SELECT url, request_url, request_site
            FROM dynamic_script_inclusions_{year}
            WHERE request_url != 'ERROR'
            AND tracker is NULL
            LIMIT 100000
        """)

        if cur.rowcount == 0:
            break
        print(f"Updating {cur.rowcount}")
        for url, r_url, request_site in cur.fetchall():
            print(request_site)
            tracker = {
                "disconnect": request_site if request_site in trackers_disconnect else "",
                "easyprivacy": request_site if trackers_easyprivacy.should_block(request_site) else "", 
            }

            cur.execute(f"""
            UPDATE dynamic_script_inclusions_{year} SET tracker=%s WHERE url=%s AND request_url=%s
            """, (tracker, url, r_url))

def main(year="2016"):
    update_trackers(year)

if __name__ == '__main__':
    main()
