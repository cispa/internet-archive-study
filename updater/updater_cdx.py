import gzip
import json
import os.path
from collections import defaultdict
from hashlib import sha256
from json import JSONDecodeError
from multiprocessing import Process
from time import sleep

import requests
from psycopg2 import connect

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD, STORAGE, APIs

SESSION = requests.Session()


def setup():
    with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        with connection.cursor() as cursor:
            print('<<< CREATE TABLE cdx_archive_headers >>>')

            cursor.execute("""
                CREATE TABLE cdx_archive_headers (
                    id SERIAL PRIMARY KEY,
                    cdx_responses_id INTEGER,
                    url_date DATE,
                    start_url VARCHAR(128),
                    end_url VARCHAR(1024),
                    headers JSONB,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    content_hash varchar(64) DEFAULT NULL,
                    status_code int default -1
                );
            """)
            print('<<< CREATE INDEX ON cdx_archive_headers >>>')
            for column in ['cdx_responses_id', 'start_url', 'end_url', 'timestamp', 'content_hash', 'status_code']:
                cursor.execute(f"CREATE INDEX ON cdx_archive_headers ({column})")
            cursor.execute('CREATE UNIQUE INDEX ON cdx_archive_headers (cdx_responses_id, url_date)')

            print('<<< SETUP COMPLETE >>>')


def get_first_200_data(data):
    result = {}
    for idx, entry in reversed(list(enumerate(data, start=1))):
        date = entry[1][:8]
        if entry[4] == "200" and date not in result:
            result[entry[1][:8]] = idx
    return result


def worker(worker_id, timeout):
    print(f'Worker {worker_id} started ...')
    with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = False
        with connection.cursor() as cursor:
            while True:
                cursor.execute("BEGIN;")
                cursor.execute(
                    "SELECT id, content_hash FROM cdx_responses WHERE NOT parsed AND status_code=200 LIMIT 1 FOR UPDATE SKIP LOCKED;")
                result = cursor.fetchone()
                if result is None:
                    cursor.execute("COMMIT;")
                    cursor.execute("END;")
                    break

                cdx_id, content_hash = result

                with gzip.open(os.path.join(STORAGE, content_hash[0], content_hash[1], f"{content_hash}.gz")) as file:
                    try:
                        data = json.load(file)
                    except JSONDecodeError:
                        cursor.execute("UPDATE cdx_responses SET parsed=TRUE, error=%s WHERE id=%s;",
                                       ("empty json", cdx_id))
                        cursor.execute("COMMIT;")
                        cursor.execute("END;")
                        continue

                if not data:
                    cursor.execute("UPDATE cdx_responses SET parsed=TRUE, error=%s WHERE id=%s;",
                                   ("empty json", cdx_id))
                    cursor.execute("COMMIT;")
                    cursor.execute("END;")
                    continue

                # get most recent 200 result
                first_200_data = get_first_200_data(data[1:])
                if not first_200_data:
                    cursor.execute("UPDATE cdx_responses SET parsed=TRUE, error=%s WHERE id=%s;",
                                   ("no snapshot with status code 200", cdx_id))
                    cursor.execute("COMMIT;")
                    cursor.execute("END;")
                    continue

                for date, index in first_200_data.items():
                    _, timestamp, url, _, status_code, _, _ = data[index]
                    assert status_code == "200"

                    endpoint = APIs['archiveorg'].format(date=timestamp, url=url)

                    with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as c2:
                        with c2.cursor() as cursor2:
                            cursor2.execute("""SELECT DISTINCT end_url FROM cdx_archive_headers""")
                            urls = {url for url, in cursor2.fetchall()}

                    if endpoint in urls:
                        with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as c2:
                            with c2.cursor() as cursor2:
                                cursor2.execute(
                                    """SELECT headers, content_hash, status_code FROM cdx_archive_headers WHERE end_url=%s""",
                                    (endpoint,))
                                response_headers, content_hash, status_code = cursor2.fetchone()

                                # persist response
                                cursor2.execute(
                                    "INSERT INTO cdx_archive_headers (cdx_responses_id, url_date, start_url, end_url, headers, content_hash, status_code) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                    (cdx_id, date, endpoint, endpoint, json.dumps(response_headers), content_hash,
                                     status_code))
                        continue

                    print(f'[W-{worker_id}] querying {endpoint}')
                    try:
                        r = SESSION.get(endpoint, timeout=timeout, allow_redirects=True)
                    except Exception as error:
                        print(f'[W-{worker_id}] ERROR querying {endpoint}')
                        cursor.execute("UPDATE cdx_responses SET error=%s WHERE id=%s;",
                                       (json.dumps(f"{error}"), cdx_id))
                    else:
                        print(f'[W-{worker_id}] SUCCESS querying {endpoint}')

                        content = r.content
                        content_hash = sha256(content).hexdigest()
                        file_dir = os.path.join(STORAGE, content_hash[0], content_hash[1])
                        if not os.path.exists(file_dir):
                            os.makedirs(file_dir)
                        file_path = os.path.join(file_dir, f"{content_hash}.gz")
                        with gzip.open(file_path, "wb") as fh:
                            fh.write(content)

                        response_headers = {h.lower(): r.headers[h] for h in r.headers}
                        response_headers = json.dumps(response_headers)

                        # persist response
                        with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as c2:
                            with c2.cursor() as insertion:
                                insertion.execute(
                                    "INSERT INTO cdx_archive_headers (cdx_responses_id, url_date, start_url, end_url, headers, content_hash, status_code) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                    (cdx_id, date, endpoint, r.url, response_headers, content_hash, r.status_code))

                cursor.execute("UPDATE cdx_responses SET parsed=TRUE WHERE id=%s;", (cdx_id,))

                cursor.execute("COMMIT;")
                cursor.execute("END;")
                sleep(1)

    print(f'Worker {worker_id} terminates!')


def update_cdx():
    print('START cdx update.....')
    processes = list()
    for wid in range(8):
        p = Process(target=worker, args=(wid, 60))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    print('DONE.')


def main():
    # setup()
    update_cdx()


if __name__ == '__main__':
    main()
