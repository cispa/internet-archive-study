import gzip
import json
import os
import signal
import time
from contextlib import contextmanager
from hashlib import sha256
from multiprocessing import Pool

import psycopg2 as psycopg2
import requests

from config import PREFIX, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD, USER_AGENT, STORAGE

""" Archival data used for section 5.3 """

TABLE_NAME = 'historical_data'

DATE = "20230417"

DEBUG = False


def setup():
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    tranco_id INTEGER,
                    domain VARCHAR(128),
                    start_url VARCHAR(128),
                    end_url TEXT DEFAULT NULL,
                    headers JSONB DEFAULT NULL,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    duration NUMERIC DEFAULT NULL,
                    content_hash VARCHAR(64) DEFAULT NULL,
                    status_code INT DEFAULT -1
                );
            """)

            print(f'<<< CREATE INDEX ON {TABLE_NAME} >>>')
            for column in ['tranco_id', 'domain', 'start_url', 'end_url', 'timestamp', 'duration', 'content_hash',
                           'status_code']:
                cursor.execute(f"CREATE INDEX ON {TABLE_NAME} ({column})")


def raise_timeout(signum, frame):
    if DEBUG:
        print('Hard kill via signal!')
        print(signum)
        print(frame)
    raise TimeoutError


@contextmanager
def timeout(seconds):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after the specified seconds.
    signal.alarm(seconds)
    try:
        yield
    finally:
        # Unregister the signal, so it won't be triggered, if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def crawl(url, headers=None, user_agent=USER_AGENT, sess=None):
    if sess is None:
        sess = requests.Session()
    if headers is None:
        headers = dict()
    headers['User-Agent'] = user_agent

    try:
        with timeout(60):
            archive_request_url = url
            start = time.time_ns()
            r = sess.get(archive_request_url, headers=headers, timeout=30)
            duration = time.time_ns() - start
    except TimeoutError:
        return False, 'Hard kill due to signal timeout!'
    except Exception as exp:
        return False, str(exp)

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

    return True, [r.url, r.status_code, response_headers, content_hash, duration]


def worker(argv):
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    conn.autocommit = True
    cursor = conn.cursor()
    sess = requests.Session()
    for id_, url in argv:
        time.sleep(0.2)
        success, data = crawl(url, sess=sess)
        if success:
            end_url, status_code, headers, content_hash, duration = data
            if status_code == 429:
                print("got 429ed")
            cursor.execute(f"""
                INSERT INTO {TABLE_NAME} (tranco_id, domain, start_url, end_url, headers, duration, status_code, content_hash) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (id_, url[len(f"https://web.archive.org/web/{DATE}/{PREFIX}"):], url, end_url, headers, duration,
                  status_code, content_hash))
        else:
            print(f"{url} failed")
            cursor.execute(f"""
                INSERT INTO {TABLE_NAME} (tranco_id, domain, start_url, end_url) 
                VALUES (%s, %s, %s, %s)
            """, (id_, url[len(f"https://web.archive.org/web/{DATE}/{PREFIX}"):], url, data))

    conn.close()


def collect_data(tranco_file):
    # reset failed attempts
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE status_code in (-1, 429)")

    cursor.execute(f"SELECT start_url FROM {TABLE_NAME}")
    worked_urls = set([x[0] for x in cursor.fetchall()])

    urls = []
    with open(tranco_file) as file:
        for line in file:
            id_, domain = line.strip().split(',')
            url = f"https://web.archive.org/web/{DATE}/{PREFIX}{domain}"
            if url in worked_urls:
                continue
            urls.append((id_, url))

    WORKERS = 8

    chunks = [urls[i:i + len(urls) // WORKERS] for i in range(0, len(urls), len(urls) // WORKERS)]

    with Pool(WORKERS) as p:
        p.map(worker, chunks)


def main(tranco_file="live_dataset.csv"):
    setup()
    collect_data(tranco_file)


if __name__ == '__main__':
    main()
