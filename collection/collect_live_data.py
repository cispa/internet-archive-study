import gzip
import json
import os
import signal
from contextlib import contextmanager
from hashlib import sha256
from multiprocessing import Pool
from time import time_ns

import psycopg2 as psycopg2
import requests

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD, USER_AGENT, STORAGE

TABLE_NAME = 'live_headers'

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


def crawl(url, headers=None, user_agent=USER_AGENT):
    if headers is None:
        headers = dict()
    headers['User-Agent'] = user_agent

    try:
        with timeout(60):
            start = time_ns()
            r = requests.get(url, headers=headers, timeout=30)
            duration = time_ns() - start
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


def worker(id_, url):
    success, data = crawl(url)
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    conn.autocommit = True
    cursor = conn.cursor()
    if success:
        end_url, status_code, headers, content_hash, duration = data
        cursor.execute(f"""
        INSERT INTO {TABLE_NAME} (tranco_id, domain, start_url, end_url, headers, duration, status_code, content_hash) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_, url.split('.', 1)[1], url, end_url, headers, duration, status_code, content_hash))
    else:
        cursor.execute(f"""
        INSERT INTO {TABLE_NAME} (tranco_id, domain, start_url, end_url) 
        VALUES (%s, %s, %s, %s)
        """, (id_, url.split('.', 1)[1], url, data))

    conn.close()


def collect_data(tranco_file):
    urls = []
    with open(tranco_file) as file:
        for line in file:
            id_, domain = line.strip().split(',')
            url = f"http://www.{domain}"
            urls.append((id_, url))

    with Pool(8) as p:
        p.starmap(worker, urls)


def main(tranco_file="live_dataset.csv"):
    setup()
    collect_data(tranco_file)


if __name__ == '__main__':
    main()
