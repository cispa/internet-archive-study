import gzip
import os
import signal
from contextlib import contextmanager
from hashlib import sha256
import collections
from multiprocessing import Pool

import psycopg2 as psycopg2
import requests

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD, STORAGE, PREFIX

DATE = "20221107"

DEBUG = False
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

RELEVANT_HEADERS = {'x-frame-options', 'content-security-policy', 'strict-transport-security'}

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
    except TimeoutError:
        pass
    finally:
        # Unregister the signal so it won't be triggered, if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def collect_data(tranco_file):
    chunks = collections.defaultdict(list)

    CHUNKS = 4

    with open(tranco_file) as fh:
        domain_list = fh.readlines()
        for i, row in enumerate(domain_list):
            id_, domain = row.strip().split(',')
            chunks[i % CHUNKS].append((id_, domain))

    with Pool(CHUNKS) as p:
        p.map(worker, chunks.values())

def worker(chunk):
    sess = requests.Session()
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    conn.autocommit = True
    cursor = conn.cursor()
    for id_, domain in chunk:
        url = f"https://web.archive.org/cdx/search/cdx?url={domain}&filter=statuscode:200&from=20221226&to=20221228" \
              f"&output=json"
        with timeout(60):
            print(domain)
            try:
                resp = sess.get(url)
                content = resp.content
                content_hash = sha256(content).hexdigest()
                file_dir = os.path.join(STORAGE, content_hash[0], content_hash[1])
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                file_path = os.path.join(file_dir, f"{content_hash}.gz")
                with gzip.open(file_path, "wb") as fh:
                    fh.write(content)
                cursor.execute("""
                INSERT INTO cdx_responses (tranco_id, domain, timestamp, content_hash) VALUES (%s, %s, NOW(), %s)
                """, (id_, domain, content_hash))
            except Exception as e:
                print(e)
        # time.sleep(1)


def main(tranco_file):
    collect_data(tranco_file)

if __name__ == '__main__':
    main('tranco_Z2QWG.csv')