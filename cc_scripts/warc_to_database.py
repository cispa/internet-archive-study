from warcio.archiveiterator import ArchiveIterator
from datetime import datetime
from hashlib import sha256
from psycopg2 import connect
from psycopg2.extras import Json
from multiprocessing import Pool
import sys
import os

DB_USER = "archive"
DB_PWD = "archive"
DB_HOST = "<database host>"
DB_PORT = 5432
DB_NAME = "<database name>"
ARCH = "c-crawl"
PROCESSES = 8

def get_wish_date_from_actual_date(actual_date):
    dates = ["20160115", "20160415", "20160715", "20161015",
            "20170115", "20170415", "20170715", "20171015",
            "20180115", "20180415", "20180715", "20181015",
            "20190115", "20190415", "20190715", "20191015",
            "20200115", "20200415", "20200715", "20201015",
            "20210115", "20210415", "20210715", "20211015",
            "20220115", "20220415", "20220715"]
    
    cur = datetime.now()
    for date in dates:
        d = datetime.strptime(date, "%Y%m%d")
        if abs(d - actual_date) < abs(cur - actual_date):
            cur = d
    return cur

def parse_warc_file(path):
    with open(path, 'rb') as stream:
        record = next(ArchiveIterator(stream))

        if record == None:
            return

        if record.rec_type != 'response':
            return

        actual_date = datetime.strptime(record.rec_headers.get_header('WARC-Date'), "%Y-%m-%dT%H:%M:%SZ")
        date = get_wish_date_from_actual_date(actual_date)
        url = os.path.basename(path).split("_")[1][:-5]
        status = record.http_headers.statusline.split(" ")[0]
        headers = {}
        for k,v in record.http_headers.headers:
            headers[k] = v

        final_url = record.rec_headers.get_header('WARC-Target-URI')

        for k,v in record.rec_headers.headers:
            # add another warc prefix, since some headers have not
            kk = f"WARC-{k}"
            headers[kk] = v

        valid_html = True if record.rec_headers.get_header('WARC-Truncated') is None else False

        content = record.content_stream().read()
        hash = sha256(content).hexdigest()

    return (ARCH, date, url, status, Json(headers), final_url, "", hash, valid_html, True, actual_date)

def parse_error_file(path):
    with open(path, 'r') as stream:
        error_msg = stream.read()

    actual_date, url = os.path.basename(path)[:-11].split("_")
    actual_date = datetime.datetime.strptime(actual_date, "%Y-%m-%d")
    date = get_wish_date_from_actual_date(actual_date)

    return (ARCH, date, url, -1, None, None, error_msg, None, False, False, actual_date)

def store_in_database(data):
    if data == None: return
    print(data)
    print(type(data))
    with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO responses (arch, date, url, status, headers, final_url, error, content_hash, valid_html, valid_headers, actual_date) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data)

def worker(path):
    f = os.path.basename(path)
    if f.endswith(".error"):
        data = parse_error_file(path)
    elif f.endswith(".warc"):
        data = parse_warc_file(path)
    else:
        data = None
    store_in_database(data)

def main():
    directory = sys.argv[1]

    files = os.listdir(directory)
    files = [os.path.join(directory, f) for f in files]

    with Pool(PROCESSES) as pool:
        pool.map(worker, files)

if __name__ == "__main__":
    main()