import gzip
import json
import logging
import os
import time
import traceback
from multiprocessing import Process
from datetime import datetime

from bs4 import BeautifulSoup
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime as dt
from hashlib import sha256
import requests

from utils.database import get_conn
from config import STORAGE, PREFIX, APIs

session = requests.Session()


def crawl_all(all_urls, table):
    workers = []
    for archive in all_urls.keys():
        p = Process(target=collect_data, args=(archive, all_urls[archive], table))
        p.start()
        workers.append(p)

    for p in workers:
        p.join()
    pass


def check_redirect(archive, response):
    # check if a redirect page is shown.
    # if so, follow it
    content = response.content
    headers = [k.lower() for k in response.headers.keys()]

    # If memento-timestamp exists and no x-archive-orig
    # we have a redirect, else not
    if not ("memento-datetime" in headers
            and "x-archive-orig" not in "".join(headers)):
        return ""

    soup = BeautifulSoup(content, 'html.parser')

    if archive == "congress":
        results = soup.select("p[class='impatient']")
        if len(results) == 0:
            # not valid
            return ""
        redirect_url = str(results[0].find("a")['href'])
    elif archive == "iceland":
        results = soup.select("div[class='redirect']")
        div = results[0].find("div")
        redirect_url = str(div.find("a")['href'])
    else:
        # Other archives are negligible
        redirect_url = ""
    return redirect_url


def collect_data(archive, urls, table):
    # urls =  (url, date)
    conn = get_conn(True)
    cur = conn.cursor()

    sess = requests.Session()
    sess.headers["Connection"] = "Keep-Alive"

    total = len(urls)

    for i, url_date in enumerate(urls):
        url, date = url_date
        endpoint = APIs[archive].replace("[DATE]", date).replace("[URL]", url)
        print(f"{i}/{total} - {endpoint}")
        start = time.time()
        try:
            response = sess.get(endpoint, allow_redirects=True, stream=True, timeout=30)
            for _ in range(3):
                redirect_url = check_redirect(archive, response)
                if redirect_url:
                    response = sess.get(redirect_url, allow_redirects=True, stream=True, timeout=30)
                    actual_date = datetime.strptime(redirect_url.split("/")[4], '%Y%m%d%H%M%S')
                    redirect_url = check_redirect(archive, response)
                    if not redirect_url:
                        break
        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, returning")
            return
        except:
            error = traceback.format_exc()
            cur.execute("INSERT INTO " + table + " (arch,date,url,status,headers,final_url,error,runtime) "
                                                      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (archive, dt.strptime(date, '%Y%m%d'), url, -1,
                         None, endpoint, error, time.time() - start))
            time.sleep(2)
            continue

        content = response.content
        content_hash = sha256(content).hexdigest()
        file_dir = os.path.join(STORAGE, content_hash[0], content_hash[1])
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        file_path = os.path.join(file_dir, f"{content_hash}.gz")
        with gzip.open(file_path, "wb") as fh:
            fh.write(content)
        response.close()
        try:
            cur.execute("INSERT INTO " + table + "(arch,date,url,status,headers,final_url,runtime,content_hash) "
                                                      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (archive, dt.strptime(date, '%Y%m%d'), url, response.status_code,
                         json.dumps(dict(response.headers)), response.url, time.time() - start, content_hash))
        except:
            with open("/tmp/error.log", "a") as fh:
                error = traceback.format_exc()
                fh.write(error + "\n\n\n")

        time.sleep(0.5)

def main(urls, table="responses"):
    logging.basicConfig(level=logging.INFO)

    conn = get_conn()
    cur = conn.cursor()

    urls_file = open(urls, "r")
    urls = ['http://' + PREFIX+ u.strip().split(',')[1] for u in urls_file.readlines()]

    all_urls = defaultdict(set)

    cur.execute("""
    SELECT arch, date, url FROM """ + table + """
    """)

    known = set([f"{x[0]}|{x[1].strftime('%Y%m%d')}|{x[2]}" for x in cur.fetchall()])

    dates = ["20160115", "20160415", "20160715", "20161015",
             "20170115", "20170415", "20170715", "20171015",
             "20180115", "20180415", "20180715", "20181015",
             "20190115", "20190415", "20190715", "20191015",
             "20200115", "20200415", "20200715", "20201015",
             "20210115", "20210415", "20210715", "20211015",
             "20220115", "20220415", "20220715"]

    for url in urls:
        for archive in APIs:
            for date in dates:
                url_id = f"{archive}|{date}|{url}"
                if url_id not in known:
                    all_urls[archive].add((url, date))

    crawl_all(all_urls, table)


if __name__ == "__main__":
    main()
