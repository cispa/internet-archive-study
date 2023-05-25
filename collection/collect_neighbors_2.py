from bs4 import BeautifulSoup
from requests import Session
from datetime import datetime
import time
import os
from hashlib import sha256
import traceback
import gzip
import json

from utils.database import get_conn
from config import STORAGE

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

def collect_data(urls, table):
    archive = "archiveorg"
    conn = get_conn(True)
    cur = conn.cursor()

    sess = Session()
    sess.headers["Connection"] = "Keep-Alive"

    total = len(urls)

    for i, url_date in enumerate(urls):
        # origin_url, url, actual_date, date, neighbor_stat = url_date
        endpoint, origin_url, date, actual_date, pos = url_date
        print(f"{i}/{total} - {endpoint} / {table}")
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
            cur.execute("INSERT INTO " + table + " (arch,date,actual_date,url,status,headers,final_url,error,runtime,pos) "
                                                      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (archive, date, actual_date, origin_url, -1,
                         None, endpoint, error, time.time() - start, pos))
            time.sleep(2)
            print(error)
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
            cur.execute("INSERT INTO " + table + " (arch,date,actual_date,url,status,headers,final_url,runtime,content_hash,length,pos) "
                                                      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (archive, date, actual_date, origin_url, response.status_code,
                         json.dumps(dict(response.headers)), response.url, time.time() - start, content_hash, len(content), pos))
        except:
            with open("/tmp/error_neighbors_2.log", "a") as fh:
                error = traceback.format_exc()
                print(error)
                fh.write(error + "\n\n\n")

        time.sleep(0.5)

def get_urls():

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(""" SELECT final_url, url, date, actual_date, pos FROM archiveorg_indices
                    WHERE error = '' AND pos IS NOT NULL AND pos between -10 and 10""")

    urls = cur.fetchall()

    cur.close()
    conn.close()
    return urls

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main(table="responses_neighbors"):
    urls = get_urls()
    urls = chunks(urls, 100)

    for u in urls:
        collect_data(u, table)


if __name__ == "__main__":
    main()