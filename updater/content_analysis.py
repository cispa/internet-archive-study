from bs4 import BeautifulSoup
from multiprocessing import Pool
import tldextract
from pprint import pprint
from psycopg2 import connect
from psycopg2.extras import Json
import gzip
import os
import re

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD, PROCESSES, APIs, STORAGE, PREFIX

def js_url_filter(url):
    # Explanation:
    # Archives rewrite the URLs like the following or similar
    # /wayback.archive-it.org/all/20220914000910js_/http://static.chartbeat.com/js/chartbeat_mab.js'
    # Therefore, we use regex to look for the timestemp and the following js_
    # Then, we group the rest.
    # Injected archive urls that only load archive foo are ignored since they do not have a timestamp
    matches = re.search(r"\d+js_\/([http:|https:]*\/\/.*)", url)
    if matches:
        return matches.group(1)
    return None

def filter_js_urls(urls):
    filtered_urls = [js_url_filter(url) for url in urls if js_url_filter(url)]
    return filtered_urls

def extract_src_urls(html_doc):
    soup = BeautifulSoup(html_doc, 'html.parser')
    results = soup.select("script[src]")
    return [r.get("src") for r in results]

def urls_to_dict(urls, end_url):
    info = {"urls": set(), "hosts": set(),"sites": set()}
    for url in urls:
        out = tldextract.extract(url)
        if out.domain == "":
            out = tldextract.extract(end_url)
            site = f"{out.domain}.{out.suffix}"
            host = f"{out.subdomain}.{out.domain}.{out.suffix}" if out.subdomain != "" else site
        else:
            site = f"{out.domain}.{out.suffix}"
            host = f"{out.subdomain}.{out.domain}.{out.suffix}" if out.subdomain != "" else site
        info["urls"].add(url)
        info["sites"].add(site)
        info["hosts"].add(host)

    info["urls"] = list(info["urls"])
    info["sites"] = list(info["sites"])
    info["hosts"] = list(info["hosts"])
    info["urls"].sort()
    info["sites"].sort()
    info["hosts"].sort()
    return info

def worker_update_table_with_documents(hash, end_url, id, arch, table):
    print(f"Do {hash}")
    path = os.path.join(STORAGE, hash[0], hash[1], f"{hash}.gz")
    with gzip.open(path, "r") as f:
        html = f.read()
    urls = extract_src_urls(html)

    if arch not in ["c-crawl", "live"]:
        urls = filter_js_urls(urls)

    info = urls_to_dict(urls, end_url)
    print(f"Result {hash}\n{info}")
    # return (Json(info), hash)

    connection = connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    cursor = connection.cursor()

    # This opens many many connections, but for `responses` it's faster than updating all results in the end
    query = f"UPDATE {table} SET script_info = %s WHERE content_hash = %s"
    cursor.execute(query, (Json(info), hash))
    connection.commit()
    cursor.close()
    connection.close()

def update_table_with_documents(table):
    while 1:
        connection = connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
        cursor = connection.cursor()

        hashes = []
        if "live" in table:
            query = f"""SELECT DISTINCT id, end_url, content_hash, script_info FROM {table}
                    WHERE script_info IS NULL AND content_hash IS NOT NULL AND end_url IS NOT NULL
                    LIMIT 5000"""
            cursor.execute(query)
            for id, end_url, hash, info in cursor.fetchall():
                hashes.append((hash, end_url, id, "live", table))
        else:
            query = f"""SELECT DISTINCT id, arch, archived_url, content_hash, script_info FROM {table}
                    WHERE script_info IS NULL AND content_hash IS NOT NULL AND archived_url IS NOT NULL
                    LIMIT 5000"""
            cursor.execute(query)
            for id, arch, end_url, hash, info in cursor.fetchall():
                hashes.append((hash, end_url, id, arch, table))
        
        if len(hashes) == 0:
            break

        with Pool(PROCESSES) as pool:
            out = pool.starmap(worker_update_table_with_documents, hashes)
            # query = f"UPDATE {table} SET script_info = %s WHERE content_hash = %s"
            # for o in tqdm(out):
                # cursor.execute(query, o)
        connection.commit()
        cursor.close()
        connection.close()
    print("Done")

def main(table):
    update_table_with_documents(table)

if __name__ == '__main__':
    main()
