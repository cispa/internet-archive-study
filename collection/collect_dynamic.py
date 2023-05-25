from playwright.sync_api import sync_playwright
from utils.database import get_conn
from collections import defaultdict, Counter
from multiprocessing import Pool
import random
import tldextract
import time
import json
import re

from tqdm import tqdm

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def url_filter(url):
    matches = re.search(r"\d+([js,im,if,cs,mp,oe,wkr]+_)?\/([http:|https:]*\/\/.*)", url)
    if matches:
        return matches.group(2)
    return url

def run_playwright(playwright, urls):
    chromium = playwright.chromium
    browser = chromium.launch()

    result = defaultdict(list)
    for url in tqdm(urls):
        print(url)
        time.sleep(5)
        try:
            page = browser.new_page()

            page.on("request", lambda request: result[url].append({
                        "status": "success",
                        "type": "request",
                        "headers": request.headers,
                        "method": request.method,
                        "resource_type": request.resource_type,
                        "url": request.url
                    }))

            def handle_response(response):
                for r in result[url]:
                    if "url" in r and r["url"] == response.url:
                        r["response_status_code"] = response.status
                        r["response_headers"] = response.headers

            page.on("response", handle_response)

            res = page.goto(url, timeout=60000)
            status_code = res.status
            # Filter out static stuff from archive
            result[url] = [r for r in result[url] if not r["url"].startswith("https://web.archive.org/_")]
            result[url].append({"status": "info", "type": "info", "status_code": status_code})
        except Exception as e:
            print(e)
            status_code = -1
            result[url] = [
                {"status": "error", "type": "error", "value": str(e)},
                {"status": "info", "type": "info", "status_code": -1}
            ]
    browser.close()

    return result

def run(bucket, year):
    with sync_playwright() as playwright:
        result = run_playwright(playwright, bucket)
    
    conn = get_conn()
    cur = conn.cursor()
    for url in result:
        external_requests = result[url]

        status_code = -1
        for req in external_requests:
            if req["status"] == "info":
                status_code = req["status_code"]

        # print(external_requests)
        for req in external_requests:
            if req["status"] == "error":
                cur.execute(f""" INSERT INTO dynamic_script_inclusions_{year} (url, status_code, request_url, request_site, result)
                        VALUES (%s, %s, %s, %s, %s) """,
                        (url, status_code, "ERROR", "ERROR", json.dumps(req)))
            elif req["status"] == "success":
                r_url = url_filter(req["url"])
                out = tldextract.extract(r_url)
                r_site = f"{out.domain}.{out.suffix}"
                cur.execute(f""" INSERT INTO dynamic_script_inclusions_{year} (url, status_code, request_url, request_site, result, response_status_code, response_headers)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) """,
                        (url, status_code, r_url, r_site, json.dumps(req), req.get("response_status_code", -2), json.dumps(req.get("response_headers", {}))))
    conn.commit()
    cur.close()
    conn.close()

def main(year="2016"):
    conn = get_conn()
    cur = conn.cursor()

    # Change the coresponding year

    query = f"SELECT DISTINCT url FROM dynamic_script_inclusions_{year}"
    cur.execute(query)
    finished_urls = list(cur.fetchall())

    d = "2016-01-15" if year == "2016" else "2022-07-15"
    
    query = f"""
        SELECT final_url FROM responses_neighbors
        WHERE date = '{d} 00:00:00'
        AND error IS NULL AND pos=0
    """
    cur.execute(query)

    split = 10
    urls = [u[0] for u in cur.fetchall() if u not in finished_urls]
    random.shuffle(urls)
    buckets = chunks(urls, split)
    cur.close()
    conn.close()

    for buck in buckets:
        run(buck, year)

if __name__ == "__main__":
    year = input("Which table do you want to fill? 2016 or 2022?")
    main(year)