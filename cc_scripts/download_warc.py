from os.path import exists
import csv
import time
import requests
import io 
import gzip
import sys

def list_warc_data(input_file):
    positions = []
    with open(input_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect=csv.excel, delimiter=';')
        positions = [dict(r) for r in reader]
    return positions

def donwload_one(warc_data, output_dir, timeout=1):
    print(warc_data)

    # Skip if file data is empty
    fetch_time = warc_data["fetch_time"]
    if fetch_time == "":
        return

    fetch_time = fetch_time.split(" ")[0]
    filename = f'{fetch_time}_{warc_data["host_name"]}.warc'
    file_path = f"{output_dir}/{filename}"

    # Skip existing files
    if exists(file_path):
        return


    data_url = "https://data.commoncrawl.org"
    url = f'{data_url}/{warc_data["warc_filename"]}'
    print(url)

    offset = int(warc_data["warc_record_offset"])
    offset_end = offset + int(warc_data["warc_record_length"]) - 1
    headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
            "Range": f"bytes={offset}-{offset_end}"
    }

    try_num = 1
    while 1:
        print(f"Try to download {try_num}")
        try_num += 1

        content = ""
        resp = requests.get(url, headers=headers)
        content = resp.content

        # cmd = ["curl", "-H", f"Range: bytes={offset}-{offset_end}", url, "--output", "tmp.txt"]
        # subprocess.run(cmd)
        # with open("./tmp.txt", "rb") as in_file:
            # content = in_file.read()

        # l = f"Resp Length for {url}: {len(resp.text)}"
        # print(l)

        time.sleep(timeout)

        if b"Please reduce your request rate." in content:
            print()
            print("TIMEOUT: %d" % timeout)
            timeout = min(60, timeout*2)
            continue

        data = ""

        try:
            zipped_file = io.BytesIO(content)
            unzipped_file = gzip.GzipFile(fileobj=zipped_file)
            raw_data: bytes = unzipped_file.read()
        except Exception as e:
            em = f"ERROR for {url}\n{url}\n{offset} - {offset_end}\n{resp.content}\n{e}"
            print("ERROR", __name__, em)
            with open(file_path + ".error", "w") as out_file:
                out_file.write(em)
            break

        try:
            data: str = raw_data.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            em = f"Warning: Could not extract file downloaded from {url}"
            print("ERROR", __name__, em)
            with open(file_path + ".error", "w") as out_file:
                out_file.write(em)
            break
            # continue


        with open(file_path, "w") as out_file:
            out_file.write(data)
        break

def main():
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    warc_data = list_warc_data(input_file)
    for wd in warc_data:
        donwload_one(wd, output_dir)

if __name__ == '__main__':
    main()