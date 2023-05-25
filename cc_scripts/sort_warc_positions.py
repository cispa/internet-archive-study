import csv
import sys
from datetime import datetime, timedelta

dates = ["20160115", "20160415", "20160715", "20161015",
         "20170115", "20170415", "20170715", "20171015",
         "20180115", "20180415", "20180715", "20181015",
         "20190115", "20190415", "20190715", "20191015",
         "20200115", "20200415", "20200715", "20201015",
         "20210115", "20210415", "20210715", "20211015",
         "20220115", "20220415", "20220715"]

dates = ["20160115", "20220115"]

def sort_positions(input_name, output_name, threshold=4):
    hosts = {}
    # with open('athena_www.csv', newline='') as csvfile:
    # Read all lines from the CSV and add it to hosts
    with open(input_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            host_name = row["url_host_name"]
            fetch_time = datetime.fromisoformat(row["fetch_time"])

            # Add host for all dates if not existing, else update if better date
            if host_name not in hosts:
                date_entries = []
                for d in dates:
                    date_entries.append({
                        "current": fetch_time,
                        "goal": datetime.strptime(d, "%Y%m%d"),
                        "content": row
                    })
                hosts[host_name] = date_entries
            else:
                for entry in hosts[host_name]:
                    if abs(entry["current"] - entry["goal"]) > abs(fetch_time -  entry["goal"]):
                        entry["current"] = fetch_time
                        entry["content"] = row

    # Empty all rows that are not in threshold
    for host_name in hosts:
        for entry in hosts[host_name]:
            current = entry["current"]
            goal = entry["goal"]
            if abs(current - goal) > timedelta(weeks=threshold):
                entry["current"] = None
                entry["content"] = None


    # Write all hosts to output
    with open(output_name, 'w', newline='') as csvfile:
        fieldnames = ['host_name', 'url', 'goal_time', 'fetch_time', 'warc_filename', 'warc_record_offset', 'warc_record_length']
        writer = csv.DictWriter(csvfile, dialect=csv.excel, delimiter=';', fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for host_name in hosts:
            for entry in hosts[host_name]:
                content = entry["content"]
                writer.writerow({
                    'host_name': host_name, 
                    'url': content['url'] if content else None, 
                    'goal_time': str(entry['goal']), 
                    'fetch_time': entry['current'], 
                    'warc_filename': content['warc_filename']if content else None, 
                    'warc_record_offset': content['warc_record_offset'] if content else None, 
                    'warc_record_length': content['warc_record_length'] if content else None
                })

def main():
    input_name = sys.argv[1]
    output_name = sys.argv[2]
    sort_positions(input_name, output_name)

if __name__ == '__main__':
    main()
