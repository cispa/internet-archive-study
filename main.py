from collection import collect_dynamic, collect_historical_data, maws_collect, collect_neighbors_1, collect_neighbors_2, collect_cdx, collect_live_data, collect_archive_data_for_fixed_date
from updater import updater_dynamic, content_analysis, updater, updater_neighbors, updater_cdx

# Section 3
def common_crawl():
    print("Please follow the description in `cc_scripts`.")

def other_archives():
    urls = input("URLs of interest (file): ")
    table_name = input("What is the table name (e.g. responses)")
    maws_collect.main(urls, table_name)
    updater.main(table_name, "archive")

# Section 4
def neighborhoods():
    collect_neighbors_1.main()
    collect_neighbors_2.main()
    updater_neighbors.main()
    content_analysis.main("responses_neighbors")

def dynamic_data():
    year = input("Which table do you want to fill? 2016 or 2022? ")
    collect_dynamic.main(year)
    updater_dynamic.main(year)
    
# Section 5
def historical_data():
    collect_historical_data.main('./misc/live_dataset.csv')
    content_analysis.main("historical_data")

def live_data():
    collect_live_data.main('./misc/live_dataset.csv')
    updater.main("live_headers", "live")
    content_analysis.main("live_headers")

def cdx_data():
    collect_cdx.main('./misc/tranco_Z2QWG.csv')
    updater_cdx.main()

def archive_fixed_date():
    collect_archive_data_for_fixed_date.main('./misc/live_dataset.csv')


def main():
    # common_crawl()
    # other_archives()
    # neighborhoods()
    dynamic_data()
    # historical_data()
    # live_data()
    # cdx_data()
    # archive_fixed_date()

if __name__ == '__main__':
    main()