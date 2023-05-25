# Steps to get results
1. Add your AWS creds and bucket in [query_athena.py](query_athena.py) and run it with your dataset.
2. Download the result from AWS.
3. Take the result as input for [sort_warc_positions.py](sort_warc_positions.py) to get a list of all entries inside our timeframe.
4. Take the previous list and give it to [download_warc.py](download_warc.py) to download all corresponding warc files.
5. Run [warc_to_database.py](warc_to_database.py) to store all information in the database.
