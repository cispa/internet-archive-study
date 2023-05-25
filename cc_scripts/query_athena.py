import boto3
import csv

BOTO_SESSION = boto3.Session(
    aws_access_key_id="<that's my secret>",
    aws_secret_access_key="<that's my secret>",
    region_name = "us-east-1"
)

dates = ["20160115", "20160415", "20160715", "20161015",
         "20170115", "20170415", "20170715", "20171015",
         "20180115", "20180415", "20180715", "20181015",
         "20190115", "20190415", "20190715", "20191015",
         "20200115", "20200415", "20200715", "20201015",
         "20210115", "20210415", "20210715", "20211015",
         "20220115", "20220415", "20220715"]

# Some of the month were not archived by CC
crawls = [
  'CC-MAIN-2016-18', # April
  'CC-MAIN-2016-22', # May
  'CC-MAIN-2016-30', # July
  'CC-MAIN-2016-44', # October

  'CC-MAIN-2017-04', # January
  'CC-MAIN-2017-17', # April
  'CC-MAIN-2017-30', # July
  'CC-MAIN-2017-43', # October

  'CC-MAIN-2018-05', # January
  'CC-MAIN-2018-17', # April
  'CC-MAIN-2018-30', # July
  'CC-MAIN-2018-43', # October

  'CC-MAIN-2019-04', # January
  'CC-MAIN-2019-18', # April
  'CC-MAIN-2019-30', # July
  'CC-MAIN-2019-43', # October

  'CC-MAIN-2020-05', # January
  'CC-MAIN-2020-29', # July
  'CC-MAIN-2020-45', # October

  'CC-MAIN-2021-04', # January
  'CC-MAIN-2021-17', # April
  'CC-MAIN-2021-31', # July
  'CC-MAIN-2021-43', # October
  'CC-MAIN-2022-05' # January
]

with open('dataset.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    data=[]
    for idx, domain in reader:
        domain = domain if domain.startswith("www.") else "www." + domain
        data.append((idx, domain))
    # data = [(idx, domain) for idx, domain in reader]

domains = [d[1] for d in data]

for year in [2016, 2017, 2018, 2019, 2020, 2021, 2022]:
    query = """
    SELECT *
    FROM "ccindex"."ccindex"
    WHERE subset = 'warc'
      AND crawl LIKE 'CC-MAIN-%s-%%'
      AND contains(ARRAY['%s'], url_host_name)
      AND (url_path = '' OR url_path = '/')
    """ % (year, "', '".join(domains))

    print(query)


    database = "ccindex"
    result_bucket = "s3://my-cc-bcket/"
    client = BOTO_SESSION.client('athena', region_name="us-east-1")

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': result_bucket}
    )

    # Download the data manually from AWS 
    # (so that we don't forget to remove it form AWS)
    print("started...")
    print(response["QueryExecutionId"])
