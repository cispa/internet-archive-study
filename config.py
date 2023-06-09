PROCESSES = 32

PREFIX = 'http://www.'
RELEVANT_HEADERS = ['x-frame-options', 'content-security-policy', 'strict-transport-security', 'referrer-policy',
                    'permissions-policy', 'cross-origin-opener-policy', 'cross-origin-resource-policy',
                    'cross-origin-embedder-policy']
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

STORAGE = "/data/"

# DATABASE
DB_USER = 'archive'
DB_PWD = 'archive'
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'XXX'

DEBUG = False


# RELEVANT WEB ARCHIVES
APIs = {
    # '14': 'http://eot.us.archive.org/eot/{date}/{url}',
    # '42': 'http://wayback.archive-it.org/10702/{date}/{url}',
    'archive-it': 'https://wayback.archive-it.org/all/{date}/{url}',
    'israel': 'http://wayback.nli.org.il:8080/{date}/{url}',
    'iceland': 'http://wayback.vefsafn.is/wayback/{date}/{url}',
    'congress': 'https://webarchive.loc.gov/all/{date}/{url}',
    'arquivo': 'https://arquivo.pt/wayback/{date}mp_/{url}',
    # '52': 'https://digital.library.yorku.ca/wayback/{date}/{url}',
    # '53': 'https://haw.nsk.hr/wayback/{date}/{url}', # offline as of August 22
    'stanford': 'https://swap.stanford.edu/{date}mp_/{url}',
    'archiveorg': 'https://web.archive.org/web/{date}/{url}',
    # 'memento': 'https://timetravel.mementoweb.org/memento/{date}/{url}'
}