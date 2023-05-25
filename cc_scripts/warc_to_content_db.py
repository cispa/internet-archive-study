from warcio.archiveiterator import ArchiveIterator
from hashlib import sha256
import os
import sys
import gzip
from multiprocessing import Pool

STORAGE = "./data_for_maws"
PROCESSES = 8

def worker(path):
    if path.endswith(".error"):
        return
    with open(path, 'rb') as stream:
            record = next(ArchiveIterator(stream))

            if record == None:
                return

            if record.rec_type != 'response':
                return
            content = record.content_stream().read()
            content_hash = sha256(content).hexdigest()

            file_dir = os.path.join(STORAGE, content_hash[0], content_hash[1])
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)
            file_path = os.path.join(file_dir, f"{content_hash}.gz")
            with gzip.open(file_path, "wb") as fh:
                fh.write(content)
def main():
    directory = sys.argv[1]

    files = os.listdir(directory)
    files = [os.path.join(directory, f) for f in files]

    with Pool(PROCESSES) as pool:
        pool.map(worker, files)

if __name__ == "__main__":
    main()