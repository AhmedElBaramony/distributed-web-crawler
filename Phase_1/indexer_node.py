# indexer_node.py

from mpi4py import MPI
import time
import logging
import csv 

# Setup organized logging
def setup_logger(role, rank):
    logger = logging.getLogger(f"{role}-{rank}")
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

index = {}

def add_to_index(url, text):
    words = text.lower().split()
    for word in words:
        if word not in index:
            index[word] = set()
        index[word].add(url)

def save_index_to_csv(filename="index.csv"):
    """Save the index dictionary into a CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Word', 'URL'])  # Header
        for word, urls in index.items():
            for url in urls:
                writer.writerow([word, url])

def indexer_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    logger = setup_logger("Indexer", rank)
    status = MPI.Status()

    logger.info(f"Indexer node {rank} started.")

    while True:
        content_package = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)

        if content_package is None:
            logger.info("Indexer received shutdown signal. Saving index to CSV and exiting.")
            save_index_to_csv() 
            break

        try:
            url = content_package['url']
            text = content_package['text']

            add_to_index(url, text)
            logger.info(f"Indexed page: {url}")
            logger.info(f"Total keywords indexed: {len(index)}")

        except Exception as e:
            logger.error(f"Error indexing content: {e}")

if __name__ == '__main__':
    indexer_process()
