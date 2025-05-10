from mpi4py import MPI
import time
import logging
from whoosh.fields import Schema, TEXT, ID
from whoosh import index
import os

# =========================
# Logging Configuration
# =========================
def setup_logger(role, rank):
    logger = logging.getLogger(f"{role}-{rank}")
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

# ============================
# Constants and Configuration
# ============================
schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# =========================
# Indexer Process
# =========================
def indexer_process():
    logger = setup_logger("Indexer", rank)
    status = MPI.Status()

    logger.info(f"Indexer node {rank} started.")

    # Initialize Whoosh index directory
    index_dir = "indexdir"
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    # Check if an index already exists in the directory
    if not index.exists_in(index_dir):
        ix = index.create_in(index_dir, schema)
        logger.info("Created new Whoosh index.")
    else:
        ix = index.open_dir(index_dir)
        logger.info("Opened existing Whoosh index.")

    # ========= MAIN LOOP: receive content from Crawlers and index it ==========
    while True:
        
        content_package = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)

        # Check for shutdown signal
        if content_package is None:
            logger.info("Indexer received shutdown signal. Exiting.")
            break

        try:
            url = content_package['url']
            text = content_package['text']

            writer = ix.writer()
            writer.update_document(url=url, content=text)
            writer.commit()

            logger.info(f"Indexed page: {url}")

        except Exception as e:
            logger.error(f"Error indexing content from {url}: {e}")

if __name__ == '__main__':
    indexer_process()