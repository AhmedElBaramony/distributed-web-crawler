from mpi4py import MPI
import time
import logging

def setup_logger(role, rank):
    logger = logging.getLogger(f"{role}-{rank}")
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logger = setup_logger("Crawler", rank)

    logger.info(f"Crawler node {rank} started.")

    while True:
        # Placeholder for receiving a URL to crawl
        logger.info("Crawler would wait for a URL to crawl here.")

        # Placeholder for crawling logic
        logger.info("Crawler would perform HTTP request and extract text and links here.")

        # Placeholder for sending extracted URLs and content
        logger.info("Crawler would send extracted data back to Master and Indexer here.")

        # Placeholder for shutdown check
        logger.info("Crawler would check for shutdown signal here.")
        break  # Remove in Phase 2

if __name__ == '__main__':
    crawler_process()