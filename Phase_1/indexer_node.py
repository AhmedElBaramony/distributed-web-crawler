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

def indexer_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    logger = setup_logger("Indexer", rank)

    logger.info(f"Indexer node {rank} started.")

    while True:
        # Placeholder for receiving page content
        logger.info("Indexer would wait for page content from Crawler here.")

        # Placeholder for indexing logic
        logger.info("Indexer would build an inverted index here.")

        # Placeholder for shutdown check
        logger.info("Indexer would check for shutdown signal here.")
        break  # Remove in Phase 2

if __name__ == '__main__':
    indexer_process()