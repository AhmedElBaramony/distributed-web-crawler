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

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logger = setup_logger("Master", rank)

    logger.info(f"Master node started with rank {rank} of {size}")

    if size < 3:
        logger.error("At least 3 processes are required: Master, Crawler(s), and Indexer.")
        return

    crawler_ranks = list(range(1, size - 1))
    indexer_rank = size - 1

    # Placeholder for seed URLs (to be replaced in Phase 2)
    seed_urls = [
        "https://example.com"
    ]

    # Placeholder for main scheduling loop
    logger.info("Master scheduling loop would go here.")

    # Placeholder for clean shutdown
    logger.info("Master would send shutdown signals here.")

if __name__ == '__main__':
    master_process()