
from mpi4py import MPI
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

def indexer_process():
    """
    Indexer Node: Simulates indexing of received content.
    Responsibilities:
    - Receive content from crawler nodes (tag 2)
    - Simulate indexing logic
    - Send status or errors to master
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    logging.info(f"Indexer node {rank} started.")

    while True:
        status = MPI.Status()
        content = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)  # Tag 2: content
        source = status.Get_source()

        if not content:
            logging.info("Received shutdown signal.")
            break

        logging.info(f"Received content from Crawler {source}")
        try:
            # Simulate processing (indexing)
            time.sleep(1)
            logging.info(f"Indexed content from Crawler {source}")
            comm.send(f"Indexed content from Crawler {source}", dest=0, tag=99)  # Tag 99: status
        except Exception as e:
            logging.error(f"Error indexing content from Crawler {source}: {e}")
            comm.send(f"Error indexing: {e}", dest=0, tag=999)  # Tag 999: error

if __name__ == '__main__':
    indexer_process()
