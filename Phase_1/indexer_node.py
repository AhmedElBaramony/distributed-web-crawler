# indexer_node.py

from mpi4py import MPI
import time
import logging

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

def indexer_process():
    """
    Indexer Node:
    - Receives crawled content from crawler nodes (tag 2)
    - Simulates basic indexing of received web page text
    - Sends status or error messages back to Master Node
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    logging.info(f"Indexer node {rank} started.")

    while True:
        status = MPI.Status()
        content_package = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)
        source = status.Get_source()

        if content_package is None:
            logging.info("Indexer received shutdown signal. Exiting.")
            break
        try:
            url = content_package.get('url')
            text = content_package.get('text')

            if url and text:
                # Simulate indexing delay
                time.sleep(1)

                logging.info(f"Successfully indexed content from URL: {url}")
                # Notify Master of success
                comm.send(f"Indexed content from {url} (from Crawler {source})", dest=0, tag=99)

            else:
                logging.warning(f"Received incomplete content package from Crawler {source}: {content_package}")

        except Exception as e:
            logging.error(f"Error indexing content from Crawler {source}: {e}")
            comm.send(f"Error indexing content from Crawler {source}: {e}", dest=0, tag=999)

if __name__ == '__main__':
    indexer_process()
