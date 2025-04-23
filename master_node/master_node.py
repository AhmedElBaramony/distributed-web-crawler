
from mpi4py import MPI
import time
import logging

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

def master_process():
    """
    Master Node: Central controller for task scheduling and system monitoring.
    Responsibilities:
    - Assign seed URLs to crawler nodes
    - Monitor crawler and indexer status
    - Handle errors and requeue failed tasks
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    # Assume one indexer node, the rest are crawler nodes
    crawler_nodes = size - 2
    indexer_nodes = 1

    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Minimum: 1 master, 1 crawler, 1 indexer required.")
        return

    # Define the ranks of worker nodes
    active_crawlers = list(range(1, 1 + crawler_nodes))
    active_indexers = list(range(1 + crawler_nodes, size))

    # Seed URLs to kickstart the crawl process
    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl_queue = seed_urls[:]
    task_count = 0
    assigned_tasks = 0

    # Main scheduling loop
    while urls_to_crawl_queue or assigned_tasks > 0:
        # Handle responses from crawler nodes
        if assigned_tasks > 0 and comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            data = comm.recv(source=source, tag=tag)

            if tag == 1:  # Received new URLs to crawl
                assigned_tasks -= 1
                urls_to_crawl_queue.extend(data)
                logging.info(f"Received URLs from Crawler {source}, queue size: {len(urls_to_crawl_queue)}")
            elif tag == 99:  # Heartbeat or status
                logging.info(f"Status from Crawler {source}: {data}")
            elif tag == 999:  # Error report
                assigned_tasks -= 1
                logging.error(f"Error from Crawler {source}: {data}")

        # Assign new tasks to crawler nodes
        while urls_to_crawl_queue and assigned_tasks < crawler_nodes:
            url = urls_to_crawl_queue.pop(0)
            target = active_crawlers[assigned_tasks % len(active_crawlers)]
            comm.send(url, dest=target, tag=0)  # Tag 0 = crawl task
            logging.info(f"Assigned URL {url} to Crawler {target}")
            assigned_tasks += 1
            task_count += 1
            time.sleep(0.1)

        time.sleep(1)

    logging.info("Master finished distributing URLs.")

if __name__ == '__main__':
    master_process()
