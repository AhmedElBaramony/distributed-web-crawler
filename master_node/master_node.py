# master_node.py

from mpi4py import MPI
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

CRAWL_DELAY = 0.1  # Small delay to avoid overwhelming message passing

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    # Validate number of processes (Master + at least 1 Crawler + 1 Indexer)
    if size < 3:
        logging.error("At least 3 processes are required: Master, Crawler(s), and Indexer.")
        return

    crawler_ranks = list(range(1, size - 1))  # Crawlers: ranks 1 to (size-2)
    indexer_rank = size - 1  # Indexer is last rank

    # Initial seed URLs
    seed_urls = [
        "http://example.com",
        "http://www.python.org",
        "https://www.wikipedia.org"
    ]
    urls_to_crawl_queue = list(seed_urls)
    crawled_urls_set = set()
    assigned_tasks = {}  # {crawler_rank: url}

    # Main loop
    while urls_to_crawl_queue or assigned_tasks:
        # Assign URLs to idle crawlers
        for crawler in crawler_ranks:
            if crawler not in assigned_tasks and urls_to_crawl_queue:
                url = urls_to_crawl_queue.pop(0)
                if url not in crawled_urls_set:
                    comm.send(url, dest=crawler, tag=0)  # Send crawl task
                    assigned_tasks[crawler] = url
                    logging.info(f"Assigned URL '{url}' to Crawler {crawler}")
                    time.sleep(CRAWL_DELAY)

        # Handle incoming messages
        while comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            data = comm.recv(source=source, tag=tag)

            if tag == 1:  # New URLs discovered
                logging.info(f"Received {len(data)} new URLs from Crawler {source}")
                for new_url in data:
                    if new_url not in crawled_urls_set and new_url not in urls_to_crawl_queue:
                        urls_to_crawl_queue.append(new_url)
                finished_url = assigned_tasks.pop(source, None)
                if finished_url:
                    crawled_urls_set.add(finished_url)

            elif tag == 99:  # Heartbeat/status
                logging.info(f"Status from Crawler {source}: {data}")

            elif tag == 999:  # Error report
                logging.error(f"Error from Crawler {source}: {data}")
                assigned_tasks.pop(source, None)  # Consider the task failed

        time.sleep(0.5)  # Master loop delay

    # After all URLs crawled
    logging.info("Crawling completed. Sending shutdown signals to crawlers.")

    # Shutdown crawlers
    for crawler in crawler_ranks:
        comm.send(None, dest=crawler, tag=0)

    logging.info("Master node finished operations.")

if __name__ == '__main__':
    master_process()
