# master_node.py

from mpi4py import MPI
import time
import logging

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

CRAWL_DELAY = 0.1  # Delay between task assignments

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    if size < 3:
        logging.error("At least 3 processes are required: Master, Crawler(s), and Indexer.")
        return

    crawler_ranks = list(range(1, size - 1))  # Crawlers = rank 1 to (size-2)
    indexer_rank = size - 1                   # Indexer = last rank

    # Seed URLs to start crawling
    seed_urls = [
        "https://quotes.toscrape.com/",
        "https://quotes.toscrape.com/page/1/",
        "https://quotes.toscrape.com/page/2/"
    ]
    urls_to_crawl_queue = list(seed_urls)
    crawled_urls_set = set()

    # Track which crawlers are busy
    busy_crawlers = {crawler: False for crawler in crawler_ranks}

    logging.info("Starting task distribution...")

    while urls_to_crawl_queue or any(busy_crawlers.values()):
        # Assign URLs to idle crawlers
        for crawler in crawler_ranks:
            if not busy_crawlers[crawler] and urls_to_crawl_queue:
                url = urls_to_crawl_queue.pop(0)
                if url not in crawled_urls_set:
                    comm.send(url, dest=crawler, tag=0)  # Send URL
                    busy_crawlers[crawler] = True
                    logging.info(f"Assigned URL '{url}' to Crawler {crawler}")
                    time.sleep(CRAWL_DELAY)

        # Handle incoming messages
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            src = status.Get_source()
            tag = status.Get_tag()
            data = comm.recv(source=src, tag=tag)

            if tag == 1:
                # Crawler sent new URLs
                logging.info(f"Received {len(data)} new URLs from Crawler {src}")
                for new_url in data:
                    if new_url not in crawled_urls_set and new_url not in urls_to_crawl_queue:
                        urls_to_crawl_queue.append(new_url)
                busy_crawlers[src] = False

            elif tag == 99:
                # Status update (heartbeat)
                logging.info(f"Status from Crawler {src}: {data}")
                busy_crawlers[src] = False

            elif tag == 999:
                # Error reported
                logging.error(f"Error from Crawler {src}: {data}")
                busy_crawlers[src] = False

        time.sleep(0.2)  # Prevent CPU overloading with busy waiting

    # Crawling completed — Send shutdown signals
    logging.info("Crawling finished. Sending shutdown signals to Crawlers and Indexer.")

    for crawler in crawler_ranks:
        comm.send(None, dest=crawler, tag=0)  # Shutdown Crawler

    comm.send(None, dest=indexer_rank, tag=2)  # Shutdown Indexer

    logging.info("Master node finished operations.")

if __name__ == '__main__':
    master_process()
