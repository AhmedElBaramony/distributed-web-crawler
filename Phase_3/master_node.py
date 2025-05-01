from mpi4py import MPI
import time
import logging
from datetime import datetime

# Setup organized logging
def setup_logger(role, rank):
    logger = logging.getLogger(f"{role}-{rank}")
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

CRAWL_DELAY = 0.1
MAX_CRAWL_DEPTH = 2
HEARTBEAT_TIMEOUT = 5 
TASK_TIMEOUT = 5 

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logger = setup_logger("Master", rank)
    status = MPI.Status()

    # Master is the only node that can rank 0
    if rank != 0:
        return

    logger.info(f"Master node started with rank {rank} of {size}")

    if size < 3:
        logger.error("At least 3 processes are required: Master, Crawler(s), and Indexer.")
        return

    crawler_ranks = list(range(1, size - 1))
    indexer_rank = size - 1

    seed_urls = [
        ("https://demo.cyotek.com/", 0)
    ]
    urls_to_crawl_queue = list(seed_urls)
    crawled_urls_set = set() # To avoid duplicate URLs
    busy_crawlers = {crawler: False for crawler in crawler_ranks}
    last_heartbeat = {crawler: datetime.now() for crawler in crawler_ranks}
    assigned_tasks = {}

    def mark_crawler_done(crawler, original_url=None):
        busy_crawlers[crawler] = False
        assigned_tasks.pop(crawler, None)
        if original_url:
            crawled_urls_set.add(original_url)
        last_heartbeat[crawler] = datetime.now()

    logger.info("Starting task distribution...")


    while urls_to_crawl_queue or any(busy_crawlers.values()):

        for crawler in crawler_ranks:
            if not busy_crawlers[crawler] and urls_to_crawl_queue:
                url, depth = urls_to_crawl_queue.pop(0)
                if url not in crawled_urls_set:
                    comm.send((url, depth), dest=crawler, tag=0)
                    busy_crawlers[crawler] = True
                    assigned_tasks[crawler] = (url, depth, datetime.now())
                    logger.info(f"Assigned URL '{url}' (Depth {depth}) to Crawler {crawler}")
                    time.sleep(CRAWL_DELAY)

        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            src = status.Get_source()
            tag = status.Get_tag()
            data = comm.recv(source=src, tag=tag)

            if tag == 1:
                original_url, original_depth = data['source_url'], data['source_depth']
                new_urls = data['new_urls']
                logger.info(f"Received {len(new_urls)} new URLs from Crawler {src} (from {original_url})")

                if original_depth + 1 <= MAX_CRAWL_DEPTH:
                    for new_url in new_urls:
                        if new_url not in crawled_urls_set and new_url not in [u for u, _ in urls_to_crawl_queue]:
                            urls_to_crawl_queue.append((new_url, original_depth + 1))

                mark_crawler_done(src, original_url)
            
            elif tag == 98:
                last_heartbeat[src] = datetime.now()
                logger.info(f"Received heartbeat from Crawler {src}")

            elif tag == 99:
                logger.info(f"Status from Crawler {src}: {data}")
                busy_crawlers[src] = False

            elif tag == 999:
                logger.error(f"Error from Crawler {src}: {data}")
                if src in assigned_tasks:
                    failed_url, failed_depth, _ = assigned_tasks.pop(src)
                    urls_to_crawl_queue.append((failed_url, failed_depth))
                    logger.info(f"Re-queued failed URL '{failed_url}' from Crawler {src}")
                busy_crawlers[src] = False
                last_heartbeat[src] = datetime.now()


        for crawler, last_time in last_heartbeat.items():
            if (datetime.now() - last_time).total_seconds() > HEARTBEAT_TIMEOUT:
                logger.warning(f"Missed heartbeat from Crawler {crawler}. May be down.")
      
        for crawler, task_info in list(assigned_tasks.items()):
            url, depth, assigned_time = task_info
            if (datetime.now() - assigned_time).total_seconds() > TASK_TIMEOUT:
                logger.warning(f"Crawler {crawler} timed out on URL '{url}'. Reassigning task.")
                urls_to_crawl_queue.append((url, depth))  # Re-queue the task
                assigned_tasks.pop(crawler)
                busy_crawlers[crawler] = False


        time.sleep(0.2)

    logger.info("Crawling finished. Sending shutdown signals to Crawlers and Indexer.")

    for crawler in crawler_ranks:
        comm.send(None, dest=crawler, tag=0)  # Shutdown signal for crawlers

    comm.send(None, dest=indexer_rank, tag=2) # Shutdown signal for indexer

    logger.info("Master node finished operations.")

if __name__ == '__main__':
    master_process()
