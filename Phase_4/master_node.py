import time
import logging
from datetime import datetime
from uuid import uuid4
from urllib.parse import urlparse, urlunparse


from sqs_config import (
    CRAWL_QUEUE_URL,
    HEARTBEAT_QUEUE_URL,
    RESULT_QUEUE_URL,
    send_message_sqs,
    receive_messages_sqs,
    delete_parsed_message_sqs
)

# ============================
# Logging Setup
# ============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Master")

# ============================
# Configuration
# ============================
CRAWL_DELAY = 0.1
MAX_CRAWL_DEPTH = 2
HEARTBEAT_TIMEOUT = 10  # seconds
TASK_TIMEOUT = 55       # seconds

# ============================
# Master Logic
# ============================

def main():
    logger.info("[Master] Starting")

    seed_urls = [("https://demo.cyotek.com/", 0)]
    urls_to_crawl_queue = list(seed_urls)
    crawled_urls_set = set()
    assigned_tasks = {}  # task_id → (url, depth, assigned_time)
    last_heartbeat = {}  # worker_id → last_seen_timestamp

    logger.info("[Master] Starting task distribution loop...")

    while urls_to_crawl_queue or assigned_tasks:
        now = datetime.now()

        # Assign new crawl tasks
        while urls_to_crawl_queue:
            url, depth = urls_to_crawl_queue.pop(0)
            if url in crawled_urls_set:
                continue

            task_id = str(uuid4())
            payload = {
                "url": url,
                "depth": depth,
                "task_id": task_id
            }
            send_message_sqs(CRAWL_QUEUE_URL, "crawl_task", payload, group_id="crawl")
            assigned_tasks[task_id] = (url, depth, now)
            logger.info(f"[Assigned] {url} at depth {depth} → Task ID {task_id}")
            time.sleep(CRAWL_DELAY)

        # Receive heartbeats
        for msg in receive_messages_sqs(HEARTBEAT_QUEUE_URL, max_messages=10):
            if not isinstance(msg, dict):
                logger.warning(f"[Malformed heartbeat message] {msg}")
                continue

            payload = msg.get("payload", {})
            if not isinstance(payload, dict):
                logger.warning(f"[Malformed payload] {payload}")
                continue

            worker_id = payload.get("worker_id")
            if worker_id:
                last_heartbeat[worker_id] = datetime.now()
                logger.info(f"[Heartbeat] from {worker_id}")

            delete_parsed_message_sqs(HEARTBEAT_QUEUE_URL, msg)

        # Receive crawl results
        for msg in receive_messages_sqs(RESULT_QUEUE_URL, max_messages=5):
            payload = msg.get("payload", {})
            task_id = payload.get("task_id")
            url = payload.get("url")
            new_urls = payload.get("new_urls", [])
            source_depth = payload.get("source_depth", 0)
            
            #if url:
            crawled_urls_set.add(url)
            if task_id in assigned_tasks:
                assigned_tasks.pop(task_id)
            else:
                logger.warning(f"[Result] Received orphaned result for task_id {task_id}")

            if source_depth + 1 <= MAX_CRAWL_DEPTH:
                for new_url in new_urls:
                    if new_url not in crawled_urls_set and new_url not in [u for u, _ in urls_to_crawl_queue]:
                        urls_to_crawl_queue.append((new_url, source_depth + 1))
            logger.info(f"[Result] {url} → {len(new_urls)} new URLs")
            delete_parsed_message_sqs(RESULT_QUEUE_URL, msg)

        # Check task timeouts
        for task_id, (url, depth, assigned_time) in list(assigned_tasks.items()):
            if (now - assigned_time).total_seconds() > TASK_TIMEOUT:
                logger.warning(f"[Timeout] Re-queueing task {url}")
                urls_to_crawl_queue.append((url, depth))
                assigned_tasks.pop(task_id)

        # Check for dead workers
        for worker_id, last_seen in list(last_heartbeat.items()):
            if (now - last_seen).total_seconds() > HEARTBEAT_TIMEOUT:
                logger.warning(f"[Missed] No heartbeat from {worker_id} in {HEARTBEAT_TIMEOUT} sec")
                last_heartbeat.pop(worker_id)

        time.sleep(0.5)

    logger.info("[Master] All tasks complete. Shutting down.")

if __name__ == "__main__":
    main()