import time
import logging
from datetime import datetime
from uuid import uuid4

from sqs_config import (
    CRAWL_QUEUE_URL,
    HEARTBEAT_QUEUE_URL,
    RESULT_QUEUE_URL,
    send_message_sqs,
    receive_messages_sqs,
    delete_parsed_message_sqs
)
from dashboard_logger import start_heartbeat, DashboardLogHandler

# ============================
# Setup
# ============================

NODE_ID = "master"
start_heartbeat(NODE_ID)

logger = logging.getLogger("Master")
logger.setLevel(logging.INFO)

# Add console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(console_handler)

# Add dashboard handler
dashboard_handler = DashboardLogHandler(NODE_ID)
dashboard_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(dashboard_handler)

# ============================
# Configuration
# ============================

CRAWL_DELAY = 0.1       # seconds
MAX_CRAWL_DEPTH = 2     # max depth to crawl
HEARTBEAT_TIMEOUT = 10  # seconds
TASK_TIMEOUT = 55       # seconds

# ============================
# Main
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

        # =========== Assign New Crawl Tasks ===========
        while urls_to_crawl_queue:
            url, depth = urls_to_crawl_queue.pop(0)
            if url in crawled_urls_set:
                continue

            task_id = str(uuid4()) # Generate a unique task ID
            payload = {
                "url": url,
                "depth": depth,
                "task_id": task_id
            }
            send_message_sqs(CRAWL_QUEUE_URL, "crawl_task", payload, group_id="crawl")
            assigned_tasks[task_id] = (url, depth, now)
            logger.info(f"[Assigned] {url} at depth {depth} → Task ID {task_id}")
            time.sleep(CRAWL_DELAY)

        # ============ Receive Heartbeats from SQS ============
        for msg in receive_messages_sqs(HEARTBEAT_QUEUE_URL, max_messages=10):
            payload = msg.get("payload", {})
            worker_id = payload.get("worker_id")

            if worker_id:
                last_heartbeat[worker_id] = datetime.now()
                logger.info(f"[Heartbeat] from {worker_id}")

            delete_parsed_message_sqs(HEARTBEAT_QUEUE_URL, msg)

        # =============== Receive Crawl Results ===============
        for msg in receive_messages_sqs(RESULT_QUEUE_URL, max_messages=5):
            
            # Extract the payload from the message
            payload = msg.get("payload", {})
            task_id = payload.get("task_id")
            url = payload.get("url")
            new_urls = payload.get("new_urls", [])
            source_depth = payload.get("source_depth", 0)

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

        # =============== Check Task Timeout ===============
        for task_id, (url, depth, assigned_time) in list(assigned_tasks.items()):
            if (now - assigned_time).total_seconds() > TASK_TIMEOUT:
                logger.warning(f"[Timeout] Re-queueing task {url}")
                urls_to_crawl_queue.append((url, depth))
                assigned_tasks.pop(task_id)

        # =============== Detect Dead Workers ===============
        for worker_id, last_seen in list(last_heartbeat.items()):
            if (now - last_seen).total_seconds() > HEARTBEAT_TIMEOUT:
                logger.warning(f"[Missed] No heartbeat from {worker_id} in {HEARTBEAT_TIMEOUT} sec")
                last_heartbeat.pop(worker_id)

        time.sleep(0.5)

    logger.info("[Master] All tasks complete. Shutting down.")

if __name__ == "__main__":
    main()