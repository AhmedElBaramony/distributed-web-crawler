import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import threading
from dashboard_logger import start_heartbeat
import urllib3
from sqs_config import (
    CRAWL_QUEUE_URL,
    HEARTBEAT_QUEUE_URL,
    RESULT_QUEUE_URL,
    send_message_sqs,
    receive_messages_sqs,
    delete_parsed_message_sqs,
    upload_to_s3,
    url_to_s3_key,
)

# ============================
# Setup
# ============================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
EXCLUDED_EXTENSIONS = ('.pdf', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.zip', '.mp4', '.doc', '.docx', '.xls', '.xlsx')
NODE_ID = os.getenv("CRAWLER_ID", "crawler1")  # crawler1 or crawler2
DASHBOARD_HOST = "http://desired-baboon-mistakenly.ngrok-free.app"

# ============================
# Logging Setup
# ============================

logger = logging.getLogger("Crawler")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(console_handler)

# Start dashboard heartbeat
start_heartbeat(NODE_ID)

# ============================
# Helpers
# ============================

def extract_links_and_text(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    all_links = [urljoin(base_url, a.get('href')) for a in soup.find_all('a', href=True)]

    valid_links = []
    for link in all_links:
        parsed = urlparse(link)
        if not parsed.path.lower().endswith(EXCLUDED_EXTENSIONS):
            valid_links.append(link)

    text = soup.get_text()
    return valid_links, text.strip()

def send_heartbeat(worker_id):
    payload = {"worker_id": worker_id}
    send_message_sqs(HEARTBEAT_QUEUE_URL, "heartbeat", payload)

def safe_post(url):
    try:
        requests.post(url, timeout=1, verify=False)
    except Exception as e:
        logger.warning(f"[Dashboard Push Failed] {e}")

# ============================
# Main
# ============================

def main():
    worker_id = f"crawler-{int(time.time())}"
    logger.info(f"[Crawler] Starting as {worker_id}")

    while True:
        send_heartbeat(worker_id)

        messages = receive_messages_sqs(CRAWL_QUEUE_URL, max_messages=1, wait_time=5)
        if not messages:
            time.sleep(2)
            continue

        for msg in messages:
            task = msg["payload"]
            url = task.get("url")
            depth = task.get("depth", 0)
            task_id = task.get("task_id")

            if not url:
                logger.warning("Received task without URL.")
                delete_parsed_message_sqs(CRAWL_QUEUE_URL, msg)
                continue

            logger.info(f"[Crawler] Crawling: {url} (depth {depth})")

            try:
                response = requests.get(url, timeout=10)
                links, text = extract_links_and_text(response.text, url)

                key = url_to_s3_key(url)
                s3_path = f"pages/{key}"
                upload_to_s3(text, s3_path, metadata={"original_url": url})

                result_payload = {
                    "url": url,
                    "source_depth": depth,
                    "new_urls": links,
                    "task_id": task_id,
                    "s3_key": s3_path
                }

                try:
                    send_message_sqs(RESULT_QUEUE_URL, "crawl_result", result_payload, group_id="results")
                except Exception as e:
                    logger.error(f"[SQS ERROR] Failed to send result to queue: {e}")
                    
                logger.info(f"[Crawler] Sent crawl result for: {url} with {len(links)} links")
                
                threading.Thread(
                    target=safe_post,
                    args=(f"{DASHBOARD_HOST}/api/pagecount/{NODE_ID}",),
                    daemon=True
                ).start()

            except Exception as e:
                logger.error(f"[Crawler] Error crawling {url}: {e}")

            delete_parsed_message_sqs(CRAWL_QUEUE_URL, msg)

        time.sleep(1)

if __name__ == '__main__':
    main()