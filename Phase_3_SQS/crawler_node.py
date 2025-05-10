import time
import logging
import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from sqs_config import (
    CRAWL_QUEUE_URL,
    HEARTBEAT_QUEUE_URL,
    RESULT_QUEUE_URL,
    send_message_sqs,
    receive_messages_sqs,
    delete_parsed_message_sqs,
)

EXCLUDED_EXTENSIONS = (
    '.pdf', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.zip',
    '.mp4', '.doc', '.docx', '.xls', '.xlsx'
)

MAX_SQS_SIZE = 250_000  # bytes

# ===============================
# Logging Setup
# ===============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Crawler")

# ===============================
# Helper Functions
# ===============================

def extract_links_and_text(html, base_url):
    """
    Parses HTML content and extracts:
    - A list of absolute hyperlinks
    - The visible text content
    """
    soup = BeautifulSoup(html, 'html.parser')
    all_links = [urljoin(base_url, a.get('href')) for a in soup.find_all('a', href=True)]

    # Filter out unwanted file types
    valid_links = []
    for link in all_links:
        parsed = urlparse(link)
        if not parsed.path.lower().endswith(EXCLUDED_EXTENSIONS):
            valid_links.append(link)

    text = soup.get_text()
    return valid_links, text.strip()


def send_heartbeat(worker_id):
    """
    Sends a heartbeat message to notify the master this crawler is alive.
    """
    payload = {"worker_id": worker_id}
    send_message_sqs(HEARTBEAT_QUEUE_URL, "heartbeat", payload)

def build_sqs_safe_payload(url, depth, links, task_id, text):
    """
    Dynamically trims text and links to ensure payload is under MAX_SQS_SIZE.
    """
    max_links = len(links)
    truncated_text = text

    while True:
        payload = {
            "url": url,
            "source_depth": depth,
            "new_urls": links[:max_links],
            "task_id": task_id,
            "html_content": truncated_text
        }

        try:
            payload_bytes = json.dumps(payload).encode("utf-8")
            if len(payload_bytes) <= MAX_SQS_SIZE:
                return payload
            else:
                # Shrink text first, then links
                if len(truncated_text) > 1000:
                    truncated_text = truncated_text[:int(len(truncated_text) * 0.9)]
                elif max_links > 0:
                    max_links = max_links - 10
                else:
                    logger.warning(f"[Payload] Too large to send even after truncation: {url}")
                    return None
        except Exception as e:
            logger.error(f"[Payload Error] Failed to encode payload: {e}")
            return None

# ===============================
# Main Crawler Loop
# ===============================

def main():
    worker_id = f"crawler-{int(time.time())}"  # Unique worker ID
    logger.info(f"[Crawler] Starting as {worker_id}")

    while True:
        send_heartbeat(worker_id)

        messages = receive_messages_sqs(CRAWL_QUEUE_URL, max_messages=1, wait_time=5)
        if not messages:
            time.sleep(2)
            continue

        for msg in messages:
            task = msg.get("payload", {})
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

                result_payload = build_sqs_safe_payload(url, depth, links, task_id, text)
                if result_payload:
                    send_message_sqs(RESULT_QUEUE_URL, "crawl_result", result_payload)
                    logger.info(f"[Crawler] Sent crawl result for: {url} with {len(result_payload['new_urls'])} links")
                else:
                    logger.error(f"[Crawler] Failed to send result for: {url} — payload too large.")

            except Exception as e:
                logger.error(f"[Crawler] Error crawling {url}: {e}")

            delete_parsed_message_sqs(CRAWL_QUEUE_URL, msg)

        time.sleep(1)

if __name__ == '__main__':
    main()