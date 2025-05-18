import time
import os
import requests
import threading
import logging
import urllib3
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from sqs_config import RESULT_QUEUE_URL, receive_messages_sqs, delete_parsed_message_sqs, download_from_s3
from dashboard_logger import start_heartbeat, DashboardLogHandler, log, DASHBOARD_HOST

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================
# Constants
# ============================

NODE_ID = "indexer"
INDEX_DIR = "indexdir"
index_lock = threading.Lock()

# Start heartbeat
start_heartbeat(NODE_ID)

# ============================
# Logging Setup
# ============================

# Create a logger
logger = logging.getLogger("Indexer")
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
# Helper Functions
# ============================

def safe_post(url):
    try:
        requests.post(url, timeout=1)
    except Exception as e:
        logger.warning(f"[Dashboard Push Failed] {e}")

# ============================
# Whoosh Index Setup
# ============================

if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)

schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))
with index_lock:
    if not os.listdir(INDEX_DIR):
        ix = create_in(INDEX_DIR, schema)
    else:
        ix = open_dir(INDEX_DIR)


# ============================
# Helper Functions
# ============================

# Function to index a URL and its content
def index_url(url, content):
    with index_lock:
        writer = ix.writer()
        writer.update_document(url=url, content=content)
        writer.commit()
    logger.info(f"[Whoosh] Indexed: {url}")
    
    # Updates the indexed counter in the dashboard asynchronously
    threading.Thread(
        target=safe_post,
        args=(f"{DASHBOARD_HOST}/api/indexed/{NODE_ID}",),
        daemon=True
    ).start()

# ============================
# Main
# ============================

def main():
    logger.info("[Indexer] Started and listening for crawl results...")

    while True:
        messages = receive_messages_sqs(RESULT_QUEUE_URL, max_messages=5)

        if not messages:
            time.sleep(2)
            continue

        for msg in messages:
            payload = msg.get("payload", {})
            s3_key = payload.get("s3_key")

            if s3_key:
                try:
                    url, content = download_from_s3(s3_key)
                    index_url(url, content)
                except Exception as e:
                    logger.error(f"[Indexer] Failed to index {s3_key}: {e}")

            delete_parsed_message_sqs(RESULT_QUEUE_URL, msg)

        time.sleep(1)

if __name__ == "__main__":
    main()