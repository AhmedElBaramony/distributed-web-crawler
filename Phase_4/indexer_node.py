import time
import os
import requests
import threading
import logging
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from sqs_config import RESULT_QUEUE_URL, receive_messages_sqs, delete_parsed_message_sqs, download_from_s3
from dashboard_logger import start_heartbeat

# ==== Constants ====
NODE_ID = "indexer"
INDEX_DIR = "indexdir"
index_lock = threading.Lock()

# ==== Start heartbeat ====
start_heartbeat(NODE_ID)

# ==== Console Logging Setup ====
logger = logging.getLogger("Indexer")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

logger.addHandler(console_handler)

# ==== Initialize Whoosh Index ====
if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)

schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))
with index_lock:
    if not os.listdir(INDEX_DIR):
        ix = create_in(INDEX_DIR, schema)
    else:
        ix = open_dir(INDEX_DIR)

# ==== Index a single URL ====
def index_url(url, content):
    with index_lock:
        writer = ix.writer()
        writer.update_document(url=url, content=content)
        writer.commit()
    logger.info(f"[Whoosh] Indexed: {url}")

# ==== Main Indexer Loop ====
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