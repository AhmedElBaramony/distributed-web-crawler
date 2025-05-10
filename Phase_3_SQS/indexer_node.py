import time
import logging
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from sqs_config import RESULT_QUEUE_URL, receive_messages_sqs, delete_parsed_message_sqs

# ===============================
# Logging Setup
# ===============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Indexer")

# ===============================
# Whoosh Index Setup
# ===============================
if not os.path.exists("indexdir"):
    os.mkdir("indexdir")

schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))
if not os.listdir("indexdir"):
    ix = create_in("indexdir", schema)
else:
    ix = open_dir("indexdir")

# ===============================
# Indexing Function
# ===============================
def index_url(url, content):
    writer = ix.writer()
    writer.update_document(url=url, content=content)
    writer.commit()
    logger.info(f"[Whoosh] Indexed: {url}")

# ===============================
# Main Loop
# ===============================
def main():
    logger.info("[Indexer] Started and listening for crawl results...")

    while True:
        messages = receive_messages_sqs(RESULT_QUEUE_URL, max_messages=5)

        if not messages:
            time.sleep(2)
            continue

        for msg in messages:
            payload = msg.get("payload", {})
            url = payload.get("url")
            content = payload.get("html_content")

            if url and content:
                index_url(url, content)

            delete_parsed_message_sqs(RESULT_QUEUE_URL, msg)

        time.sleep(1)

if __name__ == "__main__":
    main()