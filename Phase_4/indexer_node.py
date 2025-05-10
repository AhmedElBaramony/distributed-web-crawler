import time
import logging
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from sqs_config import RESULT_QUEUE_URL, receive_messages_sqs, delete_parsed_message_sqs, download_from_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Indexer")

if not os.path.exists("indexdir"):
    os.mkdir("indexdir")

schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))
if not os.listdir("indexdir"):
    ix = create_in("indexdir", schema)
else:
    ix = open_dir("indexdir")

def index_url(url, content):
    writer = ix.writer()
    writer.update_document(url=url, content=content)
    writer.commit()
    logger.info(f"[Whoosh] Indexed: {url}")

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
            s3_key = payload.get("s3_key")

            if url and s3_key:
                try:
                    content = download_from_s3(s3_key)
                    index_url(url, content)
                except Exception as e:
                    logger.error(f"[Indexer] Failed to index {url}: {e}")

            delete_parsed_message_sqs(RESULT_QUEUE_URL, msg)

        time.sleep(1)

if __name__ == "__main__":
    main()