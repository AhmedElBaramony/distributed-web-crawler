import os
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh import scoring
from sqs_config import s3, S3_BUCKET_NAME, download_from_s3

# Directory for the new index
NEW_INDEX_DIR = "s3_indexdir"

# Ensure clean rebuild
if os.path.exists(NEW_INDEX_DIR):
    for f in os.listdir(NEW_INDEX_DIR):
        os.remove(os.path.join(NEW_INDEX_DIR, f))
else:
    os.mkdir(NEW_INDEX_DIR)

# Define schema and create index
schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))
ix = create_in(NEW_INDEX_DIR, schema)
writer = ix.writer()

# List and index all .html files in 'pages/'
response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="pages/")
for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".html"):
        try:
            content = download_from_s3(key)
            url = "(unknown URL)"  # We don’t store the original URL in S3, only the hash
            writer.add_document(url=key, content=content)
        except Exception as e:
            print(f"[ERROR] Failed to index {key}: {e}")

writer.commit()
print("Indexing from S3 completed.")

# Search loop
parser = QueryParser("content", schema=ix.schema)
with ix.searcher(weighting=scoring.BM25F()) as searcher:
    while True:
        query_text = input("\n🔍 Enter search query (or 'exit'): ").strip()
        if query_text.lower() in ["exit", "quit"]:
            break
        try:
            query = parser.parse(query_text)
            results = searcher.search(query, limit=10)
            print(f"\n📄 Found {len(results)} result(s):")
            for i, hit in enumerate(results):
                print(f"\nResult #{i+1}")
                print(f"S3 Key: {hit['url']}")
                print(f"Snippet: {hit.highlights('content', top=2)}")
        except Exception as e:
            print(f"[ERROR] Search failed: {e}")
