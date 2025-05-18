# ===============================
# Import Libraries
# ===============================

from flask import Flask, render_template, jsonify, request, send_file
from collections import defaultdict, deque
from subprocess import Popen
import time, threading, os, shutil
from flask_cors import CORS
import urllib.parse
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh import scoring
from sqs_config import s3, S3_BUCKET_NAME, download_from_s3

# ============================
# Constants
# ============================

INDEX_DIR = "s3_indexdir"   # Directory for Whoosh index
MAX_LOG_LINES = 200         # Maximum number of log lines to keep in memory

# ============================
# Flask App Initialization
# ============================
app = Flask(__name__)
CORS(app)
index_lock = threading.Lock() # Lock for index creation

# ========== In-Memory Data Structures ==========
logs = defaultdict(lambda: deque(maxlen=MAX_LOG_LINES))
heartbeat_status = {}
heartbeat_counts = defaultdict(int)
pages_crawled = defaultdict(int)
urls_indexed = defaultdict(int)
queue_info = {"queued_tasks": 0, "active_crawls": 0}

# ========== Nodes & Roles ==========
NODE_IDS = ["master", "indexer", "crawler1", "crawler2", "combined"]
node_roles = {
    "master": "master",
    "indexer": "indexer",
    "crawler1": "crawler",
    "crawler2": "crawler"
}

# ============================
# Whoosh Index Handling
# ============================

# Initialize the Whoosh index
def init_s3_index():
    with index_lock:
        if not os.path.exists(INDEX_DIR):
            os.mkdir(INDEX_DIR)

        schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))

        if not exists_in(INDEX_DIR):
            ix = create_in(INDEX_DIR, schema)
            writer = ix.writer()
            print("[INDEX] Building Whoosh index from S3...")

            response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="pages/")
            for obj in response.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".html"):
                    try:
                        url, content = download_from_s3(key)
                        writer.add_document(url=url, content=content)
                    except Exception as e:
                        print(f"[ERROR] Indexing {key} failed: {e}")
            writer.commit()

            print("[INDEX] S3 indexing complete.")
        else:
            print("[INDEX] Existing Whoosh index found.")

# Background thread to refresh the index from S3 every 2 minutes
def background_reindex(interval=120):
    while True:
        print("[REINDEX] Refreshing index from S3...")
        try:
            shutil.rmtree(INDEX_DIR)
        except Exception as e:
            print(f"[REINDEX] Failed to delete old index: {e}")
        init_s3_index()
        time.sleep(interval)

threading.Thread(target=background_reindex, daemon=True).start()

# ============================
# Heartbeat and Cleanup
# ============================

# Auto-cleanup thread to reset heartbeat status for offline nodes (if no heartbeat for 10 seconds)
def auto_cleanup():
    while True:
        time.sleep(5)
        now = time.time()
        for node, role in node_roles.items():
            if (now - heartbeat_status.get(node, 0)) > 10:
                heartbeat_status[node] = 0

threading.Thread(target=auto_cleanup, daemon=True).start()

# ============================
# Routes
# ============================

# ================ Dashboard Route ================
@app.route("/")
def index():
    return render_template("index.html", node_ids=NODE_IDS, node_roles=node_roles)

# ================ Logs Routes ================
@app.route("/api/logs/<node>", methods=["GET", "POST"])
def handle_logs(node):
    if request.method == "GET":
        return jsonify(list(logs[node]))
    
    msg = request.args.get("msg", "")
    msg = urllib.parse.unquote(msg)
    if not msg.strip():
        return "empty", 400

    for line in msg.splitlines():
        timestamp = time.strftime("%H:%M:%S")
        entry = f"[{timestamp}] {line}"
        logs[node].append(entry)
        logs["combined"].append(f"[{timestamp}] ({node}) {line}")
    
    return "ok"

# ================ Heartbeat Route ================
@app.route("/api/heartbeat/<node>", methods=["GET"])
def heartbeat(node):
    heartbeat_status[node] = time.time()
    heartbeat_counts[node] += 1
    return "OK"

# ================ Crawled Pages Route ================
@app.route("/api/pagecount/<node>", methods=["POST"])
def update_pages_crawled(node):
    pages_crawled[node] += 1
    return "OK"

# ================ Indexed URLs Route ================
@app.route("/api/indexed/<node>", methods=["POST"])
def update_indexed(node):
    urls_indexed[node] += 1
    return "OK"

# ================ Queue Info Route ================
@app.route("/api/metrics")
def get_metrics():
    now = time.time()
    metrics = {
        "heartbeat_status": {},
        "heartbeat_counts": {},
        "pages_crawled": {},
        "urls_indexed": {},
    }

    for node, role in node_roles.items():
        is_online = (now - heartbeat_status.get(node, 0)) < 10
        metrics["heartbeat_status"][node] = "online" if is_online else "offline"

        if is_online:
            if role == "crawler":
                metrics["heartbeat_counts"][node] = heartbeat_counts[node]
                metrics["pages_crawled"][node] = pages_crawled[node]
            elif role == "indexer":
                metrics["urls_indexed"][node] = urls_indexed[node]
        else:
            logs[node].clear()
            if role == "crawler":
                pages_crawled[node] = 0
                heartbeat_counts[node] = 0
            elif role == "indexer":
                urls_indexed[node] = 0

    # Global Metrics
    metrics["total_pages_crawled"] = sum(pages_crawled.values())
    metrics["total_urls_indexed"] = sum(urls_indexed.values())
    metrics["active_nodes"] = sum(1 for v in metrics["heartbeat_status"].values() if v == "online")

    return jsonify(metrics)

# ================ Download Logs Route ================ "Experimental"
@app.route("/api/download/<node>")
def download_logs(node):
    filename = f"logs_{node}.txt"
    with open(filename, "w") as f:
        f.write("\n".join(logs[node]))
    return send_file(filename, as_attachment=True)

# ============================
# Search Functionality
# ============================
def search_index(query_string):
    try:
        ix = open_dir(INDEX_DIR)
    except Exception as e:
        return [f"Error opening index: {e}"]

    results = []
    with ix.searcher(weighting=scoring.BM25F()) as searcher:
        parser = QueryParser("content", schema=ix.schema)
        query = parser.parse(query_string)
        hits = searcher.search(query, limit=20)
        for hit in hits:
            results.append(hit.get("url", "Unknown URL"))
    return results

# ================ Search Route ================
@app.route("/api/search")
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    return jsonify(search_index(query))

# ================ Start Web App ================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)