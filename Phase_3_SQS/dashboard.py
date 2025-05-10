from flask import Flask, request, jsonify, send_from_directory
import subprocess
import threading
import os
import time
import platform
from whoosh.index import open_dir
from whoosh.qparser import QueryParser

app = Flask(__name__, static_folder='static')

# Globals
processes = {}
index_dir = "Phase_3/indexdir"

# --- Utilities ---
def run_process(name, cmd):
    def target():
        if platform.system() == "Darwin":  # macOS
            term_cmd = f'osascript -e \'tell app "Terminal" to do script "{cmd}"\''
            subprocess.Popen(term_cmd, shell=True)
        elif platform.system() == "Linux":
            subprocess.Popen(f'gnome-terminal -- bash -c "{cmd}; exec bash"', shell=True)
        elif platform.system() == "Windows":
            subprocess.Popen(f'start cmd /k "{cmd}"', shell=True)
        else:
            subprocess.Popen(cmd, shell=True)
    thread = threading.Thread(target=target)
    thread.start()

def stop_process(name):
    # Manual process stop still required unless tracked by PID.
    return f"Manual stop for {name} not implemented. Close terminal manually."

# --- Routes ---
@app.route('/')
def index():
    return send_from_directory('.', 'templates/dashboard.html')

@app.route('/start/<node>')
def start_node(node):
    if node == 'master':
        run_process("master", "python3 Phase_3/master_node.py")
    elif node == 'indexer':
        run_process("indexer", "python3 Phase_3/indexer_node.py")
    elif node == 'crawler':
        run_process(f"crawler_{int(time.time())}", "python3 Phase_3/crawler_node.py")
    return f"Started {node}"

@app.route('/stop/<node>')
def stop_node(node):
    return stop_process(node)

@app.route('/status')
def get_status():
    return "Status not tracked in this mode. Check terminal tabs manually."

@app.route('/seed')
def send_seed():
    url = request.args.get("url")
    if not url:
        return "No URL provided", 400
    with open("seed_input.txt", "w") as f:
        f.write(url)
    return f"Seed URL '{url}' sent to system"

@app.route('/search')
def search():
    query = request.args.get("q")
    if not query:
        return jsonify(results=[])
    try:
        ix = open_dir(index_dir)
        qp = QueryParser("content", schema=ix.schema)
        q = qp.parse(query)
        results = []
        with ix.searcher() as s:
            for r in s.search(q, limit=10):
                results.append(r.get("title", "[No Title]"))
        return jsonify(results=results)
    except Exception as e:
        return jsonify(results=[f"Error: {e}"])

@app.route('/inspect')
def inspect():
    try:
        ix = open_dir(index_dir)
        with ix.searcher() as s:
            count = s.doc_count()
        return jsonify(total_urls=count)
    except Exception as e:
        return jsonify(total_urls=0, error=str(e))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
