# dashboard_logger.py
import requests
import urllib.parse
import threading
import time
import logging

DASHBOARD_HOST = "http://192.168.1.12:5000"
LOG_ENDPOINT = "/api/logs/{node}"
HEARTBEAT_ENDPOINT = "/api/heartbeat/{node}"

def log(node, message):
    def push_line(line):
        try:
            safe_msg = urllib.parse.quote(line)
            url = f"{DASHBOARD_HOST}{LOG_ENDPOINT.format(node=node)}"
            requests.post(url, params={"msg": safe_msg}, timeout=1)
        except Exception as e:
            print(f"Failed to send log: {e}")  # Add error logging

    for line in message.splitlines():
        threading.Thread(target=push_line, args=(line,), daemon=True).start()

def start_heartbeat(node, interval=5):
    def beat():
        while True:
            try:
                requests.get(f"{DASHBOARD_HOST}{HEARTBEAT_ENDPOINT.format(node=node)}", timeout=1)
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")  # Add error logging
            time.sleep(interval)
    thread = threading.Thread(target=beat, daemon=True)
    thread.start()

class DashboardLogHandler(logging.Handler):
    def __init__(self, node):
        super().__init__()
        self.node = node

    def emit(self, record):
        try:
            msg = self.format(record)
            log(self.node, msg)
        except Exception as e:
            print(f"Failed to emit log: {e}")  # Add error logging