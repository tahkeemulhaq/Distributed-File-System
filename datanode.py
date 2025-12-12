# Cleaned up unused variables and improved readability
from flask import Flask, request, send_file, jsonify
import os
import threading
import time
import requests

METADATA_SERVER = "http://127.0.0.1:5050"
NODE_ID = os.environ.get("NODE_ID", "node1")
HOST = "127.0.0.1"
PORT = int(os.environ.get("PORT", "6001"))
STORAGE_DIR = os.environ.get("STORAGE_DIR", "./storage_node1")
HEARTBEAT_INTERVAL = 5

app = Flask(__name__)
os.makedirs(STORAGE_DIR, exist_ok=True)

def list_chunks():
    return os.listdir(STORAGE_DIR)

def send_heartbeat():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            data = {
                "node_id": NODE_ID,
                "host": HOST,
                "port": PORT,
                "chunks": list_chunks()
            }
            requests.post(f"{METADATA_SERVER}/heartbeat", json=data)
        except:
            pass

def register():
    try:
        data = {"node_id": NODE_ID, "host": HOST, "port": PORT}
        requests.post(f"{METADATA_SERVER}/register_datanode", json=data)
    except:
        pass

@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    chunk_id = request.args.get("chunk_id")
    path = os.path.join(STORAGE_DIR, chunk_id)
    with open(path, "wb") as f:
        f.write(request.data)
    return jsonify({"status": "ok"})

@app.route("/get_chunk", methods=["GET"])
def get_chunk():
    chunk_id = request.args.get("chunk_id")
    path = os.path.join(STORAGE_DIR, chunk_id)
    return send_file(path, as_attachment=True)

if __name__ == "__main__":
    register()
    t = threading.Thread(target=send_heartbeat, daemon=True)
    t.start()
    app.run(host=HOST, port=PORT)

