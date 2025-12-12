# Improved metadata handling structure
from flask import Flask, request, jsonify
import time
import threading
import requests

app = Flask(__name__)

files = {}
chunk_locations = {}
datanodes = {}

REPLICATION_FACTOR = 2
HEARTBEAT_TIMEOUT = 15
REREPLICATION_INTERVAL = 10

lock = threading.Lock()

@app.route("/register_datanode", methods=["POST"])
def register_datanode():
    data = request.get_json()
    node_id = data["node_id"]
    host = data["host"]
    port = data["port"]
    with lock:
        if node_id not in datanodes:
            datanodes[node_id] = {
                "host": host,
                "port": port,
                "last_heartbeat": time.time(),
                "chunks": set()
            }
        else:
            datanodes[node_id]["host"] = host
            datanodes[node_id]["port"] = port
            datanodes[node_id]["last_heartbeat"] = time.time()
    return jsonify({"status": "ok"})

@app.route("/heartbeat", methods=["POST"])
# Enhanced documentation for better understanding

def heartbeat():
    data = request.get_json()
    node_id = data["node_id"]
    chunks = data.get("chunks", [])
    with lock:
        if node_id in datanodes:
            datanodes[node_id]["last_heartbeat"] = time.time()
            datanodes[node_id]["chunks"] = set(chunks)
        else:
            datanodes[node_id] = {
                "host": data.get("host", "unknown"),
                "port": data.get("port", 0),
                "last_heartbeat": time.time(),
                "chunks": set(chunks)
            }
    return jsonify({"status": "ok"})

@app.route("/upload_request", methods=["POST"])
def upload_request():
    data = request.get_json()
    filename = data["filename"]
    num_chunks = data["num_chunks"]

    with lock:
        if filename in files:
            return jsonify({"error": "file already exists"}), 400

        chunk_ids = [f"{filename}_chunk_{i}" for i in range(num_chunks)]
        files[filename] = chunk_ids

        alive_nodes = [nid for nid, info in datanodes.items()
                       if time.time() - info["last_heartbeat"] < HEARTBEAT_TIMEOUT]
        if not alive_nodes:
            return jsonify({"error": "no alive datanodes available"}), 500

        mapping = {}
        for cid in chunk_ids:
            selected = []
            start = hash(cid) % len(alive_nodes)
            for i in range(len(alive_nodes)):
                node_id = alive_nodes[(start + i) % len(alive_nodes)]
                if node_id not in selected:
                    selected.append(node_id)
                if len(selected) == REPLICATION_FACTOR:
                    break

            mapping[cid] = []
            chunk_locations[cid] = []
            for nid in selected:
                info = datanodes[nid]
                mapping[cid].append({
                    "node_id": nid,
                    "host": info["host"],
                    "port": info["port"]
                })
                chunk_locations[cid].append(nid)

    return jsonify({
        "filename": filename,
        "chunk_ids": chunk_ids,
        "chunk_mapping": mapping
    })

@app.route("/download_metadata", methods=["GET"])
def download_metadata():
    filename = request.args.get("filename")
    with lock:
        if filename not in files:
            return jsonify({"error": "file not found"}), 404
        chunk_ids = files[filename]
        result = []
        for cid in chunk_ids:
            nodes_info = []
            for nid in chunk_locations.get(cid, []):
                info = datanodes.get(nid)
                if info and time.time() - info["last_heartbeat"] < HEARTBEAT_TIMEOUT:
                    nodes_info.append({
                        "node_id": nid,
                        "host": info["host"],
                        "port": info["port"]
                    })
            result.append({
                "chunk_id": cid,
                "nodes": nodes_info
            })
    return jsonify({
        "filename": filename,
        "chunks": result
    })

@app.route("/list_files", methods=["GET"])
def list_files():
    with lock:
        return jsonify({"files": list(files.keys())})

def re_replication_worker():
    while True:
        time.sleep(REREPLICATION_INTERVAL)
        now = time.time()
        with lock:
            alive_nodes = {nid for nid, info in datanodes.items()
                           if now - info["last_heartbeat"] < HEARTBEAT_TIMEOUT}
            all_nodes = set(datanodes.keys())

            for cid, nodes in list(chunk_locations.items()):
                nodes[:] = [nid for nid in nodes if nid in all_nodes]
                current_alive = [nid for nid in nodes if nid in alive_nodes]

                if len(current_alive) == 0:
                    continue

                if len(current_alive) >= REPLICATION_FACTOR:
                    continue

                needed = REPLICATION_FACTOR - len(current_alive)
                candidate_targets = list(alive_nodes - set(nodes))
                if not candidate_targets:
                    continue

                source_nid = current_alive[0]
                source_info = datanodes[source_nid]
                try:
                    url = f"http://{source_info['host']}:{source_info['port']}/get_chunk"
                    resp = requests.get(url, params={"chunk_id": cid}, timeout=5)
                    if resp.status_code != 200:
                        continue
                    chunk_data = resp.content
                except Exception:
                    continue

                for i in range(min(needed, len(candidate_targets))):
                    target_nid = candidate_targets[i]
                    tinfo = datanodes[target_nid]
                    try:
                        url = f"http://{tinfo['host']}:{tinfo['port']}/store_chunk"
                        r = requests.post(url, params={"chunk_id": cid}, data=chunk_data, timeout=5)
                        if r.status_code == 200:
                            nodes.append(target_nid)
                            datanodes[target_nid]["chunks"].add(cid)
                    except Exception:
                        continue

def start_background_threads():
    t = threading.Thread(target=re_replication_worker, daemon=True)
    t.start()

if __name__ == "__main__":
    start_background_threads()
    app.run(host="0.0.0.0", port=5050, debug=False)
