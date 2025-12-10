import sys
import os
import math
import requests

METADATA_SERVER = "http://127.0.0.1:5050"
CHUNK_SIZE = 1024 * 1024

def upload(file_path):
    if not os.path.exists(file_path):
        print("File does not exist")
        return

    filename = os.path.basename(file_path)
    size = os.path.getsize(file_path)
    num_chunks = math.ceil(size / CHUNK_SIZE)

    resp = requests.post(f"{METADATA_SERVER}/upload_request",
                         json={"filename": filename, "num_chunks": num_chunks})
    if resp.status_code != 200:
        print("Upload failed:", resp.text)
        return

    info = resp.json()
    chunk_ids = info["chunk_ids"]
    mapping = info["chunk_mapping"]

    with open(file_path, "rb") as f:
        for i, cid in enumerate(chunk_ids):
            data = f.read(CHUNK_SIZE)
            nodes = mapping[cid]
            for node in nodes:
                url = f"http://{node['host']}:{node['port']}/store_chunk"
                requests.post(url, params={"chunk_id": cid}, data=data)
            print(f"Uploaded chunk {i+1}/{len(chunk_ids)}")

    print("Upload complete.")

def download(filename, output_path):
    resp = requests.get(f"{METADATA_SERVER}/download_metadata", params={"filename": filename})
    if resp.status_code != 200:
        print("Download failed:", resp.text)
        return

    info = resp.json()
    chunks = info["chunks"]

    with open(output_path, "wb") as out:
        for i, ch in enumerate(chunks):
            cid = ch["chunk_id"]
            nodes = ch["nodes"]
            for node in nodes:
                try:
                    url = f"http://{node['host']}:{node['port']}/get_chunk"
                    r = requests.get(url, params={"chunk_id": cid}, timeout=5)
                    if r.status_code == 200:
                        out.write(r.content)
                        break
                except:
                    pass
            print(f"Downloaded chunk {i+1}/{len(chunks)}")

    print("Download complete.")

def list_files():
    resp = requests.get(f"{METADATA_SERVER}/list_files")
    print("Files:", resp.json().get("files", []))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 client.py upload <file>")
        print("  python3 client.py download <filename> <output>")
        print("  python3 client.py ls")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "upload":
        upload(sys.argv[2])
    elif cmd == "download":
        download(sys.argv[2], sys.argv[3])
    elif cmd == "ls":
        list_files()

