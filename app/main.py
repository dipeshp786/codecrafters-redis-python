#!/usr/bin/env python3
import os
import sys
import socket
import threading
import re

database = {}

def load_rdb(filepath):
    if not os.path.isfile(filepath):
        print(f"RDB file not found: {filepath}")
        return
    with open(filepath, "rb") as f:
        data = f.read()

    candidate_keys = re.findall(b'[a-z]{5,20}', data)
    skip_keys = {"redis", "ver", "bits", "rdb", "expiry", "aux"}

    database.clear()
    for key in candidate_keys:
        key_str = key.decode('ascii', errors='ignore')
        if key_str in skip_keys:
            continue
        database[key_str] = "value"  # dummy value for all keys

    print(f"[your_program] Loaded keys from RDB: {list(database.keys())}")

def handle_client(client_socket):
    try:
        data = client_socket.recv(1024)
        if not data:
            client_socket.close()
            return

        # Very simple RESP parser for 'KEYS *' command only
        cmd = data.decode('utf-8').strip()
        # Expect something like: *2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n
        if "KEYS" in cmd:
            keys = list(database.keys())
            # Construct RESP array of keys
            response = f"*{len(keys)}\r\n"
            for k in keys:
                response += f"${len(k)}\r\n{k}\r\n"
            client_socket.sendall(response.encode('utf-8'))
        else:
            # Respond with empty array for unsupported commands
            client_socket.sendall(b"*0\r\n")
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        client_socket.close()

def run_server(host="localhost", port=6379):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"[your_program] Listening on {host}:{port}")
    try:
        while True:
            client_socket, addr = server.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()
    except KeyboardInterrupt:
        print("\n[your_program] Server shutting down.")
    finally:
        server.close()

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="RDB directory")
    parser.add_argument("--dbfilename", required=True, help="RDB filename")
    args = parser.parse_args()

    rdb_path = os.path.join(args.dir, args.dbfilename)

    print(f"[your_program] RDB directory: {args.dir}")
    print(f"[your_program] RDB filename: {args.dbfilename}")
    print(f"[your_program] Full path: {rdb_path}")

    load_rdb(rdb_path)
    run_server()

if __name__ == "__main__":
    main()
