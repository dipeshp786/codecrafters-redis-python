#!/usr/bin/env python3
import os
import sys
import socket
import threading
import re
import signal

database = {}
server = None

def load_rdb(filepath):
    if not os.path.isfile(filepath):
        print(f"[your_program] RDB file not found: {filepath}")
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
        database[key_str] = "value"  # dummy value for keys

    print(f"[your_program] Loaded keys from RDB: {list(database.keys())}")

def parse_redis_command(data_bytes):
    """
    Parse a simple Redis RESP array command like:
    *2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n
    Returns list of strings, e.g. ['KEYS', '*']
    """
    try:
        data_str = data_bytes.decode('utf-8')
        lines = data_str.split('\r\n')
        if not lines or not lines[0].startswith('*'):
            return []

        num_elements = int(lines[0][1:])
        args = []
        idx = 1
        for _ in range(num_elements):
            if idx >= len(lines):
                break
            if lines[idx].startswith('$'):
                length = int(lines[idx][1:])
                idx += 1
                if idx >= len(lines):
                    break
                args.append(lines[idx])
                idx += 1
            else:
                break
        return args
    except Exception:
        return []

def handle_client(client_socket):
    try:
        data = client_socket.recv(1024)
        if not data:
            client_socket.close()
            return

        args = parse_redis_command(data)
        if not args:
            client_socket.sendall(b"-Error parsing command\r\n")
            client_socket.close()
            return

        cmd = args[0].upper()

        if cmd == "KEYS":
            # Support only simple '*' wildcard to return all keys
            pattern = args[1] if len(args) > 1 else "*"
            if pattern == "*":
                keys = list(database.keys())
                response = f"*{len(keys)}\r\n"
                for k in keys:
                    response += f"${len(k)}\r\n{k}\r\n"
                client_socket.sendall(response.encode('utf-8'))
            else:
                # Unsupported pattern — respond empty array
                client_socket.sendall(b"*0\r\n")
        else:
            # Unsupported command — empty array
            client_socket.sendall(b"*0\r\n")
    except Exception as e:
        print(f"[your_program] Error handling client: {e}")
    finally:
        client_socket.close()

def shutdown_handler(signum, frame):
    global server
    print("\n[your_program] Shutdown signal received, closing server...")
    if server:
        server.close()
    sys.exit(0)

def run_server(host="localhost", port=6379):
    global server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind((host, port))
    except OSError as e:
        print(f"[your_program] ERROR: Could not bind to {host}:{port} - {e}")
        sys.exit(1)
    server.listen(5)
    print(f"[your_program] Listening on {host}:{port}")

    # Setup graceful shutdown on Ctrl+C
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while True:
            client_socket, addr = server.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.daemon = True
            client_thread.start()
    except Exception as e:
        print(f"[your_program] Server error: {e}")
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
