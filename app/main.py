import socket
import sys
import threading
import argparse
import os

data_store = {}

def load_keys_from_file(filepath):
    # Minimal example: assume file contains keys, one per line
    # Replace with actual RDB parsing if needed
    if not os.path.isfile(filepath):
        print(f"[your_program] RDB file not found: {filepath}")
        return []
    with open(filepath, 'r') as f:
        keys = [line.strip() for line in f.readlines() if line.strip()]
    return keys

def parse_resp(data):
    try:
        lines = data.decode().split('\r\n')
        if not lines or lines[0][0] != '*':
            return None, None
        num_elems = int(lines[0][1:])
        args = []
        idx = 1
        for _ in range(num_elems):
            bulk_len_line = lines[idx]
            if not bulk_len_line.startswith('$'):
                return None, None
            idx += 1
            args.append(lines[idx])
            idx += 1
        cmd = args[0].upper()
        return cmd, args
    except Exception:
        return None, None

def send_resp_array(sock, array):
    resp = f"*{len(array)}\r\n"
    for item in array:
        resp += f"${len(item)}\r\n{item}\r\n"
    sock.sendall(resp.encode())

def send_resp_error(sock, msg):
    resp = f"-ERR {msg}\r\n"
    sock.sendall(resp.encode())

def handle_client(client_socket, addr):
    try:
        data = client_socket.recv(4096)
        if not data:
            client_socket.close()
            return
        cmd, args = parse_resp(data)
        if cmd is None:
            send_resp_error(client_socket, "Invalid command")
            client_socket.close()
            return

        if cmd == "KEYS":
            num_args = len(args) - 1
            if num_args == 0:
                keys = list(data_store.keys())
                send_resp_array(client_socket, keys)
            elif num_args == 1 and args[1] == "*":
                keys = list(data_store.keys())
                send_resp_array(client_socket, keys)
            else:
                send_resp_error(client_socket, f"Expected command to have 0 arguments, got {num_args}")
            client_socket.close()
            return

        send_resp_error(client_socket, f"Unknown command '{cmd}'")
        client_socket.close()
    except Exception as e:
        print(f"[your_program] Exception: {e}")
        client_socket.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="RDB directory")
    parser.add_argument("--dbfilename", required=True, help="RDB filename")
    args = parser.parse_args()

    rdb_path = os.path.join(args.dir, args.dbfilename)
    keys = load_keys_from_file(rdb_path)
    if keys:
        global data_store
        data_store = {key: "value" for key in keys}  # dummy values

    print(f"[your_program] RDB directory: {args.dir}")
    print(f"[your_program] RDB filename: {args.dbfilename}")
    print(f"[your_program] Full path: {rdb_path}")
    print(f"[your_program] Loaded keys from RDB: {list(data_store.keys())}")

import socket

    server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)  # Allow both IPv6 and IPv4
    server.bind(('::', 6379))
    server.listen(5)

print("Listening on all IPv6 and IPv4 addresses, port 6379")

    print("[your_program] Listening on localhost:6379")

    try:
        while True:
            client_sock, addr = server.accept()
            threading.Thread(target=handle_client, args=(client_sock, addr)).start()
    except KeyboardInterrupt:
        print("[your_program] Shutdown signal received, closing server...")
    finally:
        server.close()

if __name__ == "__main__":
    main()
