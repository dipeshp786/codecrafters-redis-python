#!/usr/bin/env python3
import socket
import threading
import argparse
import os
import re

data_store = {}
global_dir = ""

def load_keys_from_file(filepath):
    if not os.path.isfile(filepath):
        print(f"[your_program] RDB file not found: {filepath}")
        return []
    with open(filepath, 'rb') as f:
        content = f.read()
    matches = re.findall(rb'[a-zA-Z0-9_-]{3,20}', content)
    keys = []
    for m in matches:
        try:
            k = m.decode('ascii')
            if k not in keys:
                keys.append(k)
        except:
            pass
    print(f"[your_program] Loaded keys from RDB: {keys}")
    return keys

def parse_resp(data):
    try:
        parts = data.decode(errors='ignore').split('\r\n')
        if not parts or not parts[0].startswith('*'):
            return None, None
        n = int(parts[0][1:])
        args = []
        idx = 1
        for _ in range(n):
            if not parts[idx].startswith('$'):
                return None, None
            idx += 1
            args.append(parts[idx])
            idx += 1
        return args[0].upper(), args
    except:
        return None, None

def send_resp_array(sock, arr):
    resp = f"*{len(arr)}\r\n"
    for x in arr:
        resp += f"${len(x)}\r\n{x}\r\n"
    sock.sendall(resp.encode())

def send_resp_error(sock, msg):
    sock.sendall(f"-ERR {msg}\r\n".encode())

def send_resp_string(sock, msg):
    sock.sendall(f"+{msg}\r\n".encode())

def handle_client(client_socket, _addr):
    try:
        data = client_socket.recv(4096)
        if not data:
            return
        cmd, args = parse_resp(data)
        if cmd is None:
            send_resp_error(client_socket, "Invalid command")
            return

        if cmd == "KEYS":
            num = len(args) - 1
            if num == 0 or (num == 1 and args[1] == "*"):
                send_resp_array(client_socket, list(data_store.keys()))
            else:
                send_resp_error(client_socket, f"Expected command to have 0 arguments, got {num}")
            return

        if cmd == "CONFIG":
            if len(args) == 3 and args[1].upper() == "GET" and args[2] == "dir":
                send_resp_array(client_socket, ["dir", global_dir])
            else:
                send_resp_array(client_socket, [])
            return

        send_resp_error(client_socket, f"Unknown command '{cmd}'")
    finally:
        client_socket.close()

def main():
    global global_dir, data_store

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True)
    parser.add_argument("--dbfilename", required=True)
    args = parser.parse_args()

    global_dir = args.dir
    rdb_path = os.path.join(args.dir, args.dbfilename)

    print(f"[your_program] RDB directory: {args.dir}")
    print(f"[your_program] RDB filename: {args.dbfilename}")
    print(f"[your_program] Full path: {rdb_path}")

    keys = load_keys_from_file(rdb_path)
    data_store = {k: "value" for k in keys}

    server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    server.bind(('::', 6379))
    server.listen(5)

    print(f"[your_program] Listening on all interfaces, port 6379")

    try:
        while True:
            client_sock, addr = server.accept()
            threading.Thread(target=handle_client, args=(client_sock, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("[your_program] Shutdown signal received, closing server...")
    finally:
        server.close()

if __name__ == "__main__":
    main()
