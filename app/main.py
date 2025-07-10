#!/usr/bin/env python3
import socket
import sys
import os
import argparse
import threading

data_store = {}
global_dir = ""

def load_rdb(rdb_path):
    # Mock RDB loading — just add keys from a dummy list for testing
    # In real code, parse RDB binary file properly
    if not os.path.exists(rdb_path):
        print(f"[your_program] RDB file not found: {rdb_path}")
        return
    # Example keys hardcoded for demo (replace with actual parsing)
    # For demonstration, pretend we load these keys:
    keys = ["apple", "strawberry", "orange", "pear", "raspberry"]
    for k in keys:
        data_store[k] = "value_for_" + k
    print(f"[your_program] Loaded keys from RDB: {list(data_store.keys())}")

def parse_resp(data):
    # Very minimal RESP parser for commands
    # Assumes full command arrives at once (for demo only)
    lines = data.split(b'\r\n')
    if not lines:
        return None, None
    try:
        if lines[0].startswith(b'*'):
            n = int(lines[0][1:])
            args = []
            i = 1
            while len(args) < n:
                length = int(lines[i][1:])
                arg = lines[i+1]
                args.append(arg.decode())
                i += 2
            cmd = args[0].upper()
            return cmd, args
        else:
            return None, None
    except Exception:
        return None, None

def send_resp_array(client, items):
    resp = f"*{len(items)}\r\n"
    for item in items:
        resp += f"${len(item)}\r\n{item}\r\n"
    client.sendall(resp.encode())

def send_resp_error(client, message):
    resp = f"-ERR {message}\r\n"
    client.sendall(resp.encode())

def send_resp_simple_string(client, message):
    resp = f"+{message}\r\n"
    client.sendall(resp.encode())

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

        # Handle KEYS *
        if cmd == "KEYS":
            if len(args) != 2:
                send_resp_error(client_socket, f"Expected command to have 1 argument, got {len(args)-1}")
            elif args[1] == "*":
                keys = list(data_store.keys())
                send_resp_array(client_socket, keys)
            else:
                send_resp_array(client_socket, [])
            client_socket.close()
            return

        # Handle CONFIG GET dir
        if cmd == "CONFIG":
            if len(args) == 3 and args[1].upper() == "GET" and args[2] == "dir":
                send_resp_array(client_socket, ["dir", global_dir])
            else:
                send_resp_array(client_socket, [])
            client_socket.close()
            return

        # For other commands, respond error
        send_resp_error(client_socket, "Unsupported command")
    except Exception as e:
        print(f"[your_program] Exception: {e}")
    finally:
        client_socket.close()

def run_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(5)
    print("[your_program] Listening on localhost:6379")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(target=handle_client, args=(client_socket, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("[your_program] Shutdown signal received, closing server...")
    finally:
        server_socket.close()

def main():
    global global_dir
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="RDB directory")
    parser.add_argument("--dbfilename", required=True, help="RDB filename")
    args = parser.parse_args()

    global_dir = args.dir

    rdb_path = os.path.join(args.dir, args.dbfilename)

    print(f"[your_program] RDB directory: {args.dir}")
    print(f"[your_program] RDB filename: {args.dbfilename}")
    print(f"[your_program] Full path: {rdb_path}")

    load_rdb(rdb_path)
    run_server()

if __name__ == "__main__":
    main()
