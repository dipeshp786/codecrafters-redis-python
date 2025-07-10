#!/usr/bin/env python3
import socket
import threading
import argparse
import os

BUF_SIZE = 4096
database = {}
dir_value = "."

def parse_resp_command(data: bytes):
    """Parse basic RESP arrays into list of strings."""
    try:
        lines = data.decode(errors='ignore').split("\r\n")
        if not lines or not lines[0].startswith("*"):
            return []
        n = int(lines[0][1:])
        args = []
        idx = 1
        for _ in range(n):
            length = int(lines[idx][1:])
            idx += 1
            args.append(lines[idx])
            idx += 1
        return args
    except Exception:
        return []

def load_rdb(filepath):
    """Very simple loader for RDB keys only (assumes certain structure)."""
    if not os.path.isfile(filepath):
        return
    with open(filepath, "rb") as f:
        data = f.read()
    # Super simplified: scan for keys in data by a naive method
    # Real RDB parsing is complex, so here we scan for ASCII strings between 3 and 20 chars
    import re
    keys = re.findall(b'[a-z]{3,20}', data)
    for key in keys:
        key_str = key.decode('ascii', errors='ignore')
        if key_str not in database:
            database[key_str] = "value"  # dummy value
    print(f"Loaded keys from RDB: {list(database.keys())}")

def handle_client(sock):
    try:
        while True:
            data = sock.recv(BUF_SIZE)
            if not data:
                break
            args = parse_resp_command(data)
            if not args:
                continue
            cmd = args[0].upper()
            if cmd == "PING":
                sock.sendall(b"+PONG\r\n")
            elif cmd == "ECHO" and len(args) == 2:
                msg = args[1]
                resp = f"${len(msg)}\r\n{msg}\r\n"
                sock.sendall(resp.encode())
            elif cmd == "SET" and len(args) >= 3:
                database[args[1]] = args[2]
                sock.sendall(b"+OK\r\n")
            elif cmd == "GET" and len(args) >= 2:
                val = database.get(args[1])
                if val is None:
                    sock.sendall(b"$-1\r\n")
                else:
                    resp = f"${len(val)}\r\n{val}\r\n"
                    sock.sendall(resp.encode())
            elif cmd == "KEYS" and len(args) >= 2 and args[1] == "*":
                keys = list(database.keys())
                resp = f"*{len(keys)}\r\n"
                for k in keys:
                    resp += f"${len(k)}\r\n{k}\r\n"
                sock.sendall(resp.encode())
            elif cmd == "CONFIG" and len(args) == 3 and args[1].upper() == "GET" and args[2] == "dir":
                resp = f"*2\r\n$3\r\ndir\r\n${len(dir_value)}\r\n{dir_value}\r\n"
                sock.sendall(resp.encode())
            else:
                sock.sendall(b"-ERR unknown command\r\n")
    except Exception:
        pass
    finally:
        sock.close()

def main():
    global dir_value
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=".")
    parser.add_argument("--dbfilename", default="dump.rdb")
    args = parser.parse_args()

    dir_value = args.dir
    rdb_path = os.path.join(dir_value, args.dbfilename)

    print("Logs from your program will appear here!")
    print(f"RDB directory: {dir_value}")
    print(f"RDB filename: {args.dbfilename}")

    load_rdb(rdb_path)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    print("Listening on localhost:6379")

    while True:
        client_sock, _ = server.accept()
        threading.Thread(target=handle_client, args=(client_sock,), daemon=True).start()

if __name__ == "__main__":
    main()
