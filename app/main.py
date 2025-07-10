#!/usr/bin/env python3
import socket
import threading
import time
import argparse
import os

BUF_SIZE = 4096
database = {}
expiry_times = {}
dir_value = "."

def parse_resp_command(data: bytes) -> list[str]:
    lines = data.decode(errors="ignore").split("\r\n")
    if not lines or not lines[0].startswith("*"):
        return []
    args = []
    i = 2
    while i < len(lines):
        if lines[i] == "":
            break
        args.append(lines[i])
        i += 2
    return args

def read_rdb_file(filepath):
    try:
        data = open(filepath, "rb").read()
        i = data.find(b"\xfe")
        if i == -1: return
        i += 2
        while i < len(data):
            opcode = data[i]
            if opcode == 0xFB:
                i += 1
                keylen = data[i]; i += 1
                key = data[i:i+keylen].decode('utf-8', 'ignore'); i += keylen
                vallen = data[i]; i += 1
                val = data[i:i+vallen].decode('utf-8', 'ignore'); i += vallen
                database[key] = val
            elif opcode == 0xFF:
                break
            else:
                i += 1
    except Exception as e:
        print("Failed to read RDB:", e)

def handle_client_connection(client_socket):
    while True:
        try:
            data = client_socket.recv(BUF_SIZE)
            if not data: break
            args = parse_resp_command(data)
            if not args: continue
            cmd = args[0].upper()

            if cmd == "PING":
                client_socket.sendall(b"+PONG\r\n")
            elif cmd == "ECHO" and len(args) == 2:
                client_socket.sendall(f"${len(args[1])}\r\n{args[1]}\r\n".encode())
            elif cmd == "SET":
                database[args[1]] = args[2]
                client_socket.sendall(b"+OK\r\n")
            elif cmd == "GET":
                v = database.get(args[1])
                if v is None:
                    client_socket.sendall(b"$-1\r\n")
                else:
                    client_socket.sendall(f"${len(v)}\r\n{v}\r\n".encode())
            elif cmd == "CONFIG" and len(args) == 3 and args[1].upper() == "GET" and args[2] == "dir":
                resp = f"*2\r\n$3\r\ndir\r\n${len(dir_value)}\r\n{dir_value}\r\n"
                client_socket.sendall(resp.encode())
            elif cmd == "KEYS" and args[1] == "*":
                keys = list(database.keys())
                resp = f"*{len(keys)}\r\n"
                for k in keys:
                    resp += f"${len(k)}\r\n{k}\r\n"
                client_socket.sendall(resp.encode())
        except:
            break
    client_socket.close()

def main():
    global dir_value

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=".")
    parser.add_argument("--dbfilename", default="dump.rdb")
    args = parser.parse_args()

    dir_value = args.dir
    rdbfile = os.path.join(args.dir, args.dbfilename)
    print("Logs from your program will appear here!")
    print(f"RDB directory: {dir_value}")
    print(f"RDB filename: {args.dbfilename}")
    if os.path.exists(rdbfile):
        print("Loading RDB...")
        read_rdb_file(rdbfile)
    else:
        print("No RDB file found, starting fresh.")

    server = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server listening on localhost:6379")
    while True:
        client, _ = server.accept()
        threading.Thread(target=handle_client_connection, args=(client,)).start()

if __name__ == "__main__":
    main()
