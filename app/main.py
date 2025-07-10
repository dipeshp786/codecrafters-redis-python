import socket
import threading
import time
import argparse
import os
import struct

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
        with open(filepath, "rb") as f:
            data = f.read()

            if data[:5] != b"REDIS":
                print("Invalid header.")
                return

            i = data.find(b"\xfe")  # Skip to DB selector
            if i == -1:
                return

            i += 2  # skip DB selector and zero byte

            while i < len(data):
                opcode = data[i]
                if opcode == 0xFB:
                    i += 1
                    key_len = data[i]
                    i += 1
                    key = data[i:i + key_len].decode("utf-8", errors="ignore")
                    i += key_len

                    val_len = data[i]
                    i += 1
                    val = data[i:i + val_len].decode("utf-8", errors="ignore")
                    i += val_len

                    database[key] = val
                elif opcode == 0xFF:
                    break
                else:
                    i += 1
    except Exception as e:
        print(f"Failed to read RDB: {e}")

def handle_client_connection(client_socket):
    while True:
        try:
            data = client_socket.recv(BUF_SIZE)
            if not data:
                break

            args = parse_resp_command(data)
            if not args:
                continue

            command = args[0].upper()

            if command == "PING":
                client_socket.sendall(b"+PONG\r\n")

            elif command == "ECHO" and len(args) == 2:
                response = f"${len(args[1])}\r\n{args[1]}\r\n"
                client_socket.sendall(response.encode())

            elif command == "SET":
                key = args[1]
                value = args[2]
                database[key] = value

                if len(args) >= 5 and args[3].upper() == "PX":
                    try:
                        expiry_ms = int(args[4])
                        expiry_time = time.time() + expiry_ms / 1000
                        expiry_times[key] = expiry_time
                    except ValueError:
                        client_socket.sendall(b"-ERR invalid PX value\r\n")
                        continue

                client_socket.sendall(b"+OK\r\n")

            elif command == "GET":
                key = args[1]
                if key in expiry_times and time.time() > expiry_times[key]:
                    database.pop(key, None)
                    expiry_times.pop(key, None)
                    client_socket.sendall(b"$-1\r\n")
                elif key in database:
                    value = database[key]
                    response = f"${len(value)}\r\n{value}\r\n"
                    client_socket.sendall(response.encode())
                else:
                    client_socket.sendall(b"$-1\r\n")

            elif command == "CONFIG" and len(args) == 3 and args[1].upper() == "GET" and args[2] == "dir":
                response = f"*2\r\n$3\r\ndir\r\n${len(dir_value)}\r\n{dir_value}\r\n"
                client_socket.sendall(response.encode())

            elif command == "KEYS" and len(args) == 2 and args[1] == "*":
                keys = [key for key in database.keys()]
                resp = f"*{len(keys)}\r\n"
                for key in keys:
                    resp += f"${len(key)}\r\n{key}\r\n"
                client_socket.sendall(resp.encode())

        except Exception as e:
            print(f"Error: {e}")
            break

    client_socket.close()

def main():
    global dir_value

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, default=".")
