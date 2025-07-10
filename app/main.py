import socket
import threading
import time
import argparse
import os

BUF_SIZE = 4096
database = {}
expiry_times = {}
dir_value = "."  # will be set by --dir arg

def parse_resp_command(data: bytes) -> list[str]:
    lines = data.decode().split("\r\n")
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
                print("Invalid RDB header.")
                return

            # Skip header (first few metadata bytes)
            i = data.find(b"\xfe")  # first DB selector
            if i == -1:
                return

            i += 1
            while i < len(data):
                if data[i] == 0xFB:  # type 0xFB = String key-value pair
                    i += 1
                    keylen = data[i]
                    i += 1
                    key = data[i:i + keylen].decode()
                    i += keylen
                    vallen = data[i]
                    i += 1
                    value = data[i:i + vallen].decode()
                    i += vallen
                    database[key] = value
                elif data[i] == 0xFF:
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
    parser.add_argument("--dbfilename", type=str, default="dump.rdb")
    args = parser.parse_args()

    dir_value = args.dir
    rdb_path = os.path.join(args.dir, args.dbfilename)

    print("Logs from your program will appear here!")
    print(f"RDB directory: {args.dir}")
    print(f"RDB filename: {args.dbfilename}")
    print(f"Full path: {rdb_path}")

    if os.path.exists(rdb_path):
        print("Loading RDB...")
        read_rdb_file(rdb_path)
    else:
        print("RDB file not found, starting fresh.")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        client_socket, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client_connection, args=(client_socket,))
        thread.start()

if __name__ == "__main__":
    main()
