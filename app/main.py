import socket
import threading
import time

BUF_SIZE = 4096
database = {}
expiry_times = {}  # key: expiration_timestamp


def parse_resp_command(data: bytes) -> list[str]:
    """Parses RESP input and returns a list of arguments (e.g., ['SET', 'mykey', 'myval', 'PX', '100'])"""
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


def handle_expiration(key: str, ttl_ms: int):
    time.sleep(ttl_ms / 1000)
    database.pop(key, None)
    expiry_times.pop(key, None)


def handle_client_connection(client_socket):
    while True:
        try:
            chunk = client_socket.recv(BUF_SIZE)
            if not chunk:
                break

            args = parse_resp_command(chunk)

            if not args:
                continue

            command = args[0].upper()

            if command == "PING":
                client_socket.sendall(b"+PONG\r\n")

            elif command == "ECHO" and len(args) == 2:
                response = f"${len(args[1])}\r\n{args[1]}\r\n"
                client_socket.sendall(response.encode())

            elif command == "SET":
                if len(args) >= 3:
                    key, value = args[1], args[2]
                    database[key] = value
                    response = "+OK\r\n"

                    # Check for optional PX expiration
                    if len(args) >= 5 and args[3].lower() == "px":
                        try:
                            ttl_ms = int(args[4])
                            expiry_times[key] = time.time() + ttl_ms / 1000
                            threading.Thread(target=handle_expiration, args=(key, ttl_ms)).start()
                        except ValueError:
                            response = "-ERR invalid PX value\r\n"

                    client_socket.sendall(response.encode())

            elif command == "GET" and len(args) >= 2:
                key = args[1]
                # Check if key has expired
                if key in expiry_times and time.time() > expiry_times[key]:
