import socket
import threading

BUF_SIZE = 4096
database = {}  # In-memory key-value store


def parse_resp_command(data: bytes) -> list[str]:
    """Parses a RESP Array and returns list of strings (e.g., ['SET', 'mykey', 'hello'])"""
    lines = data.decode().split("\r\n")
    if not lines or lines[0][0] != "*":
        return []

    args = []
    i = 2  # Skip *count and $length of first item
    while i < len(lines):
        if lines[i] == "":
            break
        args.append(lines[i])
        i += 2  # Skip $length line and actual string
    return args


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
            elif command == "SET" and len(args) == 3:
                key, value = args[1], args[2]
                database[key] =
