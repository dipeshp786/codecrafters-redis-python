import socket
import threading

BUF_SIZE = 4096
database = {}  # In-memory key-value store


def parse_resp_command(data: bytes) -> list[str]:
    """Parses a RESP Array and returns a list of strings (e.g., ['SET', 'mykey', 'hello'])"""
    lines = data.decode().split("\r\n")
    if not lines or not lines[0].startswith("*"):
        return []

    args = []
    i = 2  # Start after the first bulk string length
    while i < len(lines):
        if lines[i] == "":
            break
        args.append(lines[i])
        i += 2  # Skip the length line and move to next argument
    return args


def handle_client_connection(client_socket):
    while True:
        try:
            chunk = client_socket.recv(BUF_SIZE)
            if not chunk:
                break  # client disconnected

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
                database[key] = value
                client_socket.sendall(b"+OK\r\n")

            elif command == "GET" and len(args) == 2:
                key = args[1]
                value = database.get(key)
                if value is not None:
                    response = f"${len(value)}\r\n{value}\r\n"
                else:
                    response = "$-1\r\n"  # RESP nil
                client_socket.sendall(response.encode())

        except ConnectionResetError:
            break  # client forcibly disconnected

    client_socket.close()


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        client_socket, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client_connection, args=(client_socket,))
        thread.start()


if __name__ == "__main__":
    main()
