import socket
import threading

BUF_SIZE = 4096


def parse_resp_command(data: bytes) -> list[str]:
    """Parses a RESP Array and returns list of strings (e.g., ['ECHO', 'hello'])"""
    lines = data.decode().split("\r\n")
    if not lines or lines[0][0] != "*":
        return []

    args = []
    i = 2  # Start after *<count> and $<length>
    while i < len(lines) and lines[i]:
        args.append(lines[i])
        i += 2  # Skip $<length> line and take the actual string
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
            else:
                client_socket.sendall(b"-ERR unknown command\r\n")

        except ConnectionResetError:
            break

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
