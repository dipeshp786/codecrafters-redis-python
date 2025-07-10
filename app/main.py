import socket
import threading
import time

BUF_SIZE = 4096
database = {}
expiry_times = {}

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

        except Exception as e:
            print(f"Error: {e}")
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
