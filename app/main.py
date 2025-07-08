import socket  # noqa: F401

def main():
    print("Logs from your program will appear here!")

    # Create server
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Accept one client connection
    connection, _ = server_socket.accept()

    # Receive data (but we don’t use it yet for PING)
    connection.recv(1024)

    # Send a valid Redis PONG response
    connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
