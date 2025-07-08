import socket

def main():
    print("Logs from your program will appear here!")

    # Create the server socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Accept a single client connection
    connection, _ = server_socket.accept()

    # Keep responding to multiple PING requests
    while True:
        request = connection.recv(1024)
        if not request:
            break  # client disconnected

        data = request.decode()

        if "ping" in data.lower():
            connection.sendall(b"+PONG\r\n")

if __name__ == "__main__":
    main()
