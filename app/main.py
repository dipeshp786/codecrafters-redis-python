import socket  # noqa: F401

def main():
    print("Logs from your program will appear here!")

    # Create server
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Accept one client connection
    connection, _ = server_socket.accept()

  

    # Send a valid Redis PONG response
    connection.sendall(b"+PONG\r\n")

while True:
        request: bytes = client_socket.recv(512)
        data: str = request.decode()

        # print(data)
        if "ping" in data.lower():
            client_socket.send("+PONG\r\n".encode())

if __name__ == "__main__":
    main()
