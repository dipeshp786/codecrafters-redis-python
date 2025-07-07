import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client


if __name__ == "__main__":
    main()
#!/bin/bash
python3 -u -c '
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 6379))
s.listen(1)
print("Listening on port 6379...")
conn, addr = s.accept()
print("Accepted connection from", addr)
conn.close()
'
