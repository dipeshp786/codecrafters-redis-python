import socket
import threading

BUF_SIZE = 4096

def handle_client_connection(client_socket):
    while True:
        try:
            chunk = client_socket.recv(BUF_SIZE)
            if not chunk:
                break  # client disconnected
            # Decode client data
            data = chunk.decode()

            # Check for "PING" command (case-insensitive)
            if "ping" in data.lower():
                client_socket.sendall(b"+PONG\r\n")
        except ConnectionResetError:
            break  # Handle client forcibly closing the connection

    client_socket.close()


from app import DatabaseHandler, ExpirationManager

COMMAND_NAME = "ECHO"


def handle_command(
    args: list, database: DatabaseHandler, expirations_manager: ExpirationManager
) -> str:
    if len(args) != 1 or len(args[0]) == 0:  # Error if no argument is provided
        return "-ERR wrong number of arguments for command\r\n"
    return f"${len(args[0])}\r\n{args[0]}\r\n"
    
    
    

def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Accept and handle multiple clients using threads
    while True:
        client_socket, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client_connection, args=(client_socket,))
        thread.start()


if __name__ == "__main__":
    main()
