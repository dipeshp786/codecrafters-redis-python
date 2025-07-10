import socket
import sys
import threading

data_store = {}  # key-value store, keys loaded from RDB file

def parse_resp(data):
    """
    Very simple RESP parser for array commands like:
    *2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n
    Returns (cmd, args) where args includes cmd itself as first element.
    """
    try:
        lines = data.decode().split('\r\n')
        if not lines or lines[0][0] != '*':
            return None, None
        num_elems = int(lines[0][1:])
        args = []
        idx = 1
        for _ in range(num_elems):
            # skip bulk string length line ($<num>)
            bulk_len_line = lines[idx]
            if not bulk_len_line.startswith('$'):
                return None, None
            bulk_len = int(bulk_len_line[1:])
            idx += 1
            args.append(lines[idx])
            idx += 1
        cmd = args[0].upper()
        return cmd, args
    except Exception:
        return None, None

def send_resp_array(sock, array):
    resp = f"*{len(array)}\r\n"
    for item in array:
        resp += f"${len(item)}\r\n{item}\r\n"
    sock.sendall(resp.encode())

def send_resp_error(sock, msg):
    resp = f"-ERR {msg}\r\n"
    sock.sendall(resp.encode())

def handle_client(client_socket, addr):
    try:
        data = client_socket.recv(4096)
        if not data:
            client_socket.close()
            return
        cmd, args = parse_resp(data)
        if cmd is None:
            send_resp_error(client_socket, "Invalid command")
            client_socket.close()
            return

        # Handle KEYS command with 0 or 1 arg
        if cmd == "KEYS":
            num_args = len(args) - 1  # exclude command itself
            if num_args == 0:
                keys = list(data_store.keys())
                send_resp_array(client_socket, keys)
            elif num_args == 1 and args[1] == "*":
                keys = list(data_store.keys())
                send_resp_array(client_socket, keys)
            else:
                send_resp_error(client_socket, f"Expected command to have 0 arguments, got {num_args}")
            client_socket.close()
            return

        # For other commands, just error for now
        send_resp_error(client_socket, f"Unknown command '{cmd}'")
        client_socket.close()
    except Exception as e:
        print(f"[your_program] Exception: {e}")
        client_socket.close()

def load_rdb_mock():
    # Mock loading keys from RDB file for demo purposes
    # Replace with your actual RDB loading code
    global data_store
    data_store = {
        "apple": "fruit",
        "orange": "fruit",
        "strawberry": "fruit"
    }
    print(f"[your_program] Loaded keys from RDB: {list(data_store.keys())}")

def main():
    load_rdb_mock()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 6379))
    server.listen(5)
    print("[your_program] Listening on localhost:6379")

    try:
        while True:
            client_sock, addr = server.accept()
            threading.Thread(target=handle_client, args=(client_sock, addr)).start()
    except KeyboardInterrupt:
        print("[your_program] Shutdown signal received, closing server...")
    finally:
        server.close()

if __name__ == "__main__":
    main()
