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

            elif command == "CONFIG" and len(args) == 3 and args[1].upper() == "GET" and args[2] == "dir":
                dir_value = "."  # Later you can replace this with a value passed from --dir
                response = f"*2\r\n$3\r\ndir\r\n${len(dir_value)}\r\n{dir_value}\r\n"
                client_socket.sendall(response.encode())

        except Exception as e:
            print(f"Error: {e}")
            break

    client_socket.close()
