import socket
import json
import logging

LOGGER = logging.getLogger(__name__)

def tcp_server(host, port, signals, handle_messages):
    """TCP Socket Server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen()
        sock.settimeout(1)
        while not signals["shutdown"]:
            try:
                connection, _ = sock.accept()
            except socket.timeout:
                continue
            connection.settimeout(1)
            with connection:
                message=[]
                while True:
                    try:
                        data = connection.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message.append(data)
            msg = b''.join(message).decode("utf-8")
            try:
                message_dict = json.loads(msg)
                handle_messages(message_dict)
            except json.JSONDecodeError:
                continue


def tcp_client(msg, host, port):
        """Send messages via TCP."""

        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the server
            sock.connect((host, port))
            
            # Send a message
            msg_str = json.dumps(msg)
            sock.sendall(msg_str.encode('utf-8'))


def udp_server(host, port, signals, handle_messages):
        """Listen for heartbeats."""
        
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)
            LOGGER.info(f"UDP Server is listening on port {port} and is hosted on {host}")
            
            while not signals["shutdown"]:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                try:
                    message = json.loads(data.decode("utf-8"))
                    handle_messages(message)
                except json.JSONDecodeError:
                    continue
                print(message)


def udp_client(msg, host, port):
    """Send messages via UDP."""

    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Send a message
        msg_str = json.dumps(msg)
        sock.sendto(msg_str.encode('utf-8'), (host, port))
