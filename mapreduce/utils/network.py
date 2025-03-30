"""TCP and UDP connection setups."""
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
        LOGGER.info("TCP listening on port %s, hosted on %s", port, host)
        while not signals["shutdown"]:
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                LOGGER.info("Connection timed out")
                continue
            conn.settimeout(1)
            with conn:
                message = []
                while True:
                    try:
                        data = conn.recv(4096)
                        LOGGER.info("TCP got data: %s", data)
                        if not data:
                            break
                    except socket.timeout:
                        LOGGER.warning("TCP socket timeout")
                        continue
                    if not data:
                        break
                    message.append(data)
            msg = (b''.join(message)).decode("utf-8")
            if msg:
                try:
                    message_dict = json.loads(msg)
                    handle_messages(message_dict)
                    LOGGER.info("TCP got message %s", message_dict)
                except json.JSONDecodeError:
                    LOGGER.info("decode error")
                    continue


def tcp_client(msg, host, port):
    """Send messages via TCP."""
    # Create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Connect to the server
        sock.connect((host, port))
        # Send a message
        msg_str = json.dumps(msg)
        LOGGER.info("TCP sent message %s", msg_str)
        sock.sendall(msg_str.encode('utf-8'))


def udp_server(host, port, signals, handle_messages):
    """Listen for heartbeats."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)
        LOGGER.info("UDP listening on port %s, hosted on %s", port, host)
        while not signals["shutdown"]:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue
            msg = data.decode("utf-8")
            try:
                message = json.loads(msg)
                LOGGER.info("UDP got message: %s", message)
                handle_messages(message)
            except json.JSONDecodeError:
                continue


def udp_client(msg, host, port):
    """Send messages via UDP."""
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # sock.connect((host, port))
        # Send a message
        msg_str = json.dumps(msg)
        LOGGER.info("UDP sending message %s", msg_str)
        sock.sendall(msg_str.encode('utf-8'), (host, port))
