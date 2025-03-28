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
        LOGGER.info(f"TCP Server is listening on port {port} and is hosted on {host}")
        while not signals["shutdown"]:
            try:
                conn, _ = sock.accept()
                LOGGER.info("Connection accepted")
            except socket.timeout:
                LOGGER.info("Connection timed out")
                continue
            conn.settimeout(1)
            with conn:
                message=[]
                while True:
                    try:
                        data = conn.recv(4096)
                        LOGGER.info(f"Data: {data}")
                        if not data:
                            LOGGER.warning("No data received from TCP connection")
                            break
                    except socket.timeout:
                        LOGGER.warning("Timeout while receiving data from connection")
                        continue
                    if not data:
                        break
                    message.append(data)
            msg = (b''.join(message)).decode("utf-8")
            if msg:
                try:
                    message_dict = json.loads(msg)
                    LOGGER.info("message %s", message_dict)
                    handle_messages(message_dict)
                    LOGGER.info(f"sent")
                except json.JSONDecodeError:
                    continue


def tcp_client(msg, host, port):
        """Send messages via TCP."""
        
        LOGGER.info(f"client")
        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the server
            LOGGER.info(f"Attempting to TCP connect to {host}:{port}")
            sock.connect((host, port))
            LOGGER.info(f"TCP connected to {host}:{port}")
            # Send a message
            msg_str = json.dumps(msg)
            LOGGER.info(f"Sending message: {msg_str}")
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
                    if not data:
                            LOGGER.warning("No data received from UDP connection")
                            break
                except socket.timeout:
                    continue
                try:
                    message = json.loads(data.decode("utf-8"))
                    handle_messages(message)
                except json.JSONDecodeError:
                    continue
                


def udp_client(msg, host, port):
    """Send messages via UDP."""
    LOGGER.info(msg)

        # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # sock.connect((host, port))
        # Send a message
        msg_str = json.dumps(msg)
        LOGGER.info(f"Sending message: {msg_str}")
        sock.sendto(msg_str.encode('utf-8'), (host, port))

