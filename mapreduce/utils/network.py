import socket
import json
import logging

LOGGER = logging.getLogger(__name__)

def tcp_server(host, port, signals, handle_func):
    while not signals["shutdown"]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            
            LOGGER.info(f"TCP Server is listening on port {port} and is hosted on {host}")
            
            #do something for each message
            for message_dict in listen_to_messages(sock):
                pass

def tcp_client(host, port, msg):
        """Test TCP Socket Client."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect(host, port)

            # send a message
            sock.sendall(msg.encode('utf-8'))


def udp_server(host, port, signals, handle_func):
    while not signals["shutdown"]:
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)
            LOGGER.info(f"UDP Server is listening on port {port} and is hosted on {host}")
            
            #do something for each message
            for message_dict in listen_to_messages(sock):
                pass

def udp_client(host, port, msg):
    # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Connect to the UDP socket on server
        sock.connect((host, port))
        # Send a message
        sock.sendall(msg.encode('utf-8'))

def listen_to_messages(sock):
    while True:
            # Wait for a connection for 1s.  The socket library avoids consuming
            # CPU while waiting for a connection.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)

            # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            
            #Return message without breaking out of function
            LOGGER.info(message_dict)
            yield message_dict