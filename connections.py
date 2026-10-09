import socket
import threading
from handshake import send_handshake, receive_handshake

# Read the four length bytes then based on that number read that many body bytes
def read_message(sock, max_message_length):
    raise NotImplementedError("Implement framed TCP reads")

# Encode then sendall under a socket's send lock.
def send_message(sock, send_lock, message_type, payload=b""):
    raise NotImplementedError("Implement synchronized sends")

# Listen and connect to earlier entries in the peer list
# Validate the handshakes and reject unknowns, duplicate or self connections
def start_connections(local_peer_id, peers, on_connected):
    peer_ids = [peer["peer_id"] for peer in peers]

    if local_peer_id not in peer_ids:
        raise ValueError("Local peer ID is not in PeerInfo.cfg")

    local_index = peer_ids.index(local_peer_id)
    local_peer = peers[local_index]

    connected_ids = set()
    connection_lock = threading.Lock()
    stop_event = threading.Event()

    def finish_connection(sock, outgoing, expected_peer_id:None):
        registered_id = None

        try:
            #Set limit for how long an incomplete handshake can block
            sock.settimeout(10)

            send_handshake(sock, local_peer_id)
            remote_id = receive_handshake(sock, expected_peer_id)

            if remote_id not in peer_ids:
                raise ValueError("Unknown remote peer ID")
            if remote_id == local_peer_id:
                raise ValueError("Cannot connect to my own ID")

            with connection_lock:
                if remote_id in connected_ids:
                    raise ValueError("Duplicate peer connection")

                connected_ids.add(remote_id)
                registered_id = remote_id

            sock.settimeout(None)
            #Will take responsibility for the socket once connection is achieved
            on_connected(remote_id, sock, outgoing)

        except Exception as error:
            sock.close()

            if registered_id is not None:
                with connection_lock:
                    connected_ids.discard(registered_id)
            print(f"Connection failed: error")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        server.bind(("0.0.0.0", local_peer["port"]))
        server.listen()
        server.settimeout(1)

    except OSError:
        server.close()
        raise

    def accept_connections():
        try:
            while not stop_event.is_set():
                try:
                    sock, address = server.accept()
                except socket.timeout:
                    continue

                finish_connection(sock, outgoing=False, expected_peer_id=None)
        finally:
            server.close()

    listener_thread = threading.Thread(target=accept_connections, daemon=True)
    listener_thread.start()

    # Initiate connections to each entry before this one
    for neighbor in peers[:local_index]:
        try:
            sock = socket.create_connection(
                (neighbor["host"], neighbor["port"]),
                timeout=10
            )
        except OSError as error:
            print(f"Unable to connect to {neighbor["peer_id"]}: {error}")
            continue

        finish_connection(sock, outgoing=True, expected_peer_id=neighbor["peer_id"])

    return stop_event, listener_thread
