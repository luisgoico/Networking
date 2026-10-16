import sys
import threading

from connections import start_connections, read_message, send_message, MAX_MESSAGE_LENGTH
from messages import BITFIELD, INTERESTED, MESSAGE_NAMES


# Testing two peers on one computer, using different ports.
peers = [
    {"peer_id": 1001, "hostname": "127.0.0.1", "port": 6001},
    {"peer_id": 1002, "hostname": "127.0.0.1", "port": 6002},
]

local_peer_id = int(sys.argv[1])

connected_sockets = []
sockets_lock = threading.Lock()


# Loop reading messages from one peer until it closes the connection
def reader(remote_id, sock):
    while True:
        try:
            message_type, payload = read_message(sock, MAX_MESSAGE_LENGTH)
        except (ConnectionError, OSError):
            print(f"Peer {remote_id} closed", flush=True)
            return
        except ValueError as error:
            print(f"Bad message from peer {remote_id}: {error}", flush=True)
            sock.close()
            return

        print(
            f"Received {MESSAGE_NAMES[message_type]} {payload.hex()} from peer {remote_id}",
            flush=True,
        )


def on_connected(remote_id, sock, outgoing):
    with sockets_lock:
        connected_sockets.append(sock)

    direction = "Outgoing" if outgoing else "Incoming"
    print(
        f"{direction}: handshake successful with peer {remote_id}",
        flush=True,
    )

    send_lock = threading.Lock()

    reader_thread = threading.Thread(target=reader, args=(remote_id, sock), daemon=True)
    reader_thread.start()

    send_message(sock, send_lock, BITFIELD, b"\xff\x80")
    send_message(sock, send_lock, INTERESTED)


stop_event, listener_thread = start_connections(
    local_peer_id,
    peers,
    on_connected,
)

try:
    input("Peer running. Press Enter to stop.\n")
finally:
    stop_event.set()
    listener_thread.join()

    with sockets_lock:
        for sock in connected_sockets:
            sock.close()
