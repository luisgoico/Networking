import sys
import threading

from connections import start_connections


# Testing two peers on one computer, using different ports.
peers = [
    {"peer_id": 1001, "host": "127.0.0.1", "port": 6001},
    {"peer_id": 1002, "host": "127.0.0.1", "port": 6002},
]

local_peer_id = int(sys.argv[1])

connected_sockets = []
sockets_lock = threading.Lock()


def on_connected(remote_id, sock, outgoing):
    with sockets_lock:
        connected_sockets.append(sock)

    direction = "Outgoing" if outgoing else "Incoming"
    print(
        f"{direction}: handshake successful with peer {remote_id}",
        flush=True,
    )


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