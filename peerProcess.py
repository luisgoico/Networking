import sys
import socket
import threading
from config import read_common, read_peers
from connections import start_connections, read_message, send_message, MAX_MESSAGE_LENGTH
from messages import MESSAGE_NAMES, INTERESTED, CHOKE, UNCHOKE, NOT_INTERESTED, HAVE
from logger import Logger
from file_manager import FileManager

class Peer:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.common = read_common()
        self.peers = read_peers()
        local_peer = next(peer for peer in self.peers if peer["peer_id"] == self.peer_id)
        self.file_manager = FileManager(self.peer_id, self.common, local_peer["has_file"])

        self.connections = {}
        self.send_locks = {}
        self.lock = threading.Lock()
        self.running = threading.Event()
        self.running.set()

        self.stop_event = None
        self.listener_thread = None

        self.logger = Logger(peer_id)

    def on_connected(self, remote_id, sock, outgoing):
        with self.lock:
            self.connections[remote_id] = sock
            self.send_locks[remote_id] = threading.Lock()

        #log the connection
        if outgoing:
            self.logger.connection_established(remote_id)
        else:
            self.logger.connection_from(remote_id)

        direction = "outgoing" if outgoing else "incoming"
        print(f"Connected to {remote_id} ({direction})")

        thread = threading.Thread(
            target=self.receive_messages,
            args=(remote_id, sock),
            daemon=True,
        )
        thread.start()

        self.send_to_peer(remote_id, INTERESTED)

    def receive_messages(self, remote_id, sock):
        try:
            while self.running.is_set():
                message_type, payload = read_message(
                    sock, MAX_MESSAGE_LENGTH
                )

                name = MESSAGE_NAMES[message_type]
                print(f"Received {name} from {remote_id}")

                if message_type == CHOKE:
                    self.logger.choked_by(remote_id)
                elif message_type == UNCHOKE:
                    self.logger.unchoked_by(remote_id)
                elif message_type == INTERESTED:
                    self.logger.received_interested(remote_id)
                elif message_type == NOT_INTERESTED:
                    self.logger.received_not_interested(remote_id)
                elif message_type == HAVE:
                    index = int.from_bytes(payload, byteorder="big")
                    self.logger.received_have(remote_id, index)
                #can't handle this yet because we don't have the pieces done but this is here as a frame; we'll need to import PIECE from messages too
                #elif message_type == PIECE:
                #    self.logger.download_piece(remote_id, index, num_pieces)

                # Message-specific processing will go here.

        except (ConnectionError, OSError, ValueError) as error:
            if self.running.is_set():
                print(f"Peer {remote_id} disconnected: {error}")

        finally:
            with self.lock:
                if self.connections.get(remote_id) is sock:
                    del self.connections[remote_id]
            sock.close()

    def send_to_peer(self, remote_id, message_type, payload=b""):
        with self.lock:
            sock = self.connections.get(remote_id)
            send_lock = self.send_locks.get(remote_id)

        if sock is None or send_lock is None:
            raise ConnectionError(f"Peer {remote_id} is not connected")

        send_message(sock, send_lock, message_type, payload)

    def start(self):
        print(f"Starting peer {self.peer_id}")

        self.stop_event, self.listener_thread = start_connections(
            self.peer_id,
            self.peers,
            self.on_connected,
        )

        print("Peer is running. Press Ctrl+C to stop.")

        try:
            while self.running.is_set():
                self.running.wait(1) if not self.running.is_set() else self.stop_event.wait(1)
        except KeyboardInterrupt:
            print("\nStopping peer...")
        finally:
            self.stop()

    def stop(self):
        self.running.clear()

        if self.stop_event is not None:
            self.stop_event.set()

        with self.lock:
            sockets = list(self.connections.values())
            self.connections.clear()

        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

        if self.listener_thread is not None:
            self.listener_thread.join(timeout=2)

        self.logger.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python peerProcess.py <peer_id>")
        sys.exit(1)

    try:
        peer_id = int(sys.argv[1])
        peer = Peer(peer_id)
        peer.start()
    except ValueError as error:
        print(f"Invalid peer: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
