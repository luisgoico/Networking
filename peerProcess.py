import sys
import socket
import random
import threading
from time import monotonic
from config import read_common, read_peers
from connections import start_connections, read_message, send_message
from messages import MESSAGE_NAMES, INTERESTED, CHOKE, UNCHOKE, NOT_INTERESTED, HAVE, BITFIELD, PIECE, pack_index, \
    unpack_index, pack_piece, unpack_piece, REQUEST
from logger import Logger
from bitfield import Bitfield, bitfield_from_bytes
from file_manager import FileManager


class Peer:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.common = read_common()
        self.peers = read_peers()
        local_peer = next(peer for peer in self.peers if peer["peer_id"] == self.peer_id)
        # FileManager owns this peer's bitfield and the file on disk
        self.file_manager = FileManager(self.peer_id, self.common, local_peer["has_file"])
        self.num_pieces = self.file_manager.num_pieces
        self.peer_bitfields = {}

        self.connections = {}
        self.send_locks = {}
        self.lock = threading.Lock()
        self.running = threading.Event()
        self.running.set()

        self.stop_event = None
        self.listener_thread = None

        self.logger = Logger(peer_id)

        self.interested_peers = set()
        self.interested_in_peers = set()

        #choke/unchoke
        # k = max # of preferred neighbors, p = unchoking interval, m = optimistic unchoke interval
        self.k = self.common["NumberOfPreferredNeighbors"]
        self.unchoke_interval = self.common["UnchokingInterval"]
        self.opt_interval = self.common["OptimisticUnchokingInterval"]
        self.request_timeout = 3 * self.opt_interval # timeout for requests to expire

        self.preferred = set()
        self.optimistic = None
        self.unchoked = set()
        self.unchoked_by_peers = set()
        self.bytes_downloaded = {}

        self.requested_pieces = {} # piece_index -> (remote_id, timestamp)

        self.completion_logged = False

    def on_connected(self, remote_id, sock, outgoing):
        send_lock = threading.Lock()
        payload = self.file_manager.bitfield_bytes() if self.file_manager.count() > 0 else None

        # Bitfield goes out before registering so no other thread can send to this peer first
        if payload is not None:
            send_message(sock, send_lock, BITFIELD, payload)

        with self.lock:
            self.connections[remote_id] = sock
            self.send_locks[remote_id] = send_lock
            self.peer_bitfields[remote_id] = Bitfield(self.num_pieces)

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

        # self.send_to_peer(remote_id, INTERESTED)

    def receive_messages(self, remote_id, sock):
        try:
            while self.running.is_set():
                message_type, payload = read_message(
                    sock, self.common["PieceSize"] + 5
                )

                name = MESSAGE_NAMES[message_type]
                print(f"Received {name} from {remote_id}")

                if message_type == CHOKE:
                    self.logger.choked_by(remote_id)
                    with self.lock:
                        self.unchoked_by_peers.discard(remote_id)
                        for index, (owner, _) in list(self.requested_pieces.items()):
                            if owner == remote_id:
                                del self.requested_pieces[index]
                    self.request_from_available_peers()

                elif message_type == UNCHOKE:
                    self.logger.unchoked_by(remote_id)
                    with self.lock:
                        self.unchoked_by_peers.add(remote_id)
                    self.send_request(remote_id)

                elif message_type == INTERESTED:
                    self.logger.received_interested(remote_id)
                    # add the remote_id to the interested_peers set to be considered for unchoking
                    with self.lock:
                        self.interested_peers.add(remote_id)

                elif message_type == NOT_INTERESTED:
                    self.logger.received_not_interested(remote_id)
                    # remove the remote_id from the interested_peers set to not be considered for unchoking
                    with self.lock:
                        self.interested_peers.discard(remote_id)

                elif message_type == HAVE:
                    index = unpack_index(payload)
                    with self.lock:
                        self.peer_bitfields[remote_id].set(index)
                    self.logger.received_have(remote_id, index)
                    self.update_interest(remote_id)
                    self.send_request(remote_id)

                elif message_type == BITFIELD:
                    peer_bitfield = bitfield_from_bytes(payload, self.num_pieces)
                    with self.lock:
                        self.peer_bitfields[remote_id] = peer_bitfield
                    self.update_interest(remote_id)
                    self.send_request(remote_id)

                elif message_type == REQUEST:
                    index = unpack_index(payload)
                    with self.lock:
                        allowed = remote_id in self.unchoked
                    if not allowed:
                        print(f"Ignoring request from choked peer {remote_id}")
                        continue
                    if not self.file_manager.has_piece(index):
                        print(f"Peer does not own piece {index}")
                        continue
                    data = self.file_manager.read_piece(index)
                    self.send_to_peer(remote_id, PIECE, pack_piece(index, data))

                elif message_type == PIECE:
                    index, data = unpack_piece(payload)
                    #Check that this piece was requested
                    with self.lock:
                        request = self.requested_pieces.get(index)
                    if request is None or request[0] != remote_id:
                        print(f"Unexpected piece {index} from {remote_id}")
                        continue

                    count = self.file_manager.save_piece(index, data)
                    with self.lock:
                        if self.requested_pieces.get(index) == request:
                            self.requested_pieces.pop(index, None)
                        neighbors = list(self.connections)

                    if count is not None:
                        self.record_download_rate(remote_id, len(data))
                        self.logger.download_piece(remote_id, index, count)
                        for neighbor_id in neighbors:
                            try:
                                self.send_to_peer(neighbor_id, HAVE, pack_index(index))
                            except (ConnectionError, OSError):
                                pass

                        if self.file_manager.is_complete():
                            with self.lock:
                                first_completion = not self.completion_logged
                                self.completion_logged = True
                            if first_completion:
                                self.logger.download_complete()
                                print("File download is complete")

                    for neighbor_id in neighbors:
                        try:
                            self.update_interest(neighbor_id)
                        except (ConnectionError, OSError):
                            pass
                    self.request_from_available_peers()

        except (ConnectionError, OSError, ValueError) as error:
            if self.running.is_set():
                print(f"Peer {remote_id} disconnected: {error}")

        finally:
            with self.lock:
                if self.connections.get(remote_id) is sock:
                    del self.connections[remote_id]
                    self.peer_bitfields.pop(remote_id, None)
                    self.interested_peers.discard(remote_id)
                    self.interested_in_peers.discard(remote_id)
                    self.unchoked_by_peers.discard(remote_id)
                    self.preferred.discard(remote_id)
                    self.unchoked.discard(remote_id)
                    self.send_locks.pop(remote_id, None)
                    self.bytes_downloaded.pop(remote_id, None)
                    self.requested_pieces = {
                        index: request
                        for index, request in self.requested_pieces.items()
                        if request[0] != remote_id
                    }
                    if self.optimistic == remote_id:
                        self.optimistic = None
            sock.close()
            if self.running.is_set():
                try:
                    self.request_from_available_peers()
                except (ConnectionError, OSError):
                    pass

    def update_interest(self, remote_id):
        with self.lock:
            bitfield = self.peer_bitfields.get(remote_id)
            if bitfield is None:
                return

            missing = self.file_manager.missing_from(bitfield)
            interested = remote_id in self.interested_in_peers

            if missing and not interested:
                message_type = INTERESTED
                self.interested_in_peers.add(remote_id)
            elif not missing and interested:
                message_type = NOT_INTERESTED
                self.interested_in_peers.discard(remote_id)
            else:
                return

        self.send_to_peer(remote_id, message_type)

    # adds nbytes to the peer's running total to determine downloading rate based on byte totals
    # called by PIECE handler
    def record_download_rate(self, remote_id, nbytes):
        with self.lock:
            self.bytes_downloaded[remote_id] = self.bytes_downloaded.get(remote_id, 0) + nbytes

    # Runs every p seconds and builds a candidate list of preferred interested neighbors
    def select_preferred(self):
        complete = self.file_manager.is_complete()
        with self.lock:
            candidates = [p for p in self.interested_peers if p in self.connections]
            random.shuffle(candidates) # Candidates are shuffled for randomization to break ties.
            if not complete:
                candidates.sort(key=lambda p: self.bytes_downloaded.get(p, 0), reverse=True) # prioritizes highest rate of previous interval if still downloading
            self.preferred = set(candidates[:self.k])
            self.bytes_downloaded.clear() # clears bytes to start each interval at zero
            chosen = candidates[:self.k]

        self.logger.preferred_neighbors(chosen)
        self.apply_choke_state()

    # Runs every m seconds to pick one random interested choked neighbor
    def select_optimistic(self):
        with self.lock:
            candidates = [p for p in self.interested_peers if p in self.connections and p not in self.preferred]
            self.optimistic = random.choice(candidates) if candidates else None
            chosen = self.optimistic

        if chosen is not None:
            self.logger.optimistic_unchoked(chosen)
        self.apply_choke_state()

    # unchoked set = preferred + optimistic
    # UNCHOKE to newly selected neighbors, CHOKE to neigbors that were unchoked but no longer selected
    # neighbors that remain unchoked do not receive a message
    def apply_choke_state(self):
        with self.lock:
            desired = set(self.preferred)
            if self.optimistic is not None:
                desired.add(self.optimistic)
            desired &= set(self.connections) # ignores disconnected neighbors
            to_unchoke = desired - self.unchoked
            to_choke = self.unchoked - desired
            self.unchoked = desired

        messages = [(pid, UNCHOKE) for pid in to_unchoke] + [(pid, CHOKE) for pid in to_choke]
        for pid, message_type in messages:
            try:
                self.send_to_peer(pid, message_type)
            except (ConnectionError, OSError):
                pass

    # calls fn every interval of seconds until stop_event
    def timer_loop(self, interval, fn):
        while not self.stop_event.wait(interval):
            fn()

    def request_from_available_peers(self):
        # The method manages one outstanding request per remote peer.
        with self.lock:
            remote_ids = list(self.unchoked_by_peers & self.connections.keys())
        for remote_id in remote_ids:
            try:
                self.send_request(remote_id)
            except (ConnectionError, OSError):
                pass

    def expire_requests(self):
        now = monotonic()
        expired = False
        with self.lock:
            for index, (remote_id, time_sent) in list(self.requested_pieces.items()):
                if now - time_sent >= self.request_timeout:
                    del self.requested_pieces[index]
                    expired = True
        if expired:
            self.request_from_available_peers()

    def send_request(self, remote_id):
        with self.lock:
            if remote_id not in self.connections or remote_id not in self.unchoked_by_peers:
                return
            # Prevent sending multiple outstanding requests to one peer.
            if any(owner == remote_id for owner, _ in self.requested_pieces.values()):
                return
            neighbor_bitfield = self.peer_bitfields.get(remote_id)
            if neighbor_bitfield is None:
                return
            missing_pieces = self.file_manager.missing_from(neighbor_bitfield)
            missing_pieces = [index for index in missing_pieces
                              if index not in self.requested_pieces]
            if not missing_pieces:
                return
            index = random.choice(missing_pieces)
            request = (remote_id, monotonic())
            self.requested_pieces[index] = request

        try:
            self.send_to_peer(remote_id, REQUEST, pack_index(index))
        except (ConnectionError, OSError):
            with self.lock:
                if self.requested_pieces.get(index) == request:
                    del self.requested_pieces[index]
            raise

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

        # threads for the timers
        threading.Thread(target=self.timer_loop, args=(self.unchoke_interval, self.select_preferred),
                         daemon=True).start()
        threading.Thread(target=self.timer_loop, args=(self.opt_interval, self.select_optimistic), daemon=True).start()
        threading.Thread(target=self.timer_loop, args=(1.0, self.expire_requests), daemon=True).start()

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

