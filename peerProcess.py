import sys
import socket
import random
import threading
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

        self.preferred = set()
        self.optimistic = None
        self.unchoked = set()
        self.bytes_downloaded = {}

        self.unchoked_by = set()
        self.pending_requests = {}
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
                    sock, self.common["PieceSize"]+5
                )

                name = MESSAGE_NAMES[message_type]
                print(f"Received {name} from {remote_id}")

                if message_type == CHOKE:
                    self.logger.choked_by(remote_id)

                    with self.lock:
                        self.unchoked_by.discard(remote_id)
                        self.pending_requests.pop(remote_id, None)

                elif message_type == UNCHOKE:
                    self.logger.unchoked_by(remote_id)

                    with self.lock:
                        self.unchoked_by.add(remote_id)

                    #Once it is unchoked, we can request a piece from this peer
                    self.request_piece(remote_id)

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
                    self.request_piece(remote_id)

                elif message_type == BITFIELD:
                    peer_bitfield = bitfield_from_bytes(payload, self.num_pieces)

                    with self.lock:
                        self.peer_bitfields[remote_id] = peer_bitfield

                    self.update_interest(remote_id)
                    self.request_piece(remote_id)

                elif message_type == REQUEST:
                    index = unpack_index(payload)

                    #Check if this peer is unchoked which allows it to download
                    with self.lock:
                        allowed = remote_id in self.unchoked

                    if not allowed:
                        print(f"Ignore request from choked peer {remote_id}")
                        continue

                    #Read requested piece from our file
                    if not self.file_manager.has_piece(index):
                        print(f"Peer does not own piece {index}")
                        continue

                    data = self.file_manager.read_piece(index)

                    #Send index and piece data
                    self.send_to_peer(remote_id, PIECE, pack_piece(index, data))



                elif message_type == PIECE:
                    index, data = unpack_piece(payload)
                    #Check that this piece was requested
                    with self.lock:
                        expected = self.pending_requests.get(remote_id)

                    if expected != index:
                        print(f"Unexpected piece {index} from {remote_id}")
                        continue

                    # save_piece returns the new piece count, or None if we already had this piece
                    count = self.file_manager.save_piece(index, data)

                    with self.lock:
                        self.pending_requests.pop(remote_id, None)

                    if count is not None:
                        self.logger.download_piece(remote_id, index, count)
                        # count bytes sent, used to rank neighbors by download rate
                        self.record_download_rate(remote_id, len(data))
                        #Announce that now this peer has the piece to all neighbors
                        with self.lock:
                            neighbors = list(self.connections)

                        for neighbor_id in neighbors:
                            try:
                                self.send_to_peer(neighbor_id, HAVE, pack_index(index))
                            except (ConnectionError, OSError):
                                pass

                        #Check if the entire file is complete now
                        if self.file_manager.is_complete():
                            with self.lock:
                                if not self.completion_logged:
                                    self.completion_logged = True
                                    self.logger.download_complete()
                                    print("File download is complete")

                    with self.lock:
                        neighbors = list(self.connections)

                    for neighbor_id in neighbors:
                        self.update_interest(neighbor_id)
                        self.request_piece(neighbor_id)


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
                    self.preferred.discard(remote_id)
                    self.unchoked.discard(remote_id)
                    self.unchoked_by.discard(remote_id)
                    self.pending_requests.pop(remote_id, None)
                    self.send_locks.pop(remote_id, None)
                    self.bytes_downloaded.pop(remote_id, None)
                    if self.optimistic == remote_id:
                        self.optimistic = None
            sock.close()

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


    def request_piece(self, remote_id):
        with self.lock:
            #Request only from connected and unchoked peers
            if remote_id not in self.connections:
                return

            if remote_id not in self.unchoked_by:
                return

            #Only one request per peer is allowed to be pending
            if remote_id in self.pending_requests:
                return

            remote_bitfield = self.peer_bitfields.get(remote_id)
            if remote_bitfield is None:
                return

            #Pieces the remote peer has that local peer does not
            available = self.file_manager.missing_from(remote_bitfield)

            #Take out any pieces that were already requested elsewhere
            requested = set(self.pending_requests.values())
            available = [idx for idx in available if idx not in requested]
            if not available:
                return

            #Random piece selection
            index = random.choice(available)

            #Reserve piece before sending the REQUEST message for it
            self.pending_requests[remote_id] = index

        try:
            self.send_to_peer(remote_id, REQUEST, pack_index(index))

        except (ConnectionError, OSError):
            with self.lock:
                if self.pending_requests.get(remote_id) == index:
                    self.pending_requests.pop(remote_id, None)
            raise




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
            candidates = [p for p in self.interested_peers if p in self.connections and p not in self.unchoked]
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
        threading.Thread(target=self.timer_loop, args=(self.unchoke_interval, self.select_preferred), daemon=True).start()
        threading.Thread(target=self.timer_loop, args=(self.opt_interval, self.select_optimistic), daemon=True).start()

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
