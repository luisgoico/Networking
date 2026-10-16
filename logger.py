import os
import threading
from datetime import datetime

class Logger:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self._lock = threading.Lock()
        self.path = os.path.join(os.getcwd(), f"log_peer_{peer_id}.log")
        self._file = open(self.path, "w", encoding="utf-8")

  def _write(self, text):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {text}\n"
    with self._lock:
        if self._file.close:
            return
        self._file.write(line)
        self._file.flush()

  def close(self):
    with self._lock:
        if not self._file.closed:
            self._file.close()

# procedures for each instance of writing to log
  def connection_established(self, other_id):
    self._write(f"Peer {self.peer_id} makes a connection to Peer {other_id}.")

  def connection_from(self, other_id):
    self._write(f"Peer {self.peer_id} is connected from Peer {other_id}.")

  def preferred_neighbors(self, neighbor_ids):
    id_list = ",".join(str(n) for n in neighbor_ids)
    self._write(f"Peer {self.peer_id} has the preferred neighbors {id_list}.")

  def optimistic_unchoked(self, neighbor_id):
    self._write(f"Peer {self.peer_id} has the optimistically unchoked neighbor {neighbor_id}.")

  def unchoked_by(self, neighbor_id):
    self._write(f"Peer {self.peer_id} is unchoked by {neighbor_id}.")

  def choked_by(self, neighbor_id):
    self._write(f"Peer {self.peer_id} is choked by {neighbor_id}.")

  def received_have(self, neighbor_id, piece_index):
    self._write(f"Peer {self.peer_id} received the 'have' message from {neighbor_id} for the piece {piece_index}.")

  def received_interested(self, neighbor_id):
    self._write(f"Peer {self.peer_id} received the 'interested' message from {neighbor_id}.")

  def received_not_interested(self, neighbor_id):
    self._write(f"Peer {self.peer_id} received the 'not interested' message from {neighbor_id}.")

  def download_piece(self, neighbor_id, piece_index, num_pieces):
    self._write(f"Peer {self.peer_id} has downloaded the piece {piece_index} from {neighbor_id}. Now the number of pieces it has is {num_pieces}.")

  def download_complete(self):
    self._write(f"Peer {self.peer_id} has downloaded the complete file.")
