from pathlib import Path
from bitfield import Bitfield
import threading

class FileManager:
    def __init__(self, peer_id, common, has_file, base_directory=None):
        self.file_size = common["FileSize"]
        self.piece_size = common["PieceSize"]

        if self.file_size <= 0 or self.piece_size <= 0:
            raise ValueError("FileSize and PieceSize must be positive numbers")

        # Rounding up in case there is the final file piece size is smaller than
        # the others
        self.num_pieces = (self.file_size + self.piece_size - 1) // self.piece_size

        if base_directory is None:
            base_directory = Path(__file__).resolve().parent

        self.directory = Path(base_directory) / f"peer_{peer_id}"
        self.directory.mkdir(parents=True, exist_ok=True)

        self.path = self.directory / common["FileName"]
        self.lock = threading.Lock()

        if has_file:
            # Must have the complete file to be a seeder
            if not self.path.is_file():
                raise FileNotFoundError(f"File not found: {self.path}")

        elif not self.path.exists():
            with self.path.open("xb") as file:
                file.truncate(self.file_size)

        if self.path.stat().st_size != self.file_size:
            raise ValueError(f"File {self.path} must have {self.file_size} bytes")

        self.bitfield = Bitfield(self.num_pieces, has_file)

    def expected_size(self, index):
        # Check the index is valid and calculate piece length
        if index < 0 or index >= self.num_pieces:
            raise ValueError(f"Invalid piece index: {index}")

        offset = index * self.piece_size
        expected_size = min(self.piece_size, self.file_size - offset)
        return expected_size

    def has_piece(self, index):
        self.expected_size(index)

        with self.lock:
            return self.bitfield.has(index)

    def read_piece(self, index):
        # Read a piece owned by this peer to send to another peer
        expected_size = self.expected_size(index)

        with self.lock:
            if not self.bitfield.has(index):
                raise ValueError(f"Piece {index} is not owned")

            with self.path.open("rb") as file:
                file.seek(index * self.piece_size)
                data = file.read(expected_size)

            if len(data) != expected_size:
                raise OSError(f"Could not read all of piece {index}")

            return data

    def save_piece(self, index, data):
        # Save a piece by a different peer.
        # Returns False if piece is already owned
        expected_size = self.expected_size(index)

        if len(data) != expected_size:
            raise ValueError(f"Piece {index} must be {expected_size} bytes. Received {len(data)} bytes")

        with self.lock:
            if self.bitfield.has(index):
                return False

            with self.path.open("rb+") as file:
                file.seek(index * self.piece_size)
                written = file.write(data)

                if written != expected_size:
                    raise OSError(f"Could not write all of piece {index}")

            # Send ownership of piece after the piece was completely written
            self.bitfield.set(index)
            return True

    def count(self):
        # Return number of pieces owned by this peer
        with self.lock:
            return self.bitfield.count()

    def is_complete(self):
        with self.lock:
            return self.bitfield.is_complete()

    def bitfield_bytes(self):
        # Build a BITFIELD message
        with self.lock:
            return self.bitfield.to_bytes()
