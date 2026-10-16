# Tracks which pieces a peer has, one bit per piece
# Wire format: byte 0 = pieces 0-7, high bit = lowest index, spare trailing bits are 0
class Bitfield:

    # Start with every piece if has_file is True, otherwise with none
    def __init__(self, num_pieces:int, has_file:bool=False):
        self.num_pieces = num_pieces
        # TODO: pick the storage for owned pieces (set of indexes, list of bools, or bytearray)
        # TODO: if has_file, mark pieces 0 to num_pieces - 1 as owned
        pass

    # True if piece i is owned
    def has(self, i:int) -> bool:
        # TODO: check piece i in storage
        pass

    # Mark piece i as owned
    def set(self, i:int) -> None:
        # TODO: optionally raise ValueError if i is out of range
        # TODO: add piece i to storage
        pass

    # Number of pieces owned
    def count(self) -> int:
        # TODO: count owned pieces
        pass

    # True once every piece is owned
    def is_complete(self) -> bool:
        # TODO: compare count() to num_pieces
        pass

    # Pieces other has that this bitfield doesn't, empty means not interested
    def missing_from(self, other:"Bitfield") -> list:
        # TODO: collect every i where other.has(i) and not self.has(i)
        pass

    # Build the bitfield message payload
    def to_bytes(self) -> bytes:
        # TODO: make (num_pieces + 7) // 8 zero bytes
        # TODO: for each owned piece i, turn on bit (i % 8) from the left in byte i // 8
        # TODO: return as bytes, padding bits stay 0
        pass

    # Build a Bitfield from a received bitfield payload
    @classmethod
    def from_bytes(cls, data:bytes, num_pieces:int) -> "Bitfield":
        # TODO: optionally raise ValueError if len(data) != (num_pieces + 7) // 8
        # TODO: create an empty Bitfield(num_pieces)
        # TODO: for i in 0 to num_pieces - 1, if bit (i % 8) from the left in byte i // 8 is on, set(i)
        # TODO: optionally raise ValueError if any padding bit is on
        # TODO: return it
        pass
