# Tracks which pieces a peer has, one bit per piece
# Wire format: byte 0 = pieces 0-7, high bit = lowest index, spare trailing bits are 0
class Bitfield:

    # Start with every piece if has_file is True, otherwise with none
    def __init__(self, num_pieces:int, has_file:bool=False):
        self.num_pieces = num_pieces
        self.owned = set()
        if has_file:
            for i in range(num_pieces):
                self.set(i)

    # Raise ValueError if i is not a valid piece index
    def _check_index(self, i:int) -> None:
        if not 0 <= i < self.num_pieces:
            raise ValueError(f"Piece index out of range: {i}")

    # True if piece i is owned
    def has(self, i:int) -> bool:
        self._check_index(i)
        return i in self.owned

    # Mark piece i as owned
    def set(self, i:int) -> None:
        self._check_index(i)
        self.owned.add(i)

    # Number of pieces owned
    def count(self) -> int:
        return len(self.owned)

    # True once every piece is owned
    def is_complete(self) -> bool:
        return len(self.owned) == self.num_pieces

    # Pieces other has that this bitfield doesn't, empty means not interested
    def missing_from(self, other:"Bitfield") -> list:
        return [i for i in range(self.num_pieces) if other.has(i) and not self.has(i)]

    # Build the bitfield message payload
    def to_bytes(self) -> bytes:
        data = bytearray()
        for start in range(0, self.num_pieces, 8):
            byte = 0
            for bit in range(8):
                i = start + bit
                if i < self.num_pieces and self.has(i):
                    byte |= 0x80 >> bit
            data.append(byte)
        return bytes(data)


# Build a Bitfield from a received bitfield payload
def bitfield_from_bytes(data:bytes, num_pieces:int) -> Bitfield:
    expected = (num_pieces + 7) // 8
    if len(data) != expected:
        raise ValueError(f"Bitfield must be {expected} bytes, got {len(data)}")
    bitfield = Bitfield(num_pieces)
    for byte_index, byte in enumerate(data):
        for bit in range(8):
            i = byte_index * 8 + bit
            if i < num_pieces and byte & (0x80 >> bit):
                bitfield.set(i)
    return bitfield
