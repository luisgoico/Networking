CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7

MESSAGE_NAMES = {
    CHOKE: "choke",
    UNCHOKE: "unchoke",
    INTERESTED: "interested",
    NOT_INTERESTED: "not interested",
    HAVE: "have",
    BITFIELD: "bitfield",
    REQUEST: "request",
    PIECE: "piece",
}

LENGTH_SIZE = 4
INDEX_SIZE = 4


# Build the wire bytes: 4 byte length, 1 byte type, then the payload
def encode_message(message_type:int, payload:bytes=b"") -> bytes:
    if message_type not in MESSAGE_NAMES:
        raise ValueError(f"Invalid message type: {message_type}")

    length = 1 + len(payload)
    return length.to_bytes(LENGTH_SIZE, byteorder="big", signed=False) + bytes([message_type]) + payload


# Split a message body (type byte + payload, length already removed) and validate the payload size
def decode_message(body:bytes) -> tuple:
    if not body:
        raise ValueError("Message body is empty")

    message_type = body[0]
    payload = body[1:]

    if message_type in (CHOKE, UNCHOKE, INTERESTED, NOT_INTERESTED):
        if len(payload) != 0:
            raise ValueError(f"{MESSAGE_NAMES[message_type]} message must have no payload")
    elif message_type in (HAVE, REQUEST):
        if len(payload) != INDEX_SIZE:
            raise ValueError(f"{MESSAGE_NAMES[message_type]} payload must be exactly 4 bytes")
    elif message_type == BITFIELD:
        if len(payload) < 1:
            raise ValueError("bitfield payload must be at least 1 byte")
    elif message_type == PIECE:
        if len(payload) < INDEX_SIZE:
            raise ValueError("piece payload must be at least 4 bytes")
    else:
        raise ValueError(f"Unknown message type: {message_type}")

    return message_type, payload


# 4 byte big-endian piece index, the payload for have and request
def pack_index(index:int) -> bytes:
    return index.to_bytes(INDEX_SIZE, byteorder="big", signed=False)


# Reverse of pack_index, check the payload is exactly 4 bytes
def unpack_index(payload:bytes) -> int:
    if len(payload) != INDEX_SIZE:
        raise ValueError(f"Index payload must be exactly {INDEX_SIZE} bytes")

    return int.from_bytes(payload, byteorder="big", signed=False)


# pack_index(index) followed by the piece data
def pack_piece(index:int, data:bytes) -> bytes:
    return pack_index(index) + data


# First 4 bytes are the index, the rest is the data
def unpack_piece(payload:bytes) -> tuple:
    if len(payload) < INDEX_SIZE:
        raise ValueError(f"Piece payload must be at least {INDEX_SIZE} bytes")

    return unpack_index(payload[:INDEX_SIZE]), payload[INDEX_SIZE:]