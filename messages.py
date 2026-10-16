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

# Allowed payload length per type as (min, max), None means no upper limit
PAYLOAD_RULES = {
    CHOKE: (0, 0),
    UNCHOKE: (0, 0),
    INTERESTED: (0, 0),
    NOT_INTERESTED: (0, 0),
    HAVE: (INDEX_SIZE, INDEX_SIZE),
    BITFIELD: (1, None),
    REQUEST: (INDEX_SIZE, INDEX_SIZE),
    PIECE: (INDEX_SIZE, None),
}


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

    if message_type not in PAYLOAD_RULES:
        raise ValueError(f"Unknown message type: {message_type}")

    min_len, max_len = PAYLOAD_RULES[message_type]
    if len(payload) < min_len or (max_len is not None and len(payload) > max_len):
        expected = f"{min_len}" if min_len == max_len else f"at least {min_len}"
        raise ValueError(f"{MESSAGE_NAMES[message_type]} payload must be {expected} bytes, got {len(payload)}")

    return message_type, payload


# 4 byte big-endian piece index, the payload for have and request
def pack_index(index:int) -> bytes:
    return index.to_bytes(INDEX_SIZE, byteorder="big", signed=False)


# Reverse of pack_index, payload size is already checked by decode_message
def unpack_index(payload:bytes) -> int:
    return int.from_bytes(payload, byteorder="big", signed=False)


# pack_index(index) followed by the piece data
def pack_piece(index:int, data:bytes) -> bytes:
    return pack_index(index) + data


# First 4 bytes are the index, the rest is the data, size already checked by decode_message
def unpack_piece(payload:bytes) -> tuple:
    return unpack_index(payload[:INDEX_SIZE]), payload[INDEX_SIZE:]