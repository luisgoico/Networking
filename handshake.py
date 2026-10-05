import socket
from typing import Optional

HEADER = b"P2PFILESHARINGPROJ"
ZERO_BYTES = bytes(10)
HANDSHAKE_SIZE = 32

# Build the handshake bytes using a positive peer ID of 4 bytes
def build_handshake(peer_id:int)-> bytes:
    if type(peer_id) is not int:
        raise TypeError("peer_id must be an integer")
    if peer_id < 1 or peer_id > 0xFFFFFFFF:
        raise ValueError("peer_id must be between 1 and 4,294,967,295")

    id_bytes = peer_id.to_bytes(length=4, byteorder="big", signed=False)
    return HEADER + ZERO_BYTES + id_bytes

# Validation of the handshake received and returns the remote peer ID
def parse_handshake(data:bytes, expected_id:Optional[int]=None) -> int:
    if len(data) != HANDSHAKE_SIZE:
        raise ValueError("Handshake must be exactly 32 bytes")
    if data[:18] != HEADER:
        raise ValueError("Invalid handshake header")
    if data[18:28] != ZERO_BYTES:
        raise ValueError("The 10 bytes following header must be all zeros")

    peer_id = int.from_bytes(data[28:32], byteorder="big", signed=False)
    if peer_id < 1 or peer_id > 0xFFFFFFFF:
        raise ValueError("peer_id must be between 1 and 4,294,967,295")
    if expected_id is not None and peer_id != expected_id:
        raise ValueError(f"Expected peer id: {expected_id}, received id: {peer_id}")

    return peer_id

# Send the complete entire handshake or until a socket error occurs
def send_handshake(socket1:socket.socket, peer_id:int) -> None:
    handshake = build_handshake(peer_id)
    socket1.sendall(handshake)

# Receive and validate only 1 handshake and return the remote peer ID
def receive_handshake(socket1:socket.socket, expected_id:Optional[int]=None)->int:
    data = bytearray()
    # TCP could deliver only part of the handshake in 1 recv call so might need multiple calls
    while len(data) < HANDSHAKE_SIZE:
        partial = socket1.recv(HANDSHAKE_SIZE - len(data)) # Limit each call so other messages remain in the socket
        if not partial:
            raise ConnectionError("Peer was disconnected before full handshake")
        data.extend(partial)

    handshake = parse_handshake(bytes(data), expected_id)
    return handshake
