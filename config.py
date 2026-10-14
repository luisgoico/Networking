# Parse the Common.cfg file and return a dictionary of key-value pairs for common properties
def read_common():
    common = {}

    with open("Common.cfg", "r") as file:
        for line in file:
            # Remove whitespace and skip empty lines
            line = line.strip()
            if not line:
                continue
            # Split the line into key and value at the first space
            property, value = line.split()
            value = value.strip()

            if property != "FileName":
                value = int(value)

            common[property.strip()] = value
    return common

# Parse the PeerInfo.cfg file and return a list of dictionaries for each peer's properties
def read_peers():
    peers = []

    with open("PeerInfo.cfg", "r") as file:
        for line in file:
            # Remove whitespace and skip empty lines
            line = line.strip()
            if not line:
                continue
            # Split the line into components
            parts = line.split()

            # Individual peers
            peer_id, hostname, port, has_file = parts
            peer_info = {
                "peer_id": int(peer_id),
                "hostname": hostname,
                "port": int(port),
                "has_file": True if has_file == "1" else False
            }
            peers.append(peer_info)
    return peers

# print("Common.cfg")
# common = read_common()
# for key, value in common.items():
#     print(f"{key}: {value}")
# print("\nPeerInfo.cfg")
# peers = read_peers()
# for peer in peers:
#     print(peer)