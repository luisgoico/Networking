from pathlib import Path

# Parse the Common.cfg file and return a dictionary of key-value pairs for common properties
def read_common():
    common = {}

    script_dir = Path(__file__).resolve().parent
    file_path = script_dir / "Common.cfg"

    with open(file_path, "r") as file:
        for line in file:
            # Remove whitespace and skip empty lines
            line = line.strip()
            if not line:
                continue
            # Split the line into key and value at the first space
            property, value = line.split()
            property = property.strip()
            value = value.strip()

            if property != "FileName":
                try:
                    value = int(value)
                except ValueError:
                    raise ValueError(f"Invalid integer value for property '{property}': {value}")

            common[property] = value

        if common["PieceSize"] > common["FileSize"]:
            raise ValueError("PieceSize cannot be greater than FileSize")
        
    return common

# Parse the PeerInfo.cfg file and return a list of dictionaries for each peer's properties
def read_peers():
    peers = []

    script_dir = Path(__file__).resolve().parent
    file_path = script_dir / "PeerInfo.cfg"

    with open(file_path, "r") as file:
        for line in file:
            # Remove whitespace and skip empty lines
            line = line.strip()
            if not line:
                continue
            # Split the line into components
            parts = line.split()

            if len(parts) != 4:
                raise ValueError(f"Invalid peer entry: {line}")

            # Individual peers
            peer_id, hostname, port, has_file = parts
            
            try:
                peer_id = int(peer_id)
                port = int(port)
            except ValueError:
                raise ValueError(f"Invalid integer value in peer entry: {line}")

            if has_file not in ("0", "1"):
                raise ValueError(f"Invalid has_file value in peer entry: {line}")
            
            peer_info = {
                "peer_id": peer_id,
                "hostname": hostname,
                "port": port,
                "has_file": True if has_file == "1" else False
            }
            peers.append(peer_info)
    return peers