from config import read_common, read_peers

print("Common.cfg")
common = read_common()

for key, value in common.items():
    print(f"{key}: {value}")

print("\nPeerInfo.cfg")
peers = read_peers()

for peer in peers:
    print(peer)