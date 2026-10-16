import os
import re
import tempfile
import threading
from logger import Logger

#regex to check timestamps
STAMP = r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]"

def read_lines(path):
  with open(path, encoding="utf-8") as f:
    return f.read().splitlines()

def test_messages():
  logger = Logger(1001)
  logger.connection_established(1002)
  logger.connection_from(1003)
  logger.preferred_neighbors([1002, 1003])
  logger.optimistic_unchoked(1002)
  logger.unchoked_by(1002)
  logger.choked_by(1003)
  logger.received_have(1002, 7)
  logger.received_interested(1002)
  logger.received_not_interested(1003)
  logger.download_piece(1002, 7, 12)
  logger.download_complete()
  logger.close()

  expected = [
  "Peer 1001 makes a connection to Peer 1002.",
  "Peer 1001 is connected from Peer 1003.",
  "Peer 1001 has the preferred neighbors 1002,1003.",
  "Peer 1001 has the optimistically unchoked neighbor 1002.",
  "Peer 1001 is unchoked by 1002.",
  "Peer 1001 is choked by 1003.",
  "Peer 1001 received the 'have' message from 1002 for the piece 7.",
  "Peer 1001 received the 'interested' message from 1002.",
  "Peer 1001 received the 'not interested' message from 1003.",
  "Peer 1001 has downloaded the piece 7 from 1002. Now the number of pieces it has is 12.",
  "Peer 1001 has downloaded the complete file.",
  ]

  lines = read_lines("log_peer_1001.log")
  assert len(lines) == len(expected), f"{len(lines)} lines, expected {len(expected)}"
  for line, want in zip(lines, expected):
    assert re.match(STAMP, line), f"bad timestamp: {line}"
    assert line.split("] ", 1)[1] == want, f"got: {line}\nwant: {want}"

def test_threads():
  logger = Logger(1002)
  def worker(n):
    for i in range(100):
      logger.received_have(n, i)
  threads = [threading.Thread(target=worker, args=(n,)) for n in range(10)]
  for t in threads:
    t.start()
  for t in threads:
    t.join()
  logger.close()

  lines = read_lines("log_peer_1002.log")
  assert len(lines) == 1000, f"{len(lines)} lines, expected 1000"
  assert all(re.match(STAMP, l) for l in lines)

def test_close():
  logger = Logger(1003)
  logger.close()
  logger.close()

if __name__ == "__main__":
  with tempfile.TemporaryDirectory() as tmp:
    os.chdir(tmp)
        test_messages()
        print("PASS: all message formats")
        test_threads()
        print("PASS: threads")
        test_close()
        print("PASS: close twice")
        print("All logger tests passed.")
