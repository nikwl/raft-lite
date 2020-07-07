## RAFT-Lite
A simple and lightweight implementation of RAFT Consensus in Python. There are several other implementations out there but for the most part I found other implementations difficult to understand or lacking networking components. The goal of this repo is the RAFT algorithm as simple (and as localized) as possible. All of the state transition code is defined in a single file and the networking components are abstracted away such that it would be easy to adapt the system to use something like ROS, or another python library, to handle networking. Originially I wanted to use the the servers (or nodes as they're called here) as a kind of failure detector for a distrubuted system. As a result servers can be spawned within a single python program or can span multiple programs. Intercommunication parameters are loaded using a single json file, or can be passed in a dictionary.

### Installation and Testing
Clone the repo, create a new python 2 environment and then run:
```bash
pip install -r requirements.txt
```

To test the system, edit the test script with your ip and then run:
```bash 
python test.py
```

### Usage
```python 
from raft import RaftNode
import time

comm_dict = {"node0": {"ip": "127.0.0.1", "port": "5567"}, 
  "node1": {"ip": "127.0.0.1", "port": "5566"}, 
  "node2": {"ip": "127.0.0.1", "port": "5565"}}

# Start a few nodes
nodes = [RaftNode(comm_dict, 'node0'),
         RaftNode(comm_dict, 'node1'), 
         RaftNode(comm_dict, 'node2')]
for n in nodes:
  n.start()

# Let a leader emerge
time.sleep(2)

# Make some requests
for val in range(5):
  nodes[0].client_request({'val': val})
time.sleep(5)

# Check and see what the most recent entry is
for n in nodes:
  print(n.check_committed_entry())

# Stop all the nodes
for n in nodes:
  n.stop()
```

### TODO
List of things that need to be changed/updated...
* Interface uses broadcast for all messages. Targeted messages are filtered on the receiving side. This causes network congestion and probably impacts performance.
* Add support for configuration change.
* Add support for log compaction.
