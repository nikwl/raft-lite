## RAFT-Lite
RAFT-Lite isn't really a good name for this repo, because it isn't exactly RAFT, though it uses much of the same terminology. Though I set out to re-implement RAFT, it kind of evolved as it came together, and as a result this system does not carry with it the same guarantees as RAFT. However, the implementation is relatively simple so upon inspection it should be easy to determine how it works. 

### Caveats
This program is designed to act as a 'Sentinel' for instances of other, larger programs. For the time being it could be thought of as a failure detector. Simply, it diverges from RAFT in the following ways:
* All messages are broadcast. Nodes may filter incoming messages but this is done on the receiving end.
* Nodes do not use a log. Instead they use terms to determine their relative progress. As a result the nodes can only be queried for their state, they cannot be used to store data. 
* The system allows dynamic adding and removing of nodes without a configuration change. The trade off is that this can result in multiple leaders emerging during an election, which will result in one or both of the leaders stepping down, similar to how a leader steps down when they observe an active election with a higher term. However a single leader will always emerge. 

This repo is under development, and will probably evolve into the standard RAFT implementation. I'm releasing this because... who knows, it might be helpful to someone, but only use it if you're okay with the above caveats. 

### Installation and Testing
Clone the repo, create a new python environment and then run:
```pip install -r requirements.txt```

To test the system run: 
```python Sentinel.py```
