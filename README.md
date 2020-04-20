## RAFT-Lite
My attempt at implementing RAFT using Python. There are several other implementations out there but for the most part I found other implementations difficult to understand or lacking networking components. The goal of this repo is an algorithm as simple (and as localized) as possible. All of the state transition code is defined in a single file and the network component is abstracted away so that it would be easy to convert it to use something like ROS or another python library. As I wanted to use the the servers (or nodes, or sentinels, as they're specified here) as a kind of failure detector for other distributed programs, they can be spawned within a single python program or can span multiple programs. Intercommunication parameters are saved using a single 'address book' json file.

### Installation and Testing
Clone the repo, create a new python environment and then run:
```bash
pip install -r requirements.txt
```

To test the system run: 
```python 
python Sentinel.py
```

To test the system on two different terminals run: 
```python 
python runmefirst.py
```
and on a second terminal run:
```python 
python runmesecond.py
```