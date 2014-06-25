#raft.js

WORK IN PROGRESS

Node.js implementation of the Raft consensus algorithm.

##What is Raft?

> Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems.

[Source](http://raftconsensus.github.io/)

##What is consensus?

> In a system composed of multiple machines (i.e. nodes) communicating over a network, it is a common requirement for the nodes to agree upon a value in a fault-tolerant manner, that is, reaching a 'consensus'. Since every machine and its connection to the network may fail at anytime, a special care is required in order to ensure correctness of the process.

## Why Raft.js?

> Raft.js is an experiment, setting out to find whether Node.js is suitable for developing distributed algorithms. We do not aim at production use, but rather at easy to follow implemetation for educational purposes.