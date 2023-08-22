## 1. Reconstruct the network connections
> Nodes are replicas of the ordering shard; Executors are replicas of the execution shards.

Codes are in *./node/src*. From ***client->node*** to: 
- Client->executor:
  - 2f+1 to f+1: *mempool.rs(handle_clients_transactions)*->*quorum_waiter.rs(run)*, in *./mempool/src/config.rs(pub fn quorum_threshold)*
- Executor->node:
- Node->executor:


## 2. Implement blocks' structures
Create three blocks:
- Execution blocks:
- Certificate blocks:
- Ordering blocks:


## 3. Support transaction execution