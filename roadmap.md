## 1. Reconstruct the network connections
> Network TODO
> Nodes are replicas of the ordering shard; Executors are replicas of the execution shards.

Codes are in *./node/src*. From ***client->node*** to: 
- Client->executor:
  - 2f+1 to f+1: *mempool.rs(handle_clients_transactions)*->*quorum_waiter.rs(run)*, in *./mempool/src/config.rs(pub fn quorum_threshold)*
- Executor->node:
- Node->executor:


## 2. Implement blocks' structures
> Block TODO
Create three blocks:
- Execution blocks:
- Certificate blocks:
- Ordering blocks:

## 3. Configs extension
- Modified fabric scripts: done
- create 1) execpool (rf. mempool); 2) certify (rf. consensus): done
- Parse single shard result *./benchmark/benchmark/logs.py* with *ShardLogParser*: done
- TODO: 
  - run executors
  - support multiple execution shards
  - Parse log *./benchmark/benchmark/logs.py*: after adding more attributes to executor parameters

## 4. Support transaction execution
> Execution TODO
> Current: rockDB key-value is used to store blocks rather than accounts, i.e., <key, value> = <block_digest, block>

Maybe
- Modify block structure: *./consensus/src/messages.rs*, replace **payload** with new type **transactions**