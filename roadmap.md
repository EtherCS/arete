## 1. Reconstruct the network connections (Zhongtang)
> Network TODO
> Nodes are replicas of the ordering shard; Executors are replicas of the execution shards.

Codes are in *./node/src*. From ***client->node*** to: 
- Client->executor:
  - 2f+1 to f+1: *mempool.rs(handle_clients_transactions)*->*quorum_waiter.rs(run)*, in *./mempool/src/config.rs(pub fn quorum_threshold)*
- Executor->node:
- Node->executor:


## 2. Implement blocks' structures
- Done
  - Execution blocks (EBlock): done in *./certify/messages.rs*
  - Certificate blocks (CBlock): done in *./certify/messages.rs*
  - Ordering blocks (OBlock): done in *./consensus/messages.rs*
- TODO:
  - extend fields of EBlock and CBlock
  - replace EBlock with CBlock in **analyze_block** function, and send CBlock to the ordering shard

## 3. Configs extension (Jianting)
- Done
  - create 1) execpool (rf. mempool); 2) certify (rf. consensus): done
  - Parse single shard result *./benchmark/benchmark/logs.py* with *ShardLogParser*: done
  - run executors: done
  - support multiple execution shards
  - Parse log *./benchmark/benchmark/logs.py*: after adding more attributes to executor parameters
- TODO
  - fix timeout problem when there are multiple shards: using multi-thread to start? or increase timeout_delay

## 4. Support transaction execution
> Execution TODO
> Current: rockDB key-value is used to store blocks rather than accounts, i.e., <key, value> = <block_digest, block>

Maybe
- Modify block structure: *./consensus/src/messages.rs*, replace **payload** with new type **transactions**