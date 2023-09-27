# ARETE
Blockchain Sharding Made Practical

## Quick Start

ARETE is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:

```bash
git clone https://github.com/EtherCS/arete.git
cd arete/benchmark
pip install -r requirements.txt
```

You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:

```bash
fab localShard
```

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.

```text
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Ordering shard size: 4 nodes
 Ordering shard fault ratio: 0.3 
 Execution shard number: 2 shards
 Execution shard size: 5 nodes
 Execution shard fault ratio: 0.4 
 Input rate per shard: 1,002 tx/s
 Transaction size: 512 B
 Execution time: 19 s

 Consensus timeout delay: 1,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool GC depth: 50 rounds
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 15,000 B
 Mempool max batch delay: 10 ms

 + RESULTS:
 Comparison Sharding:
 Consensus TPS: 1,022 tx/s
 Consensus BPS: 523,406 B/s
 Consensus latency: 11 ms
 End-to-end TPS: 954 tx/s
 End-to-end BPS: 488,310 B/s
 End-to-end intra latency: 1,461 ms
 End-to-end cross latency: 9,287 ms

 ARETE (ours):
 Consensus TPS: 862 tx/s
 Consensus BPS: 441,159 B/s
 Consensus latency (Anchor block interval): 2,812 ms
 End-to-end TPS: 812 tx/s
 End-to-end BPS: 415,961 B/s
 End-to-end arete intra latency: 2,264 ms
 End-to-end arete cross latency: 5,291 ms
-----------------------------------------
```