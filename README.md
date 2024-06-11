# ARETE
An implementation for the paper: *Sharding SMR with Optimal-size Shards for Highly Scalable Blockchains*. The consensus protocol is based on the [Hotstuff codebase](https://github.com/asonnino/hotstuff) implemented by Alberto Sonnino.

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

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py/localShard`. When the benchmark terminates, it displays a summary of the execution similarly to the one below (under a local server with 48 CPU cores, 128 GB of RAM, and a 10 TB SSD).

```text
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Ordering shard size: 10 nodes
 Ordering shard fault ratio: 0.0 
 Execution shard number: 3 shards
 Execution shard size: 4 nodes
 Execution shard fault ratio: 0.0 
 Liveness threshold: 0.41 
 Input rate per shard: 2,000 tx/s
 Transaction size: 512 B
 Cross-shard ratio: 0.2 
 Execution time: 54 s

 Consensus timeout delay: 3,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 500,000 B
 Mempool max batch delay: 1,000 ms

 + RESULTS:
 ARETE:
 Consensus TPS: 5,384 tx/s
 Consensus BPS: 2,756,707 B/s
 End-to-end TPS: 5,325 tx/s
 End-to-end BPS: 2,726,242 B/s
 End-to-end intra latency: 508 ms
 End-to-end cross latency: 740 ms
-----------------------------------------
```

## Comparison Sharding
See [branch](https://github.com/EtherCS/arete/tree/sota)

## AWS test
See [this wiki document](https://github.com/EtherCS/arete/wiki/AWS-Benchmark).