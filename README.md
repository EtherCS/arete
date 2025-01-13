# A General Sharding Protocol
This branch implements a general blockchain sharding protocol as the comparison of Arete.

## Features
The general sharding protocol supports flexibly setting different system parameters, including the shard size, the number of shards, the ratio of cross-shard transactions, the security threshold, etc. (see `localShard function` in `fabfile.py`). It also supports the performance evaluation of the sharding system under crashed attacks.

## Quick Start

Arete is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:

```bash
down the anonymous project
cd [this repo name]/benchmark
pip install -r requirements.txt
```

You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:

```bash
fab localShard
```

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below (under a local server with 48 CPU cores, 128 GB of RAM, and a 10 TB SSD).

```text
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Ordering shard size: 10 nodes
 Ordering shard fault ratio: 0.0 
 Execution shard number: 3 shards
 Execution shard size: 4 nodes
 Execution shard fault ratio: [0.0] 
 Liveness threshold: 0.3 
 Input rate per shard: 2,000 tx/s
 Transaction size: 512 B
 Cross-shard ratio: 0.2 
 Execution time: 112 s

 Consensus timeout delay: 4,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 500,000 B
 Mempool max batch delay: 1,000 ms

 + RESULTS:
 Benchmark Sharding:
 Consensus TPS: 5,405 tx/s
 Consensus BPS: 2,767,272 B/s
 End-to-end TPS: 5,377 tx/s
 End-to-end BPS: 2,752,874 B/s
 End-to-end intra latency: 491 ms
 End-to-end cross latency: 7,159 ms
-----------------------------------------
```

## AWS test
Please refer to the tutorial provided by [Hotstuff AWS Evaluation](https://github.com/asonnino/hotstuff/wiki/AWS-Benchmarks), which explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Amazon Web Services (AWS)](https://aws.amazon.com) accross multiple data centers (WAN).