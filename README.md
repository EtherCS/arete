# ARETE
Blockchain Sharding Made Practical. This implementation is based on [Alberto's Hotstuff codebase](https://github.com/asonnino/hotstuff).

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
 Ordering shard fault ratio: 0.0 
 Execution shard number: 2 shards
 Execution shard size: 5 nodes
 Execution shard fault ratio: 0.0 
 Input rate per shard: 1,000 tx/s
 Transaction size: 512 B
 Cross-shard ratio: 0.5 
 Execution time: 12 s

 Consensus timeout delay: 4,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 15,000 B
 Mempool max batch delay: 10 ms

 + RESULTS:
 SOTA Sharding:
 Consensus TPS: 824 tx/s
 Consensus BPS: 422,033 B/s
 End-to-end TPS: 752 tx/s
 End-to-end BPS: 384,787 B/s
 End-to-end intra latency: 1,441 ms
 End-to-end cross latency: 2,263 ms

 ARETE (ours):
 Consensus TPS: 822 tx/s
 Consensus BPS: 421,051 B/s
 End-to-end TPS: 750 tx/s
 End-to-end BPS: 383,970 B/s
 End-to-end arete intra latency: 1,619 ms
 End-to-end arete cross latency: 1,733 ms
-----------------------------------------
```

## AWS test
See [this wiki document](https://github.com/EtherCS/arete/wiki/AWS-Benchmark).
## More tests
Test larger shard: 1) increase `duration`; 2) increase `certify_timeout_delay`
