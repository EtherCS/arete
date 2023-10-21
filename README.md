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
 Ordering shard size: 90 nodes
 Ordering shard fault ratio: 0.0 
 Execution shard number: 4 shards
 Execution shard size: 20 nodes
 Execution shard fault ratio: 0.0 
 Liveness threshold: 0.4 
 Input rate per shard: 2,000 tx/s
 Transaction size: 512 B
 Cross-shard ratio: 0.2 
 Execution time: 103 s

 Consensus timeout delay: 4,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 40,000 B
 Mempool max batch delay: 500 ms

 + RESULTS:
 ARETE:
 Consensus TPS: 6,380 tx/s
 Consensus BPS: 3,266,363 B/s
 End-to-end TPS: 6,358 tx/s
 End-to-end BPS: 3,255,209 B/s
 End-to-end intra latency: 4,490 ms
 End-to-end cross latency: 6,812 ms
-----------------------------------------
```

## AWS test
See [this wiki document](https://github.com/EtherCS/arete/wiki/AWS-Benchmark).
## More tests
Test larger shard: 1) increase `duration`; 2) increase `certify_timeout_delay`
