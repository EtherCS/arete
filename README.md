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
 Faults: 0 nodes
 Committee size: 4 nodes
 Input rate: 1,000 tx/s
 Transaction size: 512 B
 Execution time: 20 s

 Consensus timeout delay: 1,000 ms
 Consensus sync retry delay: 10,000 ms
 Mempool GC depth: 50 rounds
 Mempool sync retry delay: 5,000 ms
 Mempool sync retry nodes: 3 nodes
 Mempool batch size: 15,000 B
 Mempool max batch delay: 10 ms

 + RESULTS:
 Consensus TPS: 967 tx/s
 Consensus BPS: 495,294 B/s
 Consensus latency: 2 ms

 End-to-end TPS: 960 tx/s
 End-to-end BPS: 491,519 B/s
 End-to-end latency: 9 ms
-----------------------------------------
```