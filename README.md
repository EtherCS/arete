# Arete
This is the implementation for the paper *Optimal Sharding for Scalable Blockchains with Deconstructed SMR* ([pdf](https://arxiv.org/pdf/2406.08252)). Arete is a highly scalable blockchain sharding protocol that decouples data dissemination, ordering, and execution of state machine replication (SMR). Currently, we adopt [Hotstuff](https://dl.acm.org/doi/abs/10.1145/3293611.3331591) as the consensus protocol in the ordering shard.

## Quick Start

Arete is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/). To deploy and benchmark a testbed on your local machine, clone the repo and install the python dependencies:

```bash
git clone https://github.com/EtherCS/arete.git
cd benchmark
pip install -r requirements.txt
```

You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric under `./benchmark`:

```bash
fab localShard
```

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in [fabfile.py/localShard](https://github.com/EtherCS/arete/blob/main/benchmark/fabfile.py#L63). Below are several key parameters:
```text
bench_params = {
    "faults": 0.0,  # liveness attacker ratio in the ordering shard
    "nodes": 10, # the ordering shard size
    "rate": 2_000,  # tx input rate to each processing shard
    "cross_shard_ratio": 0.2, # cross-shard tx ratio
    "duration": 60, # test running time
    "liveness_threshold": 0.41, # liveness threshold f_L of processing shards
    "shard_faults": 0.0,    # liveness attacker ratio in processing shards
    "shard_num": 3, # the number of processing shards
    "shard_sizes": 4,  # the processing shard size, could be different shard size [4, 8, ...]
}
```


When the benchmark terminates, it displays a summary of the execution similarly to the one below (under a local server with 48 CPU cores, 128 GB of RAM, and a 10 TB SSD).

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
> *Note that a single machine can only support a limited number of nodes because we use [pre-defined ports](https://github.com/EtherCS/arete/blob/main/benchmark/benchmark/config.py#L117) in the network layer. If you want to run a large-scale network for evaluation, consider deploy it on the [AWS environment](https://github.com/EtherCS/arete/tree/main?tab=readme-ov-file#aws-test) (pay attention that AWS is **super expensive**).*

## Sharding benchmark
We also implemented a [general sharding protocol](https://github.com/EtherCS/arete/tree/sota) based on our codebase. We designed many interfaces for you to easily develop your own sharding protocol upon it. You can also use it as a sharding benchmark in your own experiments, where you can customize the testing parameters in [*localshard*](https://github.com/EtherCS/arete/blob/sota/benchmark/fabfile.py#L63) function (parameters are similar to Arete but here every shard runs the consensus protocol).

## AWS test
AWS tests can be easily conducted with our scripts. Please refer to the tutorial [Arete AWS Evaluation](https://github.com/EtherCS/arete/wiki/AWS-Benchmark), which explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Amazon Web Services (AWS)](https://aws.amazon.com) accross multiple data centers (WAN).

## Acknowledgment
Our prototype is built on [Alberto's Hotstuff Codebase](https://github.com/asonnino/hotstuff). We thank Alberto Sonnino for his invaluable codes. If you have any questions, please feel free to contact antfinancialyujian@gmail.com. If you think this paper and repository contributes to your work, feel free to cite our paper:
```text
@article{zhang2024sharding,
  title={Optimal Sharding for Scalable Blockchains with Deconstructed SMR},
  author={Zhang, Jianting and Luo, Zhongtang and Ramesh, Raghavendra and Kate, Aniket},
  journal={arXiv preprint arXiv:2406.08252},
  year={2024}
}
```
