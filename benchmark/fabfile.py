from fabric import task

from benchmark.local import LocalBench
from benchmark.local_shard import LocalBenchShard
from benchmark.logs import ParseError, LogParser, ShardLogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError, PathMaker



@task
def local(ctx):
    """Run benchmarks on localhost"""
    bench_params = {
        "faults": 0,
        "nodes": 4,
        "rate": 1_000,
        "tx_size": 512,
        "duration": 20,
        "shard_num": 2,
        "shard_sizes": 4,  # could be different shard size [4, 8, ...]
    }
    node_params = {
        "consensus": {
            "timeout_delay": 1_000,
            "sync_retry_delay": 10_000,
        },
        "mempool": {
            "gc_depth": 50,
            "sync_retry_delay": 5_000,
            "sync_retry_nodes": 3,
            "batch_size": 15_000,
            "max_batch_delay": 10,
        },
    }
    executor_params = {
        "consensus": {
            "certify_timeout_delay": 1_000,
            "certify_sync_retry_delay": 10_000,
        },
        "mempool": {
            "certify_gc_depth": 50,
            "certify_sync_retry_delay": 5_000,
            "certify_sync_retry_nodes": 3,
            "certify_batch_size": 15_000,
            "certify_max_batch_delay": 10,
        },
    }
    try:
        ret = (
            LocalBench(bench_params, node_params, executor_params)
            .run(debug=True)
            .result()
        )
        print(ret)
    except BenchError as e:
        Print.error(e)


@task
def localShard(ctx):
    """Run benchmarks on localhost"""
    bench_params = {
        "faults": 0.0,
        "nodes": 45,
        "rate": 10_000,
        "tx_size": 512,
        "cross_shard_ratio": 0.2,
        "duration": 120,
        "liveness_threshold": 0.3,
        "shard_faults": 0.0,
        "shard_num": 2,
        "shard_sizes": 20,  # could be different shard size [4, 8, ...]
    }
    node_params = {
        "consensus": {
            "timeout_delay": 3_000,
            "sync_retry_delay": 10_000,
        },
        "mempool": {
            "gc_depth": 50,
            "sync_retry_delay": 5_000,
            "sync_retry_nodes": 3,
            "batch_size": 15_000,
            "max_batch_delay": 10,
        },
    }
    executor_params = {
        "consensus": {
            "certify_timeout_delay": 4_000,
            "certify_sync_retry_delay": 10_000,
        },
        "mempool": {
            "certify_gc_depth": 50,
            "certify_sync_retry_delay": 5_000,
            "certify_sync_retry_nodes": 3,
            "certify_batch_size": 500_000,
            "certify_max_batch_delay": 1000,
        },
    }
    try:
        ret = (
            LocalBenchShard(bench_params, node_params, executor_params)
            .run(debug=True)
            .result()
        )
        print(ret)
    except BenchError as e:
        Print.error(e)
        
@task
def parseLog(ctx, nodes = 4, orderFaults = 0.0, executionRatio=0.0, shardNum = 2, shardSizes=3, batchSize=50000, rate=10000, fl=0.3):
    ShardLogParser.process_shard(
        f"./logs", order_size=nodes, order_faults_ratio=orderFaults, execution_size=shardSizes, execution_faults_ratio=executionRatio, shardNum=shardNum
        ).print(
            PathMaker.result_file(
                batchSize, rate, executionRatio, shardNum, shardSizes, fl
            )
        )

@task
def create(ctx, instances=3):
    """Create a testbed"""
    try:
        InstanceManager.make().create_instances(instances)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    """Destroy the testbed"""
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    """Start at most `max` machines per data center"""
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    """Stop all machines"""
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    """Display connect information about all the available machines"""
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    """Install the codebase on all machines"""
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx):
    """Run benchmarks on AWS"""
    bench_params = {
        "faults": 0.0,
        "nodes": 90,
        "rate": 10_000,
        "tx_size": 512,
        "cross_shard_ratio": 0.5,
        "duration": 300,
        "liveness_threshold": 0.4,
        "shard_faults": 0.0,
        "shard_num": 2,
        "shard_sizes": 32, 
    }
    node_params = {
        "consensus": {
            "timeout_delay": 1_000,
            "sync_retry_delay": 10_000,
        },
        "mempool": {
            "gc_depth": 50,
            "sync_retry_delay": 5_000,
            "sync_retry_nodes": 3,
            "batch_size": 15_000,
            "max_batch_delay": 10,
        },
    }
    executor_params = {
        "consensus": {
            "certify_timeout_delay": 5_000,
            "certify_sync_retry_delay": 10_000,
        },
        "mempool": {
            "certify_gc_depth": 50,
            "certify_sync_retry_delay": 5_000,
            "certify_sync_retry_nodes": 3,
            "certify_batch_size": 15_000,
            "certify_max_batch_delay": 10,
        },
    }
    try:
        Bench(ctx).run(bench_params, node_params, executor_params, debug=False)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    """Plot performance using the logs generated by "fab remote" """
    plot_params = {
        "faults": [0],
        "nodes": [10, 20, 50],
        "tx_size": 512,
        "max_latency": [2_000, 5_000],
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError("Failed to plot performance", e))


@task
def kill(ctx):
    """Stop any ARETE execution on all machines"""
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    """Print a summary of the logs"""
    try:
        print(LogParser.process("./logs", faults="?").result())
    except ParseError as e:
        Print.error(BenchError("Failed to parse logs", e))
