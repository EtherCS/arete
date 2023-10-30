from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil, floor
from os.path import join
import subprocess

from benchmark.config import (
    Committee,
    ExecutionCommittee,
    Key,
    NodeParameters,
    BenchParameters,
    ExecutorParameters,
    ConfigError,
)
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import ShardLogParser, ParseError
from benchmark.instance import InstanceManager


class FabricError(Exception):
    """Wrapper for Fabric exception with a meaningfull error message."""

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError("Failed to load SSH key", e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info("Installing rust and cloning the repo...")
        cmd = [
            "sudo apt-get update",
            "sudo apt-get -y upgrade",
            "sudo apt-get -y autoremove",
            # The following dependencies prevent the error: [error: linker `cc` not found].
            "sudo apt-get -y install build-essential",
            "sudo apt-get -y install cmake",
            # Install rust (non-interactive).
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            "source $HOME/.cargo/env",
            "rustup default stable",
            # This is missing from the Rocksdb installer (needed for Rocksdb).
            "sudo apt-get install -y clang",
            # Clone the repo.
            f"(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))",
        ]
        hosts = self.manager.hosts(flat=True)
        try:
            g = Group(*hosts, user="ubuntu", connect_kwargs=self.connect)
            g.run(" && ".join(cmd), hide=True)
            Print.heading(f"Initialized testbed of {len(hosts)} instances")
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError("Failed to install repo on testbed", e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else "true"
        cmd = [delete_logs, f"({CommandMaker.kill()} || true)"]
        try:
            g = Group(*hosts, user="ubuntu", connect_kwargs=self.connect)
            g.run(" && ".join(cmd), hide=True)
        except GroupException as e:
            raise BenchError("Failed to kill nodes", FabricError(e))

    def _select_hosts(self, bench_parameters: BenchParameters):
        nodes = max(
            x + y
            for x, y in zip(
                bench_parameters.node_instances, bench_parameters.executor_instances
            )
        )

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _split_hosts(self, hosts: list[str], nodes: int):
        nodes_per_host = (nodes + len(hosts) - 1) // len(hosts)
        return [(host, i) for host in hosts for i in range(nodes_per_host)][:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'RUST_BACKTRACE=1 tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user="ubuntu", connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(f'Updating {len(hosts)} instances (branch "{self.settings.branch}")...')
        cmd = [
            f"(cd {self.settings.repo_name} && git fetch -f)",
            f"(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})",
            f"(cd {self.settings.repo_name} && git pull -f)",
            "source $HOME/.cargo/env",
            f"(cd {self.settings.repo_name}/node && {CommandMaker.compile()})",
            f"(cd {self.settings.repo_name}/executor && {CommandMaker.compile()})",
            CommandMaker.alias_shard_binaries(
                f"./{self.settings.repo_name}/target/release/"
            ),
        ]
        g = Group(*hosts, user="ubuntu", connect_kwargs=self.connect)
        g.run(" && ".join(cmd), hide=True)

    def _config(
        self,
        hosts: list[str],
        executor_hosts: list[str],
        nodes: int,
        shard_num: int,
        shard_sizes: int,
        liveness_threshold: float,
        node_parameters: NodeParameters,
        executor_parameters: ExecutorParameters,
    ):
        Print.info("Generating configuration files...")

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_shard_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files for nodes.
        node_keys = []
        node_key_files = [PathMaker.key_file(i) for i in range(nodes)]
        for node_filename in node_key_files:
            cmd = CommandMaker.generate_key(node_filename).split()
            subprocess.run(cmd, check=True)
            node_keys += [Key.from_file(node_filename)]
        node_names = [x.name for x in node_keys]
        host_nodes = self._split_hosts(hosts, nodes)
        consensus_addr = [
            f"{x}:{self.settings.consensus_port + i}" for x, i in host_nodes
        ]
        front_addr = [f"{x}:{self.settings.front_port + i}" for x, i in host_nodes]
        mempool_addr = [f"{x}:{self.settings.mempool_port + i}" for x, i in host_nodes]
        committee = Committee(
            node_names,
            consensus_addr,
            front_addr,
            mempool_addr,
            shard_num,
            2**32 - 1,
            # Confirmation address will be built later
            {},
        )
        transaction_addrs_dict = committee.get_order_transaction_addresses()

        node_parameters.print(PathMaker.parameters_file())

        # Generate configuration files for executors.
        executor_confirmation_addrs = {}  # record all executors' confirmation addresses
        executor_nodes = self._split_hosts(executor_hosts, shard_num * shard_sizes)
        for shard_id in range(shard_num):
            keys = []
            key_files = [
                PathMaker.shard_executor_key_file(shard_id, i)
                for i in range(shard_sizes)
            ]
            for filename in key_files:
                cmd = CommandMaker.generate_executor_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]
            names = [x.name for x in keys]
            shard_nodes = executor_nodes[
                shard_id * shard_sizes : (shard_id + 1) * shard_sizes
            ]
            # TODO: Maybe make ports different from ordering nodes, if we ever run executor and node on the same machine.
            consensus_addr = [
                f"{x}:{self.settings.consensus_port + i}" for x, i in shard_nodes
            ]
            front_addr = [f"{x}:{self.settings.front_port + i}" for x, i in shard_nodes]
            mempool_addr = [
                f"{x}:{self.settings.mempool_port + i}" for x, i in shard_nodes
            ]
            confirmation_addr = [
                f"{x}:{self.settings.confirmation_port + i}" for x, i in shard_nodes
            ]
            execution_committee = ExecutionCommittee(
                names,
                consensus_addr,
                front_addr,
                mempool_addr,
                confirmation_addr,
                shard_num,
                shard_id,
                transaction_addrs_dict,
                liveness_threshold,
            )
            execution_committee.print(PathMaker.shard_committee_file(shard_id))
            executor_confirmation_addrs[
                shard_id
            ] = execution_committee.get_confirm_addresses()

            executor_parameters.print(PathMaker.shard_parameters_file(shard_id))

        committee = Committee(
            committee.names,
            committee.consensus,
            committee.front,
            committee.mempool,
            shard_num,
            2**32 - 1,
            executor_confirmation_addrs,
        )
        committee.print(PathMaker.committee_file())

        # Cleanup all nodes.
        cmd = f"{CommandMaker.cleanup()} || true"
        g = Group(*hosts, user="ubuntu", connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        progress = progress_bar(hosts, prefix="Uploading node configuration files:")
        for i, host in enumerate(progress):
            c = Connection(host, user="ubuntu", connect_kwargs=self.connect)
            c.put(PathMaker.committee_file(), ".")
            c.put(PathMaker.parameters_file(), ".")
        progress = progress_bar(host_nodes, prefix="Uploading node key files:")
        for i, (host, id) in enumerate(progress):
            c = Connection(host, user="ubuntu", connect_kwargs=self.connect)
            c.put(PathMaker.key_file(i), ".")
        # TODO: Maybe only upload necessary ones
        progress = progress_bar(
            [(id, host) for id in range(shard_num) for host in executor_hosts],
            prefix="Uploading executor configuration files:",
        )
        for id, host in progress:
            c = Connection(host, user="ubuntu", connect_kwargs=self.connect)
            c.put(PathMaker.shard_committee_file(id), ".")
            c.put(PathMaker.shard_parameters_file(id), ".")
        progress = progress_bar(
            executor_nodes, prefix="Uploading executor node key files:"
        )
        for i, (host, id) in enumerate(progress):
            c = Connection(host, user="ubuntu", connect_kwargs=self.connect)
            c.put(
                PathMaker.shard_executor_key_file(i // shard_sizes, i % shard_sizes),
                ".",
            )
        return committee

    def _run_single(
        self,
        hosts: list[str],
        executor_hosts: list[str],
        nodes: int,
        faults: float,  # byzantine ratio of the ordering shard
        rate: float,
        shard_num: int,
        shard_sizes: int,
        shard_faults: float,    # byzantine ratio of execution shards
        bench_parameters: BenchParameters,
        node_parameters: NodeParameters,
        executor_parameters: ExecutorParameters,
        cross_shard_ratio: float,
        debug=False,
    ):
        Print.info("Booting testbed...")

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)
        self.kill(hosts=executor_hosts, delete_logs=True)

        order_faults = floor(nodes * faults)
        execution_faults = floor(shard_sizes * shard_faults)
        # Run the executors.
        executor_nodes = self._split_hosts(executor_hosts, shard_num * shard_sizes)
        for shard_id in range(shard_num):
            shard_nodes = executor_nodes[
                shard_id * shard_sizes : (shard_id + 1) * shard_sizes - execution_faults
            ]
            key_files = [
                PathMaker.shard_executor_key_file(shard_id, i)
                for i in range(len(shard_nodes))
            ]
            dbs = [
                PathMaker.shard_executor_db_path(shard_id, i)
                for i in range(len(shard_nodes))
            ]
            executor_logs = [
                PathMaker.shard_executor_log_file(shard_id, i)
                for i in range(len(shard_nodes))
            ]
            for host, key_file, db, log_file in zip(
                shard_nodes, key_files, dbs, executor_logs
            ):
                cmd = CommandMaker.run_executor(
                    key_file,
                    PathMaker.shard_committee_file(shard_id),
                    db,
                    PathMaker.shard_parameters_file(shard_id),
                    debug=debug,
                )
                self._background_run(host[0], cmd, log_file)

        # Wait for the nodes to synchronize
        sleep(2 * executor_parameters.certify_timeout_delay / 1000)

        # Run the nodes.
        host_nodes = self._split_hosts(hosts, nodes - order_faults)
        key_files = [PathMaker.key_file(i) for i in range(len(host_nodes))]
        dbs = [PathMaker.db_path(i) for i in range(len(host_nodes))]
        node_logs = [PathMaker.node_log_file(i) for i in range(len(host_nodes))]
        for host, key_file, db, log_file in zip(host_nodes, key_files, dbs, node_logs):
            cmd = CommandMaker.run_node(
                key_file,
                PathMaker.committee_file(),
                db,
                PathMaker.parameters_file(),
                debug=debug,
            )
            self._background_run(host[0], cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info("Waiting for the nodes to synchronize...")
        sleep(2 * node_parameters.timeout_delay / 1000)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        for shard_id in range(shard_num):
            shard_nodes = executor_nodes[
                shard_id * shard_sizes : (shard_id + 1) * shard_sizes - execution_faults
            ]

            committee = ExecutionCommittee.load(
                PathMaker.shard_committee_file(shard_id)
            )
            # Run the clients (they will wait for the executors to be ready).
            # addresses = committee.front
            front_addr = [f"{x}:{self.settings.front_port + i}" for x, i in shard_nodes]
            rate_share = ceil(rate / committee.size())
            timeout = executor_parameters.certify_timeout_delay
            client_logs = [
                PathMaker.shard_client_log_file(shard_id, i)
                for i in range(len(shard_nodes))
            ]
            for host, addr, log_file in zip(shard_nodes, front_addr, client_logs):
                cmd = CommandMaker.run_client(
                    addr,
                    bench_parameters.tx_size,
                    rate_share,
                    shard_id,
                    timeout,
                    cross_shard_ratio,
                )
                self._background_run(host[0], cmd, log_file)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f"Running benchmark ({duration} sec):"):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)
        self.kill(hosts=executor_hosts, delete_logs=False)

    def _logs(
        self,
        hosts: list[str],
        executor_hosts: list[str],
        nodes: int,
        faults: float,
        execution_ratio: float,
        shard_num: int,
        shard_sizes: int,
    ):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        # Fault number
        order_faults = floor(nodes * faults)
        execution_faults = floor(shard_sizes * execution_ratio)
        
        # Download log files.
        # host_nodes = self._split_hosts(hosts, nodes)
        # progress = progress_bar(host_nodes, prefix="Downloading node logs:")
        # for i, host in enumerate(progress):
        #     # There is no logs for fault nodes
        #     if i >= nodes-order_faults:
        #         break
        #     c = Connection(host[0], user="ubuntu", connect_kwargs=self.connect)
        #     c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))

        executor_nodes = self._split_hosts(executor_hosts, shard_num * shard_sizes)
        progress = progress_bar(
            executor_nodes, prefix="Downloading executor node logs:"
        )
        for i, host in enumerate(progress):
            # There is no logs for fault nodes
            if (i % shard_sizes) >= shard_sizes-execution_faults:
                break
            c = Connection(host[0], user="ubuntu", connect_kwargs=self.connect)
            c.get(
                PathMaker.shard_executor_log_file(i // shard_sizes, i % shard_sizes),
                local=PathMaker.shard_executor_log_file(
                    i // shard_sizes, i % shard_sizes
                ),
            )
            c.get(
                PathMaker.shard_client_log_file(i // shard_sizes, i % shard_sizes),
                local=PathMaker.shard_client_log_file(
                    i // shard_sizes, i % shard_sizes
                ),
            )

        # Parse logs and return the parser.
        Print.info("Parsing logs and computing performance...")
        return ShardLogParser.process_shard(
            f"./logs", order_size=nodes, order_faults_ratio=faults, execution_size=shard_sizes, execution_faults_ratio=execution_ratio, shardNum=shard_num
        )

    def run(
        self,
        bench_parameters_dict,
        node_parameters_dict,
        executor_parameters_dict,
        debug=False,
    ):
        assert isinstance(debug, bool)
        Print.heading("Starting remote benchmark")
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
            executor_parameters = ExecutorParameters(executor_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn("There are not enough instances available")
            return

        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError("Failed to update nodes", e)

        # Run benchmarks.
        for n, shard_num, shard_sizes, node_instances, executor_instances, cross_shard_ratio, liveness_threshold in zip(
            bench_parameters.nodes,
            bench_parameters.shard_num,
            bench_parameters.shard_sizes,
            bench_parameters.node_instances,
            bench_parameters.executor_instances,
            bench_parameters.cross_shard_ratio,
            bench_parameters.liveness_threshold,
        ):
            for r in bench_parameters.rate:
                for shard_fault in bench_parameters.shard_faults:
                    Print.heading(
                        f"\nRunning {n} nodes with {shard_num}, shards * {shard_sizes}, nodes (input rate: {r:,} tx/s), hard fault: {shard_fault}"
                    )
                    hosts = selected_hosts[:node_instances]
                    executor_hosts = selected_hosts[
                        node_instances : node_instances + executor_instances
                    ]

                    # Upload all configuration files.
                    try:
                        self._config(
                            hosts=hosts,
                            executor_hosts=executor_hosts,
                            nodes=n,
                            shard_num=shard_num,
                            shard_sizes=shard_sizes,
                            liveness_threshold=liveness_threshold,
                            node_parameters=node_parameters,
                            executor_parameters=executor_parameters,
                        )
                    except (subprocess.SubprocessError, GroupException) as e:
                        e = FabricError(e) if isinstance(e, GroupException) else e
                        Print.error(BenchError("Failed to configure nodes", e))
                        continue

                    # Run the benchmark.
                    for i in range(bench_parameters.runs):
                        Print.heading(f"Run {i+1}/{bench_parameters.runs}")
                        try:
                            self._run_single(
                                hosts=hosts,
                                executor_hosts=executor_hosts,
                                nodes=n,
                                faults=shard_fault,
                                rate=r,
                                shard_num=shard_num,
                                shard_sizes=shard_sizes,
                                shard_faults=shard_fault,
                                bench_parameters=bench_parameters,
                                node_parameters=node_parameters,
                                executor_parameters=executor_parameters,
                                cross_shard_ratio=cross_shard_ratio,
                                debug=debug,
                            )
                            self._logs(
                                hosts=hosts,
                                executor_hosts=executor_hosts,
                                nodes=n,
                                faults=shard_fault,
                                execution_ratio=shard_fault,
                                shard_num=shard_num,
                                shard_sizes=shard_sizes,
                            ).print(
                                PathMaker.result_file(
                                    executor_parameters.json["mempool"]["certify_batch_size"], r, shard_fault, shard_num, shard_sizes, liveness_threshold
                                )
                            )
                        except (
                            subprocess.SubprocessError,
                            GroupException,
                            ParseError,
                        ) as e:
                            self.kill(hosts=hosts)
                            if isinstance(e, GroupException):
                                e = FabricError(e)
                            Print.error(BenchError("Benchmark failed", e))
                            continue
