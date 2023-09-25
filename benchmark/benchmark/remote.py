from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
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
            Print.heading(f"Initialized testbed of {len(hosts)} nodes")
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
        nodes_per_host = (len(nodes) + len(hosts) - 1) // len(hosts)
        return [(host, i) for host in hosts for i in range(nodes_per_host)][:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user="ubuntu", connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...')
        cmd = [
            f"(cd {self.settings.repo_name} && git fetch -f)",
            f"(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})",
            f"(cd {self.settings.repo_name} && git pull -f)",
            "source $HOME/.cargo/env",
            f"(cd {self.settings.repo_name}/node && {CommandMaker.compile()})",
            CommandMaker.alias_binaries(f"./{self.settings.repo_name}/target/release/"),
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
        node_key_files = [PathMaker.key_file(i) for i in nodes]
        for node_filename in node_key_files:
            cmd = CommandMaker.generate_key(node_filename).split()
            subprocess.run(cmd, check=True)
            node_keys += [Key.from_file(node_filename)]
        node_names = [x.name for x in node_keys]
        nodes = self._split_hosts(hosts, nodes)
        consensus_addr = [
            f"{x}:{self.settings.consensus_port + i}" for x, i in enumerate(nodes)
        ]
        front_addr = [
            f"{x}:{self.settings.front_port + i}" for x, i in enumerate(nodes)
        ]
        mempool_addr = [
            f"{x}:{self.settings.mempool_port + i}" for x, i in enumerate(nodes)
        ]
        committee = Committee(
            node_names,
            consensus_addr,
            front_addr,
            mempool_addr,
            shard_num,
            2**32 - 1,
            # TODO: ???
            {},
        )
        transaction_addrs_dict = committee.get_order_transaction_addresses()

        node_parameters.print(PathMaker.parameters_file())

        # Generate configuration files for executors.
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
                f"{x}:{self.settings.consensus_port + i}" for x, i in range(shard_nodes)
            ]
            front_addr = [
                f"{x}:{self.settings.front_port + i}" for x, i in range(shard_nodes)
            ]
            mempool_addr = [
                f"{x}:{self.settings.mempool_port + i}" for x, i in range(shard_nodes)
            ]
            confirmation_addr = [
                f"{x}:{self.settings.confirmation_port + i}"
                for x, i in range(shard_nodes)
            ]
            committee = ExecutionCommittee(
                names,
                consensus_addr,
                front_addr,
                mempool_addr,
                confirmation_addr,
                shard_num,
                shard_id,
                transaction_addrs_dict,
            )
            committee.print(PathMaker.shard_committee_file(shard_id))

            executor_parameters.print(PathMaker.shard_parameters_file(shard_id))

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
        progress = progress_bar(nodes, prefix="Uploading node key files:")
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
            c.put(PathMaker.shard_executor_key_file(i), ".")
        return committee

    def _run_single(
        self,
        hosts: list[str],
        executor_hosts: list[str],
        nodes: int,
        faults: int,
        rate: float,
        shard_num: int,
        shard_sizes: int,
        bench_parameters: BenchParameters,
        node_parameters: NodeParameters,
        executor_parameters: ExecutorParameters,
        debug=False,
    ):
        Print.info("Booting testbed...")

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)
        self.kill(hosts=executor_hosts, delete_logs=True)

        # Run the executors.
        executor_nodes = self._split_hosts(executor_hosts, shard_num * shard_sizes)
        for shard_id in range(shard_num):
            shard_nodes = executor_nodes[
                shard_id * shard_sizes : (shard_id + 1) * shard_sizes - faults
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
            for host, key_file, db, log_file, addr in zip(
                shard_nodes, key_files, dbs, executor_logs
            ):
                cmd = CommandMaker.run_executor(
                    key_file,
                    PathMaker.shard_committee_file(shard_id),
                    db,
                    PathMaker.shard_parameters_file(shard_id),
                    addr,
                    debug=debug,
                )
                self._background_run(host[0], cmd, log_file)

        # Wait for the nodes to synchronize
        sleep(2 * self.executor_parameters.certify_timeout_delay / 1000)

        # Run the nodes.
        host_nodes = self._split_hosts(hosts, nodes)
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
                shard_id * shard_sizes : (shard_id + 1) * shard_sizes - faults
            ]

            committee = Committee.load(PathMaker.shard_committee_file(shard_id))

            # Run the clients (they will wait for the executors to be ready).
            addresses = committee.front
            rate_share = ceil(rate / len(shard_nodes))
            timeout = self.executor_parameters.certify_timeout_delay
            client_logs = [
                PathMaker.shard_client_log_file(shard_id, i)
                for i in range(len(shard_nodes))
            ]
            for host, log_file in zip(shard_nodes, client_logs):
                cmd = CommandMaker.run_client(
                    addr,
                    self.tx_size,
                    rate_share,
                    shard_id,
                    timeout,
                )
                self._background_run(host[0], cmd, log_file)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f"Running benchmark ({duration} sec):"):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(
        self,
        hosts: list[str],
        executor_hosts: list[str],
        nodes: int,
        faults: int,
        shard_num: int,
        shard_sizes: int,
    ):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        host_nodes = self._split_hosts(hosts, nodes)
        progress = progress_bar(host_nodes, prefix="Downloading node logs:")
        for i, host in enumerate(progress):
            c = Connection(host[0], user="ubuntu", connect_kwargs=self.connect)
            c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))

        executor_nodes = self._split_hosts(executor_hosts, shard_num * shard_sizes)
        progress = progress_bar(executor_nodes, prefix="Downloading node logs:")
        for i, host in enumerate(progress):
            c = Connection(host[0], user="ubuntu", connect_kwargs=self.connect)
            c.get(
                PathMaker.shard_executor_log_file(i),
                local=PathMaker.shard_executor_log_file(i),
            )
            c.get(PathMaker.client_log_file(i), local=PathMaker.client_log_file(i))

        # Parse logs and return the parser.
        Print.info("Parsing logs and computing performance...")
        return ShardLogParser.process_shard(
            f"./logs", faults=faults, shardNum=shard_num
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
        for n, shard_num, shard_sizes, r, node_instances, executor_instances in zip(
            bench_parameters.nodes,
            bench_parameters.shard_num,
            bench_parameters.shard_sizes,
            bench_parameters.rate,
            bench_parameters.node_instances,
            bench_parameters.executor_instances,
        ):
            Print.heading(
                f"\nRunning {n} nodes with {shard_num} shards * {shard_sizes} nodes (input rate: {r:,} tx/s)"
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
                        faults=bench_parameters.faults,
                        rate=r,
                        shard_num=shard_num,
                        shard_sizes=shard_sizes,
                        bench_parameters=bench_parameters,
                        node_parameters=node_parameters,
                        executor_parameters=executor_parameters,
                        debug=debug,
                    )
                    self._logs(
                        hosts=hosts,
                        executor_hosts=executor_hosts,
                        nodes=n,
                        faults=bench_parameters.faults,
                        shard_num=shard_num,
                        shard_sizes=shard_sizes,
                    ).print(
                        PathMaker.result_file(
                            bench_parameters.faults, n, r, bench_parameters.tx_size
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
