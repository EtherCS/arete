import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, LocalExecutionCommittee, ExecutorParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError, ShardLogParser
from benchmark.utils import Print, BenchError, PathMaker


class LocalBenchShard:
    BASE_PORT = 9000
    BASE_SHARD_PORT = 10000
    SHARD_ID = 0

    # def __init__(self, bench_parameters_dict, node_parameters_dict):
    #     try:
    #         self.bench_parameters = BenchParameters(bench_parameters_dict)
    #         self.node_parameters = NodeParameters(node_parameters_dict)
    #     except ConfigError as e:
    #         raise BenchError('Invalid nodes or bench parameters', e)
    
    # Config TODO
    def __init__(self, bench_parameters_dict, executor_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.executor_parameters = ExecutorParameters(executor_parameters_dict)
            # Config TODO: support multiple execution shards
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_executors(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local shard benchmark')

        # Kill any previous testbed.
        self._kill_executors()

        try:
            Print.info('Setting up testbed...')
            executors, rate = self.nodes[0], self.rate[0]
            shard_sizes = self.shard_sizes[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.executor_crate_path())

            # Create alias for the client and executors binary.
            cmd = CommandMaker.alias_shard_executor_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.shard_executor_key_file(self.SHARD_ID, i) for i in range(executors)]
            for filename in key_files:
                cmd = CommandMaker.generate_executor_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalExecutionCommittee(names, self.BASE_SHARD_PORT)
            committee.print(PathMaker.shard_committee_file())

            self.executor_parameters.print(PathMaker.shard_parameters_file())

            # Do not boot faulty nodes.
            executors = executors - self.faults

            # Run the clients (they will wait for the executors to be ready).
            # TODO: shard_client_log_file(self.SHARD_ID, i): self.SHARD_ID -> shard id
            addresses = committee.front
            rate_share = ceil(rate / executors)
            timeout = self.executor_parameters.certify_timeout_delay
            client_logs = [PathMaker.shard_client_log_file(self.SHARD_ID, i) for i in range(executors)]
            for addr, log_file in zip(addresses, client_logs):
                cmd = CommandMaker.run_client(
                    addr,
                    self.tx_size,
                    rate_share,
                    timeout,
                    #nodes=addresses
                )
                self._background_run(cmd, log_file)

            # Run the executors.
            # TODO: shard_executor_log_file(self.SHARD_ID, i): self.SHARD_ID -> shard id
            dbs = [PathMaker.executor_db_path(i) for i in range(executors)]
            executor_logs = [PathMaker.shard_executor_log_file(self.SHARD_ID, i) for i in range(executors)]
            for key_file, db, log_file in zip(key_files, dbs, executor_logs):
                cmd = CommandMaker.run_executor(
                    key_file,
                    PathMaker.shard_committee_file(),
                    db,
                    PathMaker.shard_parameters_file(),
                    debug=debug
                )
                self._background_run(cmd, log_file)

            # Config TODO: support multiple execution shards
            
            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(2 * self.executor_parameters.certify_timeout_delay / 1000)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_executors()

            # Parse logs and return the parser.
            # TODO: support to parse multiple shards
            Print.info('Parsing logs...')
            return ShardLogParser.process_shard(f'./logs', faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_executors()
            raise BenchError('Failed to run benchmark', e)
