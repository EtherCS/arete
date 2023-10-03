import subprocess
from math import ceil, floor
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, LocalExecutionCommittee, ExecutorParameters, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError, ShardLogParser
from benchmark.utils import Print, BenchError, PathMaker


class LocalBenchShard:
    BASE_NODE_PORT = 9000   # for the ordering shard
    BASE_SHARD_PORT = 10000 # for the execution shard
    SHARD_ID = 0
    ORDERING_SHARD_ID = 2**32-1

    # def __init__(self, bench_parameters_dict, node_parameters_dict):
    #     try:
    #         self.bench_parameters = BenchParameters(bench_parameters_dict)
    #         self.node_parameters = NodeParameters(node_parameters_dict)
    #     except ConfigError as e:
    #         raise BenchError('Invalid nodes or bench parameters', e)
    
    def __init__(self, bench_parameters_dict, node_params_dict, executor_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_params_dict)
            self.executor_parameters = ExecutorParameters(executor_parameters_dict)
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

    def _get_shard_port(self, shardId): # shard0: 10000; shard1: 10100; shard2: 10200
        return self.BASE_SHARD_PORT + 100*shardId
    
    def _get_node_port(self): # the base port of the ordering shard
        return self.BASE_NODE_PORT
    
    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local shard benchmark')

        # Kill any previous testbed.
        self._kill_executors()

        try:
            Print.info('Setting up testbed...')
            nodes = self.nodes[0]
            order_faults = floor(nodes * self.faults)
            
            shardSize, rate = self.shard_sizes[0], self.rate[0]
            shardNum = self.shard_num[0]
            liveness_threshold = self.liveness_threshold[0]
            cross_shard_ratio = self.cross_shard_ratio[0]
            execution_faults = floor(shardSize * self.shard_faults)
            total_executors = shardSize * shardNum

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code for executor and client.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.executor_crate_path())
            
            # Recompile the latest code for node.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())
            
            # Create alias for the client, node, and executors binary.
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
            ordering_committee = LocalCommittee(node_names, self._get_node_port(), shardNum, self.ORDERING_SHARD_ID, {})
            ordering_transaction_addrs_dict = ordering_committee.get_order_transaction_addresses()

            # Run execution shards
            executor_confirmation_addrs = {}    # record all executors' confirmation addresses
            for shardId in range(shardNum):
                # Generate configuration files for executors.
                keys = []
                key_files = [PathMaker.shard_executor_key_file(shardId, i) for i in range(shardSize)]
                for filename in key_files:
                    cmd = CommandMaker.generate_executor_key(filename).split()
                    subprocess.run(cmd, check=True)
                    keys += [Key.from_file(filename)]

                names = [x.name for x in keys]
                committee = LocalExecutionCommittee(names, self._get_shard_port(shardId), shardNum, shardId, ordering_transaction_addrs_dict, liveness_threshold)
                committee.print(PathMaker.shard_committee_file(shardId))

                self.executor_parameters.print(PathMaker.shard_parameters_file(shardId))
                
                executor_confirmation_addrs[shardId] = committee.get_confirm_addresses()

                # Do not boot faulty nodes.
                run_executors = shardSize - execution_faults

                dbs = [PathMaker.shard_executor_db_path(shardId, i) for i in range(run_executors)]
                executor_logs = [PathMaker.shard_executor_log_file(shardId, i) for i in range(run_executors)]
                for key_file, db, log_file in zip(key_files, dbs, executor_logs):
                    cmd = CommandMaker.run_executor(
                        key_file,
                        PathMaker.shard_committee_file(shardId),
                        db,
                        PathMaker.shard_parameters_file(shardId),
                        debug=debug
                    )
                    self._background_run(cmd, log_file)
                # Wait for the nodes to synchronize
                Print.info(f'Waiting for shard {shardId} the nodes to synchronize...')
            sleep(2 * self.executor_parameters.certify_timeout_delay / 1000)

            # update ordering committee with executors' information
            ordering_committee_with_confirm_addrs = LocalCommittee(node_names, self._get_node_port(), shardNum, self.ORDERING_SHARD_ID, executor_confirmation_addrs)
            ordering_committee_with_confirm_addrs.print(PathMaker.committee_file())
            self.node_parameters.print(PathMaker.parameters_file())            
            run_nodes = nodes - order_faults
            
            # Run the nodes
            node_dbs = [PathMaker.db_path(i) for i in range(run_nodes)]
            node_logs = [PathMaker.node_log_file(i) for i in range(run_nodes)]
            for node_key_file, node_db, node_log_file in zip(node_key_files, node_dbs, node_logs):
                cmd = CommandMaker.run_node(
                    node_key_file,
                    PathMaker.committee_file(),
                    node_db,
                    PathMaker.parameters_file(),
                    debug=debug
                )
                self._background_run(cmd, node_log_file)
            
            # Run the clients (they will wait for the executors to be ready).
            for shardId in range(shardNum):
                # Generate configuration files for executors.
                keys = []
                key_files = [PathMaker.shard_executor_key_file(shardId, i) for i in range(shardSize)]
                for filename in key_files:
                    cmd = CommandMaker.generate_executor_key(filename).split()
                    subprocess.run(cmd, check=True)
                    keys += [Key.from_file(filename)]

                names = [x.name for x in keys]
                committee = LocalExecutionCommittee(names, self._get_shard_port(shardId), shardNum, shardId, ordering_transaction_addrs_dict, liveness_threshold)

                # Do not boot faulty nodes.
                run_executors = shardSize - execution_faults
                addresses = committee.front
                rate_share = ceil(rate / run_executors)
                timeout = self.executor_parameters.certify_timeout_delay
                client_logs = [PathMaker.shard_client_log_file(shardId, i) for i in range(run_executors)]
                for addr, log_file in zip(addresses, client_logs):
                    cmd = CommandMaker.run_client(
                        addr,
                        self.tx_size,
                        rate_share,
                        shardId,
                        timeout,
                        cross_shard_ratio,
                        #nodes=addresses
                    )
                    self._background_run(cmd, log_file)
                    
            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_executors()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return ShardLogParser.process_shard(f'./logs', order_size=nodes, order_faults_ratio=self.faults, execution_size=shardSize, execution_faults_ration=self.shard_faults, shardNum=shardNum)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_executors()
            raise BenchError('Failed to run benchmark', e)
