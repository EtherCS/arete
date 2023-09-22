from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
import copy

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, nodes, faults):
        inputs = [clients, nodes]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(nodes) + int(faults)
        else:
            self.committee_size = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse node logs: {e}')
        proposals, commits, sizes, self.received_samples, timeouts, self.configs \
            = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        self.timeouts = max(timeouts)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

        # Check whether the nodes timed out.
        # Note that nodes are expected to time out once at the beginning.
        if self.timeouts > 2:
            Print.warn(f'Nodes timed out {self.timeouts:,} time(s)')

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_nodes(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Node(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+ -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+ -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        tmp = findall(r'.* WARN .* Timeout', log)
        timeouts = len(tmp)

        configs = {
            'consensus': {
                'timeout_delay': int(
                    search(r'Timeout delay .* (\d+)', log).group(1)
                ),
                'sync_retry_delay': int(
                    search(
                        r'consensus.* Sync retry delay .* (\d+)', log
                    ).group(1)
                ),
            },
            'mempool': {
                'gc_depth': int(
                    search(r'Garbage collection .* (\d+)', log).group(1)
                ),
                'sync_retry_delay': int(
                    search(r'mempool.* Sync retry delay .* (\d+)', log).group(1)
                ),
                'sync_retry_nodes': int(
                    search(r'Sync retry nodes .* (\d+)', log).group(1)
                ),
                'batch_size': int(
                    search(r'Batch size .* (\d+)', log).group(1)
                ),
                'max_batch_delay': int(
                    search(r'Max batch delay .* (\d+)', log).group(1)
                ),
            }
        }

        return proposals, commits, sizes, samples, timeouts, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def result(self):
        consensus_latency = self._consensus_latency() * 1000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000

        consensus_timeout_delay = self.configs[0]['consensus']['timeout_delay']
        consensus_sync_retry_delay = self.configs[0]['consensus']['sync_retry_delay']
        mempool_gc_depth = self.configs[0]['mempool']['gc_depth']
        mempool_sync_retry_delay = self.configs[0]['mempool']['sync_retry_delay']
        mempool_sync_retry_nodes = self.configs[0]['mempool']['sync_retry_nodes']
        mempool_batch_size = self.configs[0]['mempool']['batch_size']
        mempool_max_batch_delay = self.configs[0]['mempool']['max_batch_delay']

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} nodes\n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Consensus timeout delay: {consensus_timeout_delay:,} ms\n'
            f' Consensus sync retry delay: {consensus_sync_retry_delay:,} ms\n'
            f' Mempool GC depth: {mempool_gc_depth:,} rounds\n'
            f' Mempool sync retry delay: {mempool_sync_retry_delay:,} ms\n'
            f' Mempool sync retry nodes: {mempool_sync_retry_nodes:,} nodes\n'
            f' Mempool batch size: {mempool_batch_size:,} B\n'
            f' Mempool max batch delay: {mempool_max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        nodes = []
        for filename in sorted(glob(join(directory, 'node-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(clients, nodes, faults)
    
    @classmethod
    def process_shard(cls, directory, faults):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        executors = []
        for filename in sorted(glob(join(directory, 'executor-*.log'))):
            with open(filename, 'r') as f:
                executors += [f.read()]

        return cls(clients, executors, faults)


class ShardLogParser:
    # clients: integration of all clients' files
    # executors: integration of all executors' files
    def __init__(self, clients, executors, faults, shardNum):
        inputs = [clients, executors]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        # Jianting: shard number
        self.shard_num = shardNum
        if isinstance(faults, int):
            self.committee_size = len(executors) + int(faults)
        else:
            self.committee_size = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the executors logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_executors, executors)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse executor logs: {e}')
        proposals, commits, arete_commits_to_round, arete_rounds_to_timestamp, sizes, self.received_samples, timeouts, self.configs \
            = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        # self.arete_commits_to_round -> dict{string: string}
        self.arete_commits_to_round = self._get_dict_merge_arete_round_results([x.items() for x in arete_commits_to_round])
        
        # self.arete_consensus_rounds_to_ts -> dict{int: string}
        # This records consensus round with its timestamp (used for calculating cross-shard latency)
        temp = self._update_merge_arete_commit_time_results([x.items() for x in arete_rounds_to_timestamp])
        sorted_temp = sorted(temp.items())
        self.arete_consensus_rounds_to_ts = self._update_arete_consensus_time_results(dict(sorted_temp))
        
        # self.arete_rounds_to_timestamp -> dict{int: string}
        self.arete_rounds_to_timestamp = self._update_arete_commit_time_results()
        
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        self.timeouts = max(timeouts)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

        # Check whether the executors timed out.
        # Note that executors are expected to time out once at the beginning.
        if self.timeouts > 2:
            Print.warn(f'Executors timed out {self.timeouts:,} time(s)')

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged
    
    # Get commits_to_round[batch_digest] = executed_batch_round
    # TODO: currently we only calculate latency for shard 0
    def _merge_arete_round_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        digest_to_time = {}
        for x in input:
            for s, d, r, t in x:
                if int(s) == 0:
                    if not d in digest_to_time or digest_to_time[d] > t:
                    # if not k in merged or merged[k] > r:
                        digest_to_time[d] = t
                        merged[d] = r
        return merged
    
    # Get dict for _merge_arete_round_results
    def _get_dict_merge_arete_round_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, r in x:
                if not k in merged or merged[k] > r:
                    merged[k] = r
        return merged
    
    # Get arete_commits[executed_batch_round] = committed_timestamp
    def _merge_arete_commit_time_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for s, d, t in x:
                if int(s) == 0:
                    if not d in merged or merged[d] > t:
                        merged[d] = t
        return merged
    
    # Get arete_commits[(int)executed_batch_round] = committed_timestamp
    def _update_merge_arete_commit_time_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                int_k = int(k)
                if not int_k in merged or merged[int_k] > v:
                    merged[int_k] = v
        return merged

    # Execution shard may receive ts[round_A]>ts[round_B], where round_A < round_B
    # This func is used for correcting this issue
    def _update_arete_consensus_time_results(self, input):
        merged = {}
        for r, ts in input.items():
            merged[int(r)] = ts
        reversed_items = list(input.items())[::-1]
        for list_r, list_ts in reversed_items:
            for r, ts in input.items():
                if int(r) > int(list_r):
                    break
                if merged[int(r)] > list_ts:
                    merged[int(r)] = list_ts
        return merged
    
    # map 'all_execute_round' to 'committed timestamp'
    def _update_arete_commit_time_results(self):
        merged = {}
        last_round = int(0)
        # print("debug: committed round ts is ", input)
        for r, t in self.arete_consensus_rounds_to_ts.items():
            for index in range(last_round, int(r)+1):
                merged[index] = t
            last_round = int(r)+1
        return merged
    
    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}
        # print("Client tx number is", len(samples))

        return size, rate, start, misses, samples

    def _parse_executors(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Executor(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+ -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+ -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])
        
        # batch_digest is picked by the ordering shard
        # arete_commits_to_round[batch_digest] = executed_batch_round
        tmp = findall(r'\[(.*Z) .* ARETE shard (\d+) Committed B\d+ -> ([^ ]+=) in round (\d+)', log)
        tmp = [(s, d, r, self._to_posix(t)) for t, s, d, r in tmp]
        arete_commits_to_round = self._merge_arete_round_results([tmp])
        
        # [{executed_batch_round, committed_timestamp}]
        # arete_rounds_to_timestamp[executed_batch_round] = committed_timestamp
        tmp = findall(r'\[(.*Z) .* ARETE shard (\d+) commit anchor block for execution round (\d+)', log)
        tmp = [(s, r, self._to_posix(t)) for t, s, r in tmp]
        arete_rounds_to_timestamp = self._merge_arete_commit_time_results([tmp])    # get map[batch_digest]=earliest_commit_timestamp

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        tmp = findall(r'.* WARN .* Timeout', log)
        timeouts = len(tmp)

        configs = {
            'consensus': {
                'certify_timeout_delay': int(
                    search(r'Timeout delay .* (\d+)', log).group(1)
                ),
                'certify_sync_retry_delay': int(
                    search(
                        r'certify.* Sync retry delay .* (\d+)', log
                    ).group(1)
                ),
            },
            'mempool': {
                'certify_gc_depth': int(
                    search(r'Garbage collection .* (\d+)', log).group(1)
                ),
                'certify_sync_retry_delay': int(
                    search(r'execpool.* Sync retry delay .* (\d+)', log).group(1)
                ),
                'certify_sync_retry_nodes': int(
                    search(r'Sync retry nodes .* (\d+)', log).group(1)
                ),
                'certify_batch_size': int(
                    search(r'Batch size .* (\d+)', log).group(1)
                ),
                'certify_max_batch_delay': int(
                    search(r'Max batch delay .* (\d+)', log).group(1)
                ),
            }
        }

        # return proposals, commits, sizes, samples, timeouts, configs, block_intervals
        return proposals, commits, arete_commits_to_round, arete_rounds_to_timestamp, sizes, samples, timeouts, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _avg_consensus_interval(self):
        interval = []
        temp = 0.0
        for _, ts in self.arete_consensus_rounds_to_ts.items():
            interval += [float(ts) - temp]
            temp = float(ts)
        return mean(interval[1:]) if interval else 0
    
        
    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0
    
    def _arete_end_to_end_intra_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.arete_commits_to_round: # this batch is picked
                    exec_round = self.arete_commits_to_round[batch_id] # its execution round
                    int_exec_round = int(exec_round)
                    if int_exec_round in self.arete_rounds_to_timestamp:  
                        start = sent[tx_id]
                        end = self.arete_rounds_to_timestamp[int_exec_round]    # commit timestamp
                        latency += [end-start]
        # since cross latency doesn't cover the last samples, remove them
        return mean(latency[:int(3*len(latency)/4)]) if latency else 0
    
    def _arete_end_to_end_cross_latency(self):
        latency = []
        intra_latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.arete_commits_to_round: # this batch is picked
                    exec_round = self.arete_commits_to_round[batch_id] # its execution round
                    int_exec_round = int(exec_round)
                    if int_exec_round in self.arete_rounds_to_timestamp:  
                        start = sent[tx_id]
                        intra_latency += [self.arete_rounds_to_timestamp[int_exec_round]-start]
                        flag = int(0)
                        for consensus_round, ts in self.arete_consensus_rounds_to_ts.items():
                            if flag == 1:   # now, it is the timestamp of next round
                                end = ts
                                latency += [end-start]
                                if intra_latency[-1] > latency[-1]:
                                    print(f"Debug: find intra {intra_latency[-1]} is larger than {latency[-1]} in round {int_exec_round} \n")
                                    print(f"self.arete_rounds_to_timestamp[{int_exec_round}] is {self.arete_rounds_to_timestamp[int_exec_round]}\n")
                                    print(f"self.arete_rounds_to_timestamp[{int(consensus_round)}] is {self.arete_rounds_to_timestamp[int(consensus_round)]}, self.arete_consensus_rounds_to_ts[{int(consensus_round)}] is {ts} \n")
                                break
                            if int_exec_round <= int(consensus_round):
                                flag = 1
        return mean(latency) if latency else 0

    def result(self):
        arete_consensus_latenct = self._avg_consensus_interval() * 1000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000
        arete_end_to_end_intra_latency = self._arete_end_to_end_intra_latency() * 1000
        arete_end_to_end_cross_latency = self._arete_end_to_end_cross_latency() * 1000

        consensus_timeout_delay = self.configs[0]['consensus']['certify_timeout_delay']
        consensus_sync_retry_delay = self.configs[0]['consensus']['certify_sync_retry_delay']
        mempool_gc_depth = self.configs[0]['mempool']['certify_gc_depth']
        mempool_sync_retry_delay = self.configs[0]['mempool']['certify_sync_retry_delay']
        mempool_sync_retry_nodes = self.configs[0]['mempool']['certify_sync_retry_nodes']
        mempool_batch_size = self.configs[0]['mempool']['certify_batch_size']
        mempool_max_batch_delay = self.configs[0]['mempool']['certify_max_batch_delay']

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Shard number: {self.shard_num} shards\n'
            f' Faults: {self.faults} nodes\n'
            f' Committee size: {int(self.committee_size/self.shard_num)} nodes\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Consensus timeout delay: {consensus_timeout_delay:,} ms\n'
            f' Consensus sync retry delay: {consensus_sync_retry_delay:,} ms\n'
            f' Mempool GC depth: {mempool_gc_depth:,} rounds\n'
            f' Mempool sync retry delay: {mempool_sync_retry_delay:,} ms\n'
            f' Mempool sync retry nodes: {mempool_sync_retry_nodes:,} nodes\n'
            f' Mempool batch size: {mempool_batch_size:,} B\n'
            f' Mempool max batch delay: {mempool_max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency (Anchor block interval): {round(arete_consensus_latenct):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end Hotstuff latency: {round(end_to_end_latency):,} ms\n'
            f' End-to-end arete intra latency: {round(arete_end_to_end_intra_latency):,} ms\n'
            f' End-to-end arete cross latency: {round(arete_end_to_end_cross_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        nodes = []
        for filename in sorted(glob(join(directory, 'node-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(clients, nodes, faults)
    
    @classmethod
    def process_shard(cls, directory, faults, shardNum):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, '*client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        executors = []
        for filename in sorted(glob(join(directory, '*executor-*.log'))):
            with open(filename, 'r') as f:
                executors += [f.read()]

        return cls(clients, executors, faults, shardNum)
