from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
from math import ceil, floor
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
    def __init__(self, clients, executors, order_size, order_faults_ratio, execution_size, execution_faults_ration, shardNum):
        inputs = [clients, executors]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.order_size = order_size
        self.order_faults_ratio = order_faults_ratio
        self.execution_faults_ratio = execution_faults_ration
        self.shard_num = shardNum
        self.execution_size = execution_size

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse client logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples, self.ratio \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the executors logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_executors, executors)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse executor logs: {e}')
        self.liveness_threshold, proposals, commits, shard_one_commits, arete_commits_to_round, arete_rounds_to_timestamp, sizes, self.received_samples, timeouts, self.configs \
            = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.shard_one_commits = self._merge_results([x.items() for x in shard_one_commits])
        # self.arete_commits_to_round -> dict{string: string}
        self.arete_commits_to_round = self._get_dict_merge_arete_round_results([x.items() for x in arete_commits_to_round])
        
        # self.arete_consensus_rounds_to_ts -> dict{int: string}
        # This records consensus round with its timestamp (used for calculating cross-shard latency)
        temp = self._update_merge_arete_commit_time_results([x.items() for x in arete_rounds_to_timestamp])
        sorted_temp = sorted(temp.items())
        self.arete_consensus_rounds_to_ts = self._update_arete_consensus_time_results(dict(sorted_temp))
        
        # self.arete_rounds_to_timestamp -> dict{int: string}
        # Map all execution round to commit_timestamp
        self.arete_rounds_to_timestamp = self._update_arete_commit_time_results()
        
        # calculate concurrent throughput
        self.concurrent_start_time = min(self.start)
        self.concurrent_commits = self._get_concurrent_results([x.items() for x in commits], self.concurrent_start_time)
        self.concurrent_sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.concurrent_commits
        }
        
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        self.timeouts = max(timeouts)

        # Check whether clients missed their target rate.
        # if self.misses != 0:
        #     Print.warn(
        #         f'Clients missed their target rate {self.misses:,} time(s)'
        #     )

        # Check whether the executors timed out.
        # Note that executors are expected to time out once at the beginning.
        # if self.timeouts > 2:
        #     Print.warn(f'Executors timed out {self.timeouts:,} time(s)')

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged
    
    def _get_concurrent_results(self, input, start):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if start < v and (not k in merged or merged[k] > v):
                    merged[k] = v
        return merged
    
    def _merge_proposals_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for s, k, v in x:
                # if int(s) == 0:
                    if not k in merged or merged[k] > v:
                        merged[k] = v
        return merged
    
    def _merge_commits_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for s, k, v in x:
                # if int(s) == 0:
                    if not k in merged or merged[k] > v:
                        merged[k] = v
        return merged
    def _merge_shard_one_commits_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for s, k, v in x:
                if int(s) == 0:
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
        # if search(r'Error', log) is not None:
        #     raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))
        ratio = float(search(r'Cross-shard ratio: (\d+.\d+|\d+)', log).group(1))
        
        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}
        # print("Client tx number is", len(samples))
        
        return size, rate, start, misses, samples, ratio

    def _parse_executors(self, log):
        # if search(r'panic', log) is not None:
        #     raise ParseError('Executor(s) panicked')

        liveness_threshold = float(search(r'Liveness threshold is: (\d+.\d+|\d+)', log).group(1))
        
        tmp = findall(r'\[(.*Z) .* Shard (\d+) Created B\d+ -> ([^ ]+=)', log)
        tmp = [(s, d, self._to_posix(t)) for t, s, d in tmp]
        proposals = self._merge_proposals_results([tmp])

        # tmp = findall(r'\[(.*Z) .* Shard (\d+) Committed B\d+ -> ([^ ]+=)', log)
        # tmp = [(s, d, self._to_posix(t)) for t, s, d in tmp]
        # commits = self._merge_commits_results([tmp])
        # shard_one_commits = self._merge_shard_one_commits_results([tmp])
        
        # batch_digest is picked by the ordering shard
        # arete_commits_to_round[batch_digest] = executed_batch_round
        tmp = findall(r'\[(.*Z) .* ARETE shard (\d+) Committed B\d+ -> ([^ ]+=) in round (\d+)', log)
        tmp_commits = [(s, d, self._to_posix(t)) for t, s, d, _ in tmp]
        commits = self._merge_commits_results([tmp_commits])
        shard_one_commits = self._merge_shard_one_commits_results([tmp_commits])
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
        return liveness_threshold, proposals, commits, shard_one_commits, arete_commits_to_round, arete_rounds_to_timestamp, sizes, samples, timeouts, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)
    
    def get_second_max(self, numbers):
        if len(numbers) < 2:
            return "List must have at least two elements"
        max_value = max(numbers[0], numbers[1])
        second_max = min(numbers[0], numbers[1])
        for num in numbers[2:]:
            if num > max_value:
                second_max = max_value
                max_value = num
            elif num > second_max and num != max_value:
                second_max = num
        return second_max
    
    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        actual_bps = (bps * self.ratio[0])/2 + bps*(1-self.ratio[0])
        actual_tps = (tps * self.ratio[0])/2 + tps*(1-self.ratio[0])
        return actual_tps, actual_bps, duration
    
    def _arete_consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        temp = end
        flag = int(0)
        for _, ts in self.arete_consensus_rounds_to_ts.items():
            if flag == 1:   # now, it is the timestamp of next round
                end = ts
                break
            if end <= float(ts):
                flag = 1
        if temp == end: 
            end += self._avg_consensus_interval()
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        actual_bps = (bps * self.ratio[0])/2 + bps*(1-self.ratio[0])
        actual_tps = (tps * self.ratio[0])/2 + tps*(1-self.ratio[0])
        return actual_tps, actual_bps, duration

    def _consensus_latency(self):
        latency = []
        if len(self.arete_consensus_rounds_to_ts) < 2:
            return 0
        first_consensus_ts = list(self.arete_consensus_rounds_to_ts.values())[1]
        for d, c in self.commits.items():
            if self.proposals[d] > first_consensus_ts:
                latency += [c - self.proposals[d]]
        return mean(latency) if latency else 0

    def _avg_consensus_interval(self):
        interval = []
        temp_ts = list(self.arete_consensus_rounds_to_ts.values())
        last_ts = 0.0
        if len(temp_ts) >= 2:
            last_ts = float(temp_ts[0])
            for i in range(1, len(temp_ts)):
                interval += [float(temp_ts[i]) - last_ts]
                last_ts = float(temp_ts[i])
            return mean(interval[1:]) if interval else 0
        else:
            return 0
        
    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        # start = self.concurrent_start_time
        start = min(self.start)
        end = max(self.commits.values())
        duration = end - start
        # bytes = sum(self.concurrent_sizes.values())
        bytes = sum(self.sizes.values())
        # consider cross-shard txs
        # 1 cross-shard tx = 2 sub-intra-shard txs
        bps = bytes / duration
        actual_bps = (bps * self.ratio[0])/2 + bps*(1-self.ratio[0])
        tps = bps / self.size[0]
        actual_tps = (tps * self.ratio[0])/2 + tps*(1-self.ratio[0])
        return actual_tps, actual_bps, duration
    
    def _end_to_end_arete_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        temp = end
        # ARETE: txs are confirmed with two rounds
        flag = int(0)
        for _, ts in self.arete_consensus_rounds_to_ts.items():
            if flag == 1:   # now, it is the timestamp of next round
                end = ts
                break
            if end <= float(ts):
                flag = 1
        if temp == end:     # no more txs are handled, using avg interval to evaluate
            end += self._avg_consensus_interval()
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        bps = bytes / duration
        actual_bps = (bps * self.ratio[0])/2 + bps*(1-self.ratio[0])
        tps = bps / self.size[0]
        actual_tps = (tps * self.ratio[0])/2 + tps*(1-self.ratio[0])
        return actual_tps, actual_bps, duration

    def _end_to_end_intra_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.shard_one_commits:
                    if not tx_id in sent:
                        continue
                    # assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.shard_one_commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0
    
    # Compared sharding protocols: adopt a lock-based protocol
    def _end_to_end_cross_latency(self):
        latency = []
        consensus_round_latency = list(self.arete_consensus_rounds_to_ts.values())
        index = 0
        last_ts = 0.0
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.arete_commits_to_round: # this batch is picked
                    exec_round = self.arete_commits_to_round[batch_id] # its execution round
                    int_exec_round = int(exec_round)
                    if int_exec_round in self.arete_rounds_to_timestamp:
                        if not tx_id in sent:
                            continue
                        start = sent[tx_id]
                        if self.arete_rounds_to_timestamp[int_exec_round] > last_ts:
                            while self.arete_rounds_to_timestamp[int_exec_round] > consensus_round_latency[index]:
                                index += 1
                                if index >= len(consensus_round_latency):
                                    return mean(latency) if latency else 0
                        else:
                            index += 1
                            if index >= len(consensus_round_latency):
                                return mean(latency) if latency else 0
                        last_ts = consensus_round_latency[index]
                            
                        end = consensus_round_latency[index]
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
                        if not tx_id in sent:
                            continue
                        start = sent[tx_id]
                        end = self.arete_rounds_to_timestamp[int_exec_round]    # commit timestamp
                        if end > list(self.arete_consensus_rounds_to_ts.values())[-1]:
                            # since cross latency doesn't cover the last samples
                            return mean(latency) if latency else 0
                        latency += [end-start]
        return mean(latency) if latency else 0
    
    def _arete_end_to_end_cross_latency(self):
        latency = []
        intra_latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.arete_commits_to_round: # this batch is picked
                    exec_round = self.arete_commits_to_round[batch_id] # its execution round
                    int_exec_round = int(exec_round)
                    if int_exec_round in self.arete_rounds_to_timestamp:  
                        if not tx_id in sent:
                            continue
                        start = sent[tx_id]
                        intra_latency += [self.arete_rounds_to_timestamp[int_exec_round]-start]
                        flag = int(0)
                        for consensus_round, ts in self.arete_consensus_rounds_to_ts.items():
                            if flag == 1:   # now, it is the timestamp of next round
                                end = ts
                                latency += [end-start]
                                break
                            if int_exec_round <= int(consensus_round):
                                flag = 1
        return mean(latency) if latency else 0

    def result(self):
        # consensus_latency = self._consensus_latency() * 1000
        consensus_tps, consensus_bps, duration = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, _ = self._end_to_end_throughput()
        end_to_end_intra_latency = self._end_to_end_intra_latency() * 1000
        end_to_end_cross_latency = self._end_to_end_cross_latency() * 1000
        if end_to_end_intra_latency >= end_to_end_cross_latency:
            raise ParseError('Running time is too short to get cross-shard transactions committed')
        
        # arete_consensus_latency = self._avg_consensus_interval() * 1000
        # arete_consensus_tps, arete_consensus_bps, _ = self._arete_consensus_throughput()
        # arete_end_to_end_tps, arete_end_to_end_bps, arete_duration = self._end_to_end_arete_throughput()
        # arete_end_to_end_intra_latency = self._arete_end_to_end_intra_latency() * 1000
        # arete_end_to_end_cross_latency = self._arete_end_to_end_cross_latency() * 1000

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
            f' Ordering shard size: {self.order_size} nodes\n'
            f' Ordering shard fault ratio: {self.order_faults_ratio} \n'
            f' Execution shard number: {self.shard_num} shards\n'
            f' Execution shard size: {self.execution_size} nodes\n'
            f' Execution shard fault ratio: {self.execution_faults_ratio} \n'
            f' Liveness threshold: {self.liveness_threshold[0]:,} \n'
            # f' Committee size: {int(self.execution_size/self.shard_num)} nodes\n'
            f' Input rate per shard: {ceil(sum(self.rate)/self.shard_num):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Cross-shard ratio: {self.ratio[0]:,} \n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Consensus timeout delay: {consensus_timeout_delay:,} ms\n'
            f' Consensus sync retry delay: {consensus_sync_retry_delay:,} ms\n'
            f' Mempool sync retry delay: {mempool_sync_retry_delay:,} ms\n'
            f' Mempool sync retry nodes: {mempool_sync_retry_nodes:,} nodes\n'
            f' Mempool batch size: {mempool_batch_size:,} B\n'
            f' Mempool max batch delay: {mempool_max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Benchmark Sharding:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end intra latency: {round(end_to_end_intra_latency):,} ms\n'
            f' End-to-end cross latency: {round(end_to_end_cross_latency):,} ms\n'
            # '\n'
            # f' ARETE (ours):\n'
            # f' Consensus TPS: {round(arete_consensus_tps):,} tx/s\n'
            # f' Consensus BPS: {round(arete_consensus_bps):,} B/s\n'
            # f' End-to-end TPS: {round(arete_end_to_end_tps):,} tx/s\n'
            # f' End-to-end BPS: {round(arete_end_to_end_bps):,} B/s\n'
            # f' End-to-end arete intra latency: {round(arete_end_to_end_intra_latency):,} ms\n'
            # f' End-to-end arete cross latency: {round(arete_end_to_end_cross_latency):,} ms\n'
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
    def process_shard(cls, directory, order_size, order_faults_ratio, execution_size, execution_faults_ratio, shardNum):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, '*client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        executors = []
        for filename in sorted(glob(join(directory, '*executor*.log'))):
            with open(filename, 'r') as f:
                executors += [f.read()]

        return cls(clients, executors, order_size, order_faults_ratio, execution_size, execution_faults_ratio, shardNum)
