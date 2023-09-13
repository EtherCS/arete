from json import dump, load


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    def __init__(self, names, consensus_addr, transactions_addr, mempool_addr, shardNum, shardId):
        inputs = [names, consensus_addr, transactions_addr, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.names = names
        self.consensus = consensus_addr
        self.front = transactions_addr
        self.mempool = mempool_addr

        self.json = {
            'shard': self._build_shard_info(shardNum, shardId),
            'consensus': self._build_consensus(),
            'mempool': self._build_mempool()
        }
    def _build_shard_info(self, _shardNum, _shardId):
        return {'number': _shardNum, 'id': _shardId}
    
    def _build_consensus(self):
        node = {}
        for a, n in zip(self.consensus, self.names):
            node[n] = {'name': n, 'stake': 1, 'address': a}
        return {'authorities': node, 'epoch': 1}

    def _build_mempool(self):
        node = {}
        for n, f, m in zip(self.names, self.front, self.mempool):
            node[n] = {
                'name': n,
                'stake': 1,
                'transactions_address': f,
                'mempool_address': m
            }
        return {'authorities': node, 'epoch': 1}

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def size(self):
        return len(self.json['consensus']['authorities'])

    @classmethod
    def load(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)

        consensus_authorities = data['consensus']['authorities'].values()
        mempool_authorities = data['mempool']['authorities'].values()

        names = [x['name'] for x in consensus_authorities]
        consensus_addr = [x['address'] for x in consensus_authorities]
        transactions_addr = [
            x['transactions_address'] for x in mempool_authorities
        ]
        mempool_addr = [x['mempool_address'] for x in mempool_authorities]
        return cls(names, consensus_addr, transactions_addr, mempool_addr)


class LocalCommittee(Committee):
    def __init__(self, names, port, shardNum, shardId):
        assert isinstance(names, list) and all(
            isinstance(x, str) for x in names)
        assert isinstance(port, int)
        size = len(names)
        consensus = [f'127.0.0.1:{port + i}' for i in range(size)]
        front = [f'127.0.0.1:{port + i + size}' for i in range(size)]
        mempool = [f'127.0.0.1:{port + i + 2*size}' for i in range(size)]
        super().__init__(names, consensus, front, mempool, shardNum, shardId)

class ExecutionCommittee:
    def __init__(self, names, consensus_addr, transactions_addr, mempool_addr, confirmation_addr, shardNum, shardId):
        inputs = [names, consensus_addr, transactions_addr, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.names = names
        self.consensus = consensus_addr
        self.front = transactions_addr
        self.mempool = mempool_addr
        self.confirmation = confirmation_addr   # execution shard receives the ordered txs from ordering shard

        self.json = {
            'shard': self._build_shard_info(shardNum, shardId),
            'consensus': self._build_consensus(),
            'mempool': self._build_mempool()
        }
    def _build_shard_info(self, _shardNum, _shardId):
        return {'number': _shardNum, 'id': _shardId}
        
        
    def _build_consensus(self):
        node = {}
        for a, n in zip(self.consensus, self.names):
            node[n] = {'name': n, 'stake': 1, 'address': a}
        return {'authorities': node, 'epoch': 1}

    def _build_mempool(self):
        node = {}
        for n, f, m, c in zip(self.names, self.front, self.mempool, self.confirmation):
            node[n] = {
                'name': n,
                'stake': 1,
                'transactions_address': f,
                'mempool_address': m,
                'confirmation_address': c
            }
        return {'authorities': node, 'epoch': 1}

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def size(self):
        return len(self.json['consensus']['authorities'])
    
    def shard_id(self):
        return self.json['shard']['id']

    @classmethod
    def load(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)

        consensus_authorities = data['consensus']['authorities'].values()
        mempool_authorities = data['mempool']['authorities'].values()

        names = [x['name'] for x in consensus_authorities]
        consensus_addr = [x['address'] for x in consensus_authorities]
        transactions_addr = [
            x['transactions_address'] for x in mempool_authorities
        ]
        mempool_addr = [x['mempool_address'] for x in mempool_authorities]
        confirmation_addr = [x['confirmation_address'] for x in mempool_authorities]
        return cls(names, consensus_addr, transactions_addr, mempool_addr, confirmation_addr)


class LocalExecutionCommittee(ExecutionCommittee):
    def __init__(self, names, port, shardNum, shardId):
        assert isinstance(names, list) and all(
            isinstance(x, str) for x in names)
        assert isinstance(port, int)
        size = len(names)
        consensus = [f'127.0.0.1:{port + i}' for i in range(size)]
        front = [f'127.0.0.1:{port + i + size}' for i in range(size)]
        mempool = [f'127.0.0.1:{port + i + 2*size}' for i in range(size)]
        confirmation = [f'127.0.0.1:{port + i + 3*size}' for i in range(size)]
        super().__init__(names, consensus, front, mempool, confirmation, shardNum, shardId)
        
class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['consensus']['timeout_delay']]
            inputs += [json['consensus']['sync_retry_delay']]
            inputs += [json['mempool']['gc_depth']]
            inputs += [json['mempool']['sync_retry_delay']]
            inputs += [json['mempool']['sync_retry_nodes']]
            inputs += [json['mempool']['batch_size']]
            inputs += [json['mempool']['max_batch_delay']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')

        self.timeout_delay = json['consensus']['timeout_delay']
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)
# Config TODO: support multiple execution shards
class ExecutorParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['consensus']['certify_timeout_delay']]
            inputs += [json['consensus']['certify_sync_retry_delay']]
            inputs += [json['mempool']['certify_gc_depth']]
            inputs += [json['mempool']['certify_sync_retry_delay']]
            inputs += [json['mempool']['certify_sync_retry_nodes']]
            inputs += [json['mempool']['certify_batch_size']]
            inputs += [json['mempool']['certify_max_batch_delay']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')

        self.certify_timeout_delay = json['consensus']['certify_timeout_delay']
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

class BenchParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 1 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')

            self.nodes = [int(x) for x in nodes]
            self.rate = [int(x) for x in rate]
            self.tx_size = int(json['tx_size'])
            self.faults = int(json['faults'])
            self.duration = int(json['duration'])
            self.runs = int(json['runs']) if 'runs' in json else 1
            # Config
            self.shard_num = int(json['shard_num'])
            shard_sizes = json['shard_sizes']
            shard_sizes = shard_sizes if isinstance(shard_sizes, list) else [shard_sizes]
            if not shard_sizes or any(x <= 1 for x in shard_sizes):
                raise ConfigError('Missing or invalid shard size')
            self.shard_sizes = [int(x) for x in shard_sizes]
            
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if min(self.nodes) <= self.faults:
            raise ConfigError('There should be more nodes than faults')


class PlotParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            self.tx_size = int(json['tx_size'])

            faults = json['faults']
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            max_lat = json['max_latency']
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')
