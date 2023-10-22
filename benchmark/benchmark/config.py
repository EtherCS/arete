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
        with open(filename, "r") as f:
            data = load(f)
        return cls(data["name"], data["secret"])


class Committee:
    def __init__(
        self,
        names,
        consensus_addr,
        transactions_addr,
        mempool_addr,
        shardNum: int,
        shardId: int,
        confirm_addrs,
    ):
        inputs = [names, consensus_addr, transactions_addr, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.names = names
        self.consensus = consensus_addr
        self.front = transactions_addr
        self.mempool = mempool_addr
        self.confirm_addrs = confirm_addrs

        self.json = {
            "shard": self._build_shard_info(shardNum, shardId),
            "consensus": self._build_consensus(),
            "mempool": self._build_mempool(),
            "executor_confirmation_addresses": self._buld_executor_confirmation_addresses(),
        }

    def _buld_executor_confirmation_addresses(self):
        # self.confirm_addrs = {"0": {"node_name_0": confirm_addr0, "node_name_1": confirm_addr1, ..}, "1": {}, ..}
        return self.confirm_addrs

    def _build_shard_info(self, _shardNum, _shardId):
        return {"number": _shardNum, "id": _shardId}

    def _build_consensus(self):
        node = {}
        for a, n in zip(self.consensus, self.names):
            node[n] = {"name": n, "stake": 1, "address": a}
        return {"authorities": node, "epoch": 1}

    def _build_mempool(self):
        node = {}
        for n, f, m in zip(self.names, self.front, self.mempool):
            node[n] = {
                "name": n,
                "stake": 1,
                "transactions_address": f,
                "mempool_address": m,
            }
        return {"authorities": node, "epoch": 1}

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, "w") as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def size(self):
        return len(self.json["consensus"]["authorities"])

    def get_order_transaction_addresses(self):
        order_tx_addrs = {}
        for n, f in zip(self.names, self.front):
            order_tx_addrs[n] = f
        return order_tx_addrs

    @classmethod
    def load(cls, filename):
        assert isinstance(filename, str)
        with open(filename, "r") as f:
            data = load(f)

        consensus_authorities = data["consensus"]["authorities"].values()
        mempool_authorities = data["mempool"]["authorities"].values()

        names = [x["name"] for x in consensus_authorities]
        consensus_addr = [x["address"] for x in consensus_authorities]
        transactions_addr = [x["transactions_address"] for x in mempool_authorities]
        mempool_addr = [x["mempool_address"] for x in mempool_authorities]
        return cls(
            names,
            consensus_addr,
            transactions_addr,
            mempool_addr,
            data["shard"]["number"],
            data["shard"]["id"],
            data["executor_confirmation_addresses"],
        )


class LocalCommittee(Committee):
    def __init__(self, names, port, shardNum, shardId, confirm_addrs):
        assert isinstance(names, list) and all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        size = len(names)
        consensus = [f"127.0.0.1:{port + i}" for i in range(size)]
        front = [f"127.0.0.1:{port + i + size}" for i in range(size)]
        mempool = [f"127.0.0.1:{port + i + 2*size}" for i in range(size)]
        super().__init__(
            names, consensus, front, mempool, shardNum, shardId, confirm_addrs
        )


class ExecutionCommittee:
    def __init__(
        self,
        names,
        consensus_addr,
        transactions_addr,
        mempool_addr,
        confirmation_addr,
        shardNum,
        shardId,
        order_transaction_addrs,
        liveness_threshold,
    ):
        inputs = [names, consensus_addr, transactions_addr, mempool_addr]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert len({len(x) for x in inputs}) == 1

        self.names = names
        self.consensus = consensus_addr
        self.front = transactions_addr
        self.mempool = mempool_addr
        self.confirmation = confirmation_addr  # execution shard receives the ordered txs from ordering shard
        self.order_transaction_addresses = order_transaction_addrs
        self.liveness_threshold = liveness_threshold

        self.json = {
            "shard": self._build_shard_info(shardNum, shardId),
            "consensus": self._build_consensus(),
            "mempool": self._build_mempool(),
            "order_transaction_addresses": self._build_order_transaction_addresses(),
        }

    def _build_order_transaction_addresses(self):
        return self.order_transaction_addresses

    def _build_shard_info(self, _shardNum, _shardId):
        return {"number": _shardNum, "id": _shardId}

    def _build_consensus(self):
        node = {}
        for a, n in zip(self.consensus, self.names):
            node[n] = {"name": n, "stake": 1, "address": a}
        return {"authorities": node, "epoch": 1, "liveness_threshold": self.liveness_threshold}

    def _build_mempool(self):
        node = {}
        for n, f, m, c in zip(self.names, self.front, self.mempool, self.confirmation):
            node[n] = {
                "name": n,
                "stake": 1,
                "transactions_address": f,
                "mempool_address": m,
                "confirmation_address": c,
            }
        return {"authorities": node, "epoch": 1, "liveness_threshold": self.liveness_threshold}

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, "w") as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def size(self):
        return len(self.json["consensus"]["authorities"])

    def shard_id(self):
        return self.json["shard"]["id"]

    def get_confirm_addresses(self):
        confirm_addrs = {}
        for n, c in zip(self.names, self.confirmation):
            confirm_addrs[n] = c
        return confirm_addrs

    def get_confirm_addresses(self):
        confirm_addrs = {}
        for n, c in zip(self.names, self.confirmation):
            confirm_addrs[n] = c
        return confirm_addrs
    
    @classmethod
    def load(cls, filename):
        assert isinstance(filename, str)
        with open(filename, "r") as f:
            data = load(f)

        consensus_authorities = data["consensus"]["authorities"].values()
        mempool_authorities = data["mempool"]["authorities"].values()

        names = [x["name"] for x in consensus_authorities]
        consensus_addr = [x["address"] for x in consensus_authorities]
        transactions_addr = [x["transactions_address"] for x in mempool_authorities]
        mempool_addr = [x["mempool_address"] for x in mempool_authorities]
        confirmation_addr = [x["confirmation_address"] for x in mempool_authorities]
        return cls(
            names,
            consensus_addr,
            transactions_addr,
            mempool_addr,
            confirmation_addr,
            data["shard"]["number"],
            data["shard"]["id"],
            data["order_transaction_addresses"],
            data["consensus"]["liveness_threshold"],
        )


class LocalExecutionCommittee(ExecutionCommittee):
    def __init__(self, names, port, shardNum, shardId, order_transaction_addrs, liveness_threshold):
        assert isinstance(names, list) and all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        size = len(names)
        consensus = [f"127.0.0.1:{port + i}" for i in range(size)]
        front = [f"127.0.0.1:{port + i + size}" for i in range(size)]
        mempool = [f"127.0.0.1:{port + i + 2*size}" for i in range(size)]
        confirmation = [f"127.0.0.1:{port + i + 3*size}" for i in range(size)]
        super().__init__(
            names,
            consensus,
            front,
            mempool,
            confirmation,
            shardNum,
            shardId,
            order_transaction_addrs,
            liveness_threshold,
        )


class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json["consensus"]["timeout_delay"]]
            inputs += [json["consensus"]["sync_retry_delay"]]
            inputs += [json["consensus"]["cblock_batch_size"]]
            inputs += [json["mempool"]["gc_depth"]]
            inputs += [json["mempool"]["sync_retry_delay"]]
            inputs += [json["mempool"]["sync_retry_nodes"]]
            inputs += [json["mempool"]["batch_size"]]
            inputs += [json["mempool"]["max_batch_delay"]]
        except KeyError as e:
            raise ConfigError(f"Malformed parameters: missing key {e}")

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError("Invalid parameters type")

        self.timeout_delay = json["consensus"]["timeout_delay"]
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, "w") as f:
            dump(self.json, f, indent=4, sort_keys=True)


class ExecutorParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json["consensus"]["certify_timeout_delay"]]
            inputs += [json["consensus"]["certify_sync_retry_delay"]]
            inputs += [json["mempool"]["certify_cross_shard_ratio"]]
            inputs += [json["mempool"]["certify_gc_depth"]]
            inputs += [json["mempool"]["certify_sync_retry_delay"]]
            inputs += [json["mempool"]["certify_sync_retry_nodes"]]
            inputs += [json["mempool"]["certify_batch_size"]]
            inputs += [json["mempool"]["certify_max_batch_delay"]]
        except KeyError as e:
            raise ConfigError(f"Malformed parameters: missing key {e}")

        # if not all(isinstance(x, int) for x in inputs):
        #     raise ConfigError("Invalid parameters type")

        self.certify_timeout_delay = json["consensus"]["certify_timeout_delay"]
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, "w") as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            nodes = json["nodes"]
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 1 for x in nodes):
                raise ConfigError("Missing or invalid number of nodes")

            rate = json["rate"]
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError("Missing input rate")

            self.nodes = [int(x) for x in nodes]
            self.rate = [int(x) for x in rate]
            self.tx_size = int(json["tx_size"])
            self.faults = float(json["faults"])
            self.shard_faults = float(json["shard_faults"])
            self.duration = int(json["duration"])
            self.runs = int(json["runs"]) if "runs" in json else 1
            
            # setting liveness threshold
            liveness_threshold = json["liveness_threshold"]
            liveness_threshold = (
                liveness_threshold if isinstance(liveness_threshold, list) else [liveness_threshold]
            )
            if not liveness_threshold or any(x < 0 for x in liveness_threshold):
                raise ConfigError("Missing or invalid liveness_threshold")
            self.liveness_threshold = [float(x) for x in liveness_threshold]
            
            # setting cross-shard txs ratios 
            cross_shard_ratio = json["cross_shard_ratio"]
            cross_shard_ratio = (
                cross_shard_ratio if isinstance(cross_shard_ratio, list) else [cross_shard_ratio]
            )
            if not cross_shard_ratio or any(x < 0 or x > 1 for x in cross_shard_ratio):
                raise ConfigError("Missing or invalid cross_shard_ratio")
            self.cross_shard_ratio = [float(x) for x in cross_shard_ratio]
            
            # Config
            shard_num = json["shard_num"]
            shard_num = shard_num if isinstance(shard_num, list) else [shard_num]

            if not shard_num or any(x < 1 for x in shard_num):
                raise ConfigError("Missing or invalid shard number")
            self.shard_num = [int(x) for x in shard_num]

            shard_sizes = json["shard_sizes"]
            shard_sizes = (
                shard_sizes if isinstance(shard_sizes, list) else [shard_sizes]
            )

            if not shard_sizes or any(x <= 1 for x in shard_sizes):
                raise ConfigError("Missing or invalid shard size")
            self.shard_sizes = [int(x) for x in shard_sizes]

            node_instances = json["node_instances"] if "node_instances" in json else 1
            node_instances = (
                node_instances
                if isinstance(node_instances, list)
                else [node_instances] * len(nodes)
            )
            self.node_instances = [int(x) for x in node_instances]
            executor_instances = (
                json["executor_instances"]
                if "executor_instances" in json
                else self.shard_num
            )
            executor_instances = (
                executor_instances
                if isinstance(executor_instances, list)
                else [executor_instances] * len(nodes)
            )
            self.executor_instances = [int(x) for x in executor_instances]

            if len(self.node_instances) < len(self.nodes) or len(
                self.executor_instances
            ) < len(self.nodes):
                raise ConfigError("Missing or invalid instance size")

        except KeyError as e:
            raise ConfigError(f"Malformed bench parameters: missing key {e}")

        except ValueError:
            raise ConfigError("Invalid parameters type")

        if min(self.nodes) <= self.faults:
            raise ConfigError("There should be more nodes than faults")


class PlotParameters:
    def __init__(self, json):
        try:
            nodes = json["nodes"]
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError("Missing number of nodes")
            self.nodes = [int(x) for x in nodes]

            self.tx_size = int(json["tx_size"])

            faults = json["faults"]
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            max_lat = json["max_latency"]
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError("Missing max latency")
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f"Malformed bench parameters: missing key {e}")

        except ValueError:
            raise ConfigError("Invalid parameters type")
