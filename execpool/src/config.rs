use crypto::PublicKey;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Deserialize, Serialize)]
pub struct CertifyParameters {
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub certify_gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub certify_sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub certify_sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub certify_batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub certify_max_batch_delay: u64,
}

impl Default for CertifyParameters {
    fn default() -> Self {
        Self {
            certify_gc_depth: 50,
            certify_sync_retry_delay: 5_000,
            certify_sync_retry_nodes: 3,
            certify_batch_size: 500_000,
            certify_max_batch_delay: 100,
        }
    }
}

impl CertifyParameters {
    pub fn log(&self) {
        // NOTE: These log entries are used to compute performance.
        info!("Garbage collection depth set to {} rounds", self.certify_gc_depth);
        info!("Sync retry delay set to {} ms", self.certify_sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.certify_sync_retry_nodes);
        info!("Batch size set to {} B", self.certify_batch_size);
        info!("Max batch delay set to {} ms", self.certify_max_batch_delay);
    }
}

pub type EpochNumber = u128;
pub type Stake = u32;

#[derive(Clone, Deserialize, Serialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// Address to receive client transactions.
    pub transactions_address: SocketAddr,
    /// Address to receive messages from other nodes.
    pub mempool_address: SocketAddr,
    /// Address to confirmation messages from the ordering shard.
    pub confirmation_address: SocketAddr,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ExecutionCommittee {
    pub authorities: HashMap<PublicKey, Authority>,
    pub epoch: EpochNumber,
}

impl ExecutionCommittee {
    pub fn new(info: Vec<(PublicKey, Stake, SocketAddr, SocketAddr, SocketAddr)>, epoch: EpochNumber) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, stake, transactions_address, mempool_address, confirmation_address)| {
                    let authority = Authority {
                        stake,
                        transactions_address,
                        mempool_address,
                        confirmation_address,
                    };
                    (name, authority)
                })
                .collect(),
            epoch,
        }
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake required to reach a quorum (f+1) for execution shard.
    pub fn quorum_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes / 2 + 1
    }

    /// Returns the address to receive client transactions.
    pub fn transactions_address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.transactions_address)
    }

    /// Returns the mempool addresses of a specific node.
    pub fn mempool_address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.mempool_address)
    }

    /// Returns the confirmation addresses of a specific node.
    pub fn confirmation_address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.confirmation_address)
    }

    /// Returns the mempool addresses of all nodes except `myself`.
    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, x)| (*name, x.mempool_address))
            .collect()
    }
}