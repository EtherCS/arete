use crypto::PublicKey;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

pub type Stake = u32;
pub type EpochNumber = u128;

#[derive(Serialize, Deserialize)]
pub struct CertifyParameters {
    pub certify_timeout_delay: u64,
    pub certify_sync_retry_delay: u64,
}

impl Default for CertifyParameters {
    fn default() -> Self {
        Self {
            certify_timeout_delay: 5_000,
            certify_sync_retry_delay: 10_000,
        }
    }
}

impl CertifyParameters {
    pub fn log(&self) {
        // NOTE: These log entries are used to compute performance.
        info!("Timeout delay set to {} rounds", self.certify_timeout_delay);
        info!("Sync retry delay set to {} ms", self.certify_sync_retry_delay);
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub stake: Stake,
    pub address: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExecutionCommittee {
    pub authorities: HashMap<PublicKey, Authority>,
    pub epoch: EpochNumber,
    pub liveness_threshold: f32,
}

impl ExecutionCommittee {
    pub fn new(info: Vec<(PublicKey, Stake, SocketAddr)>, epoch: EpochNumber, liveness_threshold: f32) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, stake, address)| {
                    let authority = Authority { stake, address };
                    (name, authority)
                })
                .collect(),
            epoch,
            liveness_threshold,
        }
    }

    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    // need (1-liveness_threshold)*total_stake
    pub fn quorum_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        // ((1.0-self.liveness_threshold).floor() as u32) * total_votes + 1
        ((1.0-self.liveness_threshold) * total_votes as f32).ceil() as u32
    }

    pub fn address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.address)
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, x)| (*name, x.address))
            .collect()
    }
}