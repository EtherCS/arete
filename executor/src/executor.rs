use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use certify::{EBlock, Consensus};
use crypto::SignatureService;
use log::{debug, info};
use execpool::Mempool;
use store::Store;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use tokio::sync::mpsc::{channel, Receiver};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

// Executor is the replica in the ordering shard
pub struct Executor {
    pub commit: Receiver<EBlock>,
    pub ordering_addr: SocketAddr,
}

impl Executor {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
        target: Option<SocketAddr>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Get the connected ordering node address.
        let target_addr: SocketAddr = match target {
            Some(addr) => addr,
            None => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000),
        };

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Make a new mempool.
        Mempool::spawn(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            signature_service,
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Executor {} successfully booted", name);
        // info!("Executor connects nodes with address {}", target);
        Ok(Self { commit: rx_commit, ordering_addr: target_addr })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // debug!("Ordering node addr is {}", self.ordering_addr);
            // This is where we can further process committed block.
            // Jianting: we send certificate block to the ordering shard here
            debug!("Executor commits block {:?} successfully", _block); // {:?} means: display based on the Debug function
        }
    }
}
