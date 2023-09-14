use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use consensus::{OBlock, Consensus};
use crypto::{Hash, SignatureService};
use log::{info, debug};
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use types::ConfirmMessage;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use network::SimpleSender;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

// Node is the replica in the ordering shard
pub struct Node {
    pub commit: Receiver<OBlock>,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
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

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        let mut sender = SimpleSender::new();
        // TODO replace confirm_addr
        let confirm_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10011);
        while let Some(_block) = self.commit.recv().await {
            // TODO: send this msg to each shard with a specific shard_id
            let confim_msg = ConfirmMessage::new(
                0,
                _block.digest(), 
                _block.round, 
                _block.payload.clone(), 
                _block.signature.clone()).await;

            let message = bincode::serialize(&confim_msg.clone())
                .expect("fail to serialize the ConfirmMessage");
            sender.send(confirm_addr, Into::into(message)).await;

            debug!("send a confirm message {:?} to the execution shard", confim_msg.clone());
            info!("Executor commits block {:?} successfully", _block); // {:?} means: display based on the Debug function
        }
    }
}
