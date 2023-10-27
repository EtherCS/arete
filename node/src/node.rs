use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use consensus::{OBlock, Consensus};
use crypto::SignatureService;
use log::{info, debug};
use mempool::Mempool;
use rand::seq::IteratorRandom;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use types::{ConfirmMessage, ShardInfo};
use std::collections::HashMap;
use std::net::SocketAddr;
use network::ReliableSender;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

// Node is the replica in the ordering shard
pub struct Node {
    pub commit: Receiver<OBlock>,
    pub shard_info: ShardInfo,
    pub shard_confirmation_addrs: HashMap<u32, SocketAddr>,
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

        // Pick one comfirmation address for each execution shard
        let mut shard_confirmation_addrs = HashMap::new();
        for (shard_id, map_addrs) in committee.executor_confirmation_addresses {
            if let Some(name_addr) = map_addrs.iter().choose(&mut rand::thread_rng()) {
                let (_name, _confirm_addr) = name_addr;
                shard_confirmation_addrs.insert(shard_id, *_confirm_addr);
            }
        }
        info!("Node chooses ordering shard address {:?}", shard_confirmation_addrs);

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
            committee.shard.clone(),
            signature_service,
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit, shard_info: committee.shard, shard_confirmation_addrs: shard_confirmation_addrs })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        let mut sender = ReliableSender::new();
        while let Some(_block) = self.commit.recv().await {
            for i in _block.payload.clone() {
                let confim_msg = ConfirmMessage::new(
                    i.shard_id,
                    i.hash, 
                    i.round,    // corresponding execution shard's round
                    _block.round, 
                    _block.get_digests(), 
                    _block.signature.clone()).await;
    
                let message = bincode::serialize(&confim_msg.clone())
                    .expect("fail to serialize the ConfirmMessage");
                if let Some(_addr) = self.shard_confirmation_addrs.get(&i.shard_id).copied() {
                    sender.send(_addr, Into::into(message)).await;
                    debug!("send a confirm message {:?} to the execution shard {}", confim_msg.clone(), i.shard_id);
                }
            }
            
            info!("Node commits block {:?} successfully", _block); // {:?} means: display based on the Debug function
        }
    }
}
