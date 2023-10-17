use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use consensus::{Consensus, OBlock};
use crypto::SignatureService;
use log::{debug, info};
use mempool::Mempool;
use network::SimpleSender;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use types::{ConfirmMessage, ShardInfo};

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

        // channel for CrossTransactionVote between mempool and consensus
        let (tx_ctx_vote, rx_ctx_vote) = channel(CHANNEL_CAPACITY);

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
        info!(
            "Node chooses ordering shard address {:?}",
            shard_confirmation_addrs
        );

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
            tx_ctx_vote,
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
            rx_ctx_vote,
            tx_consensus_to_mempool,
            tx_commit,
        );

        info!("Node {} successfully booted", name);
        Ok(Self {
            commit: rx_commit,
            shard_info: committee.shard,
            shard_confirmation_addrs: shard_confirmation_addrs,
        })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        let mut sender = SimpleSender::new();
        while let Some(_block) = self.commit.recv().await {
            let mut confirm_msgs: HashMap<u32, ConfirmMessage> = HashMap::new();
            for i in _block.payload.clone() {
                // multiple blocks are packed for shard i.shard_id
                if confirm_msgs.contains_key(&i.shard_id) {
                    if let Some(cmsg) = confirm_msgs.get_mut(&i.shard_id) {
                        cmsg.block_hashes.insert(i.author, i.ebhash);
                        cmsg.ordered_ctxs.extend(i.ctx_hashes);
                    }
                } else {
                    let mut map_ebhash = HashMap::new();
                    map_ebhash.insert(i.author, i.ebhash);
                    let confim_msg = ConfirmMessage::new(
                        i.shard_id,
                        map_ebhash.clone(),
                        // i.round,    // corresponding execution shard's round
                        _block.round,
                        i.ctx_hashes.clone(),
                        _block.aggregators.clone(),
                        _block.signature.clone(),
                    )
                    .await;
                    confirm_msgs.insert(i.shard_id, confim_msg);
                }
            }
            // Send confirmation message to the specific execution shard
            for (shard, confim_msg) in &confirm_msgs {
                let message = bincode::serialize(&confim_msg.clone())
                    .expect("fail to serialize the ConfirmMessage");
                if let Some(_addr) = self.shard_confirmation_addrs.get(&shard).copied() {
                    sender.send(_addr, Into::into(message)).await;
                    debug!(
                        "send a confirm message {:?} to the execution shard {}",
                        confim_msg.clone(),
                        shard
                    );
                }
            }

            info!("Node commits block {:?} successfully", _block); // {:?} means: display based on the Debug function
        }
    }
}
