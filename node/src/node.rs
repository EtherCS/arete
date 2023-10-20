use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use consensus::{Consensus, OBlock};
use crypto::{Signature, SignatureService};
use log::{debug, error, info};
use mempool::Mempool;
use network::SimpleSender;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use types::{BlockCreator, ConfirmMessage, ShardInfo};

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;
pub const CONNECTIONS_NODES: usize = 3;

// Node is the replica in the ordering shard
pub struct Node {
    pub commit: Receiver<OBlock>,
    pub shard_info: ShardInfo,
    pub shard_confirmation_addrs: HashMap<u32, Vec<SocketAddr>>,
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
        let mut shard_confirmation_addrs: HashMap<u32, Vec<SocketAddr>> = HashMap::new();
        for (shard_id, map_addrs) in committee.executor_confirmation_addresses {
            let mut sample_addresses = Vec::new();
            let addrs: Vec<_> = map_addrs.values().cloned().collect();
            if addrs.len() >= CONNECTIONS_NODES {
                let random_addrs = addrs
                    .iter()
                    .clone()
                    .choose_multiple(&mut rand::thread_rng(), CONNECTIONS_NODES);
                sample_addresses.extend(random_addrs.clone());
                shard_confirmation_addrs.insert(shard_id, sample_addresses);
            } else {
                error!(
                    "The execution shard should has at least {} nodes",
                    CONNECTIONS_NODES
                );
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
            let heartbeat_sig: Signature = _block.signature.clone();
            let heartbeat_round: u64 = _block.round;
            for i in _block.payload.clone() {
                // multiple blocks are packed for shard i.shard_id
                if confirm_msgs.contains_key(&i.shard_id) {
                    if let Some(cmsg) = confirm_msgs.get_mut(&i.shard_id) {
                        let temp_block_creator = BlockCreator::new(i.author, i.ebhash).await;
                        cmsg.block_hashes.push(temp_block_creator);
                        cmsg.ordered_ctxs.extend(i.ctx_hashes);
                    }
                } else {
                    debug!(
                        "ARETE trace: oblock for shard {} in round {} get vote results {}",
                        i.shard_id,
                        _block.round,
                        _block.aggregators.clone().len()
                    );
                    let temp_block_creator = BlockCreator::new(i.author, i.ebhash).await;
                    let mut map_ebhash = Vec::new();
                    map_ebhash.push(temp_block_creator);
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
                if let Some(_addrs) = self.shard_confirmation_addrs.get(&shard) {
                    sender.broadcast(_addrs.clone(), Into::into(message)).await;
                    debug!(
                        "send a confirm message {:?} to the execution shard {}",
                        confim_msg.clone(),
                        shard
                    );
                }
            }
            // For cross-shard transaction test.
            // Currently, we dont ask the ordering shard to maintain a table
            // for tracing relevant shards who are responsible for voting an ordering round
            for heartbeat_shard in 0..2 {
                let heartbeat_confirm_msg = ConfirmMessage::new(
                    3,
                    Vec::new(),
                    heartbeat_round,
                    Vec::new(),
                    Vec::new(),
                    heartbeat_sig.clone(),
                )
                .await;
                let message = bincode::serialize(&heartbeat_confirm_msg.clone())
                    .expect("fail to serialize the ConfirmMessage");
                if let Some(_addrs) = self.shard_confirmation_addrs.get(&heartbeat_shard) {
                    sender.broadcast(_addrs.clone(), Into::into(message)).await;
                    debug!(
                        "send a heartbeat confirm message {:?} to the execution shard {}",
                        heartbeat_confirm_msg.clone(),
                        heartbeat_shard
                    );
                }
            }
            info!("Node commits block {:?} successfully", _block); // {:?} means: display based on the Debug function
        }
    }
}
