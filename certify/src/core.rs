use crate::config::ExecutionCommittee;
use crate::consensus::{ConsensusMessage, Round};
use crate::error::{ConsensusError, ConsensusResult};
use crate::mempool::MempoolDriver;
// use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::{debug, error, info, warn};
use network::SimpleSender;
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use types::{CertifyMessage, ConfirmMessage, CrossTransactionVote, EBlock, ShardInfo, Transaction};

pub struct Core {
    name: PublicKey,
    committee: ExecutionCommittee,
    shard_info: ShardInfo,
    store: Store,
    signature_service: SignatureService,
    // leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    // synchronizer: Synchronizer,
    rx_message: Receiver<ConsensusMessage>,
    rx_loopback: Receiver<EBlock>,
    tx_order_ctx: Sender<CrossTransactionVote>, // send executed vote results to vote_maker
    tx_certify: Sender<CertifyMessage>,
    round: Round,
    network: SimpleSender,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: ExecutionCommittee,
        shard_info: ShardInfo,
        signature_service: SignatureService,
        store: Store,
        mempool_driver: MempoolDriver,
        // synchronizer: Synchronizer,
        rx_message: Receiver<ConsensusMessage>,
        rx_loopback: Receiver<EBlock>,
        tx_order_ctx: Sender<CrossTransactionVote>, // send cross-shard transactions to vote_maker after execution
        tx_certify: Sender<CertifyMessage>,         // send certify vote message
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                shard_info: shard_info.clone(),
                signature_service,
                store,
                mempool_driver,
                // synchronizer,
                rx_message,
                rx_loopback,
                tx_order_ctx,
                tx_certify,
                round: 1,
                network: SimpleSender::new(),
            }
            .run()
            .await
        });
    }

    async fn store_block(&mut self, block: &EBlock) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn commit(&mut self, block: EBlock) -> ConsensusResult<()> {
        // Ensure we commit the entire chain. This is needed after view-change.
        let mut to_commit = VecDeque::new();
        to_commit.push_front(block.clone());

        // Save the last committed block.
        // self.last_committed_round = block.round;

        // Get shard id
        let _s_id = self.shard_info.id;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            // if !block.payload.is_empty() {
            info!("Committed {}", block);

            #[cfg(feature = "benchmark")]
            {
                let b_round = block.round;
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Shard {} Committed {} -> {:?}", _s_id, block, x);
                    info!(
                        "ARETE shard {} Committed {} -> {:?} in round {}",
                        _s_id, block, x, b_round
                    );
                }
            }
            // }
            debug!("Committed {:?}", block);
            // if let Err(e) = self.tx_commit.send(block).await {
            //     warn!("Failed to send block through the commit channel: {}", e);
            // }
        }
        Ok(())
    }

    async fn generate_vote(&mut self, crosstxs: Vec<Transaction>) -> HashMap<Digest, u8> {
        let mut vote = HashMap::new();
        for ctx in crosstxs {
            // ARETE TODO: execute ctx;
            // Assume all are successful now
            vote.insert(
                Digest(Sha512::digest(&ctx).as_slice()[..32].try_into().unwrap()),
                1,
            );
        }
        vote
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &EBlock) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        self.commit(block.clone()).await?;

        Ok(())
    }

    #[async_recursion]
    async fn process_confirm(&mut self, confirm_msg: ConfirmMessage,) -> ConsensusResult<()> {
        for (_, eblock_hash) in confirm_msg.block_hashes.clone() {
            match self.store.read(eblock_hash.to_vec()).await? {
                Some(bytes) => {
                    let block: EBlock = bincode::deserialize(&bytes).expect("Failed to deserialize EBlock");
                    self.store_block(&block).await;
                    self.commit(block.clone()).await?;
                },
                None => {
                    // if let Err(e) = self.inner_channel.send(block.clone()).await {
                    //     panic!("Failed to send request to synchronizer: {}", e);
                    // }
                    // Ok(None)
                }
            }
        }
        Ok(())
    }

    async fn handle_confirmation_message(
        &mut self,
        confirm_msg: ConfirmMessage,
    ) -> ConsensusResult<()> {
        // ARETE TODO: execute transactions, create vote for ctxs
        #[cfg(feature = "benchmark")]
        // {
        info!(
            "ARETE shard {} commit anchor block for execution round {}",
            confirm_msg.shard_id, confirm_msg.round
        );
        // }
        debug!("receive a confirm message from peer {:?}", confirm_msg);

        let digest = confirm_msg.digest();
        if !self.mempool_driver.verify(confirm_msg.clone()).await? {
            debug!("Processing of {} suspended: missing some EBlock", digest);
            return Ok(());
        }
        // Otherwise, have all EBlocks, and execute
        self.process_confirm(confirm_msg).await
    }

    pub async fn run(&mut self) {
        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMessage::ExecutionBlock(block) => self.process_block(&block).await,   
                    ConsensusMessage::ConfirmMsg(confirm_message) => self.handle_confirmation_message(confirm_message).await,
                    _ => panic!("Unexpected protocol message")
                },
                // Get the block after synchronization
                // Now commit it
                Some(block) = self.rx_loopback.recv() => self.process_block(&block).await,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
