// use crate::config::ExecutionCommittee;
use crate::consensus::{ConsensusMessage, Round};
use crate::error::{ConsensusError, ConsensusResult};
use crate::mempool::MempoolDriver;
use async_recursion::async_recursion;
use crypto::Digest;
use crypto::Hash as _;
// use crypto::{Digest, PublicKey};
#[cfg(feature = "benchmark")]
use log::info;
use log::{debug, error, warn};
// use network::SimpleSender;
use std::collections::{HashMap, VecDeque};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use types::{ConfirmMessage, CrossTransactionVote, EBlock, ShardInfo};

pub struct Core {
    // name: PublicKey,
    // committee: ExecutionCommittee,
    shard_info: ShardInfo,
    store: Store,
    // signature_service: SignatureService,
    // leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    // synchronizer: Synchronizer,
    rx_message: Receiver<ConsensusMessage>,
    rx_loopback: Receiver<EBlock>,
    tx_order_ctx: Sender<CrossTransactionVote>, // send executed vote results to vote_maker
    // tx_certify: Sender<CertifyMessage>,
    round: Round,
    // network: SimpleSender,
    commit_round: HashMap<u64, u8>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        // name: PublicKey,
        // committee: ExecutionCommittee,
        shard_info: ShardInfo,
        // signature_service: SignatureService,
        store: Store,
        mempool_driver: MempoolDriver,
        // synchronizer: Synchronizer,
        rx_message: Receiver<ConsensusMessage>,
        rx_loopback: Receiver<EBlock>,
        tx_order_ctx: Sender<CrossTransactionVote>, // send cross-shard transactions to vote_maker after execution
                                                    // tx_certify: Sender<CertifyMessage>,         // send certify vote message
    ) {
        tokio::spawn(async move {
            Self {
                // name,
                // committee: committee.clone(),
                shard_info: shard_info.clone(),
                // signature_service,
                store,
                mempool_driver,
                // synchronizer,
                rx_message,
                rx_loopback,
                tx_order_ctx,
                // tx_certify,
                round: 1,
                // network: SimpleSender::new(),
                commit_round: HashMap::new(),
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

    // async fn commit(&mut self, block: EBlock) -> ConsensusResult<()> {
    //     // Ensure we commit the entire chain. This is needed after view-change.
    //     let mut to_commit = VecDeque::new();
    //     to_commit.push_front(block.clone());
    //     Ok(())
    // }

    async fn generate_vote(&mut self, crosstxs: Vec<Digest>) -> HashMap<Digest, u8> {
        let mut vote = HashMap::new();
        for ctx in crosstxs {
            // ARETE TODO: execute ctx;
            // Assume all are successful now
            vote.insert(ctx, 1);
        }
        vote
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &EBlock) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        // self.commit(block.clone()).await?;

        Ok(())
    }

    // #[async_recursion]
    async fn process_confirm(&mut self, confirm_msg: ConfirmMessage) -> ConsensusResult<()> {
        let mut _to_commit: VecDeque<Digest> = VecDeque::new();
        // First handle the ordered intra-shard transactions
        for block_creator in confirm_msg.block_hashes.clone() {
            match self.store.read(block_creator.ebhash.to_vec()).await? {
                Some(bytes) => {
                    #[cfg(feature = "benchmark")]
                    _to_commit.push_front(block_creator.ebhash);
                    let block: EBlock =
                        bincode::deserialize(&bytes).expect("Failed to deserialize EBlock");
                    self.store_block(&block).await;
                    // self.commit(block.clone()).await?;
                }
                None => {
                    // if let Err(e) = self.inner_channel.send(block.clone()).await {
                    //     panic!("Failed to send request to synchronizer: {}", e);
                    // }
                    // Ok(None)
                }
            }
        }
        // Then, handle the ordered cross-shard transactions
        let votes = self.generate_vote(confirm_msg.ordered_ctxs).await;
        let cross_tx_vote: CrossTransactionVote = CrossTransactionVote::new(
            self.shard_info.id,
            confirm_msg.order_round,
            votes.clone(),
            Vec::new(),
        )
        .await;
        // Send it to vote_maker for collecting quorum of certificates
        self.tx_order_ctx
            .send(cross_tx_vote)
            .await
            .expect("Failed to send cross-transaction vote");

        self.round = confirm_msg.order_round;
        self.commit_round.insert(confirm_msg.order_round, 1);
        // Get shard id
        // let _s_id = self.shard_info.id;
        // Print for performance calculation

        #[cfg(feature = "benchmark")]
        {
            while let Some(block_digest) = _to_commit.pop_back() {
                info!(
                    "Shard {} Committed EBlock in round {} -> {:?}",
                    self.shard_info.id, confirm_msg.order_round, block_digest
                );
                info!(
                    "ARETE shard {} Committed -> {:?} in round {}",
                    self.shard_info.id, block_digest, confirm_msg.order_round
                );
            }
        }

        Ok(())
    }

    async fn handle_confirmation_message(
        &mut self,
        confirm_msg: ConfirmMessage,
    ) -> ConsensusResult<()> {
        // ARETE TODO: commit voted cross-shard transactions
        let confirm_digest = confirm_msg.clone().digest();
        if !self.store.read(confirm_digest.to_vec()).await?.is_none() {
            // we have already handle this round message
            debug!(
                "ARETE trace: we already handle this round message {}",
                confirm_msg
            );
            return Ok(());
        }

        let value =
            bincode::serialize(&confirm_msg.clone()).expect("Failed to serialize confirm message");
        self.store.write(confirm_digest.to_vec(), value).await;

        #[cfg(feature = "benchmark")]
        {
            if !confirm_msg.clone().votes.is_empty() {
                // Ensure this round has been committed
                for vote_result in confirm_msg.clone().votes {
                    if self.commit_round.contains_key(&vote_result.round) {
                        info!(
                            "Shard {} Committed Vote in round {}",
                            self.shard_info.id, vote_result.round,
                        );
                    }
                    
                }
            }
        }
        if confirm_msg.shard_id != self.shard_info.id {
            // Respond this heartbeat confirmation message with vote
            debug!(
                "ARETE trace: receive a heartbeat message in round {}",
                confirm_msg.order_round
            );
            let cross_tx_vote: CrossTransactionVote = CrossTransactionVote::new(
                self.shard_info.id,
                confirm_msg.order_round,
                HashMap::new(),
                Vec::new(),
            )
            .await;
            // Send it to vote_maker for collecting quorum of certificates
            self.tx_order_ctx
                .send(cross_tx_vote)
                .await
                .expect("Failed to send cross-transaction vote");
            return Ok(());
        }

        let digest = confirm_msg.digest();
        if !self.mempool_driver.verify(confirm_msg.clone()).await? {
            debug!("Processing of {} suspended: missing some EBlock", digest);
            let votes = self.generate_vote(confirm_msg.ordered_ctxs).await;
            let cross_tx_vote: CrossTransactionVote = CrossTransactionVote::new(
                self.shard_info.id,
                confirm_msg.order_round,
                votes.clone(),
                Vec::new(),
            )
            .await;
            // Send it to vote_maker for collecting quorum of certificates
            self.tx_order_ctx
                .send(cross_tx_vote)
                .await
                .expect("Failed to send cross-transaction vote");
            // self.round = confirm_msg.order_round;
            self.commit_round.insert(confirm_msg.order_round, 1);
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
