use crate::consensus::CHANNEL_CAPACITY;
use crate::error::ConsensusResult;
use crypto::Digest;
use crypto::Hash as _;
use execpool::ExecutionMempoolMessage;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::error;
#[cfg(feature = "benchmark")]
use log::info;
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use types::{ConfirmMessage, EBlock};

pub struct MempoolDriver {
    store: Store,
    tx_mempool: Sender<ExecutionMempoolMessage>,
    tx_payload_waiter: Sender<PayloadWaiterMessage>,
}

impl MempoolDriver {
    pub fn new(
        store: Store,
        tx_mempool: Sender<ExecutionMempoolMessage>,
        tx_loopback: Sender<EBlock>,
    ) -> Self {
        let (tx_payload_waiter, rx_payload_waiter) = channel(CHANNEL_CAPACITY);

        // Spawn the payload waiter.
        PayloadWaiter::spawn(store.clone(), rx_payload_waiter, tx_loopback);

        // Returns the mempool driver.
        Self {
            store,
            tx_mempool,
            tx_payload_waiter,
        }
    }

    pub async fn verify(&mut self, confirm_msg: ConfirmMessage) -> ConsensusResult<bool> {
        let mut missing: bool = false;
        for block_creator in &confirm_msg.block_hashes {
            if self
                .store
                .read(block_creator.ebhash.to_vec())
                .await?
                .is_none()
            {
                missing = true;
                // missing this eblock
                let message = ExecutionMempoolMessage::Synchronize(
                    block_creator.ebhash.clone(),
                    block_creator.author.clone(),
                );
                self.tx_mempool
                    .send(message)
                    .await
                    .expect("Failed to send sync message");

                self.tx_payload_waiter
                    .send(PayloadWaiterMessage::Wait(block_creator.ebhash.clone()))
                    .await
                    .expect("Failed to send message to payload waiter");
            } else {
                #[cfg(feature = "benchmark")]
                {
                    // while let Some(block_digest) = _to_commit.pop_back() {
                    info!(
                        "Shard {} Committed EBlock in round {} -> {:?}",
                        confirm_msg.shard_id, confirm_msg.order_round, block_creator.ebhash,
                    );
                    // }
                }
            }
        }
        if missing {
            return Ok(false);
        } else {
            return Ok(true);
        }
    }
}

#[derive(Debug)]
enum PayloadWaiterMessage {
    Wait(Digest),
}

struct PayloadWaiter {
    store: Store,
    rx_message: Receiver<PayloadWaiterMessage>,
    tx_loopback: Sender<EBlock>,
}

impl PayloadWaiter {
    pub fn spawn(
        store: Store,
        rx_message: Receiver<PayloadWaiterMessage>,
        tx_loopback: Sender<EBlock>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                rx_message,
                tx_loopback,
            }
            .run()
            .await;
        });
    }

    async fn waiter(
        mut missing: Vec<(Digest, Store)>,
        // deliver: EBlock,
        mut handler: Receiver<()>,
    ) -> ConsensusResult<Option<Box<EBlock>>> {
        let _: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        // check if our store has the eblock
        tokio::select! {
            _ = handler.recv() => Ok(None),
        }
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();
        let mut pending = HashMap::new();

        let store_copy = self.store.clone();
        loop {
            tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    PayloadWaiterMessage::Wait(missing) => {
                        let block_digest = missing.clone();

                        if pending.contains_key(&block_digest) {
                            continue;
                        }

                        let (tx_cancel, rx_cancel) = channel(1);
                        pending.insert(block_digest, (1, tx_cancel));
                        let wait_for = vec![(missing.clone(), store_copy.clone())];
                        let fut = Self::waiter(wait_for, rx_cancel);
                        waiting.push(fut);
                    },
                },
                Some(result) = waiting.next() => {
                    match result {
                        Ok(Some(block)) => {
                            let _ = pending.remove(&block.digest());
                            self.tx_loopback.send(*block).await.expect("Failed to send consensus message");
                        },
                        Ok(None) => (),
                        Err(e) => error!("{}", e)
                    }
                }
            }
        }
    }
}
