// use crate::consensus::{Round, CHANNEL_CAPACITY};
use crate::consensus::CHANNEL_CAPACITY;
// use crate::error::{ConsensusError, ConsensusResult};
use crate::error::ConsensusResult;
// use crate::messages::EBlock;
use crypto::Digest;
use crypto::Hash as _;
// use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::error;
use execpool::ExecutionMempoolMessage;
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use types::{EBlock, ConfirmMessage};

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
        for (author, hash) in &confirm_msg.block_hashes {
            if self.store.read(hash.to_vec()).await?.is_none() {
                // missing this eblock
                let message = ExecutionMempoolMessage::Synchronize(hash.clone(), author.clone());
                self.tx_mempool
                    .send(message)
                    .await
                    .expect("Failed to send sync message");

                self.tx_payload_waiter
                    .send(PayloadWaiterMessage::Wait(hash.clone()))
                    .await
                    .expect("Failed to send message to payload waiter");
            }
        }


        // let mut missing = Vec::new();
        // for (author, hash) in &confirm_msg.block_hashes {
        //     if self.store.read(x.to_vec()).await?.is_none() {
        //         missing.push(x.clone());
        //     }
        // }

        // if missing.is_empty() {
        //     return Ok(true);
        // }

        // let message = ExecutionMempoolMessage::Synchronize(missing.clone(), block.author);
        // self.tx_mempool
        //     .send(message)
        //     .await
        //     .expect("Failed to send sync message");

        // self.tx_payload_waiter
        //     .send(PayloadWaiterMessage::Wait(missing, Box::new(block)))
        //     .await
        //     .expect("Failed to send message to payload waiter");

        Ok(false)
    }

    // pub async fn cleanup(&mut self, round: Round) {
    //     // Cleanup the mempool.
    //     self.tx_mempool
    //         .send(ExecutionMempoolMessage::Cleanup(round))
    //         .await
    //         .expect("Failed to send cleanup message");

    //     // Cleanup the payload waiter.
    //     // self.tx_payload_waiter
    //     //     .send(PayloadWaiterMessage::Cleanup(round))
    //     //     .await
    //     //     .expect("Failed to send cleanup message");
    // }
}

#[derive(Debug)]
enum PayloadWaiterMessage {
    Wait(Digest),
    // Cleanup(Round),
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
            // result = try_join_all(waiting) => {
            //     result.map(|_| Some(deliver)).map_err(ConsensusError::from)
            // }
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
                        // = missing.iter().cloned().map(|x| (x, store_copy.clone())).collect();
                        let fut = Self::waiter(wait_for, rx_cancel);
                        waiting.push(fut);
                    },
                    // PayloadWaiterMessage::Cleanup(mut round) => {
                    //     for (r, handler) in pending.values() {
                    //         if r <= &round {
                    //             let _ = handler.send(()).await;
                    //         }
                    //     }
                    //     pending.retain(|_, (r, _)| r > &mut round);
                    // }
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
