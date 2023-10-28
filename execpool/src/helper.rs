use crate::{config::ExecutionCommittee, mempool::MempoolMessage};
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;
use types::EBlock;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The committee information.
    committee: ExecutionCommittee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Digest, PublicKey)>,
    /// A network sender to send the batches to the other mempools.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        committee: ExecutionCommittee,
        store: Store,
        rx_request: Receiver<(Digest, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_request,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digest, origin)) = self.rx_request.recv().await {
            // get the requestors address.
            let address = match self.committee.mempool_address(&origin) {
                Some(x) => x,
                None => {
                    warn!("Received batch request from unknown authority: {}", origin);
                    continue;
                }
            };

            // Reply to the request (the best we can).
            match self.store.read(digest.to_vec()).await {
                Ok(Some(data)) => {
                    let eblock: EBlock =
                        bincode::deserialize(&data).expect("failed to deseriablize eblock");
                    let message = MempoolMessage::SyncEBlock(eblock.clone());
                    let serialized = bincode::serialize(&message)
                        .expect("Failed to serialize our own MempoolMessage EBlock");
                    self.network.send(address, Bytes::from(serialized)).await;
                }
                Ok(None) => (),
                Err(e) => error!("{}", e),
            }
        }
    }
}
