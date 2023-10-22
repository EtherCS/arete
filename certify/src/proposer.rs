use tokio::sync::mpsc::{Receiver, Sender};
use types::{CBlock, CertifyMessage};
/// Proposer module: forward a certified CBlock to the ordering shard
pub struct Proposer {
    rx_mempool: Receiver<CBlock>, // receive certificate block from execpool
    tx_certify: Sender<CertifyMessage>, // send the received certificate block to executor.analyze_block()
}

impl Proposer {
    pub fn spawn(rx_mempool: Receiver<CBlock>, tx_certify: Sender<CertifyMessage>) {
        tokio::spawn(async move {
            Self {
                rx_mempool,
                tx_certify,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(cblock) = self.rx_mempool.recv() => {
                    self.tx_certify.send(CertifyMessage::CBlock(cblock.clone())).await.expect("Failed send cblock");
                },
            }
        }
    }
}
