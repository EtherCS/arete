use bytes::Bytes;
use crypto::{Digest, PublicKey};
use types::ConfirmMessage;

pub struct ConfirmExecutor {
    name: PublicKey,
    committee: ExecutionCommittee,
    signature_service: SignatureService,
    rx_mempool: Receiver<Digest>,
    rx_confirm_message: Receiver<ConfirmMessage>,
    network: ReliableSender,
}

impl ConfirmExecutor {
    pub fn spawn(
        name: PublicKey,
        committee: ExecutionCommittee,
        signature_service: SignatureService,
        rx_confirm_message: Receiver<ConfirmMessage>,   // receive from mempool
        network: ReliableSender,    // broadcast ConfirmMessage as ConsensusMessage
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_mempool,
                rx_confirm_message,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }
}