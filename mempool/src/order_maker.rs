use crate::batch_maker::Transaction;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use consensus::OBlock;

pub struct OrderMaker {
    /// Shard number.
    shard_num: u32,
    /// Map execution_shard_id -> shard round.
    execution_shard_round: HashMap<u32, u64>,
    /// Map execution_shard_id -> if receive its certificate block in current ordering round.
    execution_shard_ready: HashMap<u32, bool>,
    /// Ordering round.
    ordering_round: u64,
    /// If communicator, broadcast.
    is_communicator: bool,
    /// Channel to receive transactions from clients.
    rx_transaction: Sender<Transaction>,
    /// Channel to 
}

impl OrderMaker {
    pub fn spawn (
        shard_num: u32,
        is_communicator: bool,
        tx_transaction: Sender<Transaction>,
    ) {
        tokio::spawn(async move {
            Self {
                shard_num,
                execution_shard_round: HashMap::new(),
                execution_shard_ready: HashMap::new(),
                ordering_round: 0,
                is_communicator,
                tx_transaction,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {

    }
}
