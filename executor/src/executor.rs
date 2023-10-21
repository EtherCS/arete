use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use certify::Consensus;
use crypto::SignatureService;
use log::info;
use execpool::Mempool;
use rand::seq::IteratorRandom;
use store::Store;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use tokio::sync::mpsc::{channel, Receiver};
use anyhow::Result;
use network::SimpleSender;
use types::CertifyMessage;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

// Executor is the replica in the ordering shard
pub struct Executor {
    pub certify: Receiver<CertifyMessage>,  
    pub ordering_addr: SocketAddr,
    pub shard_id: u32,
}

impl Executor {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
    ) -> Result<Self, ConfigError> {
        let (tx_certify, rx_certify) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_confirm_mempool_to_consensus, rx_confirm_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;
        let shard_id = committee.shard.id;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(&filename)?,
            None => Parameters::default(),
        };

        // Get the connected ordering node address.
        // Randomly pick one
        let mut target_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000);
        if let Some(name_addr) = committee.order_transaction_addresses.iter().choose(&mut rand::thread_rng()) {
            let (_name, _target_addr) = name_addr;
            target_addr = *_target_addr;
        }
        info!("Executor chooses ordering shard address {}", target_addr);

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create store");

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Make a new mempool.
        Mempool::spawn(
            name,
            committee.shard.clone(),
            signature_service.clone(),
            committee.mempool,
            parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
            tx_confirm_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            committee.shard.clone(),
            signature_service.clone(),
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_certify,
            rx_confirm_mempool_to_consensus,
        );

        info!("Executor {} successfully booted", name);
        // info!("Executor connects nodes with address {}", target);
        Ok(Self { certify: rx_certify, ordering_addr: target_addr, shard_id: shard_id})
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn send_certificate_message(&mut self) -> Result<()> {
        let mut sender = SimpleSender::new();
        while let Some(_cmsg) = self.certify.recv().await {
            let message = bincode::serialize(&_cmsg.clone())
                .expect("fail to serialize the CBlock");
            sender.send(self.ordering_addr, Into::into(message)).await;

            // debug!("Executor send a certificate message {:?} to the ordering shard", _cmsg.clone());
            // info!("Executor commits block {:?} successfully", _cmsg); // {:?} means: display based on the Debug function
        }
        Ok(())
    }
}
