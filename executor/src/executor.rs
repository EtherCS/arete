use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use certify::{EBlock, Consensus, CBlock};
use crypto::SignatureService;
use log::{debug, info, warn};
use execpool::Mempool;
use store::Store;
use tokio::net::TcpStream;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use anyhow::{Context, Result};
use futures::sink::SinkExt as _;

/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;

// Executor is the replica in the ordering shard
pub struct Executor {
    pub commit: Receiver<EBlock>,   // TODO: replace tx_commit with rx_processor
    pub ordering_addr: SocketAddr,
    pub shard_id: u32,
    // pub certificate_send: Framed<TcpStream, LengthDelimitedCodec>
}

impl Executor {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<String>,
        target: Option<SocketAddr>,
    ) -> Result<Self, ConfigError> {
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);

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
        let target_addr: SocketAddr = match target {
            Some(addr) => addr,
            None => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000),
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
            tx_mempool_to_consensus,
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            committee.consensus,
            parameters.consensus,
            signature_service,
            store,
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
        );

        // Connect to the ordering node.
        // let stream = TcpStream::connect(target_addr)
        //     .await
        //     .context(format!("failed to connect to {}", target_addr))?;
        // let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

        info!("Executor {} successfully booted", name);
        // info!("Executor connects nodes with address {}", target);
        Ok(Self { commit: rx_commit, ordering_addr: target_addr, shard_id: shard_id})
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) -> Result<()> {
        let stream = TcpStream::connect(self.ordering_addr)
            .await
            .context(format!("failed to connect to {}", self.ordering_addr))?;
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        while let Some(_block) = self.commit.recv().await {
            // TODO1: reliable sender
            // TODO2: send a certificate block
            let certify_block = CBlock::new(
                self.shard_id, 
                _block.author, 
                _block.round, 
                _block.payload.clone(), 
                _block.signature.clone()).await;
            let message = bincode::serialize(&certify_block.clone())
                .expect("fail to serialize the CBlock");
            if let Err(e) = transport.send(Into::into(message)).await {
                warn!("Failed to send block: {}", e);
                break;
            }
            debug!("send a certificate block {:?} to the ordering shard", certify_block.clone());
            info!("Executor commits block {:?} successfully", _block); // {:?} means: display based on the Debug function
        }
        Ok(())
    }
}
