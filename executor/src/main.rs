mod config;
mod executor;

use crate::config::Export as _;
use crate::config::{Committee, Secret};
use crate::executor::Executor;
use clap::{Parser, Subcommand};
use certify::ExecutionCommittee as ConsensusCommittee;
use env_logger::Env;
use futures::future::join_all;
use log::error;
use execpool::ExecutionCommittee as MempoolCommittee;
use std::collections::HashMap;
use std::fs;
use tokio::task::JoinHandle;
use types::ShardInfo;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Turn debugging information on.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// The command to execute.
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a new keypair.
    Keys {
        /// The file where to print the new key pair.
        #[clap(short, long, value_parser, value_name = "FILE")]
        filename: String,
    },
    /// Run a single executor.
    Run {
        /// The file containing the executor keys.
        #[clap(short, long, value_parser, value_name = "FILE")]
        keys: String,
        /// The file containing committee information.
        #[clap(short, long, value_parser, value_name = "FILE")]
        committee: String,
        /// Optional file containing the executor parameters.
        #[clap(short, long, value_parser, value_name = "FILE")]
        parameters: Option<String>,
        /// The path where to create the data store.
        #[clap(short, long, value_parser, value_name = "PATH")]
        store: String,
    },
    /// Deploy a local testbed with the specified number of executors.
    Deploy {
        #[clap(short, long, value_parser = clap::value_parser!(u16).range(4..))]
        executors: u16,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = match cli.verbose {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match cli.command {
        Command::Keys { filename } => {
            if let Err(e) = Executor::print_key_file(&filename) {
                error!("{}", e);
            }
        }
        Command::Run {
            keys,
            committee,
            parameters,
            store,
        } => match Executor::new(&committee, &keys, &store, parameters).await {
            Ok(mut executor) => {
                tokio::spawn(async move {
                    let _ = executor.send_certificate_message().await;
                })
                .await
                .expect("Failed to send certificate message to the ordering shard");
            }
            Err(e) => error!("{}", e),
        },
        Command::Deploy { executors } => match deploy_testbed(executors) {
            Ok(handles) => {
                let _ = join_all(handles).await;
            }
            Err(e) => error!("Failed to deploy testbed: {}", e),
        },
    }
}

fn deploy_testbed(executors: u16) -> Result<Vec<JoinHandle<()>>, Box<dyn std::error::Error>> {
    let keys: Vec<_> = (0..executors).map(|_| Secret::new()).collect();

    // Print the committee file.
    let epoch = 1;
    let mempool_committee = MempoolCommittee::new(
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let name = key.name;
                let stake = 1;
                let front = format!("127.0.0.1:{}", 25_000 + i).parse().unwrap();
                let mempool = format!("127.0.0.1:{}", 25_100 + i).parse().unwrap();
                let confirmation = format!("127.0.0.1:{}", 25_100 + i).parse().unwrap();
                (name, stake, front, mempool, confirmation)
            })
            .collect(),
        epoch,
        0.3,
    );
    let consensus_committee = ConsensusCommittee::new(
        keys.iter()
            .enumerate()
            .map(|(i, key)| {
                let name = key.name;
                let stake = 1;
                let addresses = format!("127.0.0.1:{}", 25_200 + i).parse().unwrap();
                (name, stake, addresses)
            })
            .collect(),
        epoch,
        0.3,
    );

    let committee_file = "committee.json";
    // let committee_file = format!("shard-{}_committee.json", shardId);
    let _ = fs::remove_file(committee_file);
    let shard_info = ShardInfo::default();
    Committee {
        mempool: mempool_committee,
        consensus: consensus_committee,
        shard: shard_info,
        order_transaction_addresses: HashMap::new(),
    }
    .write(committee_file)?;

    // Write the key files and spawn all executors.
    keys.iter()
        .enumerate()
        .map(|(i, keypair)| {
            let key_file = format!("executor_{}.json", i);
            let _ = fs::remove_file(&key_file);
            keypair.write(&key_file)?;

            let store_path = format!("db_{}", i);
            let _ = fs::remove_dir_all(&store_path);

            Ok(tokio::spawn(async move {
                match Executor::new(committee_file, &key_file, &store_path, None).await {
                    Ok(_) => {
                        // Sink the commit channel.
                        // while executor.commit.recv().await.is_some() {}
                    }
                    Err(e) => error!("{}", e),
                }
            }))
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()
}
