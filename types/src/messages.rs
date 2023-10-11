use crate::config::ExecutionCommittee;
use crate::ensure;
use crate::error::{CertifyError, CertifyResult};
use crypto::{Digest, Hash, Signature, PublicKey, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;
use std::collections::HashMap;



pub type Round = u64;
pub type Transaction = Vec<u8>;

// Execution shards receive the ordered txs from the ordering shard
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ConfirmMessage {
    pub shard_id: u32,  
    pub block_hash: Digest, // Corresponding CBlock's hash
    pub round: u64,     // corresponding execution shard's round 
    pub order_round: u64,    // consensus round in the ordering shard
    pub payload: Vec<Digest>,   // new cross-shard transactions
    pub signature: Signature,
}

impl ConfirmMessage {
    pub async fn new(
        shard_id: u32,
        block_hash: Digest,
        round: u64,
        order_round:u64,
        payload: Vec<Digest>,
        signature: Signature,
    ) -> Self {
        let confirm_message = Self {
            shard_id,
            block_hash,
            round,
            order_round,
            payload,
            signature,
        };
        confirm_message
        // let signature = signature_service.request_signature(confirm_message.digest()).await;
        // Self { confirm_message }
    }

}

impl Hash for ConfirmMessage {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.block_hash);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.order_round.to_le_bytes());
        for x in &self.payload {
            hasher.update(x);
        }
        hasher.update(&self.shard_id.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for ConfirmMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: confirm message({}, {}, {})",
            self.digest(),
            self.block_hash,
            self.round,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for ConfirmMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "ConfirmMsg{}", self.round)
    }
}

// Execution block
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct EBlock {  
    pub shard_id: u32,
    pub author: PublicKey,
    pub round: Round,
    pub intratxs: Vec<Transaction>,
    pub crosstxs: Vec<Transaction>,
    pub signature: Signature,
}

impl EBlock {
    pub async fn new(
        shard_id: u32,
        author: PublicKey,
        round: Round,
        intratxs: Vec<Transaction>,
        crosstxs: Vec<Transaction>,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            shard_id,
            author,
            round,
            intratxs,
            crosstxs,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        EBlock::default()
    }

    pub fn verify(&self, committee: &ExecutionCommittee) -> CertifyResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            CertifyError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        Ok(())
    }
}

impl Hash for EBlock {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        for x in &self.intratxs {
            hasher.update(x);
        }
        for x in &self.crosstxs {
            hasher.update(x);
        }
        hasher.update(&self.shard_id.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for EBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: EB({}, {}, {})",
            self.digest(),
            self.author,
            self.intratxs.iter().map(|x| x.len()).sum::<usize>(),
            self.crosstxs.iter().map(|x| x.len()).sum::<usize>(),
        )
    }
}

impl fmt::Display for EBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "EB{}", self.digest())
    }
}

// For certify the EBlock
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct NodeSignature {
    pub name: PublicKey,
    pub signature: Signature,
}

impl NodeSignature {
    pub async fn new(
        name: PublicKey,
        signature: Signature,
    ) -> Self {
        let nodesignature: NodeSignature = Self { 
            name, 
            signature, 
        };
        Self { ..nodesignature }
    }
}
impl Hash for NodeSignature {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.name.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}
impl fmt::Debug for NodeSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "name{}, signagure{:?}",
            self.name,
            self.signature,
        )
    }
}

// Certificate block
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct CBlock {
    pub shard_id: u32, 
    pub author: PublicKey,
    pub round: Round,
    pub ebhash: Digest,
    pub ctx_hashes: Vec<Digest>,   // TODO cross-shard txs hash
    // pub votes: HashMap<Digest, u8>,  // votes[ctx_hash] = 0, 1
    pub multisignatures: Vec<NodeSignature>,   // signatures for availability
    // pub signature: Signature,
}

impl CBlock {
    pub async fn new(
        shard_id: u32,
        author: PublicKey,
        round: Round,
        ebhash: Digest,
        ctx_hashes: Vec<Digest>,
        // votes: HashMap<Digest, u8>,
        multisignatures: Vec<NodeSignature>,
        // signature: Signature,
    ) -> Self {
        let block = Self {
            shard_id,
            author,
            round,
            ebhash,
            ctx_hashes,
            // votes,
            multisignatures,
            // signature,
        };
        Self { ..block }
    }

    pub fn genesis() -> Self {
        CBlock::default()
    }
}

impl Hash for CBlock {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        for x in &self.ctx_hashes {
            hasher.update(x);
        }
        // for (x, y) in &self.votes {
        //     let serialized_data = bincode::serialize(&(x, y)).expect("Serialization failed");
        //     hasher.update(serialized_data);
        // }
        // for x in &self.multisignatures {
        //     hasher.update(x.digest());
        // }
        hasher.update(&self.ebhash);
        hasher.update(&self.shard_id.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for CBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: CB({}, round {}, shard id {}, {})",
            self.digest(),
            self.author,
            self.round,
            self.shard_id,
            self.ctx_hashes.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for CBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "CB{}", self.round)
    }
}

// used for consensus in Ordering shard
#[derive(Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize, Ord, PartialOrd)]
pub struct CBlockMeta {
    pub shard_id: u32, 
    pub round: Round,
    pub hash: Digest,
}

impl CBlockMeta {
    pub async fn new(
        shard_id: u32,
        round: u64,
        hash: Digest,
    ) -> Self {
        let cblockmeta = Self {
            shard_id,
            round,
            hash,
        };
        Self {..cblockmeta}
    }
    pub fn size(&self) -> usize {
        self.hash.size()
    }
}

impl Hash for CBlockMeta {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.round.to_le_bytes());
        hasher.update(&self.hash);
        hasher.update(&self.shard_id.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for CBlockMeta {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: CBMeta(round {}, shard id {})",
            self.digest(),
            self.round,
            self.shard_id,
        )
    }
}

impl fmt::Display for CBlockMeta {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "CBMeta (round {}, shard id {})", self.round, self.shard_id)
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ShardInfo {
    pub id: u32,
    pub number: u32,
}

impl Default for ShardInfo {
    fn default() -> Self {
        Self {
            id: 0,
            number: 1,
        }
    }
}