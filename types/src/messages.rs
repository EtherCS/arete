use crypto::{Digest, Hash, Signature, PublicKey};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;

pub type Round = u64;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ConfirmMessage {
    pub shard_id: u32,  
    pub block_hash: Digest, // Corresponding CBlock's hash
    pub round: u64,     // consensus round in the ordering shard
    pub payload: Vec<Digest>,   // new cross-shard transactions
    pub signature: Signature,
}

impl ConfirmMessage {
    pub async fn new(
        shard_id: u32,
        block_hash: Digest,
        round: u64,
        payload: Vec<Digest>,
        signature: Signature,
    ) -> Self {
        let confirm_message = Self {
            shard_id,
            block_hash,
            round,
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

// Certificate block
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct CBlock {
    pub shard_id: u32, 
    pub author: PublicKey,
    pub round: Round,
    pub payload: Vec<Digest>,   // TODO: map[ctx, succOrFail]
    pub signature: Signature,
}

impl CBlock {
    pub async fn new(
        shard_id: u32,
        author: PublicKey,
        round: Round,
        payload: Vec<Digest>,
        signature: Signature,
    ) -> Self {
        let block = Self {
            shard_id,
            author,
            round,
            payload,
            signature,
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
        for x in &self.payload {
            hasher.update(x);
        }
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
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
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
