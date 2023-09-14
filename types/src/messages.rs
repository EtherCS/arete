use crypto::{Digest, Hash, Signature};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ConfirmMessage {
    pub shard_id: u32,  
    pub block_hash: Digest,
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