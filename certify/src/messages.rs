use crate::config::ExecutionCommittee;
use crate::consensus::Round;
use crate::error::{ConsensusError, ConsensusResult};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;

// #[cfg(test)]
// #[path = "tests/messages_tests.rs"]
// pub mod messages_tests;

// Execution block
// #[derive(Serialize, Deserialize, Default, Clone)]
// pub struct EBlock {  
//     pub shard_id: u32,
//     pub qc: QC, 
//     pub tc: Option<TC>, 
//     pub author: PublicKey,
//     pub round: Round,
//     pub payload: Vec<Digest>,
//     pub signature: Signature,
// }

// impl EBlock {
//     pub async fn new(
//         shard_id: u32,
//         qc: QC,
//         tc: Option<TC>, 
//         author: PublicKey,
//         round: Round,
//         payload: Vec<Digest>,
//         mut signature_service: SignatureService,
//     ) -> Self {
//         let block = Self {
//             shard_id,
//             qc,
//             tc,
//             author,
//             round,
//             payload,
//             signature: Signature::default(),
//         };
//         let signature = signature_service.request_signature(block.digest()).await;
//         Self { signature, ..block }
//     }

//     pub fn genesis() -> Self {
//         EBlock::default()
//     }

//     pub fn parent(&self) -> &Digest {
//         &self.qc.hash
//     }

//     pub fn verify(&self, committee: &ExecutionCommittee) -> ConsensusResult<()> {
//         // Ensure the authority has voting rights.
//         let voting_rights = committee.stake(&self.author);
//         ensure!(
//             voting_rights > 0,
//             ConsensusError::UnknownAuthority(self.author)
//         );

//         // Check the signature.
//         self.signature.verify(&self.digest(), &self.author)?;

//         // Check the embedded QC.
//         if self.qc != QC::genesis() {
//             self.qc.verify(committee)?;
//         }

//         // Check the TC embedded in the block (if any).
//         if let Some(ref tc) = self.tc {
//             tc.verify(committee)?;
//         }
//         Ok(())
//     }
// }

// impl Hash for EBlock {
//     fn digest(&self) -> Digest {
//         let mut hasher = Sha512::new();
//         hasher.update(self.author.0);
//         hasher.update(self.round.to_le_bytes());
//         for x in &self.payload {
//             hasher.update(x);
//         }
//         hasher.update(&self.qc.hash);
//         hasher.update(&self.shard_id.to_le_bytes());
//         Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
//     }
// }

// impl fmt::Debug for EBlock {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         write!(
//             f,
//             "{}: B({}, {}, {:?}, {})",
//             self.digest(),
//             self.author,
//             self.round,
//             self.qc,
//             self.payload.iter().map(|x| x.size()).sum::<usize>(),
//         )
//     }
// }

// impl fmt::Display for EBlock {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         write!(f, "B{}", self.round)
//     }
// }


// #[derive(Clone, Serialize, Deserialize)]
// pub struct Vote {
//     pub hash: Digest,
//     pub round: Round,
//     pub author: PublicKey,
//     pub signature: Signature,
// }

// impl Vote {
//     pub async fn new(
//         block: &EBlock,
//         author: PublicKey,
//         mut signature_service: SignatureService,
//     ) -> Self {
//         let vote = Self {
//             hash: block.digest(),
//             round: block.round,
//             author,
//             signature: Signature::default(),
//         };
//         let signature = signature_service.request_signature(vote.digest()).await;
//         Self { signature, ..vote }
//     }

//     pub fn verify(&self, committee: &ExecutionCommittee) -> ConsensusResult<()> {
//         // Ensure the authority has voting rights.
//         ensure!(
//             committee.stake(&self.author) > 0,
//             ConsensusError::UnknownAuthority(self.author)
//         );

//         // Check the signature.
//         self.signature.verify(&self.digest(), &self.author)?;
//         Ok(())
//     }
// }

// impl Hash for Vote {
//     fn digest(&self) -> Digest {
//         let mut hasher = Sha512::new();
//         hasher.update(&self.hash);
//         hasher.update(self.round.to_le_bytes());
//         Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
//     }
// }

// impl fmt::Debug for Vote {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         write!(f, "V({}, {}, {})", self.author, self.round, self.hash)
//     }
// }

// #[derive(Clone, Serialize, Deserialize, Default)]
// pub struct QC {
//     pub hash: Digest,
//     pub round: Round,
//     pub votes: Vec<(PublicKey, Signature)>,
// }

// impl QC {
//     pub fn genesis() -> Self {
//         QC::default()
//     }

//     pub fn timeout(&self) -> bool {
//         self.hash == Digest::default() && self.round != 0
//     }

//     pub fn verify(&self, committee: &ExecutionCommittee) -> ConsensusResult<()> {
//         // Ensure the QC has a quorum.
//         let mut weight = 0;
//         let mut used = HashSet::new();
//         for (name, _) in self.votes.iter() {
//             ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
//             let voting_rights = committee.stake(name);
//             ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
//             used.insert(*name);
//             weight += voting_rights;
//         }
//         ensure!(
//             weight >= committee.quorum_threshold(),
//             ConsensusError::QCRequiresQuorum
//         );

//         // Check the signatures.
//         Signature::verify_batch(&self.digest(), &self.votes).map_err(ConsensusError::from)
//     }
// }

// impl Hash for QC {
//     fn digest(&self) -> Digest {
//         let mut hasher = Sha512::new();
//         hasher.update(&self.hash);
//         hasher.update(self.round.to_le_bytes());
//         Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
//     }
// }

// impl fmt::Debug for QC {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         write!(f, "QC({}, {})", self.hash, self.round)
//     }
// }

// impl PartialEq for QC {
//     fn eq(&self, other: &Self) -> bool {
//         self.hash == other.hash && self.round == other.round
//     }
// }

// #[derive(Clone, Serialize, Deserialize)]
// pub struct Timeout {
//     pub high_qc: QC,
//     pub round: Round,
//     pub author: PublicKey,
//     pub signature: Signature,
// }

// impl Timeout {
//     pub async fn new(
//         high_qc: QC,
//         round: Round,
//         author: PublicKey,
//         mut signature_service: SignatureService,
//     ) -> Self {
//         let timeout = Self {
//             high_qc,
//             round,
//             author,
//             signature: Signature::default(),
//         };
//         let signature = signature_service.request_signature(timeout.digest()).await;
//         Self {
//             signature,
//             ..timeout
//         }
//     }

//     pub fn verify(&self, committee: &ExecutionCommittee) -> ConsensusResult<()> {
//         // Ensure the authority has voting rights.
//         ensure!(
//             committee.stake(&self.author) > 0,
//             ConsensusError::UnknownAuthority(self.author)
//         );

//         // Check the signature.
//         self.signature.verify(&self.digest(), &self.author)?;

//         // Check the embedded QC.
//         if self.high_qc != QC::genesis() {
//             self.high_qc.verify(committee)?;
//         }
//         Ok(())
//     }
// }

// impl Hash for Timeout {
//     fn digest(&self) -> Digest {
//         let mut hasher = Sha512::new();
//         hasher.update(self.round.to_le_bytes());
//         hasher.update(self.high_qc.round.to_le_bytes());
//         Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
//     }
// }

// impl fmt::Debug for Timeout {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         write!(f, "TV({}, {}, {:?})", self.author, self.round, self.high_qc)
//     }
// }

// // timeout certificate
// #[derive(Clone, Serialize, Deserialize)]
// pub struct TC {
//     pub round: Round,
//     pub votes: Vec<(PublicKey, Signature, Round)>,
// }

// impl TC {
//     pub fn verify(&self, committee: &ExecutionCommittee) -> ConsensusResult<()> {
//         // Ensure the QC has a quorum.
//         let mut weight = 0;
//         let mut used = HashSet::new();
//         for (name, _, _) in self.votes.iter() {
//             ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
//             let voting_rights = committee.stake(name);
//             ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
//             used.insert(*name);
//             weight += voting_rights;
//         }
//         ensure!(
//             weight >= committee.quorum_threshold(),
//             ConsensusError::TCRequiresQuorum
//         );

//         // Check the signatures.
//         for (author, signature, high_qc_round) in &self.votes {
//             let mut hasher = Sha512::new();
//             hasher.update(self.round.to_le_bytes());
//             hasher.update(high_qc_round.to_le_bytes());
//             let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
//             signature.verify(&digest, author)?;
//         }
//         Ok(())
//     }

//     pub fn high_qc_rounds(&self) -> Vec<Round> {
//         self.votes.iter().map(|(_, _, r)| r).cloned().collect()
//     }
// }

// impl fmt::Debug for TC {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         write!(f, "TC({}, {:?})", self.round, self.high_qc_rounds())
//     }
// }
