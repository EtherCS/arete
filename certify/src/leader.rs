use crate::config::ExecutionCommittee;
use crate::consensus::Round;
use crypto::PublicKey;

pub type LeaderElector = RRLeaderElector;

pub struct RRLeaderElector {
    committee: ExecutionCommittee,
}

impl RRLeaderElector {
    pub fn new(committee: ExecutionCommittee) -> Self {
        Self { committee }
    }

    pub fn get_leader(&self, round: Round) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }
}
