//! What a lane shows once the nodes over it have drained it: what the test
//! of two node processes over one lane (`tests/spawned`) judges by. Behind
//! `test-support`: a test's, never a roll's.

use std::collections::BTreeSet;
use std::path::Path;

use super::holders;
use super::pickup::{ledger, list};

/// A drained lane, as its done records tell it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Drained {
    /// Items still in the lane, unclaimed.
    pub left: usize,
    /// Items with a holder.
    pub held: usize,
    /// Items with more than one holder.
    pub held_twice: usize,
    /// The processes that held an item: a holder is `<process>-<reader>`.
    pub processes: usize,
}

/// What `lane` shows: what is left in it, and who held what was taken.
#[must_use]
pub fn drained(lane: &Path) -> Drained {
    let records = ledger(lane);
    let holders = holders(&records);
    let processes: BTreeSet<&str> = records
        .iter()
        .filter_map(|record| record.holder.split_once('-'))
        .map(|(process, _)| process)
        .collect();
    Drained {
        left: list(lane, "item_").len(),
        held: holders.len(),
        held_twice: holders.values().filter(|who| who.len() > 1).count(),
        processes: processes.len(),
    }
}
