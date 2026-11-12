//! One round of a roll, assembled in one place: the scenarios it runs
//! itself — none where it reads every node — the cluster process's file,
//! what each node the run reads published and drew of itself, Carried's
//! verdicts, and the drawing from all of them (ADR-0028 and ADR-0052,
//! amendments 2026-11-11). `src/bin/roll.rs` calls it every round; a test
//! calls it over a served run's fixture, so what the owner sees beneath a
//! served cluster — the nodes and nothing a test is called — is held here.
//!
//! Carried's verdicts are recorded before the drawing is made, so a
//! Location the fifty-second fault strikes is drawn as the Monitor shows it.
//! Until 2026-11-11 the drawing came first, and the struck Location stayed
//! Fine on the Topology.

use std::time::Duration;

use observe::topology::window::Window;
use observe::{Snapshot, Topology};

use super::{drawn, ticked};
use crate::carried::Carried;
use crate::cluster::Spawned;
use crate::in_process::InProcess;
use crate::{Roster, Spread};

/// What a round is drawn by: the roster, whether `RoundTrip` is relayed
/// over its stages, the Parties' spread over the simulated nodes'
/// transports, and the nodes the run reads.
pub struct Drawing<'a> {
    pub roster: &'a Roster,
    pub relayed: bool,
    pub spread: &'a Spread,
    pub read: &'a [String],
}

impl<'a> Drawing<'a> {
    /// The drawing of a run over `roster`, `relayed` or not, the Parties'
    /// `spread`, and the nodes it `read`s.
    #[must_use]
    pub const fn of(
        roster: &'a Roster,
        relayed: bool,
        spread: &'a Spread,
        read: &'a [String],
    ) -> Self {
        Self {
            roster,
            relayed,
            spread,
            read,
        }
    }
}

/// What each node the run reads published now, merged into the snapshot,
/// and what it drew of itself by its name, at a time
/// (`real::published_into`).
pub type Reading<'a> = dyn FnMut(&mut Snapshot, i64) -> Vec<(String, Topology)> + 'a;

/// What a round is assembled from.
pub struct Rolling<'a> {
    /// The scenarios the roll runs itself ([`scenarios`]).
    pub in_process: Option<&'a mut InProcess>,
    /// The cluster process, where simulated nodes run.
    pub spawned: Option<&'a mut Spawned>,
    /// What the nodes the run reads published ([`Reading`]).
    pub reading: &'a mut Reading<'a>,
    /// The configured flows the run drives, where it drives them.
    pub carried: Option<&'a mut Carried>,
}

/// The scenarios a roll runs in its own process: none where it reads every
/// node (`served_only`) — such a run carries the cluster's configured flows
/// and nothing else — and `make`'s otherwise.
pub fn scenarios(served_only: bool, make: impl FnOnce() -> InProcess) -> Option<InProcess> {
    (!served_only).then(make)
}

/// One round at `now`, beneath `root`, the scenarios on `simulated` time:
/// its snapshot, and its drawing where one is drawn, `window` stating what
/// moved.
pub fn assembled(
    root: &str,
    (now, simulated): (i64, Duration),
    rolling: Rolling<'_>,
    drawing: &Drawing<'_>,
    window: &mut Window,
) -> (Snapshot, Option<Topology>) {
    let Rolling {
        in_process,
        mut spawned,
        reading,
        carried,
    } = rolling;
    let mut snapshot = ticked(in_process, simulated);
    if let Some(spawned) = spawned.as_deref_mut() {
        snapshot.merge(spawned.tick());
    }
    let drawings = reading(&mut snapshot, now);
    if let Some(carried) = carried {
        carried.round(root, &mut snapshot, now);
    }
    let topology = drawn(
        &snapshot,
        (spawned.as_deref(), drawing.read, &drawings),
        (drawing.roster, drawing.relayed, drawing.spread),
        window,
    );
    (snapshot, topology)
}

#[cfg(test)]
mod tests;
