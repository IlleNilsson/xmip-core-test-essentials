//! The handoff links between the stages of different nodes: what only a
//! roll draws, beside the cluster `observe::topology::draw` draws.

use node::Stage;
use observe::topology::draw::{mood, node_id, worst};
use observe::topology::{Relation, link_id, relate};
use observe::{Origin, Pattern, Snapshot, TopologyLink};

use crate::handoff::Hop;
use crate::roster::Roster;
use crate::support::cluster_root;

/// One pair of stages a handoff runs between: from a node's stage to a
/// node's stage.
type Pair = (String, Stage, String, Stage);

/// The handoff links: every pair of stages the configuration says a handoff
/// may run between, and every pair a handoff was delivered over, once each.
///
/// What is configured is the roster — who declared receive, process and send
/// (ADR-0056) — when the run drives `RoundTrip` over the stages: a receiving
/// node may hand to any node that declared process, and a processing node to
/// any that declared send, since the roster picks one by a hash of the pair
/// (`Roster::target`). What is observed is the hops. A pair configured and
/// handed over is both; configured and not yet handed over is drawn as
/// configured, its volume zero and its evidence saying so, so a path with no
/// traffic on it is seen rather than missing (the owner, 2026-09-25: *even
/// when testing, the topology does not show configured traffic or its
/// usage*); a pair handed over that the roster does not configure is
/// observed. The volume is the hops, the mood the worst leaf at either end.
pub(super) fn handoff_links(
    snapshot: &Snapshot,
    configured: Option<&Roster>,
    hops: &[Hop],
) -> Vec<TopologyLink> {
    let mut pairs: Vec<(Pair, bool, u64)> = configured
        .map(configured_pairs)
        .unwrap_or_default()
        .into_iter()
        .map(|pair| (pair, true, 0))
        .collect();
    for hop in hops {
        let (Some(from), Some(to)) = (Stage::named(&hop.from_stage), Stage::named(&hop.to_stage))
        else {
            continue;
        };
        let pair = (hop.from.clone(), from, hop.to.clone(), to);
        match pairs.iter_mut().find(|(known, _, _)| *known == pair) {
            Some((_, _, count)) => *count += hop.count,
            None => pairs.push((pair, false, hop.count)),
        }
    }
    pairs
        .into_iter()
        .map(|(pair, configured, count)| link(snapshot, &pair, configured, count))
        .collect()
}

/// Every pair of stages the roster configures a handoff between: receive to
/// process, process to send, over the nodes that declared each, in the order
/// they were named — the one pairing of the cluster's relations
/// (`observe::topology::relate::handed`; ADR-0052, amendment 2026-11-13).
fn configured_pairs(roster: &Roster) -> Vec<Pair> {
    relate::handed(&roster.members())
}

/// One handoff link, drawn from what is known of its pair.
fn link(snapshot: &Snapshot, pair: &Pair, configured: bool, count: u64) -> TopologyLink {
    let (sender, from, receiver, to) = pair;
    let root = cluster_root();
    let ends = [
        format!("{root}/node/{sender}/{}", from.name()),
        format!("{root}/node/{receiver}/{}", to.name()),
    ];
    let worse = ends
        .iter()
        .filter_map(|scope| worst(snapshot, scope))
        .max_by_key(|record| (record.health, record.severity));
    let (state, worst_said) = mood(worse.as_ref());
    let (origin, said) = match (configured, count) {
        (true, 0) => (
            Origin::Configured,
            format!("configured {sender} to {receiver}; no handoff observed yet"),
        ),
        (true, _) => (
            Origin::Both,
            format!("{count} handoffs {sender} to {receiver}"),
        ),
        (false, _) => (
            Origin::Observed,
            format!("{count} handoffs {sender} to {receiver}, which no configuration declares"),
        ),
    };
    let (from, to) = (
        format!("{}/{}", node_id(sender), from.name()),
        format!("{}/{}", node_id(receiver), to.name()),
    );
    TopologyLink {
        id: link_id(&from, &to, Relation::Traffic),
        from,
        to,
        pattern: Pattern::SendReceive,
        origin,
        relation: Relation::Traffic,
        protocol: "handoff".to_string(),
        state,
        volume: count,
        rate: 0.0,
        latency_ms: 0.0,
        progress: 0.0,
        attempts: 0,
        evidence: format!("{said}; worst leaf at either end: {worst_said}"),
    }
}
