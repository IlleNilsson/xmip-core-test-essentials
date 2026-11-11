//! The roll's library: what `xmip-essentials-roll` decides, sorts, spawns,
//! draws and leaves behind. The program is `src/bin/roll.rs`, which keeps its
//! `main` and says what a roll does; this is the rest of it, as `cluster.rs`
//! is the cluster process's.

mod nodes;

use std::path::{Path, PathBuf};

use observe::topology::window::Window;
use observe::{Run, now_unix_nanos};
use xaudit::program_audit::ProgramAudit;

use crate::carried::Carried;
use crate::cluster::{Orders, Spawned, cluster_binary};
use crate::drop_box::Dropping;
use crate::environment;
use crate::scenario::{ROUND_TRIP, drives};
use crate::served::Served;
use crate::{Roster, Spread, cluster_topology, image, process_audit};

pub use nodes::{Nodes, nodes};

/// What a roll did, said as it stops.
pub struct Ran {
    pub round: u64,
    pub dropping: Option<Dropping>,
    pub carried: Option<Carried>,
}

/// The roll of `cluster` leaving, having `ran`: the cluster process asked
/// to leave first, so it stops its own nodes on the way out and nothing is
/// orphaned; every file dropped into the nodes the run started given its
/// time to arrive, then those nodes drained before what they keep goes
/// with the run's `base`, and the cluster's file at `path`; the images go
/// with them, and the stop is audited.
pub fn leave(
    audit: &ProgramAudit,
    cluster: &str,
    ran: &Ran,
    (spawned, served): (Option<Spawned>, Option<Served>),
    (base, path): (&Path, &Path),
) {
    if let Some(mut spawned) = spawned {
        spawned.stop();
    }
    if let Some(carried) = &ran.carried {
        carried.settle();
    }
    if let Some(served) = served {
        served.stop();
    }
    std::fs::remove_dir_all(base).ok();
    std::fs::remove_file(path).ok();
    // This process still holds its own image, so Stop-XmipTest takes the
    // rest, and the next roll on this cluster clears the directory first.
    image::clear();
    let rounds = ran.round.to_string();
    let taken = ran
        .dropping
        .as_ref()
        .map(Dropping::words)
        .unwrap_or_default();
    let carried = ran.carried.as_ref().map(Carried::words).unwrap_or_default();
    process_audit::stop(
        audit,
        &[
            ("cluster", cluster),
            ("rounds", &rounds),
            ("drop-box", &taken),
            ("carried", &carried),
        ],
    );
}

/// The topology of this round's `snapshot`, where a cluster process ran
/// (`spawned`) or nodes real or started as xmip-service published
/// (`real`): a run with no cluster process too, whose every node is one of
/// those. The hops between nodes are the cluster process's alone, counted
/// by its simulated relay. What moved — each link's rate, each drawn
/// thing's activity — is stated by `window`, the one the roll keeps over its
/// rounds (`observe::topology::window`; ADR-0052, amendment 2026-11-10).
#[must_use]
pub fn drawn(
    snapshot: &observe::Snapshot,
    (spawned, real): (Option<&Spawned>, bool),
    (roster, relayed, spread): (&Roster, bool, &Spread),
    window: &mut Window,
) -> Option<observe::Topology> {
    (spawned.is_some() || real).then(|| {
        let hops = spawned.map_or(&[][..], Spawned::hops);
        let handed = relayed && spawned.is_some();
        let now = now_unix_nanos();
        let mut drawn = cluster_topology(snapshot, roster, handed, hops, spread, now);
        window.state(&mut drawn, snapshot);
        drawn
    })
}

/// What this process says of itself while it runs (ADR-0053): the roll is
/// the test over the cluster it starts, and everything the Essentials runs
/// is test. Every record from here carries the same location, so a reader
/// knows whose it is (ADR-0062, amendment 2026-09-29). The declaration
/// stays while what this answers is held.
#[must_use]
pub fn declared(audit: &ProgramAudit, called: String, root: &str) -> impl Sized {
    audit.locate(root);
    ::node::Declaration::new(called, root, ::node::Purpose::Test)
        .and_then(|declaration| process_audit::declared(audit, declaration))
        .and_then(|declaration| declaration.declare().map_err(|error| error.to_string()))
        .map_err(|error| {
            let problem = format!("roll: could not declare itself: {error}");
            process_audit::fail(audit, "declare", &problem);
        })
}

/// The roll's `start` record: what the run was started with, as its `[run]`
/// header says it, the Parties it trades with, how many rounds, and where it
/// publishes.
pub fn say_started(
    audit: &ProgramAudit,
    ran: (&Run, &Spread),
    limit: Option<u64>,
    snapshot: &Path,
) {
    let (run, spread) = ran;
    let rounds = limit.map_or_else(|| "until stopped".to_string(), |limit| limit.to_string());
    process_audit::start(
        audit,
        &[
            ("cluster", &run.cluster),
            ("stress", &run.stress),
            ("tests", &run.tests.join(",")),
            ("nodes", &run.roles.join(",")),
            ("online", &run.online.join(",")),
            ("real", &run.real.join(",")),
            ("parties", &spread.parties().names().join(",")),
            ("rounds", &rounds),
            ("snapshot", &snapshot.display().to_string()),
        ],
    );
}

/// Where the roll of a cluster publishes in an area, one `name=path` line
/// each for the snapshot, the history and the activity. The cluster and the
/// area are the arguments and the environment is not read.
///
/// # Errors
///
/// REFUSED where it was not told exactly a cluster and an area.
pub fn the_publication(told: &[String]) -> Result<String, String> {
    let [cluster, area] = told else {
        return Err("REFUSED: --publication takes a cluster and an area.".to_string());
    };
    let (snapshot, history, activity) = environment::publish_paths(cluster, Path::new(area));
    Ok(format!(
        "snapshot={}\nhistory={}\nactivity={}",
        snapshot.display(),
        history.display(),
        activity.display()
    ))
}

/// Whether `RoundTrip` runs across the cluster's nodes rather than in this
/// process: it was chosen, and some node's roles serve a stage of the path.
/// A stage no node serves is REFUSED before anything is spawned, naming the
/// role that went undeclared (ADR-0056).
///
/// # Errors
///
/// REFUSED, as [`Roster::refusal`] says, where `RoundTrip` was chosen and a
/// stage of the path is served by no node.
pub fn relayed(chosen: &[String], roster: &Roster) -> Result<bool, String> {
    if !drives(chosen, ROUND_TRIP) || !roster.serves_any_stage() {
        return Ok(false);
    }
    roster.refusal().map_or(Ok(true), Err)
}

/// One cluster process, told the nodes to spawn and the scenarios chosen,
/// over the shared directory it will own. Nothing is spawned when no node was
/// named; a cluster that cannot start is said so, audited as the failure to
/// `spawn-cluster`, and the roll goes on without one, as it did when the
/// nodes were its own.
#[must_use]
pub fn spawn_cluster(
    audit: &ProgramAudit,
    cluster: &str,
    orders: &Orders,
    base: &Path,
    path: &Path,
) -> Option<Spawned> {
    if orders.roster.is_empty() {
        return None;
    }
    let shared = shared(base);
    let started =
        cluster_binary().and_then(|binary| Spawned::start(&binary, cluster, orders, &shared, path));
    match started {
        Ok(spawned) => Some(spawned),
        Err(error) => {
            process_audit::fail(audit, "spawn-cluster", &format!("no cluster: {error}"));
            None
        }
    }
}

/// The directory the cluster owns and its nodes share, beneath the roll's.
#[must_use]
pub fn shared(base: &Path) -> PathBuf {
    base.join("shared")
}

#[cfg(test)]
mod tests {
    use super::*;
    use configure::fixture::test_cluster;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    #[test]
    fn round_trip_is_the_nodes_when_any_declares_a_stage_and_it_was_chosen() {
        let cluster = test_cluster();
        let [receiving, processing, sending] =
            ["receiving", "processing", "sending"].map(|role| &cluster.with_role(role).name);
        let path = roster(&format!(
            "{receiving}=receiving,{processing}=processing,{sending}=sending"
        ));
        assert_eq!(relayed(&[], &path), Ok(true));
        assert_eq!(relayed(&names(&["round-trip"]), &path), Ok(true));
        assert_eq!(relayed(&names(&["heavy-load"]), &path), Ok(false));
        let bare = roster(&format!("{receiving},{processing}"));
        assert_eq!(relayed(&[], &bare), Ok(false));
        assert_eq!(relayed(&[], &Roster::default()), Ok(false));
        // A capability missing is no refusal when RoundTrip was not chosen.
        let receives = roster(&format!("{receiving}=receiving"));
        assert_eq!(relayed(&names(&["filing"]), &receives), Ok(false));
        let refused = relayed(&[], &receives).expect_err("no node processes");
        assert!(refused.starts_with("REFUSED"), "{refused}");
    }

    /// What one receiving node published at a round of a run of real
    /// nodes, having received `received` Streams over tcp.
    fn published(receiving: &str, received: u64) -> observe::Snapshot {
        let at = format!("{}/node/{receiving}", crate::cluster_root());
        let mut snapshot = observe::Snapshot::new();
        let declares = ::node::Capability::parse("receiving")
            .expect("a declaration")
            .evidence();
        for (leaf, evidence) in [
            ("capability", declares.as_str()),
            ("receive/tcp/json", "3/3 rounds passed"),
        ] {
            snapshot.record_health(observe::HealthRecord {
                scope: format!("{at}/{leaf}"),
                health: observe::Health::Fine,
                severity: 0,
                evidence: evidence.to_string(),
                observed_unix_nanos: 7,
            });
        }
        snapshot.record_count(observe::Count {
            scope: format!("{at}/receive/tcp"),
            counted: observe::Counted::Streams,
            value: received,
            window_start_unix_nanos: 7,
            window_end_unix_nanos: 7,
            observed_unix_nanos: 7,
        });
        snapshot
    }

    /// The owner, 2026-11-10: motion follows traffic. Each round's drawing
    /// is stated over the window the roll keeps (ADR-0052, amendment
    /// 2026-11-10): the Party sending into a real node moves once its
    /// receive stage counted more than at the round before.
    #[test]
    fn each_round_is_drawn_with_what_moved_over_the_window_the_roll_keeps() {
        let receiving = test_cluster().with_role("receiving").name.clone();
        let roster = roster(&format!("{receiving}=receiving"));
        let spread = crate::Parties::configured().spread(&["tcp"]);
        let mut window = Window::new();
        let mut round = |received| {
            drawn(
                &published(&receiving, received),
                (None, true),
                (&roster, false, &spread),
                &mut window,
            )
            .expect("a run of real nodes is drawn")
        };
        let (first, later) = (round(0), round(5));
        let stage = format!("node/{receiving}/receive");
        let into = |topology: &observe::Topology| {
            topology
                .links
                .iter()
                .find(|link| link.to == stage)
                .map(|link| link.rate)
        };
        assert_eq!(into(&first), Some(0.0), "nothing to measure from yet");
        assert!(into(&later).is_some_and(|rate| rate > 0.0), "{later:?}");
    }

    #[test]
    fn the_publication_names_the_cluster_s_files_in_the_area() {
        let cluster = test_cluster().name;
        let said = the_publication(&names(&[&cluster, "area"])).expect("a cluster and an area");
        let area = Path::new("area");
        assert_eq!(
            said,
            format!(
                "snapshot={}\nhistory={}\nactivity={}",
                area.join(format!("{cluster}-snapshot.xmip")).display(),
                area.join(format!("{cluster}-history.toml")).display(),
                area.join(format!("{cluster}-activity.toml")).display()
            )
        );
        let refused = the_publication(&names(&[&cluster])).expect_err("no area");
        assert!(refused.starts_with("REFUSED"), "{refused}");
    }
}
