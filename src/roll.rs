//! The roll's library: what `xmip-essentials-roll` decides, sorts, spawns,
//! draws and leaves behind. The program is `src/bin/roll.rs`, which keeps its
//! `main` and says what a roll does; this is the rest of it, as `cluster.rs`
//! is the cluster process's.

mod nodes;
pub mod round;

use std::path::{Path, PathBuf};
use std::time::Duration;

use observe::topology::window::Window;
use observe::{Activity, Run, Snapshot, now_unix_nanos};
use xaudit::program_audit::ProgramAudit;

use crate::carried::Carried;
use crate::cluster::{Orders, Spawned, cluster_binary};
use crate::drop_box::Dropping;
use crate::environment;
use crate::far_end::FarEnd;
use crate::in_process::InProcess;
use crate::real::RealNode;
use crate::rotation::leveled;
use crate::scenario::{ROUND_TRIP, drives};
use crate::served::Served;
use crate::stress::Stress;
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
/// those. The hops between simulated nodes are the cluster process's,
/// counted by its simulated relay; what each node the run reads drew of
/// itself, `drawings` by its name — its Party links, a Receive Location
/// feeding its Subscription, the traffic a Storage node measured from and
/// to the other nodes — is merged in, the node owning what is beneath it
/// but for a mood the roll's snapshot holds worse, a Location's delivery
/// verdict among them (`observe::Topology::merge`; ADR-0052, amendment
/// 2026-11-11). What moved — each link's rate, each drawn thing's activity
/// — is stated by `window`, the one the roll keeps over its rounds
/// (`observe::topology::window`; ADR-0052, amendment 2026-11-10).
#[must_use]
pub fn drawn(
    snapshot: &observe::Snapshot,
    (spawned, real, drawings): (Option<&Spawned>, &[String], &[(String, observe::Topology)]),
    (roster, relayed, spread): (&Roster, bool, &Spread),
    window: &mut Window,
) -> Option<observe::Topology> {
    (spawned.is_some() || !real.is_empty()).then(|| {
        let hops = spawned.map_or(&[][..], Spawned::hops);
        let handed = relayed && spawned.is_some();
        let now = now_unix_nanos();
        let mut drawn = cluster_topology(snapshot, roster, handed, hops, (spread, real), now);
        for (name, drawing) in drawings {
            drawn.merge(name, drawing);
        }
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

/// Whether the run reads every node and simulates none — every node started
/// as `xmip-service` or on a machine of its own — so it carries only the
/// configured flows, with real documents (`carried.rs`), and runs none of
/// the scenarios in its own process: the 86-transport matrix belongs to the
/// simulated runs and the cargo tests (the owner, 2026-11-10 22:40;
/// ADR-0028, amendment 2026-11-11). Decided by the roster alone: none of
/// its nodes simulated, and `read` the rest.
#[must_use]
pub fn served_only(simulated: &Roster, read: &[String]) -> bool {
    simulated.is_empty() && !read.is_empty()
}

/// Whether the run reads every node ([`served_only`]), refused where a
/// scenario is named for such a run ([`in_process_refused`]) or where it
/// has nothing to carry: every node read and no Receive Location placed on
/// them, so nothing would run at all — never the scenarios in the roll's
/// own process instead. `carried`, where the run drives Locations, is told
/// whether the fault every fifty seconds strikes at `stress`
/// (`rotation::leveled`).
///
/// # Errors
///
/// REFUSED, in words, as above.
pub fn reading(
    chosen: &[String],
    (simulated, read): (&Roster, &[String]),
    carried: Option<&mut Carried>,
    stress: Stress,
) -> Result<bool, String> {
    let served = served_only(simulated, read);
    in_process_refused(chosen, served)?;
    let Some(carried) = carried else {
        return if served {
            Err(format!(
                "REFUSED: every node of this run is read ({}) and no Receive Location of the \
                 cluster's flows is placed on them, so there is nothing to carry; place the \
                 Applications on the nodes the environment made, or run with simulated nodes",
                read.join(", ")
            ))
        } else {
            Ok(false)
        };
    };
    carried.strike(leveled(stress).1);
    Ok(served)
}

/// The run's first lines said: `first`, which nodes of `real` answered
/// where, and the Parties' far end where one is served.
pub fn announce(mut first: Vec<String>, real: &[RealNode], far_end: Option<&FarEnd>) {
    first.extend(crate::run::found(real));
    first.extend(far_end.map(FarEnd::said));
    for line in first {
        println!("{line}");
    }
}

/// One round of the scenarios the roll runs itself, on `simulated` time;
/// an empty snapshot where it runs none.
pub fn ticked(in_process: Option<&mut InProcess>, simulated: Duration) -> Snapshot {
    in_process.map_or_else(Snapshot::new, |in_process| in_process.tick(simulated))
}

/// What the run did recently, for the activity file: the scenarios' where
/// the roll runs them, Carried's where it reads every node, `idle` where
/// neither.
#[must_use]
pub fn activity<'a>(
    (in_process, carried): (Option<&'a InProcess>, Option<&'a Carried>),
    idle: &'a Activity,
) -> &'a Activity {
    in_process
        .map(InProcess::activity)
        .or_else(|| carried.map(Carried::activity))
        .unwrap_or(idle)
}

/// Whether the scenarios `chosen` can run: in a run that reads every node
/// none can, and one named is REFUSED in words before anything starts
/// (ADR-0055); nothing named is the configured flows alone.
///
/// # Errors
///
/// REFUSED, naming the scenarios, where every node is read.
pub fn in_process_refused(chosen: &[String], served_only: bool) -> Result<(), String> {
    if !served_only || chosen.is_empty() {
        return Ok(());
    }
    Err(format!(
        "REFUSED: {} cannot run where every node is read: such a run carries the cluster's \
         configured flows with real documents and nothing else; name no test, or run the \
         scenarios with simulated nodes",
        chosen.join(", ")
    ))
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
    fn a_run_reading_every_node_carries_the_flows_alone_and_refuses_a_scenario() {
        let none = Roster::default();
        let read = names(&[test_cluster().node(0).name.as_str()]);
        assert!(served_only(&none, &read), "decided by the roster");
        assert!(!served_only(&none, &[]), "nothing read, nothing served");
        let simulated = roster(&test_cluster().node(1).name);
        assert!(!served_only(&simulated, &read), "a node simulated");
        assert_eq!(in_process_refused(&[], true), Ok(()));
        let refused = in_process_refused(&names(&["heavy-load"]), true).expect_err("refused");
        assert!(refused.starts_with("REFUSED: heavy-load"), "{refused}");
        assert_eq!(in_process_refused(&names(&["heavy-load"]), false), Ok(()));
    }

    /// Every node read and none of the flows placed on them: refused in
    /// words, never the scenarios in the roll's own process instead
    /// (ADR-0028, amendment 2026-11-11).
    #[test]
    fn a_run_of_read_nodes_with_nothing_placed_on_them_is_refused() {
        let read = names(&[test_cluster().node(0).name.as_str()]);
        let none = Roster::default();
        let refused =
            reading(&[], (&none, &read), None, Stress::Realistic).expect_err("nothing to carry");
        assert!(
            refused.starts_with("REFUSED") && refused.contains("no Receive Location"),
            "{refused}"
        );
        let simulated = roster(&test_cluster().node(1).name);
        assert_eq!(
            reading(&[], (&simulated, &read), None, Stress::Realistic),
            Ok(false),
            "a run with simulated nodes runs its scenarios"
        );
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
                (None, std::slice::from_ref(&receiving), &[]),
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

    /// ADR-0052, amendment 2026-11-11: what a node the run reads drew of
    /// itself joins the roll's drawing, so the traffic its Storage node
    /// measured travels between the nodes, at the rate the roll's window
    /// states over its rounds.
    #[test]
    fn a_read_nodes_drawing_joins_the_cluster_and_its_traffic_moves_between_nodes() {
        let receiving = test_cluster().with_role("receiving").name.clone();
        let roster = roster(&format!("{receiving}=receiving"));
        let spread = crate::Parties::configured().spread(&["tcp"]);
        let storage = test_cluster().with_role("storage").name.clone();
        let (from, to) = (format!("node/{receiving}"), format!("node/{storage}"));
        let crossing = |volume| observe::Topology {
            links: vec![observe::TopologyLink {
                id: observe::topology::link_id(&from, &to, observe::Relation::WritesInto),
                from: from.clone(),
                to: to.clone(),
                pattern: observe::Pattern::PublishConsume,
                origin: observe::Origin::Observed,
                relation: observe::Relation::WritesInto,
                protocol: observe::topology::XMIP_STORAGE.to_string(),
                state: observe::Health::Fine,
                volume,
                rate: 0.0,
                latency_ms: 0.0,
                progress: 0.0,
                attempts: 0,
                evidence: String::new(),
            }],
            ..observe::Topology::default()
        };
        let mut window = Window::new();
        let names = [receiving.clone(), storage.clone()];
        let mut round = |volume| {
            let drawings = [(storage.clone(), crossing(volume))];
            drawn(
                &published(&receiving, 0),
                (None, &names, &drawings),
                (&roster, false, &spread),
                &mut window,
            )
            .expect("drawn")
        };
        let (_, later) = (round(0), round(6));
        let between = later
            .links
            .iter()
            .find(|link| link.from == from && link.to == to)
            .expect("the traffic between the nodes is drawn");
        assert!(between.rate > 0.0, "{between:?}");
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
