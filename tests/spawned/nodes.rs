//! A cluster's nodes as processes: what they publish, how they leave, and
//! how they are refused.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use configure::fixture::test_cluster;
use observe::{Health, now_unix_nanos};
use xmip_core_test_essentials::cluster::Orders;
use xmip_core_test_essentials::heartbeat::{self, Beat};
use xmip_core_test_essentials::{Cluster, Roster, Stress, cluster_root};

use crate::{alone, merge_into, node_binary, path, path_roster, scratch, snapshot_record};

/// The test cluster's name, which every node a test spawns is told.
fn named() -> String {
    test_cluster().name
}

/// The scope of the cluster's first node.
pub fn first(cluster: &Cluster) -> String {
    let name = cluster.names().next().expect("a node");
    format!("{}/node/{name}", cluster_root())
}

/// Orders for the test cluster's first `count` nodes, by place, none
/// declaring a stage.
pub fn orders(stress: Stress, count: usize, rounds: u64) -> Orders {
    let names: Vec<String> = (0..count)
        .map(|place| test_cluster().node(place).name.clone())
        .collect();
    Orders::of(stress, Roster::of(&names), rounds).in_cluster(&named())
}

/// A cluster of the test cluster's first `count` nodes.
fn spawn(stress: Stress, count: usize, rounds: u64) -> (PathBuf, Cluster) {
    let dir = scratch("cluster");
    let cluster = Cluster::spawn(
        node_binary(),
        &orders(stress, count, rounds),
        &dir.join("shared"),
        &dir.join("snapshots"),
    )
    .expect("the cluster spawns");
    (dir, cluster)
}

/// Every node's snapshot arrives, the rollup exists, no claim across
/// processes was double-picked, and stop leaves no child running.
#[test]
fn a_realistic_cluster_publishes_rolls_up_and_stops_clean() {
    let _alone = alone();
    let (dir, mut cluster) = spawn(Stress::Calm, 3, 0);
    assert_eq!(cluster.alive(), 3);

    let mut snapshot = cluster.tick().clone();
    snapshot = merge_into(snapshot, cluster.tick());

    let root = cluster_root();
    let names: Vec<String> = cluster.names().map(ToString::to_string).collect();
    for name in &names {
        let claim = format!("{root}/node/{name}/exclusive-claim/file");
        let verdicts = snapshot.health(&claim);
        assert_eq!(verdicts.len(), 3, "{name}: three styles published");
        for record in verdicts {
            assert!(
                !record.evidence.contains("holders at once"),
                "{}: {}",
                record.scope,
                record.evidence
            );
        }
        assert!(
            snapshot
                .worst(&format!("{root}/node/{name}/daily-backlog"))
                .is_some(),
            "{name}: the DailyBacklog drain published"
        );
        assert_eq!(
            snapshot.worst(&format!("{root}/node/{name}/system-process")),
            Some(Health::Fine),
            "{name} is alive"
        );
    }
    let rollup = snapshot_record(&snapshot, &format!("{root}/node"));
    assert!(
        rollup.evidence.starts_with("3 nodes"),
        "{}",
        rollup.evidence
    );

    cluster.stop();
    assert_eq!(cluster.alive(), 0, "stop leaves no child running");
    assert_eq!(cluster.restarts(), 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_node_that_exits_is_reported_with_its_code_and_the_cluster_stays_up() {
    let _alone = alone();
    let (dir, mut cluster) = spawn(Stress::Calm, 1, 1);
    let mut last = cluster.tick().clone();
    for _ in 0..8 {
        last = cluster.tick().clone();
        if cluster.alive() == 0 {
            break;
        }
    }
    let process = snapshot_record(&last, &format!("{}/system-process", first(&cluster)));
    assert_eq!(process.health, Health::Fine, "{}", process.evidence);
    assert!(
        process.evidence.starts_with("exited 0"),
        "{}",
        process.evidence
    );
    cluster.stop();
    std::fs::remove_dir_all(&dir).ok();
}

/// The owner, 2026-09-26: more than a millisecond, apart from load, is
/// wrong. A pass over idle nodes reads each one's beat — a few bytes — and
/// neither reads nor parses a publication whose round has not moved; on
/// 2026-09-26 the pass re-read every node's whole file and one pass over
/// nineteen brutal nodes took longer than ten seconds.
#[test]
fn a_pass_over_idle_nodes_reads_their_beats_and_parses_nothing() {
    let _alone = alone();
    // What a pass costs a node is the timing program's (tests/measured).
    let (dir, mut cluster) = spawn(Stress::Calm, 4, 0);
    std::fs::create_dir_all(&dir).expect("the scratch directory");
    let idle = first(&cluster);
    let pass = cluster.idle_pass(&dir, &idle, 100);
    assert!(
        pass.parsed_once,
        "an idle node's publication is not parsed again"
    );
    cluster.stop();
    std::fs::remove_dir_all(&dir).ok();
}

/// The owner's shape, 2026-09-19: nodes run the test that was named, and a
/// node serves the stages it declared. `RoundTrip` alone over three nodes
/// whose **names say nothing** is three processes handing each pair on,
/// each publishing its own stage, the hops recorded per link — and neither
/// shared-directory test runs.
#[test]
fn declared_nodes_run_the_named_test_and_hand_each_pair_along_the_path() {
    let _alone = alone();
    let dir = scratch("cluster-declared");
    let test = test_cluster();
    let [receiving, processing, sending] = path(&test);
    let root = cluster_root();
    let roster = Roster::parse(&path_roster(&test)).expect("a well-formed roster");
    let orders = Orders::of(Stress::Calm, roster, 0)
        .driving(&["round-trip".to_string()])
        .in_cluster(&named());
    let mut cluster = Cluster::spawn(
        node_binary(),
        &orders,
        &dir.join("shared"),
        &dir.join("snapshots"),
    )
    .expect("the cluster spawns");

    let mut snapshot = cluster.tick().clone();
    for _ in 0..40 {
        snapshot = merge_into(snapshot, cluster.tick());
        if !snapshot
            .health(&format!("{root}/node/{sending}/send"))
            .is_empty()
        {
            break;
        }
    }
    for (name, stage) in [
        (receiving, "receive"),
        (processing, "process"),
        (sending, "send"),
    ] {
        let leaves = snapshot.health(&format!("{root}/node/{name}/{stage}"));
        assert!(!leaves.is_empty(), "{name} published its {stage} stage");
        for other in [
            "receive",
            "process",
            "send",
            "exclusive-claim",
            "daily-backlog",
        ] {
            let foreign = snapshot.health(&format!("{root}/node/{name}/{other}"));
            assert!(other == stage || foreign.is_empty(), "{name} ran {other}");
        }
    }
    let links: Vec<(String, String)> = cluster
        .hops()
        .into_iter()
        .inspect(|hop| assert!(hop.count > 0))
        .map(|hop| (hop.from, hop.to))
        .collect();
    assert!(
        links.contains(&(receiving.to_string(), processing.to_string())),
        "{links:?}"
    );
    assert!(
        links.contains(&(processing.to_string(), sending.to_string())),
        "{links:?}"
    );

    cluster.stop();
    assert_eq!(cluster.alive(), 0);
    std::fs::remove_dir_all(&dir).ok();
}

/// The owner, 2026-09-26: *if something takes more than a millisecond,
/// apart from load, something is wrong.* A node's first beat is the first
/// thing it does, so it lands within milliseconds of the process being
/// asked to start — measured from the spawn, which counts the operating
/// system's own start-up, so the bound is generous; it is not seconds.
#[test]
fn a_node_beats_within_milliseconds_of_its_start() {
    let _alone = alone();
    let dir = scratch("cluster-first-beat");
    let test = test_cluster();
    let name = test.node(0).name.as_str();
    let snapshot = dir.join(format!("{name}.xmip"));
    // Once refused first: the operating system's scan of an image it has
    // not run before is the machine's load, not the node's start.
    Command::new(node_binary())
        .args(["--refused", "x"])
        .output()
        .ok();
    let asked = now_unix_nanos();
    let mut child = Command::new(node_binary())
        .env("XMIP_ESSENTIALS_CLUSTER", &test.name)
        .args(["--name", name, "--stress", "calm", "--rounds", "0"])
        .args(["--beat-ms", "60000"])
        .arg("--shared")
        .arg(dir.join("shared"))
        .arg("--snapshot")
        .arg(&snapshot)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("the node binary runs");
    let waiting = Instant::now();
    let beat = loop {
        let written = std::fs::read_to_string(heartbeat::beside(&snapshot)).ok();
        if let Some(beat) = written.as_deref().and_then(Beat::read) {
            break beat;
        }
        assert!(waiting.elapsed() < Duration::from_secs(10), "no beat");
        std::thread::sleep(Duration::from_millis(1));
    };
    std::fs::write(dir.join("shared").join("stop"), b"stop").ok();
    child.kill().ok();
    child.wait().ok();
    let took = Duration::from_nanos((beat.unix_nanos - asked).unsigned_abs());
    assert_eq!(beat.rounds, 0, "before its first round");
    assert!(
        took < Duration::from_secs(1),
        "the first beat took {took:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_node_told_an_unknown_scenario_is_refused_with_exit_code_two() {
    let _alone = alone();
    let dir = scratch("cluster-refused");
    let test = test_cluster();
    let name = test.with_role("receiving").name.as_str();
    let snapshot = dir.join(format!("{name}.xmip"));
    let output = Command::new(node_binary())
        .env("XMIP_ESSENTIALS_CLUSTER", &test.name)
        .args(["--name", name, "--stress", "calm", "--rounds", "1"])
        .args(["--role", "receiving"])
        .args(["--scenarios", "round-trip,pingpong"])
        .arg("--shared")
        .arg(dir.join("shared"))
        .arg("--snapshot")
        .arg(&snapshot)
        .output()
        .expect("the node binary runs");
    assert_eq!(output.status.code(), Some(2));
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(said.contains("REFUSED"), "{said}");
    assert!(
        said.contains("pingpong") && said.contains("daily-backlog"),
        "{said}"
    );
    assert!(!snapshot.exists(), "nothing ran");
    std::fs::remove_dir_all(&dir).ok();
}

/// Every node of the test cluster at the brutal level: forty processes, the
/// owner's ceiling, where `XMIP_TEST_CLUSTER` names a cluster of forty. For
/// the runner, not the gate.
#[test]
#[ignore = "the whole test cluster at brutal; run on purpose"]
fn brutal_cluster_of_every_node() {
    let _alone = alone();
    let every = test_cluster().nodes.len();
    let (dir, mut cluster) = spawn(Stress::Brutal, every, 0);
    let mut snapshot = cluster.tick().clone();
    for _ in 0..4 {
        snapshot = merge_into(snapshot, cluster.tick());
    }
    let root = cluster_root();
    let published = cluster
        .names()
        .filter(|name| {
            snapshot
                .worst(&format!("{root}/node/{name}/exclusive-claim"))
                .is_some()
        })
        .count();
    assert_eq!(published, every, "every one of {every} nodes published");
    cluster.stop();
    assert_eq!(cluster.alive(), 0);
    std::fs::remove_dir_all(&dir).ok();
}
