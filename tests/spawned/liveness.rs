//! Whether a node process is alive: beats, never rounds (the owner,
//! 2026-09-26), judged quickly enough for a test.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use observe::{Health, Snapshot, now_unix_nanos};
use xmip_core_test_essentials::heartbeat::{self, Beat};
use xmip_core_test_essentials::{Cluster, Stress, cluster_root};

use crate::nodes::{first, orders};
use crate::{alone, node_binary, scratch, snapshot_record};

/// One node of the test cluster, judged quickly, `starting` to show it is
/// alive.
fn quick(starting: Duration) -> (PathBuf, Cluster) {
    let dir = scratch("cluster");
    let cluster = Cluster::quick(
        node_binary(),
        &orders(Stress::Calm, 1, 0),
        &dir.join("shared"),
        &dir.join("snapshots"),
        starting,
    )
    .expect("the cluster spawns");
    (dir, cluster)
}

/// The owner, 2026-09-26: a node that stops beating is hung, and the
/// restart is a yellow.
#[test]
fn a_node_that_stops_beating_is_restarted_and_the_restart_is_a_yellow() {
    let _alone = alone();
    let (dir, mut cluster) = quick(Duration::from_secs(5));
    // Once it has beaten, point the cluster at a copy of the node's file
    // with a beat beside it that is never written again; the real node is
    // then "hung". One tick is no promise the node has published yet.
    let waiting = Instant::now();
    while !cluster.publication(0).is_file() {
        assert!(
            waiting.elapsed() < Duration::from_secs(10),
            "no publication"
        );
        cluster.tick();
    }
    let frozen = dir.join("frozen.xmip");
    std::fs::copy(cluster.publication(0), &frozen).expect("a frozen copy");
    cluster.read_from(0, frozen);
    let started = Instant::now();
    let mut last = cluster.tick().clone();
    while cluster.restarts() == 0 && started.elapsed() < Duration::from_secs(10) {
        last = cluster.tick().clone();
    }
    assert_eq!(
        cluster.restarts(),
        1,
        "silent past its missed beats is hung"
    );
    let process = snapshot_record(&last, &format!("{}/system-process", first(&cluster)));
    assert_eq!(process.health, Health::Stressed, "{}", process.evidence);
    assert!(process.evidence.contains("hung"), "{}", process.evidence);
    let rollup = snapshot_record(&last, &format!("{}/node", cluster_root()));
    assert_ne!(rollup.health, Health::Fine, "the rollup carries the fault");
    cluster.stop();
    assert_eq!(cluster.alive(), 0);
    std::fs::remove_dir_all(&dir).ok();
}

/// The owner, 2026-09-26: a node that beats is alive while its first round
/// runs, however long past `starting` and past the missed beats that round
/// takes, and it is said to be starting.
#[test]
fn a_node_that_beats_but_has_not_finished_a_round_is_never_restarted() {
    let _alone = alone();
    let (dir, mut cluster) = quick(Duration::from_secs(1));
    // A publication nobody writes and a beat only this test writes: beats,
    // and never a finished round.
    let beating = dir.join("beating.xmip");
    cluster.read_from(0, beating.clone());
    std::fs::create_dir_all(&dir).expect("the scratch directory");
    let scope = first(&cluster);
    let beater = std::thread::spawn(move || {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(4) {
            let beat = Beat {
                unix_nanos: now_unix_nanos(),
                rounds: 0,
            };
            let text = toml::to_string(&beat).expect("a beat serializes");
            std::fs::write(heartbeat::beside(&beating), text).ok();
            std::thread::sleep(Duration::from_millis(50));
        }
    });
    let started = Instant::now();
    let mut last = Snapshot::new();
    while started.elapsed() < Duration::from_secs(3) {
        last = cluster.tick().clone();
    }
    beater.join().expect("the beats were written");
    assert_eq!(cluster.restarts(), 0, "a beating node is not hung");
    assert_eq!(cluster.alive(), 1);
    let process = snapshot_record(&last, &format!("{scope}/system-process"));
    assert!(
        process.evidence.starts_with("starting: beating"),
        "{}",
        process.evidence
    );
    cluster.stop();
    std::fs::remove_dir_all(&dir).ok();
}

/// The owner, 2026-09-26: a node that never beats is starting until its
/// allowance is over — the owner's ten seconds, here five — and then it is
/// restarted.
#[test]
fn a_node_that_never_beats_is_restarted_once_starting_is_over() {
    let _alone = alone();
    let (dir, mut cluster) = quick(Duration::from_secs(5));
    // A file the node never writes: as far as the cluster can tell, it has
    // not beaten once.
    cluster.read_from(0, dir.join("never-written.xmip"));
    let process = format!("{}/system-process", first(&cluster));
    cluster.tick();
    let last = cluster.tick().clone();
    assert_eq!(cluster.restarts(), 0, "starting, within its allowance");
    let record = snapshot_record(&last, &process);
    assert!(
        record.evidence.starts_with("starting, no beat"),
        "{}",
        record.evidence
    );
    cluster.tick();
    cluster.tick();
    assert_eq!(cluster.restarts(), 1, "silent past starting is hung");
    cluster.stop();
    std::fs::remove_dir_all(&dir).ok();
}
