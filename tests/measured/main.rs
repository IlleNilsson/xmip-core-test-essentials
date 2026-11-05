//! The crate's timing program (`doc/governance/rust-style.md` section 5c):
//! the Essentials' own machinery held to the millisecond rule. A plain
//! `cargo test` leaves it out; the landing runs it after every other test
//! of the crate, alone, one test at a time (`cargo test --test measured
//! --all-features -- --test-threads=1`), so what is measured is its work and
//! not its neighbors':
//!
//! - a pass over idle nodes costs each under a millisecond
//!   (`Cluster::idle_pass`, which `tests/spawned` reads too);
//! - a heartbeat's first beat is on disk within milliseconds of its start.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use xmip_core_test_essentials::cluster::Orders;
use xmip_core_test_essentials::heartbeat::Heartbeat;
use xmip_core_test_essentials::{Cluster, Roster, Stress};

/// A directory of a test's own, empty.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "xmip-essentials-measured-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the scratch directory");
    dir
}

/// The owner, 2026-09-26: more than a millisecond, apart from load, is
/// wrong. A pass over idle nodes reads each one's beat — a few bytes — and
/// neither reads nor parses a publication whose round has not moved; on
/// 2026-09-26 the pass re-read every node's whole file and one pass over
/// nineteen brutal nodes took longer than ten seconds.
#[test]
fn a_pass_over_idle_nodes_costs_each_under_a_millisecond() {
    let cluster = configure::fixture::test_cluster();
    let names: Vec<String> = (0..4)
        .map(|place| cluster.node(place).name.clone())
        .collect();
    let dir = scratch("idle");
    let orders = Orders::of(Stress::Calm, Roster::of(&names), 0).in_cluster(&cluster.name);
    let mut spawned = Cluster::spawn(
        Path::new(env!("CARGO_BIN_EXE_xmip-essentials-node")),
        &orders,
        &dir.join("shared"),
        &dir.join("snapshots"),
    )
    .expect("the cluster spawns");
    let idle = format!("xmip:///{}/node/{}", cluster.name, names[0]);
    let pass = spawned.idle_pass(&dir, &idle, 100);
    spawned.stop();
    std::fs::remove_dir_all(&dir).ok();
    assert!(
        pass.parsed_once,
        "an idle node's publication is not parsed again"
    );
    assert!(
        pass.per_node < Duration::from_millis(1),
        "a pass cost {:?} per idle node",
        pass.per_node
    );
}

/// The owner, 2026-09-26: more than a millisecond, apart from load, is
/// wrong. The first beat is on disk when `start` returns, within
/// milliseconds — the bound is generous for a loaded runner, and it is not
/// seconds.
#[test]
fn the_first_beat_is_written_within_milliseconds_of_the_start() {
    let dir = scratch("heartbeat-first");
    let path = dir.join("node.xmip");
    let started = Instant::now();
    let heart = Heartbeat::start(&path, Duration::from_secs(60));
    let took = started.elapsed();
    assert!(
        took < Duration::from_millis(100),
        "the first beat took {took:?}"
    );
    drop(heart);
    std::fs::remove_dir_all(&dir).ok();
}
