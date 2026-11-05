//! The tests that spawn this crate's binaries: a cluster's nodes as
//! processes, a cluster as a process, and two nodes over one lane.
//!
//! An integration program, so cargo builds `xmip-essentials-node` and
//! `xmip-essentials-cluster` before it, with the features it was built with,
//! and names them in `CARGO_BIN_EXE_<name>`: no test runs cargo. Until
//! 2026-11-04 these were unit tests that built the binaries themselves, a
//! second cargo whose features differed from the test's, so each rebuilt the
//! runtime beneath it, a test waited minutes, and a landing stopped on it
//! (the owner: *something is wrong if we had to change all that for some
//! test*).

mod claim;
mod liveness;
mod nodes;
mod tree;

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use configure::fixture::TestCluster;
use observe::{HealthRecord, Snapshot};
use xmip_core_test_essentials::cluster::merge;

/// Held for the whole of each test, so this program's tests run one at a
/// time. Each stands up node processes that work every core they find, and
/// eleven at once judged one another's load as a node's: a round not
/// published in two ticks, a beat late, a node taken for hung — each passed
/// alone, every time (2026-11-04). Cargo runs test programs one after
/// another, so held here, nothing else of the crate runs beside them either.
fn alone() -> MutexGuard<'static, ()> {
    static ALONE: Mutex<()> = Mutex::new(());
    ALONE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The node binary cargo built for this program.
fn node_binary() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_xmip-essentials-node"))
}

/// The cluster binary cargo built for this program.
fn cluster_binary() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_xmip-essentials-cluster"))
}

/// A fresh, empty scratch directory for a test, unique per name and run so
/// parallel tests never collide. The test removes it when done.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("xmip-play-{name}-{}", observe::now_unix_nanos()));
    std::fs::remove_dir_all(&dir).ok();
    dir
}

/// The test cluster's receiving, processing and sending node, a
/// `RoundTrip`'s whole path, in that order.
fn path(cluster: &TestCluster) -> [&str; 3] {
    ["receiving", "processing", "sending"].map(|role| cluster.with_role(role).name.as_str())
}

/// The roster of the test cluster's whole path, each node declaring its
/// part of it.
fn path_roster(cluster: &TestCluster) -> String {
    let [receiving, processing, sending] = path(cluster);
    format!("{receiving}=receiving,{processing}=processing,{sending}=sending")
}

/// `from` merged into `into`.
fn merge_into(mut into: Snapshot, from: &Snapshot) -> Snapshot {
    merge(&mut into, from);
    into
}

/// The record at exactly `scope`.
fn snapshot_record(snapshot: &Snapshot, scope: &str) -> HealthRecord {
    snapshot
        .health(scope)
        .into_iter()
        .find(|record| record.scope == scope)
        .unwrap_or_else(|| panic!("{scope} is recorded"))
}
