//! `LowLatency` held to its budgets: a pair's health is its measured p99
//! against a budget under a second, so these are timing tests, run alone
//! (rust-style.md section 5c). Under a landing's parallel suite the file
//! pair went Holding for the load alone (2026-11-08); alone, loopback beats
//! every budget, and `file`, which never spikes, stays green while the
//! others spike. What the spikes do to the rollup, whatever the load, is
//! `low_latency`'s own tests'.

use std::path::Path;

use observe::{Health, Snapshot};
use xmip_core_test_essentials::{
    FileRoundTrip, LowLatency, RoundTrip, Stress, TcpRoundTrip, UdpRoundTrip, cluster_root,
};

use super::scratch;

/// The scope a `LowLatency` publishes under, and its leaves.
fn scope(path: &str) -> String {
    format!("{}/{path}", cluster_root())
}

/// The three transports the tests judge: file never spikes, and one of the
/// others must.
fn sample(dir: &Path) -> Vec<Box<dyn RoundTrip>> {
    vec![
        Box::new(FileRoundTrip::new(dir)),
        Box::new(TcpRoundTrip),
        Box::new(UdpRoundTrip),
    ]
}

/// `rounds` ticks of `exercise`, and the last snapshot.
fn ticked(exercise: &mut LowLatency, rounds: u64) -> Snapshot {
    let mut snapshot = exercise.tick();
    for _ in 1..rounds {
        snapshot = exercise.tick();
    }
    snapshot
}

#[test]
fn a_clean_run_stays_within_budget_and_is_green() {
    let dir = scratch("low-latency-clean");
    let mut exercise = LowLatency::new(scope("low-latency"), &dir).over(sample(&dir));
    let snapshot = ticked(&mut exercise, 16);
    assert_eq!(
        snapshot.worst(&scope("low-latency")),
        Some(Health::Fine),
        "loopback beats every budget with no spikes"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn file_stays_within_its_budget_while_the_others_spike() {
    for (stress, rounds) in [
        (Stress::Realistic, 81),
        (Stress::Harsh, Stress::Harsh.rounds()),
    ] {
        let dir = scratch("low-latency-file");
        let mut exercise = LowLatency::new(scope("low-latency"), &dir)
            .over(sample(&dir))
            .at(stress);
        let snapshot = ticked(&mut exercise, rounds);
        assert_eq!(
            snapshot.worst(&scope("low-latency/file")),
            Some(Health::Fine),
            "file is left fast at {stress:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
