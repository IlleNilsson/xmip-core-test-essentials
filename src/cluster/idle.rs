//! A cluster's nodes made idle by hand, and what a pass over them then
//! costs: what the idle-pass test (`cluster.rs`) and the crate's timing
//! program (`tests/measured`) both read. Behind `test-support`: a test's,
//! never a roll's.

use std::path::Path;
use std::time::{Duration, Instant};

use observe::{Health, HealthRecord, Snapshot};

use super::Cluster;
use crate::heartbeat::{self, Beat};

/// What one idle pass over a cluster's nodes showed.
#[derive(Clone, Copy, Debug)]
pub struct IdlePass {
    /// What a pass cost a node, over every pass.
    pub per_node: Duration,
    /// Whether each node's publication was parsed once, and never again.
    pub parsed_once: bool,
}

impl Cluster {
    /// Each node's publication written in `dir` as the size a brutal node
    /// writes, under `idle`'s scope, and a beat beside it saying its first
    /// round is done, each only the caller writes; read once, and then
    /// `passes` times more.
    ///
    /// # Panics
    ///
    /// Where a publication or a beat cannot be written.
    pub fn idle_pass(&mut self, dir: &Path, idle: &str, passes: u32) -> IdlePass {
        let mut published = Snapshot::new();
        for leaf in 0..2_000 {
            published.record_health(HealthRecord {
                scope: format!("{idle}/exclusive-claim/file/{leaf}"),
                health: Health::Fine,
                severity: 0,
                evidence: "one holder at a time".to_string(),
                observed_unix_nanos: 7,
            });
        }
        let text = crate::report::node_toml(idle, &published, Vec::new());
        let beat = toml::to_string(&Beat {
            unix_nanos: 7,
            rounds: 1,
        })
        .expect("a beat serializes");
        for (index, node) in self.nodes.iter_mut().enumerate() {
            node.path = dir.join(format!("idle-{index}.toml"));
            std::fs::write(&node.path, &text).expect("a publication");
            std::fs::write(heartbeat::beside(&node.path), &beat).expect("a beat");
        }
        self.read_all();
        let first = self.nodes.iter().all(|node| node.parses == 1);
        let started = Instant::now();
        for _ in 0..passes {
            self.read_all();
        }
        let nodes = u32::try_from(self.nodes.len()).expect("few");
        let per_node = started.elapsed() / (passes * nodes);
        IdlePass {
            per_node,
            parsed_once: first && self.nodes.iter().all(|node| node.parses == 1),
        }
    }
}
