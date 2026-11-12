//! The cluster: its nodes as processes, spawned and watched.
//!
//! ADR-0028 clause 2: nodes run as System Processes, and a process that hangs
//! is killed and restarted like any other Host Service. Until 2026-09-09 no
//! scenario spawned one; the owner's requirement — *about 10-40 processes
//! emulating nodes* — is this. A [`Cluster`] starts one copy of the `node`
//! binary per node beside the current executable, each told the tests that
//! were named and every node's name, all over one shared directory, so the
//! contention and the handoffs are between processes, and each publishing its
//! own snapshot file. 2026-09-19, the owner: what the Essentials spawns is a
//! cluster and its nodes; the word this file was named for until then is
//! retired.
//!
//! 2026-09-19, the owner again: *even clusters have to be spawned as processes
//! during tests.* This is that process's library. `xmip-essentials-cluster`
//! (`src/bin/cluster.rs`) is a [`Cluster`] and nothing else; the roll holds it
//! from outside as a [`Spawned`] and reads the one file it publishes.
//!
//! [`Cluster::tick`] is the surface's half of ADR-0027 decision 8: a node
//! answers for itself, and the cluster view is assembled by whoever asks each
//! node. Every node's latest file is read and merged — scopes are disjoint per
//! node — and the cluster adds what no node can say about itself: a health
//! record per node (alive, exited with its code, or hung) and the rollup at
//! `xmip:///<cluster>/node`, worst of all. A node that stops beating is hung:
//! it is killed and restarted, and the restart is recorded as a fault — a
//! yellow that stays for the cluster's life — never silently. Liveness is
//! beats, never rounds (`liveness.rs`, the owner 2026-09-26): a node beats
//! within milliseconds of its start and every tenth of a second while it
//! works, a node that has not beaten is starting for ten seconds and hung
//! after, and one that has is hung after ten seconds without a beat. A
//! brutal first round that runs for minutes is a node at work, not a hang
//! (2026-09-25: judging by rounds restarted every node into the same wait,
//! over and over).

mod binary;
#[cfg(feature = "test-support")]
mod idle;
mod liveness;
mod member;
mod orders;
#[cfg(feature = "test-support")]
mod quick;
mod rollup;
mod spawned;

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use observe::Snapshot;

pub use binary::{cluster_binary, node_binary};
#[cfg(feature = "test-support")]
pub use idle::IdlePass;
pub(crate) use liveness::Liveness;
use member::Member;
pub use orders::Orders;
use rollup::rollup;
pub use spawned::Spawned;

use crate::handoff::Hop;
use crate::stress::Stress;
use observe::now_unix_nanos;

/// How long a tick waits for every live node to publish something new before
/// it judges with what it has.
const GRACE: Duration = Duration::from_secs(2);
/// How long `stop` waits for the nodes to leave on their own.
const STOP_WAIT: Duration = Duration::from_secs(5);

/// The node processes, and what the cluster knows about each.
pub struct Cluster {
    binary: PathBuf,
    shared: PathBuf,
    /// Who the nodes are and what they were told — the one value the cluster
    /// hands on to every node it starts.
    orders: Orders,
    /// How a node shows it is alive, and when its silence is a hang.
    liveness: Liveness,
    nodes: Vec<Member>,
    /// The cluster's view as last assembled, rebuilt only when something in
    /// it changed: a node's round read, or what the cluster says of a
    /// node's process.
    view: Snapshot,
    /// What the cluster last said of each node's process.
    said: Vec<(observe::Health, String)>,
    /// Whether the last tick changed the view.
    changed: bool,
}

impl Cluster {
    /// Spawn one node process per name in `orders` over `shared`, each
    /// publishing to `snapshots/<name>.xmip` and each running an image named
    /// for the cluster the orders are for and for itself.
    ///
    /// # Errors
    ///
    /// When a process cannot be started.
    pub fn spawn(
        binary: &Path,
        orders: &Orders,
        shared: &Path,
        snapshots: &Path,
    ) -> io::Result<Self> {
        Self::judged(binary, orders, shared, snapshots, Liveness::OWNERS)
    }

    /// [`Cluster::spawn`], judging liveness by `liveness`.
    fn judged(
        binary: &Path,
        orders: &Orders,
        shared: &Path,
        snapshots: &Path,
        liveness: Liveness,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(shared)?;
        std::fs::create_dir_all(snapshots)?;
        std::fs::remove_file(shared.join("stop")).ok();

        let mut cluster = Self {
            binary: binary.to_path_buf(),
            shared: shared.to_path_buf(),
            orders: orders.clone(),
            liveness,
            nodes: Vec::new(),
            view: Snapshot::new(),
            said: Vec::new(),
            changed: false,
        };
        for name in orders.names().iter().map(ToString::to_string) {
            let path = snapshots.join(format!("{name}.xmip"));
            let child = cluster.start(&name, &path)?;
            cluster.nodes.push(Member::started(name, child, path));
        }
        Ok(cluster)
    }

    /// Start the node called `name` — the orders say what it is declared with
    /// (ADR-0056), whether it may assume the internet, and what tests it is
    /// told to run. Nothing about it is worked out from its name.
    fn start(&self, name: &str, path: &Path) -> io::Result<Child> {
        /// How often a node ticks. A quarter second where a test wants
        /// contention now; two seconds in a brutal roll, where forty nodes
        /// ticking four times a second burned five cores between them
        /// (2026-09-11) and the budget is half of what is free.
        fn node_interval_ms(stress: Stress) -> u64 {
            match stress {
                Stress::Calm | Stress::Realistic => 250,
                Stress::Harsh => 1_000,
                Stress::Brutal => 2_000,
            }
        }

        let orders = &self.orders;
        // One image per node, so the operating system's list says which node
        // of which cluster each row is (ADR-0053, amendment 2026-09-20).
        let image = crate::image::of_node(&self.binary, &orders.cluster, name)?;
        Command::new(image)
            // The cluster the node's scope root is, said rather than
            // inherited.
            .env("XMIP_ESSENTIALS_CLUSTER", &orders.cluster)
            .args(["--name", name, "--stress", orders.stress.name()])
            .args(["--rounds", &orders.rounds.to_string()])
            .args([
                "--interval-ms",
                &node_interval_ms(orders.stress).to_string(),
            ])
            // The node beats as often as this cluster judges it by.
            .args(["--beat-ms", &self.liveness.beat.as_millis().to_string()])
            .args(["--nodes", &orders.roster.text()])
            // None named is every scenario, which is what no flag means.
            .args(
                (!orders.scenarios.is_empty())
                    .then(|| ["--scenarios".to_string(), orders.scenarios.join(",")])
                    .into_iter()
                    .flatten(),
            )
            .args(crate::capability::flags(&orders.capability(name)))
            .arg("--shared")
            .arg(&self.shared)
            .arg("--snapshot")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
    }

    /// One round of the surface: wait, within a grace period, for every live
    /// node to publish anew; restart the hung; and, where anything changed,
    /// merge what each published and add the per-node health and the rollup.
    /// [`Cluster::changed`] says whether it did; an unchanged view is not
    /// assembled, or published, again.
    pub fn tick(&mut self) -> &Snapshot {
        let started = Instant::now();
        for node in &mut self.nodes {
            node.fresh = false;
        }
        loop {
            self.read_all();
            let all_fresh = self.nodes.iter().all(|node| node.fresh || !node.alive());
            if all_fresh || started.elapsed() > GRACE {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        for index in 0..self.nodes.len() {
            self.judge_process(index);
        }

        let now = now_unix_nanos();
        let records: Vec<_> = self
            .nodes
            .iter()
            .map(|node| node.process_record(now))
            .collect();
        let said: Vec<_> = records
            .iter()
            .map(|record| (record.health, record.evidence.clone()))
            .collect();
        self.changed = said != self.said || self.nodes.iter().any(|node| node.fresh);
        if self.changed {
            let mut view = Snapshot::new();
            for (node, record) in self.nodes.iter().zip(records) {
                view.merge(&node.published);
                view.record_health(record);
            }
            view.record_health(rollup(&view, self.nodes.len(), now));
            self.view = view;
            self.said = said;
        }
        &self.view
    }

    /// The view as the last [`Cluster::tick`] left it.
    #[must_use]
    pub const fn view(&self) -> &Snapshot {
        &self.view
    }

    /// Whether the last [`Cluster::tick`] changed the view — what the
    /// cluster process publishes on, and only on.
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.changed
    }

    /// Read every node's file.
    fn read_all(&mut self) {
        for node in &mut self.nodes {
            node.read();
        }
    }

    /// Reap an exit, and restart a node that stopped beating.
    fn judge_process(&mut self, index: usize) {
        let node = &mut self.nodes[index];
        node.reap();
        // The beat as it is now: reading every publication can take the
        // cluster longer than a node may stay silent, and that is not the
        // node's silence.
        node.listen();
        if node.hung(&self.liveness) {
            node.kill();
            node.restarts += 1;
            let (name, path) = (node.name.clone(), node.path.clone());
            match self.start(&name, &path) {
                Ok(child) => self.nodes[index].restarted(child),
                Err(error) => eprintln!("cluster: could not restart {name}: {error}"),
            }
        }
    }

    /// Ask every node to leave — the stop file — wait for them, and kill what
    /// remains after the wait.
    pub fn stop(&mut self) {
        std::fs::write(self.shared.join("stop"), b"stop").ok();
        let started = Instant::now();
        while self.alive() > 0 && started.elapsed() < STOP_WAIT {
            for node in &mut self.nodes {
                node.reap();
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        for node in &mut self.nodes {
            node.kill();
        }
    }

    /// How many node processes are still running.
    #[must_use]
    pub fn alive(&self) -> usize {
        self.nodes.iter().filter(|node| node.alive()).count()
    }

    /// The nodes' names, `node-01` up.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.name.as_str())
    }

    /// How many restarts the cluster has recorded, over every node.
    #[must_use]
    pub fn restarts(&self) -> u32 {
        self.nodes.iter().map(|node| node.restarts).sum()
    }

    /// The handoffs every node says it delivered, per link — what the
    /// topology draws between the nodes.
    #[must_use]
    pub fn hops(&self) -> Vec<Hop> {
        self.nodes
            .iter()
            .flat_map(|node| node.hops.iter().cloned())
            .collect()
    }
}

impl Drop for Cluster {
    /// A failing test leaves no orphans.
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }
}
