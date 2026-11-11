//! Roll the essentials: run every scenario continuously.
//!
//! `cargo run` ticks all four scenarios on an interval and redraws the combined
//! board each round — the tests as ADR-0028 means them, over time and never
//! stopping:
//!
//!   - **`RoundTrip`** — every transport by every contract round-trips and holds
//!     its contract; the message-path stages, with injected faults.
//!   - **`LowLatency`** — the same pairs, timed against a latency budget (p50/p99).
//!   - **`HeavyLoad`** — a megabyte per pair; does it arrive whole and validate.
//!   - **Retention** — retention and archiving: retain, then archive by age
//!     (Xmip does not delete, ADR-0040).
//!   - **Filing** — every archive technology by every contract: file a probe
//!     item through the real store and restore it whole.
//!   - **`ExclusiveClaim`** — exclusive pickup: one holder per item under contention,
//!     per execution style (sequential, parallel, concurrent).
//!   - **`DailyBacklog`** — drain a backlog as fast as possible; tweak, add a node.
//!
//! Each publishes under its own subtree of `xmip:///<cluster>`, merged into one
//! snapshot so the rollup covers all four and an operator drills scenario →
//! detail → the failing leaf.
//!
//! Pass a number to run that many rounds and stop; omit it to roll until
//! interrupted. `XMIP_ESSENTIALS_SCENARIOS` names the scenarios to drive, comma
//! separated (`round-trip,heavy-load`); unset, every one rolls, so nothing changed
//! quietly. Two time limits bound any roll (ADR-0028): a maximum wall-clock
//! time, `XMIP_ESSENTIALS_MAX_SECONDS`, and a factor on time,
//! `XMIP_ESSENTIALS_TIME_FACTOR`, which stretches a **simulated clock** — `1.0`
//! mimics real time, retracted below one runs simulated time faster, so a long
//! horizon plays out in a short run (three simulated years in fifteen real
//! minutes is `MAX_SECONDS=900` with `TIME_FACTOR≈9.5e-6`). The round cadence
//! stays real; the factor stretches simulated time, which the Retention test ages on.
//!
//! When stdout is a terminal the board is redrawn in place; when it is piped,
//! one summary line per round is appended. After every tick the snapshot,
//! history and activity are written to the TOML files the monitoring GUI reads,
//! named for the cluster in `XMIP_ESSENTIALS_AREA` (the temp directory unset);
//! `roll --publication <cluster> <area>` prints where they are and starts
//! nothing. Every variable is read in one place, `environment.rs`.
//!
//! **The cluster is a process too** (the owner, 2026-09-19). The roll spawns
//! exactly one `xmip-essentials-cluster` process, and that process spawns and
//! supervises one `xmip-essentials-node` per node (ADR-0028 clause 2). The
//! roll merges the one file the cluster publishes into its own snapshot each
//! round; the board shows the nodes' rollup row, and a node's leaf only when
//! it is not fine.
//!
//! **The nodes are `cluster.xmip`** (ADR-0056, amendment
//! 2026-10-03): `XMIP_TEST_CLUSTER` names the file, and every node under its
//! `[nodes]` is one process, declaring the roles its `roles` says
//! (`environment::roster`). `Start-XmipTest` writes the run's file from
//! `-Cluster`, `-Nodes` and `-NodeRole`, or names the test cluster's where
//! `-Nodes` was not given; nothing is counted or numbered.
//! `XMIP_ESSENTIALS_ONLINE_NODES` names the ones among them that may assume
//! the internet (ADR-0045); unset, every node reads `XMIP_ONLINE`.
//!
//! **A node declares its roles** (ADR-0056, amendment 2026-10-01) —
//! `receiving`, `processing+sending`, or `executing` for all
//! three in one process — and every node runs its part of the scenarios
//! named. Where a node's roles serve a stage, `RoundTrip` is the nodes' —
//! each pair handed receive to process to send between the processes, or
//! kept in an executing node's one process — and the roll does not also run
//! it in-process; a stage no node serves is REFUSED at the start, naming the
//! role. A node serving no stage runs the shared-directory tests whole, as
//! before. A node's name decides none of this.
//!
//! **A real node is used where its name resolves** (ADR-0028, amendments
//! 2026-09-19 and 2026-10-21): the names the operator typed,
//! `XMIP_ESSENTIALS_NAMED`, and the cluster's, are resolved, and one that
//! answers as that node of this cluster over its operate listener, asked
//! with the operator certificate `XMIP_ESSENTIALS_CERTIFICATE` names, is
//! real — never spawned, configured or stopped, its publication read each
//! round; one that resolves and does not is REFUSED by name. The cluster
//! process is given only the simulated nodes, and `[run] real` and the
//! first lines say which are which (`real.rs`).
//!
//! **Every other node is started as `xmip-service`** where the run was
//! given one, `XMIP_ESSENTIALS_SERVICE`, and one of those nodes declares the
//! Storage role (ADR-0028, amendment 2026-10-22): the run gives each its
//! slice, its certificate, its addresses on loopback and its bindings
//! (`served.rs`), reads it as a real node, and drives it through its Receive
//! Locations each round (`carried.rs`); simulated where not.
//!
//! **A run with real nodes drives them through the Parties' far end**
//! (ADR-0028, amendment 2026-10-22): the rig serves SFTP on this computer
//! (`far_end.rs`), each Location the real nodes' environment placed on them
//! a user of its own there (`xmip-service --slice --bind`), drops into the
//! Receive Locations' and is told of what the Send Ports put
//! (`carried.rs`); no node is started as `xmip-service` beside them.
//!
//! **It audits** (ADR-0062): `start` once the roll knows what it is, `stop`
//! when its rounds are done, every refusal and failed write as a failure, and
//! every panic as `unhandled` — into `XMIP_AUDIT_DIRECTORY`, else the
//! operating system's log (`process_audit.rs`).
//!
//! What it decides, sorts, spawns, draws and leaves behind is the library's
//! `roll.rs`; this file is its `main`.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use observe::History;
use observe::now_unix_nanos;
use observe::topology::window::Window;
use xaudit::program_audit::ProgramAudit;
use xmip_core_test_essentials::cluster::{Orders, merge};
use xmip_core_test_essentials::drop_box::Dropping;
use xmip_core_test_essentials::environment::{self, max_seconds, time_factor};
use xmip_core_test_essentials::far_end::FarEnd;
use xmip_core_test_essentials::in_process::InProcess;
use xmip_core_test_essentials::operator_orders;
use xmip_core_test_essentials::publication::Publication;
use xmip_core_test_essentials::real;
use xmip_core_test_essentials::roll::{
    Nodes, Ran, declared, drawn, leave, nodes, relayed, say_started, shared, spawn_cluster,
    the_publication,
};
use xmip_core_test_essentials::run::{announced, found};
use xmip_core_test_essentials::{
    Budget, Headroom, Parties, Stress, cluster_name, cluster_root, record_round, redraw, started,
    summarize,
};
use xmip_core_test_essentials::{image, process_audit};

fn main() {
    // The name is the image's own — xmip-essentials-<cluster>-roll where
    // Start-XmipTest linked one — so the declaration and the audit say what
    // Get-Process says (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-essentials-roll");
    let audit = ProgramAudit::new(&called, None);
    audit.watch_panics();

    if answered_publication(&audit) {
        return;
    }

    let (cluster, root, base) = or_refuse(&audit, this_cluster());
    let root = root.as_str();
    let _declared = declared(&audit, called, root);
    let stress = Stress::from_env();
    let chosen = or_refuse(&audit, environment::scenarios());
    let Nodes {
        real: real_nodes,
        names: real_names,
        roster,
        simulated,
        served,
        mut carried,
        far_end,
    } = or_refuse(&audit, nodes(&cluster, &base));
    let relayed = or_refuse(&audit, relayed(&chosen, &simulated));
    let run = started(&cluster, &chosen, &roster, stress, &real_names);

    let spread = Parties::configured().over_every_transport();
    let mut first = announced(&cluster, stress, &roster, &spread);
    first.extend(found(&real_nodes));
    first.extend(far_end.as_ref().map(FarEnd::said));
    for line in first {
        println!("{line}");
    }

    // Every scenario this process runs itself (`in_process.rs`).
    let mut in_process = InProcess::new(root, &base, stress, chosen.clone(), relayed);
    // The Parties' bursts into the drop box the receiving nodes share.
    let mut dropping = relayed.then(|| Dropping::new(&shared(&base)));

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    // The nodes take an operator's orders under the directory they share,
    // and every snapshot says where (ADR-0065, amendment 2026-09-29; ADR-0013,
    // amendment 2026-09-30).
    let publication = Publication::of(&cluster, run.clone(), audit.clone())
        .taking_orders(&operator_orders::orders(&shared(&base)));
    say_started(&audit, (&run, &spread), limit, &publication.snapshot);

    // One cluster per roll (ADR-0028), and since 2026-09-19 a process of its
    // own: it spawns and supervises the nodes and publishes beside this
    // roll's snapshot, which merges that one file each round.
    let cluster_path = publication.beside(&format!("{cluster}-cluster.xmip"));
    let orders = Orders::of(stress, simulated.clone(), 0).driving(&chosen);
    let mut spawned = spawn_cluster(&audit, &cluster, &orders, &base, &cluster_path);

    let live = std::io::stdout().is_terminal();
    let real = Duration::from_millis(1000);
    let budget = Budget::new(max_seconds(), time_factor());

    if !live {
        println!("publishing snapshots to {}", publication.snapshot.display());
    }

    let mut round: u64 = 0;
    // What moved over the last half minute of rounds, stated on each
    // round's drawing (ADR-0052, amendment 2026-11-10).
    let mut window = Window::new();
    loop {
        round += 1;

        // What everyone else is using, measured now: the levels size this
        // round's pairs to half of what is left (ADR-0028, 2026-09-11). The
        // level's own count of nodes was sized the same way.
        let headroom = Headroom::refresh();

        let mut snapshot = in_process.tick(budget.simulated_elapsed());
        // The cluster's own file, and what the real nodes and those started
        // as xmip-service published, from both of which the topology draws.
        if let Some(spawned) = spawned.as_mut() {
            merge(&mut snapshot, spawned.tick());
        }
        real::published_into(&real_nodes, root, &mut snapshot, now_unix_nanos());
        let drawing = (spawned.as_ref(), !real_nodes.is_empty());
        let topology = drawn(&snapshot, drawing, (&roster, relayed, &spread), &mut window);
        if let Some(carried) = carried.as_mut() {
            carried.round(root, &mut snapshot, now_unix_nanos());
        }

        // Every scope's series, and the rollup at the root that the history
        // file is written from: a scenario counts beneath the node, so a roll
        // that recorded only the snapshot published `points = []` for as long
        // as the Essentials has had a history (curve.rs, 2026-09-19).
        if let Some(dropping) = dropping.as_mut() {
            dropping.round(root, &mut snapshot, now_unix_nanos());
        }
        record_round(&mut history, root, &snapshot);
        publication.round(root, &snapshot, topology, &history, in_process.activity());

        if live {
            redraw(root, round, &snapshot);
            if let Some(line) = found(&real_nodes) {
                println!("{line}");
            }
            println!("  publishing to {}", publication.snapshot.display());
        } else {
            summarize(root, round, &snapshot);
        }
        println!("  headroom: {}", headroom.describe());

        if limit.is_some_and(|limit| round >= limit) || budget.expired() {
            break;
        }
        std::thread::sleep(real);
    }

    let ran = Ran {
        round,
        dropping,
        carried,
    };
    leave(
        &audit,
        &cluster,
        &ran,
        (spawned, served),
        (&base, &cluster_path),
    );
}

/// Asked where a cluster publishes, it answers the same way: the names are
/// decided here (`environment::publish_paths`), and `Start-XmipTest`
/// records what it is told rather than naming the files again. Whether it
/// was asked, and answered.
fn answered_publication(audit: &ProgramAudit) -> bool {
    if std::env::args().nth(1).as_deref() != Some("--publication") {
        return false;
    }
    let told: Vec<String> = std::env::args().skip(2).collect();
    println!("{}", or_refuse(audit, the_publication(&told)));
    true
}

/// What the environment told this roll, or REFUSED: a value it cannot read is
/// said, audited as the failure to `start`, and the roll does not start —
/// until 2026-09-19 an unknown scenario was said on stderr and dropped, and
/// the roll carried on as something nobody asked for. A word that is no
/// role is refused the same way.
fn or_refuse<T>(audit: &ProgramAudit, told: Result<T, String>) -> T {
    told.unwrap_or_else(|refusal| {
        process_audit::fail(audit, "start", &refusal);
        std::process::exit(2);
    })
}

/// One cluster per roll (ADR-0028): its name, its scope root, and its own
/// scratch directory, emptied now, so a second cluster beside it neither wipes
/// nor shares this one's directories. REFUSED when the owner named none.
fn this_cluster() -> Result<(String, String, PathBuf), String> {
    let Some(cluster) = cluster_name() else {
        return Err(
            "REFUSED: a roll is a cluster and the owner names it; set XMIP_ESSENTIALS_CLUSTER \
             (Start-XmipTest -Cluster <name>). A test spawns nodes, never a cluster."
                .to_string(),
        );
    };
    let base = std::env::temp_dir().join("essentials").join(&cluster);
    std::fs::remove_dir_all(&base).ok();
    Ok((cluster, cluster_root(), base))
}
