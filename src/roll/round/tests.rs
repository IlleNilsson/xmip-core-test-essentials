//! A served round assembled over a fixture: two nodes the run reads, each
//! publishing what a running node publishes, and Carried driving the one
//! Receive Location placed on them through folders of the test's own.

use std::path::PathBuf;

use configure::placed::{Placed, Placement};
use observe::{Health, HealthRecord, NodeKind, TopologyNode};

use super::*;
use crate::carried::{Binding, Spot};
use crate::rotation::PERIOD;
use crate::support::{scratch, test_cluster};

const LOCATION: &str = "Sales orders from Party-A";
const PORT: &str = "Sales orders to ERP";

fn bound(placed: Placed, name: &str, node: &str, folder: PathBuf) -> Binding {
    Binding {
        placement: Placement {
            placed,
            application: "Order to cash".to_string(),
            name: name.to_string(),
            node: node.to_string(),
        },
        spot: Spot::Folder(folder),
    }
}

/// What the receiving and the sending node published, as a running node
/// publishes: its Location or its Port, Fine. The receiving node's own
/// drawing has its Location Fine too: it knows nothing of the rig's verdict.
fn published(
    root: &str,
    (receiving, sending): (&str, &str),
) -> impl FnMut(&mut Snapshot, i64) -> Vec<(String, Topology)> {
    let (root, receiving, sending) = (root.to_string(), receiving.to_string(), sending.to_string());
    move |snapshot: &mut Snapshot, _| {
        for (node, leaf) in [
            (&receiving, format!("receive/{LOCATION}")),
            (&sending, format!("send/{PORT}")),
        ] {
            snapshot.record_health(HealthRecord {
                scope: format!("{root}/node/{node}/{leaf}"),
                health: Health::Fine,
                severity: 0,
                evidence: "started".to_string(),
                observed_unix_nanos: 7,
            });
        }
        let endpoint = TopologyNode {
            id: format!("node/{receiving}/receive/{LOCATION}"),
            parent: format!("node/{receiving}/receive"),
            label: LOCATION.to_string(),
            kind: NodeKind::Endpoint,
            scope: format!("{root}/node/{receiving}/receive/{LOCATION}"),
            state: Health::Fine,
            origin: observe::Origin::Both,
            load: 0.0,
            activity: 0.0,
            evidence: "its own".to_string(),
        };
        let own = Topology {
            nodes: vec![endpoint],
            ..Topology::default()
        };
        vec![(receiving.clone(), own)]
    }
}

/// The owner's complaint of 2026-11-10, held: beneath a served cluster lie
/// the nodes and nothing a test is called, and a Location the fault strikes
/// is drawn as the Monitor shows it.
#[test]
fn a_served_round_holds_only_the_nodes_and_draws_a_struck_location_as_it_is() {
    let base = scratch("served-round");
    let cluster = test_cluster();
    let (receiving, sending) = (cluster.node(0).name.clone(), cluster.node(1).name.clone());
    let root = crate::cluster_root();
    let bindings = [
        bound(Placed::Receive, LOCATION, &receiving, base.join("in")),
        bound(Placed::Send, PORT, &sending, base.join("out")),
    ];
    let mut carried = Carried::new(&bindings, Carried::arrivals()).expect("driven");
    carried.strike(true);
    let read = [receiving.clone(), sending.clone()];
    let roster =
        Roster::parse(&format!("{receiving}=receiving,{sending}=sending")).expect("a roster");
    let spread = crate::Parties::configured().spread(&["tcp"]);
    let drawing = Drawing {
        roster: &roster,
        relayed: false,
        spread: &spread,
        read: &read,
    };
    // The first second of a turn: the one Location placed is struck.
    let struck = i64::try_from(PERIOD * 1_000_000).expect("small") * 1_000_000_000;
    let mut reading = published(&root, (&receiving, &sending));
    let mut in_process = scenarios(true, || panic!("a served run runs no scenario"));

    let rolling = Rolling {
        in_process: in_process.as_mut(),
        spawned: None,
        reading: &mut reading,
        carried: Some(&mut carried),
    };
    let (snapshot, topology) = assembled(
        &root,
        (struck, Duration::ZERO),
        rolling,
        &drawing,
        &mut Window::new(),
    );

    let records: Vec<&str> = snapshot
        .health_records()
        .map(|record| record.scope.as_str())
        .collect();
    assert!(!records.is_empty(), "the round published something");
    let nodes = format!("{root}/node/");
    let astray: Vec<&&str> = records
        .iter()
        .filter(|scope| !scope.starts_with(&nodes))
        .collect();
    assert!(astray.is_empty(), "beneath the root only node: {astray:?}");

    let topology = topology.expect("drawn");
    let id = format!("node/{receiving}/receive/{LOCATION}");
    let location = topology
        .nodes
        .iter()
        .find(|node| node.id == id)
        .unwrap_or_else(|| panic!("{id} drawn: {topology:?}"));
    assert_ne!(location.state, Health::Fine, "{}", location.evidence);
    assert!(
        location.evidence.contains("Dead Message Queue"),
        "drawn as the Monitor shows it: {}",
        location.evidence
    );
    drop(carried);
    let _ = std::fs::remove_dir_all(&base);
}
