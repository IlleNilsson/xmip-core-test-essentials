//! Carried over two folders of its own: one Receive Location it drops into
//! and the Send Ports a file can arrive at, each judged as the roll judges
//! it (ADR-0028, amendment 2026-11-11). Nothing here runs a node: the test
//! is the node, copying what was dropped to where it delivers it.

use std::path::{Path, PathBuf};

use configure::placed::{Placed, Placement};
use observe::{Health, HealthRecord, Snapshot, now_unix_nanos};

use super::*;
use crate::support::{scratch, test_cluster};

/// `name` of the shipped cluster's order to cash Application placed on
/// `node`, `placed`, over the folder `folder`.
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

fn folder(binding: &Binding) -> &Path {
    match &binding.spot {
        Spot::Folder(folder) => folder,
        Spot::FarEnd(_) => panic!("a folder"),
    }
}

/// The file the round numbered `number` dropped into `drop`.
fn dropped(drop: &Binding, number: u64) -> String {
    let ending = format!("-{number}.edi");
    std::fs::read_dir(folder(drop))
        .expect("the drop")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .find(|name| name.ends_with(&ending))
        .unwrap_or_else(|| panic!("nothing ending {ending} dropped"))
}

/// `name` delivered by the test, the node it plays, from `drop` to `port`,
/// and told of by the folder's watch.
fn delivered(name: &str, (drop, port): (&Binding, &Binding), arrived: &Arrived) {
    std::fs::copy(folder(drop).join(name), folder(port).join(name)).expect("delivered");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !arrived
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(name)
    {
        assert!(Instant::now() < deadline, "{name} was never told of");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The verdict at `scope`, which must be there and say something.
fn verdict(snapshot: &Snapshot, scope: &str) -> HealthRecord {
    let record = snapshot
        .health(scope)
        .into_iter()
        .find(|record| record.scope == scope)
        .unwrap_or_else(|| panic!("no verdict at {scope}"));
    assert!(!record.evidence.is_empty(), "a verdict says what it judged");
    record
}

#[test]
fn a_file_delivered_where_its_route_sends_it_is_fine_and_one_at_another_port_is_done() {
    let base = scratch("carried");
    let cluster = test_cluster();
    let (receiving, sending) = (&cluster.node(0).name, &cluster.node(1).name);
    let drop = bound(
        Placed::Receive,
        "Sales orders from Party-A",
        receiving,
        base.join("in"),
    );
    let erp = bound(
        Placed::Send,
        "Sales orders to ERP",
        sending,
        base.join("erp"),
    );
    let astray = bound(
        Placed::Send,
        "Invoices to Party-A",
        sending,
        base.join("astray"),
    );
    let arrived = Carried::arrivals();
    let bindings = [drop.clone(), erp.clone(), astray.clone()];
    let mut carried = Carried::new(&bindings, Arc::clone(&arrived)).expect("driven");
    let root = cluster.scope();
    let scope = delivery(&root, &drop.placement);
    assert_eq!(
        scope,
        format!("{root}/node/{receiving}/receive/Sales orders from Party-A/delivery"),
        "beneath the Receive Location, on the node it is placed on"
    );
    let mut snapshot = Snapshot::new();

    // Dropped, and delivered whole where its route sends it.
    carried.round(&root, &mut snapshot, now_unix_nanos());
    let first = dropped(&drop, 0);
    delivered(&first, (&drop, &erp), &arrived);
    carried.round(&root, &mut snapshot, now_unix_nanos());
    let fine = verdict(&snapshot, &scope);
    assert_eq!(fine.health, Health::Fine, "{}", fine.evidence);
    assert!(
        fine.evidence
            .contains("1 document(s) delivered whole to Sales orders to ERP"),
        "{}",
        fine.evidence
    );
    assert!(
        !folder(&erp).join(&first).exists(),
        "taken off the Send Port's folder once judged"
    );
    assert!(
        arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty(),
        "an arrival is let go of once judged"
    );

    // The next, delivered at another Send Port than its route names.
    let second = dropped(&drop, 1);
    delivered(&second, (&drop, &astray), &arrived);
    carried.round(&root, &mut snapshot, now_unix_nanos());
    let done = verdict(&snapshot, &scope);
    assert_eq!(done.health, Health::Done, "{}", done.evidence);
    assert!(
        done.evidence.contains(&format!(
            "{second} at Invoices to Party-A, routed to Sales orders to ERP"
        )),
        "{}",
        done.evidence
    );
    drop_all(carried, &base);
}

/// The watches stopped before their folders go.
fn drop_all(carried: Carried, base: &Path) {
    drop(carried);
    let _ = std::fs::remove_dir_all(base);
}
