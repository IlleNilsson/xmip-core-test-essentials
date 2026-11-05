//! The exclusive claim across processes (ADR-0024): two node processes
//! over one lane.

use std::process::{Child, Command};

use configure::fixture::test_cluster;
use observe::Health;
use xmip_core_test_essentials::cluster_root;
use xmip_core_test_essentials::exclusive_claim::drained;
use xmip_core_test_essentials::report::node_from_toml;

use crate::{alone, node_binary, scratch};

/// Two real System Processes over one shared directory: the property
/// ADR-0024's claim exists to prove, `O_EXCL` across processes. The test
/// stages a lane of a thousand items both nodes find on their first scan, so
/// their readers contend for the same items, and judges the ledger itself;
/// both nodes' own verdicts must hold as well.
#[test]
fn two_processes_never_both_claim_the_same_item() {
    let _alone = alone();
    let dir = scratch("two-processes");
    let shared = dir.join("shared");
    let contended = shared.join("exclusive-claim/parallel/contended-0");
    let staging = contended.with_file_name(".contended-0");
    std::fs::create_dir_all(&staging).expect("staging dir");
    for n in 0..1_000 {
        std::fs::write(staging.join(format!("item_0_{n}")), b"x").expect("an item");
    }
    std::fs::rename(&staging, &contended).expect("the lane into place");
    let cluster = test_cluster();
    let names = [0, 1].map(|place| cluster.node(place).name.as_str());

    let children: Vec<Child> = names
        .iter()
        .map(|name| {
            Command::new(node_binary())
                .env("XMIP_ESSENTIALS_CLUSTER", &cluster.name)
                .args(["--name", name, "--stress", "calm", "--rounds", "4"])
                .args(["--interval-ms", "0"])
                .arg("--shared")
                .arg(&shared)
                .arg("--snapshot")
                .arg(dir.join(format!("{name}.xmip")))
                .spawn()
                .expect("spawn the node binary")
        })
        .collect();
    for mut child in children {
        let status = child.wait().expect("wait for the node");
        assert!(status.success(), "a node exits cleanly: {status}");
    }

    let lane = drained(&contended);
    assert_eq!(lane.left, 0, "every item was taken");
    assert_eq!(lane.held_twice, 0, "no item has two holders");
    assert_eq!(lane.held, 1_000, "every item has one holder");
    assert_eq!(lane.processes, 2, "both processes took items from the lane");

    let root = cluster_root();
    for name in names {
        let text = std::fs::read_to_string(dir.join(format!("{name}.xmip"))).expect("snapshot");
        let (snapshot, _) = node_from_toml(&text).expect("a node's snapshot parses");
        let scope = format!("{root}/node/{name}/exclusive-claim/file");
        for record in snapshot.health(&scope) {
            assert_eq!(
                record.health,
                Health::Fine,
                "{}: {}",
                record.scope,
                record.evidence
            );
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}
