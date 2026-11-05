//! The tree the owner asked for, 2026-09-19: *even clusters have to be
//! spawned as processes during tests*.

use configure::fixture::test_cluster;
use xmip_core_test_essentials::cluster::{Orders, Spawned};
use xmip_core_test_essentials::{Roster, Stress};

use crate::{alone, cluster_binary, path, path_roster, scratch};

/// The test spawns a cluster, the cluster spawns its nodes, each publishes,
/// and the roll's half reads one file.
#[test]
fn a_spawned_cluster_publishes_its_nodes_and_stops_them_with_itself() {
    let _alone = alone();
    let dir = scratch("spawned");
    let test = test_cluster();
    let [receiving, processing, sending] = path(&test);
    let roster = Roster::parse(&path_roster(&test)).expect("a well-formed roster");
    let orders = Orders::of(Stress::Calm, roster, 0)
        .driving(&["round-trip".to_string()])
        .with_online(Some(vec![receiving.to_string()]));
    let published_to = dir.join(format!("{}-cluster.xmip", test.name));
    let mut cluster = Spawned::start(
        cluster_binary(),
        &test.name,
        &orders,
        &dir.join("shared"),
        &published_to,
    )
    .expect("the cluster process starts");

    let root = test.scope();
    let mut published = false;
    for _ in 0..40 {
        let snapshot = cluster.tick();
        published = !snapshot
            .health(&format!("{root}/node/{sending}/send"))
            .is_empty();
        if published {
            break;
        }
    }
    assert!(
        published,
        "{sending} published its send stage through the cluster"
    );

    let snapshot = cluster.tick();
    for name in [receiving, processing, sending] {
        let process = format!("{root}/node/{name}/system-process");
        assert!(
            !snapshot.health(&process).is_empty(),
            "the cluster says whether {name} is alive"
        );
    }
    assert!(
        !snapshot.health(&format!("{root}/node")).is_empty(),
        "the cluster adds the rollup its surface owes"
    );
    let links: Vec<(&str, &str)> = cluster
        .hops()
        .iter()
        .map(|hop| (hop.from.as_str(), hop.to.as_str()))
        .collect();
    assert!(links.contains(&(receiving, processing)), "{links:?}");
    assert!(links.contains(&(processing, sending)), "{links:?}");

    cluster.stop();
    assert!(!cluster.alive(), "the cluster left when it was asked");
    std::fs::remove_dir_all(&dir).ok();
}
