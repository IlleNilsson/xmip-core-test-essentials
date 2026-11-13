//! The roll's drawing of the Essentials' own cluster file related and
//! stood by role, and the same with every node renamed (ADR-0052 and
//! ADR-0056, amendments 2026-11-13; the owner, 2026-11-12: *Related by its
//! role in the cluster, not by name*). The renamed copy gives the receiving
//! nodes the Storage nodes' names and the rest names with no letters to
//! read; read back through the renaming, the relations and every node's
//! band and column are the same.

use std::collections::{BTreeMap, BTreeSet};

use observe::topology::draw::CLUSTER;
use observe::topology::layout::{Band, Column, layout};
use observe::{Publication, Relation, Snapshot, Topology};

use super::cluster_topology;
use crate::parties::Parties;
use crate::report::roll_toml;
use crate::roster::Roster;
use crate::subscribing::CLUSTER as FILE;
use crate::support::cluster_root;

/// The roll's drawing of the cluster file `text`, before any traffic,
/// written and read back as a surface reads it.
fn drawn(text: &str) -> Topology {
    let roster = Roster::of_cluster(text).expect("the cluster file reads");
    let spread = Parties::configured().spread(&["file"]);
    let topology = cluster_topology(&Snapshot::new(), &roster, false, &[], (&spread, &[]), 9);
    let written = roll_toml(&cluster_root(), &Snapshot::new(), Some(topology), None, "");
    Publication::read(&written)
        .expect("reads back")
        .topology
        .expect("drawn")
}

/// The file with every node renamed by `renaming`, through placeholders so
/// two names swapped never meet.
fn renamed(renaming: &BTreeMap<String, String>) -> String {
    let mut text = FILE.to_string();
    let forms = |name: &str| {
        [
            format!("[nodes.{name}]"),
            format!("[nodes.{name}."),
            format!("\"{name}:"),
            format!("\"{name}\""),
        ]
    };
    for (at, old) in renaming.keys().enumerate() {
        for (form, held) in forms(old).iter().zip(forms(&format!("@{at}@"))) {
            text = text.replace(form, &held);
        }
    }
    for (at, new) in renaming.values().enumerate() {
        for (form, held) in forms(new).iter().zip(forms(&format!("@{at}@"))) {
            text = text.replace(&held, form);
        }
    }
    text
}

/// Each relation between two nodes, by their names, and what it is.
fn related(topology: &Topology) -> BTreeSet<(String, String, Relation)> {
    let name = |id: &str| id.strip_prefix("node/").map(str::to_string);
    topology
        .links
        .iter()
        .filter(|link| link.relation != Relation::Traffic)
        .filter_map(|link| Some((name(&link.from)?, name(&link.to)?, link.relation)))
        .collect()
}

/// Each node's band and column, by its name.
fn stands(topology: &Topology) -> BTreeMap<String, (Band, Column)> {
    layout(topology, CLUSTER, &[])
        .into_iter()
        .filter_map(|place| {
            let name = place.id.strip_prefix("node/")?.to_string();
            Some((name, (place.band, place.column)))
        })
        .collect()
}

#[test]
fn a_renamed_cluster_file_draws_the_same_relations_and_stands_each_node_the_same() {
    let declared = configure::cluster::roles(FILE).expect("the cluster file reads");
    let declaring = |role: &str| -> Vec<String> {
        declared
            .iter()
            .filter(|(_, roles)| roles.iter().any(|one| one == role))
            .map(|(name, _)| name.clone())
            .collect()
    };
    let mut renaming: BTreeMap<String, String> = BTreeMap::new();
    for (one, other) in declaring("receiving").iter().zip(&declaring("storage")) {
        renaming.insert(one.clone(), other.clone());
        renaming.insert(other.clone(), one.clone());
    }
    for (at, (name, _)) in declared.iter().enumerate() {
        renaming
            .entry(name.clone())
            .or_insert_with(|| format!("n{}-{at}", 7 * at + 3));
    }
    let back: BTreeMap<&String, &String> = renaming.iter().map(|(old, new)| (new, old)).collect();

    let (first, again) = (drawn(FILE), drawn(&renamed(&renaming)));
    let relations = related(&first);
    assert!(
        relations
            .iter()
            .any(|(.., relation)| *relation == Relation::CopiesTo),
        "the file's Storage nodes are related: {relations:?}"
    );
    let read_back: BTreeSet<(String, String, Relation)> = related(&again)
        .into_iter()
        .map(|(from, to, relation)| (back[&from].clone(), back[&to].clone(), relation))
        .collect();
    assert_eq!(relations, read_back);

    let (stood, stood_again) = (stands(&first), stands(&again));
    assert_eq!(stood.len(), declared.len(), "{stood:?}");
    for (name, at) in &stood {
        assert_eq!(Some(at), stood_again.get(&renaming[name]), "{name}");
    }
}
