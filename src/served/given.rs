//! What the run gives each node it starts, as an environment gives a
//! machine's node (`super`): the bindings of the file's Applications, its
//! slice of the cluster's file with its addresses on loopback and the
//! Storage nodes' names resolved, and its certificate from the run's
//! authority.

use std::path::{Path, PathBuf};

use toml::{Table, Value};

use configure::placed::{self, Placed, Placement};
use node::{Capability, NodeRole};

use crate::carried::{Binding, Spot};
use crate::subscribing::CLUSTER;

/// A port no one holds on loopback now.
fn free() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(|error| format!("no port on loopback: {error}"))
}

/// Each Receive Location, Send Port and Work Process the file designs
/// placed over the run's nodes `declared` by what each declares
/// (`configure::placed`), a Receive Location a drop folder of its own, a
/// Send Port a folder the rig reads; work no node of the run satisfies
/// refused, as `xmip-service --slice --bind` refuses it.
pub(super) fn placed(
    declared: &[(String, Capability)],
    area: &Path,
) -> Result<Vec<Binding>, String> {
    Ok(placed::placements(CLUSTER, declared)?
        .into_iter()
        .map(|placement| {
            let under = match placement.placed {
                Placed::Receive => "drop",
                Placed::Send => "out",
                // Never made: the rig drops into and reads nothing of a
                // Work Process (`crate::carried`).
                Placed::Process => "process",
            };
            let folder = area.join(under).join(placement.folded());
            Binding {
                placement,
                spot: Spot::Folder(folder),
            }
        })
        .collect())
}

/// The cluster's file as the run gives it to the nodes it starts: the
/// Essentials' own with the `bindings` the run placed, each node's roles as
/// the run declares them, each Storage node's name resolved to the address
/// on loopback it serves at, each node its own data folder, its local
/// endpoint named for the run's cluster, its operate listener on
/// loopback, and the account the run runs as its one relay, for the
/// tester's web host (`[operate] relay`), and the tester among its
/// operators, by that account and by its operator certificate ([`testers`]).
pub(super) fn cluster_text(
    cluster: &str,
    declared: &[(String, Capability)],
    bindings: &[Binding],
) -> Result<String, String> {
    let placements: Vec<Placement> = bindings.iter().map(|one| one.placement.clone()).collect();
    let bound = placed::written(&placements, |placement| {
        let folder = bindings
            .iter()
            .find(|one| one.placement == *placement)
            .and_then(|one| match &one.spot {
                Spot::Folder(folder) => Some(folder.display().to_string().replace('\\', "/")),
                Spot::FarEnd(_) => None,
            });
        placed::Binding {
            address: folder.unwrap_or_default(),
            settings: Vec::new(),
            secret: None,
            accepting: placement.placed == Placed::Receive,
        }
    });
    let mut file: Table = format!("{CLUSTER}\n{bound}")
        .parse()
        .map_err(|error| format!("the cluster's file: {error}"))?;
    let mut serving: Vec<(String, String)> = Vec::new();
    for (name, declares) in declared {
        if declares.roles().contains(&NodeRole::Storage) {
            serving.push((name.clone(), format!("127.0.0.1:{}", free()?)));
        }
    }
    let resolved = |listed: &str| {
        let host = listed.rsplit_once(':').map_or(listed, |(host, _)| host);
        serving
            .iter()
            .find(|(name, _)| name == host)
            .map_or_else(|| listed.to_string(), |(_, at)| at.clone())
    };
    let storage = table(&mut file, &["storage"]);
    if let Some(Value::Array(nodes)) = storage.get_mut("nodes") {
        for listed in nodes.iter_mut() {
            if let Value::String(text) = listed {
                *text = resolved(text);
            }
        }
    }
    let tester = runtime::plane::local::running_as()?;
    let mut operators = vec![Value::String(tester.clone())];
    operators.extend(testers()?.into_iter().map(Value::String));
    for (name, declares) in declared {
        let node = table(&mut file, &["nodes", name]);
        node.insert("roles".to_string(), Value::String(declares.words()));
        table(&mut file, &["nodes", name, "service"])
            .insert("data".to_string(), Value::String("data".to_string()));
        let operate = table(&mut file, &["nodes", name, "operate"]);
        operate.insert("local".to_string(), Value::String(endpoint(cluster, name)));
        operate.insert(
            "listen".to_string(),
            Value::String(format!("127.0.0.1:{}", free()?)),
        );
        // The tester's web host runs as the tester, as the run does, and
        // relays the tester's browser to the node, which decides by the
        // browser's own certificate (ADR-0009, amendment 2026-11-01). The
        // tester operates each node by name (amendment 2026-11-07): on its
        // local endpoint by the account it runs as, and in the browser by
        // the operator certificate the Essentials' authority signed it.
        operate.insert(
            "relay".to_string(),
            Value::Array(vec![Value::String(tester.clone())]),
        );
        operate.insert("operators".to_string(), Value::Array(operators.clone()));
        if let Some((_, at)) = serving.iter().find(|(serving, _)| serving == name) {
            table(&mut file, &["nodes", name, "storage"])
                .insert("listen".to_string(), Value::String(at.clone()));
        }
    }
    toml::to_string(&file).map_err(|error| format!("the cluster's file: {error}"))
}

/// The testers: every person whose operator certificate the Essentials'
/// authority signed and still stands — the one `New-XmipCertificate
/// -Request` has signed for the tester before the web host starts — each
/// `CN=<person>`, as `Start-XmipOperationWeb -Tester` names the tester to
/// the web host. None where the authority signed none.
///
/// # Errors
///
/// Where the authority's register cannot be read.
fn testers() -> Result<Vec<String>, String> {
    let now = observe::now_unix_nanos() / 1_000_000_000;
    let register = provision::register::Register::at(&crate::environment::authority());
    let records = register
        .records()
        .map_err(|error| format!("the Essentials' authority: {error}"))?;
    Ok(records
        .into_iter()
        .filter(|record| record.purpose == provision::Purpose::Operator.word())
        .filter(|record| matches!(record.standing(now), "current" | "renewal due"))
        .map(|record| format!("CN={}", record.subject))
        .collect())
}

/// The table at `path` in `file`, made where it is not.
fn table<'a>(file: &'a mut Table, path: &[&str]) -> &'a mut Table {
    let mut at = file;
    for key in path {
        let entry = at
            .entry((*key).to_string())
            .or_insert_with(|| Value::Table(Table::new()));
        if !entry.is_table() {
            *entry = Value::Table(Table::new());
        }
        at = entry
            .as_table_mut()
            .unwrap_or_else(|| unreachable!("made a table"));
    }
    at
}

/// The local endpoint the run names the node `node` of `cluster`'s, so two
/// clusters' runs on one computer never meet on `xmip-<node>`.
#[must_use]
pub fn endpoint(cluster: &str, node: &str) -> String {
    format!("xmip-{cluster}-{node}")
}

/// `node`'s configuration, sliced from `text` under `cluster` into a folder
/// of its own in `area`, with its certificate from the Essentials'
/// authority (`environment::authority`) beside it: the configuration's
/// path.
pub(super) fn configured(
    text: &str,
    (cluster, node): (&str, &str),
    area: &Path,
) -> Result<PathBuf, String> {
    let folder = area.join(node);
    std::fs::create_dir_all(&folder).map_err(|error| format!("{}: {error}", folder.display()))?;
    let sliced = configure::slice_named(text, node, cluster)?;
    let path = folder.join(format!("{node}.xmip"));
    let now = observe::now_unix_nanos() / 1_000_000_000;
    let authority = crate::environment::authority();
    let name = authority_name(&authority)?;
    let issuer = provision::host::authority(&authority, &name, now)
        .map_err(|error| format!("the Essentials' authority: {error}"))?;
    runtime::signed::delivered(&path, &issuer, text, &sliced)?;
    provision::node::issue_into(
        &authority,
        &name,
        node,
        &["127.0.0.1".to_string(), "localhost".to_string()],
        &folder.join("tls"),
        now,
    )
    .map_err(|error| format!("{node}'s certificate: {error}"))?;
    Ok(path)
}

/// The name the Essentials' authority kept in `place` was made under, read
/// from its certificate: `Initialize-XmipEssentialsAuthority` alone names
/// it, and makes it before a run starts any node.
///
/// # Errors
///
/// Where no authority is kept there, or its certificate names nothing.
fn authority_name(place: &Path) -> Result<String, String> {
    let path = place.join(provision::issuer::CERTIFICATE);
    let text = std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "no Essentials authority at {}: {error}; Start-XmipTest makes it",
            path.display()
        )
    })?;
    tls::certificate::Certificate::first(&text)
        .and_then(|parsed| parsed.common_name())
        .map_err(|error| format!("{}: {error}", path.display()))?
        .ok_or_else(|| {
            format!(
                "the Essentials' authority at {} names nothing",
                path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_runs_issue_their_nodes_under_the_one_essentials_authority() {
        // A node that keeps Xmip Storage and one that does not, as the
        // Essentials' own file declares them, and the test cluster's name.
        let roles = configure::cluster::capabilities(CLUSTER).expect("the file's roles");
        let storage = |node: &&(String, Capability)| node.1.roles().contains(&NodeRole::Storage);
        let declared: Vec<(String, Capability)> = [
            roles.iter().find(storage),
            roles.iter().find(|node| !storage(node)),
        ]
        .into_iter()
        .map(|node| node.expect("a node of each kind").clone())
        .collect();
        let cluster = crate::cluster_name().expect("the test cluster is named");
        let base = std::env::temp_dir().join(format!("xmip-given-{}", std::process::id()));
        let now = observe::now_unix_nanos() / 1_000_000_000;
        let issuer = provision::Issuer::kept_or_made(
            &crate::environment::authority(),
            "Xmip Essentials test authority",
            now,
            3600,
        )
        .expect("the authority Start-XmipTest makes");
        let mut anchors = Vec::new();
        let mut nodes = Vec::new();
        for run in ["first", "second"] {
            let area = base.join(run);
            let text = cluster_text(
                &cluster,
                &declared,
                &placed(&declared, &area).expect("placed"),
            )
            .expect("the cluster's text");
            for (node, _) in &declared {
                nodes.push(configured(&text, (&cluster, node), &area).expect("configured"));
                let tls = area.join(node).join("tls");
                anchors
                    .push(std::fs::read(tls.join(provision::node::ANCHORS_FILE)).expect("anchors"));
            }
        }
        let authority = crate::environment::authority().join(provision::issuer::CERTIFICATE);
        let kept = std::fs::read(authority).expect("the Essentials' authority");
        assert!(
            anchors.iter().all(|anchor| *anchor == kept),
            "one authority for every run"
        );

        // The tester's operator certificate, signed by that same authority as
        // `New-XmipCertificate -Request` has it signed, is one every node the
        // runs started relays for the tester's web host, which runs as the
        // run does (ADR-0009, the second amendment of 2026-11-01).
        let asked = provision::operator::request("tester@example.org").expect("a request");
        let der = codec::pem::decode(provision::operator::REQUEST_LABEL, &asked.request)
            .expect("the request")
            .remove(0);
        let chain = issuer
            .sign(&der, provision::Purpose::Operator, now - 60, now + 3600)
            .expect("signed");
        let leaf = codec::pem::decode("CERTIFICATE", &chain)
            .expect("the chain")
            .remove(0);
        // And the web host's listener, which `Start-XmipOperationWeb` has the
        // same authority issue, is one each node's anchors hold, as the
        // browser's handshake is checked against (ADR-0009, amendment
        // 2026-11-02).
        let place = crate::environment::authority();
        let host = provision::host::issue_into(
            &place,
            &authority_name(&place).expect("named"),
            "127.0.0.1",
            &base.join("host"),
            now,
        )
        .expect("the host's certificate");
        let host = std::fs::read_to_string(host.certificate).expect("read");
        let host = codec::pem::decode("CERTIFICATE", &host)
            .expect("the host's chain")
            .remove(0);
        let tester = runtime::plane::local::running_as().expect("the run's account");
        for path in &nodes {
            assert!(
                relayed(path, &tester, &leaf, &host),
                "{} relays the tester",
                path.display()
            );
        }

        // A run started once the tester holds that certificate lists the
        // tester an operator on every node, by the run's account and by the
        // certificate's subject (ADR-0009, amendment 2026-11-07).
        let later = base.join("third");
        let text = cluster_text(
            &cluster,
            &declared,
            &placed(&declared, &later).expect("placed"),
        )
        .expect("the cluster's text");
        let file: Table = text.parse().expect("TOML");
        for (node, _) in &declared {
            let operators = file["nodes"][node]["operate"]["operators"]
                .as_array()
                .expect("operators");
            for name in [tester.as_str(), "CN=tester@example.org"] {
                assert!(
                    operators.iter().any(|it| it.as_str() == Some(name)),
                    "{name}"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    // Whether the node configured at `path` relays for `through` the caller
    // `leaf` (DER) proves, met at the web host's listener `host` (DER): its
    // own `[operate] relay` and `anchors`, read as the node reads them, and
    // its own verification of both (`plane::vouched::verified`, as
    // `plane::possessed` asks it).
    fn relayed(path: &Path, through: &str, leaf: &[u8], host: &[u8]) -> bool {
        let text = std::fs::read_to_string(path).expect("the node's configuration");
        let table: Table = text.parse().expect("TOML");
        let operate = table["operate"].as_table().expect("[operate]");
        let relay: Vec<String> = operate["relay"]
            .as_array()
            .expect("relay")
            .iter()
            .filter_map(|named| named.as_str().map(str::to_string))
            .collect();
        let anchors = path
            .parent()
            .expect("its folder")
            .join(operate["anchors"].as_str().expect("anchors"));
        let anchors = std::fs::read_to_string(anchors).expect("the node's anchors");
        let relaying = runtime::plane::vouched::Relaying::new(relay, &anchors).expect("relaying");
        let verified = |certificate: &[u8], usage| {
            runtime::plane::vouched::verified(&relaying, &[certificate.to_vec()], usage).is_ok()
        };
        relaying.relays(through)
            && verified(leaf, authenticate::x509::Usage::ClientAuth)
            && verified(host, authenticate::x509::Usage::Any)
    }
}
