//! The roll carries an operator's orders to the nodes it reads (ADR-0028,
//! amendment 2026-11-11). A surface reading the roll's snapshot leaves an
//! order where the snapshot says the cluster takes them,
//! `<shared>/orders/<node>` (`observe::Order::place`). The simulated nodes
//! take theirs there each round (`operator_orders`); a node started as
//! `xmip-service` or on a machine of its own takes orders only over its
//! operate listener, so the roll watches each such node's place and,
//! told by the operating system that an order was left, takes it and hands
//! it to that node's [`Act`] at once. Until 2026-11-11 nothing took them,
//! and a Pause, Replay, Retry or Dismiss from the Web or Desktop over the
//! roll's snapshot never reached a served node.
//!
//! An order the node refuses, or one that is no order, is audited as the
//! failure to `order`, in the words that refused it.

use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use observe::Order;
use runtime::directory_watch::DirectoryWatch;
use xaudit::program_audit::ProgramAudit;

use crate::process_audit;

/// How an order reaches one node: what it came to, or the refusal in words.
pub type Act = Arc<dyn Fn(&Order) -> Result<(), String> + Send + Sync>;

/// The places watched, one per node an order is carried to.
pub struct Carrying {
    _watching: Vec<DirectoryWatch>,
}

impl Carrying {
    /// Watch the place beneath `orders` of each node of `nodes` — its scope,
    /// `xmip:///<cluster>/node/<name>`, and its [`Act`] — and carry every
    /// order left there to it: those already left first, then each as it
    /// is told of. A place of a node not named here is left to whoever reads
    /// it.
    ///
    /// # Errors
    ///
    /// A scope that names no node, or a place that cannot be made or
    /// watched, in words.
    pub fn start(
        orders: &Path,
        nodes: Vec<(String, Act)>,
        audit: &ProgramAudit,
    ) -> Result<Self, String> {
        let mut watching = Vec::new();
        for (node, act) in nodes {
            let place = Order::place(orders, &node)?;
            std::fs::create_dir_all(&place)
                .map_err(|error| format!("{}: {error}", place.display()))?;
            let (orders, audit) = (orders.to_path_buf(), audit.clone());
            // One carrying at a time: the watch's and the first take below
            // never read the same place at once.
            let one = Mutex::new(());
            let taking = Arc::new(move || {
                let _one = one.lock().unwrap_or_else(PoisonError::into_inner);
                carry(&orders, &node, act.as_ref(), &audit);
            });
            let told = Arc::clone(&taking);
            // Watched first, then what was left before taken once: an order
            // left in between is told of, never left waiting for the next.
            let watch = DirectoryWatch::spawn(&place, move || told())
                .map_err(|error| format!("{}: {error}", place.display()))?;
            taking();
            watching.push(watch);
        }
        Ok(Self {
            _watching: watching,
        })
    }
}

impl Carrying {
    /// [`Carrying::start`] for every node of `real` — the nodes the run
    /// reads, each beneath `root` — over its operate listener
    /// (`RealNode::acting`).
    ///
    /// # Errors
    ///
    /// As [`Carrying::start`].
    pub fn reaching(
        orders: &Path,
        root: &str,
        real: &[crate::real::RealNode],
        audit: &ProgramAudit,
    ) -> Result<Self, String> {
        let nodes = real
            .iter()
            .map(|node| (format!("{root}/node/{}", node.name), node.acting()))
            .collect();
        Self::start(orders, nodes, audit)
    }
}

/// Every order left for `node` under `orders`, oldest first, handed to
/// `act`; each taken from its place as it is read.
fn carry(
    orders: &Path,
    node: &str,
    act: &(dyn Fn(&Order) -> Result<(), String> + Send + Sync),
    audit: &ProgramAudit,
) {
    for taken in Order::take(orders, node) {
        let carried = taken
            .map_err(|problem| format!("an order no node can take: {problem}"))
            .and_then(|order| act(&order));
        if let Err(problem) = carried {
            process_audit::fail(audit, "order", &problem);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use observe::{Act as Acted, Noun};

    use crate::support::test_cluster;

    fn order(node: &str, target: &str) -> Order {
        Order {
            node: node.to_string(),
            noun: Noun::Subscription,
            target: target.to_string(),
            act: Acted::Pause,
            who: "ilian".to_string(),
        }
    }

    #[test]
    fn an_order_left_for_a_served_node_is_carried_to_it_and_one_for_another_is_left() {
        let cluster = test_cluster();
        let (served, simulated) = (cluster.node_scope(0), cluster.node_scope(1));
        let orders = std::env::temp_dir().join(format!("essentials-orders-{}", std::process::id()));
        std::fs::remove_dir_all(&orders).ok();
        let handed: Arc<Mutex<Vec<String>>> = Arc::default();
        let into = Arc::clone(&handed);
        let act: Act = Arc::new(move |order: &Order| {
            into.lock().expect("not poisoned").push(format!(
                "{} {}",
                order.act.word(),
                order.target
            ));
            Ok(())
        });
        let audit = ProgramAudit::new("xmip-essentials-test", None);
        let before = order(&served, "Sales orders");
        before.leave(&orders).expect("left before the watch");

        let _carrying =
            Carrying::start(&orders, vec![(served.clone(), act)], &audit).expect("watched");
        order(&served, "Invoices").leave(&orders).expect("left");
        let other = order(&simulated, "Payments").leave(&orders).expect("left");

        let deadline = Instant::now() + Duration::from_secs(5);
        while handed.lock().expect("not poisoned").len() < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            *handed.lock().expect("not poisoned"),
            ["pause Sales orders", "pause Invoices"]
        );
        let place = Order::place(&orders, &served).expect("a node");
        assert_eq!(
            std::fs::read_dir(&place).expect("the place").count(),
            0,
            "taken from its place"
        );
        assert!(other.exists(), "a simulated node's order is left for it");
        std::fs::remove_dir_all(&orders).ok();
    }
}
