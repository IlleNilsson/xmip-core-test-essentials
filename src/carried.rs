//! Carried: the rig is the Parties and the company's ERP, trading real
//! documents through the cluster's configured flows (the owner, 2026-11-10:
//! *Do your Essentials test, promotion thing*; ADR-0028, amendments
//! 2026-10-22 and 2026-11-11). Each round it drops into each Receive
//! Location placed (`configure::placed`) the next of that Location's own
//! documents (`documents.rs`), named `<document>-<n>.edi`. The node routes it
//! by what the document says — its message type and recipient, promoted by
//! the cluster's Subscriptions — to a Send Port another node sends, and the
//! file arrives where that Send Port is placed. The receiving node writes it
//! to the Ledger on a Storage node, the sending node takes its Message from
//! there: neither keeps Xmip Storage itself.
//!
//! Where it drops and reads is a [`Spot`]: a folder of the run's for the
//! nodes it starts on this computer (`served`), and on the Parties' far
//! end for the nodes on machines of their own, each Location a user's
//! directory there (`far_end`) — the same placement either way.
//!
//! **The judge** (`judged.rs`): a file must arrive whole, its bytes the
//! ones dropped, where the Send Port its route names is placed; one at
//! another Send Port, or with other bytes, is Done. Each Receive Location's
//! verdict is published beneath it on the node it is placed on,
//! `<cluster>/node/<node>/receive/<Location>/delivery`: Fine while every
//! file arrived or is younger than [`GRACE`], Stressed while one is older
//! and not yet [`LOST`], Done naming it once one is, with the fastest and
//! the median time from drop to arrival and the Send Ports it was routed
//! to. The rig publishes no counts: the figures are the nodes'.
//!
//! **The fault every fifty seconds** (`rotation::struck`): where the run
//! strikes, the struck Location's Party also sends, once in its turn, a
//! real document no Subscription takes. The node keeps it in its Dead
//! Message Queue, where Replay acts on it, and the Location's verdict is
//! Stressed for the strike, saying why.
//!
//! Arrivals are told — by the file system for a folder, by the far end as a
//! client closes what it wrote — never looked for on a timer. Each drop and
//! each arrival is a recent item with where it came from and went to
//! (ADR-0032), the activity a run of served nodes publishes.

mod judged;
mod spot;

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use configure::placed::{Placed, Placement};
use observe::{Activity, Item, ItemKind, Snapshot};
use runtime::directory_watch::DirectoryWatch;

use crate::documents::{Document, Documents};
pub use spot::{Arrived, Binding, Spot, delivery, told};
use spot::{located, placed_name, seen, taken};

/// How long a file may take before it is overdue: a bound on the test,
/// never on Xmip.
pub const GRACE: Duration = Duration::from_secs(5);

/// How long an overdue file may take before it is lost.
pub const LOST: Duration = Duration::from_secs(60);

/// One file dropped: into which Location, when, its bytes, and the Send
/// Port its route names — none for a document no Subscription takes.
struct Dropped {
    location: usize,
    at: Instant,
    port: Option<String>,
    bytes: Vec<u8>,
}

/// One Receive Location placed, its spot, its own documents and the next.
struct Location {
    binding: Binding,
    documents: Vec<Document>,
    next: usize,
}

/// The drops, the files dropped into them, and what arrived.
pub struct Carried {
    drops: Vec<Location>,
    ports: BTreeMap<String, Binding>,
    unrouted: Vec<Document>,
    documents: Documents,
    dropped: BTreeMap<String, Dropped>,
    arrived: Arrived,
    judged: judged::Judged,
    activity: Activity,
    striking: bool,
    struck_turn: Option<u64>,
    _watching: Vec<DirectoryWatch>,
    next: u64,
}

impl Carried {
    /// Drive `bindings`: each folder made and each Send Port's watched; a
    /// far end's Send Port directories tell `arrived`, which they were made
    /// with ([`told`]). Each Receive Location takes its own documents.
    ///
    /// # Errors
    ///
    /// A folder that cannot be made or watched, the documents that do not
    /// read, or a Receive Location with none, in words.
    pub fn new(bindings: &[Binding], arrived: Arrived) -> Result<Self, String> {
        let documents = Documents::shipped()?;
        let mut watching = Vec::new();
        let mut ports = BTreeMap::new();
        let mut drops = Vec::new();
        for binding in bindings {
            // A Work Process runs on its nodes; the rig drops into and reads
            // nothing of it.
            if binding.placement.placed == Placed::Process {
                continue;
            }
            if let Spot::Folder(folder) = &binding.spot {
                std::fs::create_dir_all(folder)
                    .map_err(|error| format!("{}: {error}", folder.display()))?;
            }
            match binding.placement.placed {
                Placed::Send => {
                    if let Spot::Folder(folder) = &binding.spot {
                        let (seeing, telling) = (folder.clone(), Arc::clone(&arrived));
                        let port = binding.placement.name.clone();
                        let watch = DirectoryWatch::spawn(folder, move || {
                            seen(&seeing, &port, &telling, Instant::now());
                        })
                        .map_err(|error| format!("{}: {error}", folder.display()))?;
                        watching.push(watch);
                    }
                    ports.insert(binding.placement.name.clone(), binding.clone());
                }
                Placed::Receive => drops.push(Location {
                    binding: binding.clone(),
                    documents: documents
                        .of(&binding.placement.name, &binding.placement.folded())?
                        .to_vec(),
                    next: 0,
                }),
                Placed::Process => {}
            }
        }
        Ok(Self {
            drops,
            ports,
            unrouted: documents.unrouted().to_vec(),
            documents,
            dropped: BTreeMap::new(),
            arrived,
            judged: judged::Judged::default(),
            activity: Activity::with_capacity(observe::DEFAULT_ITEM_CAPACITY),
            striking: false,
            struck_turn: None,
            _watching: watching,
            next: 0,
        })
    }

    /// What arrives is recorded in: a new, empty record.
    #[must_use]
    pub fn arrivals() -> Arrived {
        Arc::default()
    }

    /// Whether the fault every fifty seconds strikes: the run's stress
    /// level says (`rotation::leveled`).
    pub fn strike(&mut self, striking: bool) {
        self.striking = striking;
    }

    /// One round: the next document dropped into each drop — into a folder
    /// written beside it and renamed in, as a producer that means it whole
    /// drops one; onto a far end whole at once — a document no Subscription
    /// takes into the struck one once in its turn, and each Location's
    /// judgment so far recorded beneath it on its node, under `root`.
    pub fn round(&mut self, root: &str, snapshot: &mut Snapshot, now: i64) {
        let seconds = u64::try_from(now / 1_000_000_000).unwrap_or(0);
        let struck = self
            .striking
            .then(|| crate::rotation::struck(self.drops.len(), seconds))
            .flatten();
        let mut failed: Vec<(usize, String)> = Vec::new();
        for location in 0..self.drops.len() {
            let place = &mut self.drops[location];
            let document = place.documents[place.next % place.documents.len()].clone();
            place.next += 1;
            if let Err(problem) = self.dropping(root, location, &document, now) {
                failed.push((location, problem));
            }
        }
        let turn = seconds / crate::rotation::PERIOD;
        if let Some(location) = struck.filter(|_| self.struck_turn != Some(turn)) {
            self.struck_turn = Some(turn);
            let index = usize::try_from(turn).unwrap_or(0) % self.unrouted.len().max(1);
            if let Some(document) = self.unrouted.get(index).cloned()
                && let Err(problem) = self.dropping(root, location, &document, now)
            {
                failed.push((location, problem));
            }
        }
        self.judge(root, now);
        for (location, place) in self.drops.iter().enumerate() {
            let problems: Vec<&str> = failed
                .iter()
                .filter(|(at, _)| *at == location)
                .map(|(_, problem)| problem.as_str())
                .collect();
            let unrouted = (struck == Some(location)).then(|| {
                self.unrouted
                    .iter()
                    .map(|document| document.stem.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            let scope = delivery(root, &place.binding.placement);
            snapshot.record_health(self.judged.record(
                &scope,
                location,
                (&problems, unrouted.as_deref()),
                now,
            ));
        }
    }

    /// `document` dropped into the Location at `location`, recorded with
    /// the Send Port its route names, and said as a recent item.
    fn dropping(
        &mut self,
        root: &str,
        location: usize,
        document: &Document,
        now: i64,
    ) -> Result<(), String> {
        let name = format!("{}-{}.edi", document.stem, self.next);
        self.next += 1;
        let binding = &self.drops[location].binding;
        let written = match &binding.spot {
            Spot::Folder(folder) => {
                let beside = folder.with_extension("beside");
                std::fs::create_dir_all(&beside)
                    .and_then(|()| std::fs::write(beside.join(&name), &document.bytes))
                    .and_then(|()| std::fs::rename(beside.join(&name), folder.join(&name)))
                    .map_err(|error| error.to_string())
            }
            Spot::FarEnd(directory) => {
                directory
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .files
                    .insert(name.clone(), document.bytes.clone());
                Ok(())
            }
        };
        written.map_err(|error| format!("{name} into {}: {error}", binding.placement.name))?;
        let port = self.documents.routed(&document.bytes).into_iter().next();
        let to = port.as_ref().map_or_else(
            || "no Subscription".to_string(),
            |port| self.at(port).map_or_else(|| port.clone(), placed_name),
        );
        self.activity.record(Item {
            kind: ItemKind::Stream,
            scope: located(root, &binding.placement),
            id: name.clone(),
            bytes: document.bytes.len() as u64,
            detail: format!("from {} → to {to}", placed_name(&binding.placement)),
            observed_unix_nanos: now,
        });
        self.dropped.insert(
            name,
            Dropped {
                location,
                at: Instant::now(),
                port,
                bytes: document.bytes.clone(),
            },
        );
        Ok(())
    }

    /// Where the Send Port `port` is placed.
    fn at(&self, port: &str) -> Option<&Placement> {
        self.ports.get(port).map(|binding| &binding.placement)
    }

    /// Every file that arrived since the last judgment, judged once and
    /// let go of: whole where its route sends it, elsewhere, or with other
    /// bytes; each arrival said as a recent item, and taken off the Send
    /// Port's spot, as its Party takes what it is delivered. What has not
    /// arrived is how long it has waited. Nothing it keeps grows with the
    /// run's age: an arrival is let go of once judged, one of no file this
    /// run dropped as it is seen, and a document no Subscription takes once
    /// it is older than [`LOST`].
    fn judge(&mut self, root: &str, now: i64) {
        let came: Vec<(String, (String, Instant))> = {
            let mut arrived = self.arrived.lock().unwrap_or_else(PoisonError::into_inner);
            let came = self
                .dropped
                .keys()
                .filter_map(|name| arrived.remove(name).map(|seen| (name.clone(), seen)))
                .collect();
            arrived.retain(|name, _| self.dropped.contains_key(name));
            came
        };
        let mut judged = Vec::new();
        for (name, (port, seen)) in &came {
            let (Some(dropped), Some(placed)) = (self.dropped.get(name), self.ports.get(port))
            else {
                continue;
            };
            let whole = taken(&placed.spot, name).is_some_and(|bytes| bytes == dropped.bytes);
            let took = seen.saturating_duration_since(dropped.at);
            let routed = (dropped.port.as_deref(), port.as_str());
            self.judged
                .arrived(name, dropped.location, routed, (whole, took));
            self.activity.record(Item {
                kind: ItemKind::Message,
                scope: format!(
                    "{root}/node/{}/send/{}",
                    placed.placement.node, placed.placement.name
                ),
                id: name.clone(),
                bytes: dropped.bytes.len() as u64,
                detail: format!(
                    "from {} → to {}",
                    placed_name(&self.drops[dropped.location].binding.placement),
                    placed_name(&placed.placement)
                ),
                observed_unix_nanos: now,
            });
            judged.push(name.clone());
        }
        for name in judged {
            self.dropped.remove(&name);
        }
        let at = Instant::now();
        self.dropped
            .retain(|_, dropped| dropped.port.is_some() || at.duration_since(dropped.at) < LOST);
        let waiting = self
            .dropped
            .iter()
            .filter(|(_, dropped)| dropped.port.is_some())
            .map(|(name, dropped)| {
                (
                    name.as_str(),
                    dropped.location,
                    at.duration_since(dropped.at),
                )
            });
        self.judged.waiting(waiting);
    }

    /// What was dropped and arrived, as recent items with their from and
    /// to: a run of served nodes' activity (ADR-0032).
    #[must_use]
    pub const fn activity(&self) -> &Activity {
        &self.activity
    }

    /// Wait, as the run ends, for every file dropped that a Subscription
    /// takes to arrive, for at most [`LOST`]: what is not carried by then
    /// is lost. Asked each tenth of a second, the carrying being what it
    /// waits out.
    pub fn settle(&self) {
        let deadline = Instant::now() + LOST;
        while Instant::now() < deadline {
            let arrived = self.arrived.lock().unwrap_or_else(PoisonError::into_inner);
            let mut routed = self
                .dropped
                .iter()
                .filter(|(_, dropped)| dropped.port.is_some());
            if routed.all(|(name, _)| arrived.contains_key(name)) {
                return;
            }
            drop(arrived);
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The judgment so far, in words.
    #[must_use]
    pub fn words(&self) -> String {
        let (delivered, routed) = self.judged.tally();
        let waiting = self
            .dropped
            .values()
            .filter(|dropped| dropped.port.is_some())
            .count();
        format!(
            "{delivered} of {} carried",
            routed + u64::try_from(waiting).unwrap_or(0)
        )
    }
}

#[cfg(test)]
mod tests;
