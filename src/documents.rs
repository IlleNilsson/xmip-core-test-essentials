//! The documents the Parties and the company's ERP trade, as real trading
//! Parties send them (the owner, 2026-11-10: *Do your Essentials test,
//! promotion thing*; ADR-0028, amendment 2026-11-11): UN/EDIFACT
//! interchanges of the latest published directory, D.24A — orders, order
//! responses, dispatch advices, invoices, remittance advices, receiving
//! advices, inventory reports, price catalogues and multiple payment
//! orders — written by hand, each Party's own ids in its interchange header
//! and the company's `ESSENTIALS`.
//!
//! They are data, not code: `documents/<Location>/` holds each Receive
//! Location's own, the folder named as `configure::placed::Placement::folded`
//! names it, and `documents/unrouted/` a real document no Subscription
//! takes, which the 50-second fault drops. A placed Receive Location with no
//! documents is REFUSED as the run starts, naming it.
//!
//! **The route is the product's.** Which Send Port a document goes to is
//! what the cluster's Subscriptions say of the properties promoted from it,
//! read as a node reads them: the `content` route technology over the `edi`
//! path language, gathered by `route::Gathering` and decided by
//! `route::publish`. Nothing here routes again. A document a Subscription
//! hands to a Work Process is followed through it by what its design
//! assigns, applied as a node applies it (`assign::apply`), and routed again
//! the same way (`crate::order_approval`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use assign::Assignment;
use context::MessageContext;
use message::{Message, MessageSection, MessageTreatment};
use route::{Gathering, Promoted, Subscriber, Subscription, publish};
use route_content::ContentSource;
use stream::Stream;
use xcore::{MessageId, SectionId, StreamId};

use crate::subscribing::CLUSTER;

/// The folder of a document no Subscription takes.
pub const UNROUTED: &str = "unrouted";

/// One document: what its file is called, less `.edi`, and its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Document {
    pub stem: String,
    pub bytes: Vec<u8>,
}

/// The shipped documents, by folder, and the cluster's Subscriptions with
/// what they read of a document.
pub struct Documents {
    by_folder: BTreeMap<String, Vec<Document>>,
    subscriptions: Vec<Subscription>,
    /// Each Work Process the rig follows a document through, by name, with
    /// what it assigns before it publishes (`crate::order_approval`).
    processes: Vec<(String, Vec<Assignment>)>,
    gathering: Gathering,
}

/// Where the documents are shipped: beside this crate's sources.
#[must_use]
pub fn shipped_folder() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("documents")
}

impl Documents {
    /// The documents shipped with the Essentials, and the Subscriptions of
    /// its cluster file.
    ///
    /// # Errors
    ///
    /// REFUSED, in words, where the folder or a document does not read, or
    /// the cluster file's Applications do not.
    pub fn shipped() -> Result<Self, String> {
        Self::read(&shipped_folder(), CLUSTER)
    }

    /// The documents in `folder`, routed by the Subscriptions of `cluster`.
    ///
    /// # Errors
    ///
    /// As [`Documents::shipped`].
    pub fn read(folder: &Path, cluster: &str) -> Result<Self, String> {
        let refused = |what: &Path, error: std::io::Error| {
            format!(
                "REFUSED: the Essentials' documents, {}: {error}",
                what.display()
            )
        };
        let mut by_folder: BTreeMap<String, Vec<Document>> = BTreeMap::new();
        for entry in std::fs::read_dir(folder).map_err(|error| refused(folder, error))? {
            let place = entry.map_err(|error| refused(folder, error))?.path();
            if !place.is_dir() {
                continue;
            }
            let mut documents = Vec::new();
            for file in std::fs::read_dir(&place).map_err(|error| refused(&place, error))? {
                let file = file.map_err(|error| refused(&place, error))?.path();
                if file.extension().is_none_or(|extension| extension != "edi") {
                    continue;
                }
                let bytes = std::fs::read(&file).map_err(|error| refused(&file, error))?;
                let stem = file
                    .file_stem()
                    .map_or_default(|stem| stem.to_string_lossy().into_owned());
                documents.push(Document { stem, bytes });
            }
            documents.sort_by(|a, b| a.stem.cmp(&b.stem));
            let name = place
                .file_name()
                .map_or_default(|name| name.to_string_lossy().into_owned());
            by_folder.insert(name, documents);
        }
        let applications = configure::applications(cluster)?;
        // What each Work Process whose unit is the Essentials' own assigns
        // before it publishes, so a document is followed through it.
        let assigned = crate::order_approval::assignments()?;
        let processes: Vec<(String, Vec<Assignment>)> = applications
            .iter()
            .flat_map(|application| application.work_processes.iter())
            .filter(|process| {
                process.module.as_deref() == Some(crate::order_approval::ORIGIN.module)
            })
            .map(|process| (process.name.clone(), assigned.clone()))
            .collect();
        let subscriptions: Vec<Subscription> = applications
            .into_iter()
            .flat_map(|application| application.subscriptions)
            .collect();
        let source = content();
        let sources: [&dyn route::Source; 1] = [&source];
        let gathering = Gathering::of(&sources, &subscriptions);
        if let Some(refused) = gathering.refusals().next() {
            return Err(format!(
                "REFUSED: a Subscription's filter cannot be read: {refused}"
            ));
        }
        Ok(Self {
            by_folder,
            subscriptions,
            processes,
            gathering,
        })
    }

    /// The documents of the Receive Location whose folded name is `folded`.
    ///
    /// # Errors
    ///
    /// REFUSED, naming it, where it has none.
    pub fn of(&self, location: &str, folded: &str) -> Result<&[Document], String> {
        self.by_folder
            .get(folded)
            .filter(|documents| !documents.is_empty())
            .map(Vec::as_slice)
            .ok_or_else(|| {
                format!(
                    "REFUSED: the Receive Location '{location}' has no documents to send; \
                     put them in documents/{folded}"
                )
            })
    }

    /// Every Location's documents, folder by folder, the unrouted left out:
    /// what a simulated relay routes its pairs as.
    #[must_use]
    pub fn routable(&self) -> Vec<Document> {
        self.by_folder
            .iter()
            .filter(|(folder, _)| folder.as_str() != UNROUTED)
            .flat_map(|(_, documents)| documents.iter().cloned())
            .collect()
    }

    /// The documents no Subscription takes, which a strike drops.
    #[must_use]
    pub fn unrouted(&self) -> &[Document] {
        self.by_folder.get(UNROUTED).map_or(&[], Vec::as_slice)
    }

    /// What the Subscriptions read of `bytes`, promoted as a node promotes
    /// it.
    ///
    /// # Errors
    ///
    /// The source's refusal, where the document does not read.
    pub fn promoted(&self, bytes: &[u8]) -> Result<Promoted, String> {
        self.promoted_in(bytes, MessageContext::new())
    }

    /// What the Subscriptions read of `bytes` in a Message whose context is
    /// `context`, promoted as a node promotes it.
    ///
    /// # Errors
    ///
    /// As [`Documents::promoted`].
    pub fn promoted_in(&self, bytes: &[u8], context: MessageContext) -> Result<Promoted, String> {
        self.gathering
            .promote(&received(bytes, context))
            .map_err(|error| error.to_string())
    }

    /// The Message the Work Processes publish of `bytes` before a Send Port
    /// takes it, as its context ([`Documents::processed`]) and what the
    /// Subscriptions read of it: what a node that runs no Work Process step
    /// of its own, as the Essentials' simulated node, publishes in its place
    /// (`crate::subscribing`).
    ///
    /// # Errors
    ///
    /// As [`Documents::promoted`].
    pub fn published(&self, bytes: &[u8]) -> Result<(MessageContext, Promoted), String> {
        let context = self.processed(bytes);
        let promoted = self.promoted_in(bytes, context.clone())?;
        Ok((context, promoted))
    }

    /// The context of the Message the Work Processes publish of `bytes` before
    /// a Send Port takes it: at each step the first Subscription that takes
    /// it, and where that one hands it to a Work Process the rig follows,
    /// what its design assigns. Empty where none does.
    #[must_use]
    pub fn processed(&self, bytes: &[u8]) -> MessageContext {
        let mut context = MessageContext::new();
        let mut depth = 0;
        while let Ok(promoted) = self.promoted_in(bytes, context.clone()) {
            let routing = publish(&promoted, &self.subscriptions);
            let first = routing
                .evaluations
                .iter()
                .find(|evaluation| evaluation.matched());
            let Some(Subscriber::WorkProcess(name)) = first.map(|one| &one.destination) else {
                break;
            };
            let Some((_, assigned)) = self.processes.iter().find(|(process, _)| process == name)
            else {
                break;
            };
            depth += 1;
            if !journey::ChainLimit::DEFAULT.permits(depth) {
                break;
            }
            context = assign::apply(context, assigned);
        }
        context
    }

    /// The destinations the cluster's Subscriptions route `bytes` to, each
    /// a Send Port's name, in the Subscriptions' order: none for a document
    /// no Subscription takes. A document a Subscription hands to a Work
    /// Process is followed through it: the Message it publishes, carrying
    /// what it assigned, is routed again as the node routes it
    /// (`runtime::work_step::acts`), so its destinations are where that
    /// Message goes.
    #[must_use]
    pub fn routed(&self, bytes: &[u8]) -> Vec<String> {
        let Ok((_, promoted)) = self.published(bytes) else {
            return Vec::new();
        };
        publish(&promoted, &self.subscriptions)
            .evaluations
            .iter()
            .filter(|evaluation| evaluation.matched())
            .filter(|evaluation| !matches!(evaluation.destination, Subscriber::WorkProcess(_)))
            .map(|evaluation| evaluation.destination.name().to_string())
            .collect()
    }
}

/// The `content` route technology over the path languages linked: what a
/// node built from the personal-computer site reads a filter with, made in
/// the one place xmip-service makes it.
#[must_use]
pub fn content() -> ContentSource {
    ContentSource::linked()
}

/// `bytes` as a Message received whole, its one section the document, its
/// context `context`.
fn received(bytes: &[u8], context: MessageContext) -> Message {
    let section = MessageSection {
        section_id: SectionId::new(1),
        name: None,
        stream: Stream::new(StreamId::new(1), bytes.to_vec(), None),
        contract: None,
    };
    Message::received(
        MessageId::new(1),
        vec![section],
        context,
        MessageTreatment::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_receive_location_has_documents_and_each_is_routed_to_one_send_port() {
        let documents = Documents::shipped().expect("the shipped documents read");
        let applications = configure::applications(CLUSTER).expect("the cluster reads");
        let ports: Vec<&str> = applications
            .iter()
            .flat_map(|application| &application.send_ports)
            .map(|port| port.name.as_str())
            .collect();
        let mut locations = 0;
        for application in &applications {
            for location in &application.receive_locations {
                let folded = configure::placed::Placement {
                    placed: configure::placed::Placed::Receive,
                    application: application.name.clone(),
                    name: location.name.clone(),
                    node: String::new(),
                }
                .folded();
                let own = documents
                    .of(&location.name, &folded)
                    .expect("its documents");
                for document in own {
                    let routed = documents.routed(&document.bytes);
                    assert_eq!(routed.len(), 1, "{}: {routed:?}", document.stem);
                    assert!(ports.contains(&routed[0].as_str()), "{routed:?}");
                }
                locations += 1;
            }
        }
        assert!(locations >= 8, "the Parties' and the ERP's, {locations}");
    }

    #[test]
    fn a_document_is_routed_by_its_message_type_and_its_recipient() {
        let documents = Documents::shipped().expect("read");
        let order = &documents
            .of("Sales orders from Party-A", "sales-orders-from-party-a")
            .expect("documents")[0];
        let promoted = documents.promoted(&order.bytes).expect("promoted");
        assert_eq!(promoted.get("content:edi:UNH/2/1"), Some("ORDERS"));
        assert_eq!(promoted.get("content:edi:UNB/3/1"), Some("ESSENTIALS"));
        assert_eq!(documents.routed(&order.bytes), ["Sales orders to ERP"]);
    }

    #[test]
    fn the_unrouted_document_is_taken_by_no_subscription_and_a_location_with_none_is_refused() {
        let documents = Documents::shipped().expect("read");
        let first = documents.unrouted().first();
        assert!(
            first.is_some(),
            "a document no Subscription takes is shipped"
        );
        for document in documents.unrouted() {
            assert_eq!(documents.routed(&document.bytes), Vec::<String>::new());
        }
        let refused = documents.of("Nowhere", "nowhere").expect_err("none");
        assert!(
            refused.starts_with("REFUSED") && refused.contains("Nowhere"),
            "{refused}"
        );
    }

    /// A sales order is handed to the Work Process "Order approval" alone,
    /// and the Message it publishes, marked approved, to the ERP's Send
    /// Port: the route the judge expects it at.
    #[test]
    fn a_sales_order_is_approved_by_the_work_process_before_the_erp_takes_it() {
        let documents = Documents::shipped().expect("read");
        let order = &documents
            .of("Sales orders from Party-C", "sales-orders-from-party-c")
            .expect("documents")[0];
        let promoted = documents.promoted(&order.bytes).expect("promoted");
        let first: Vec<Subscriber> = publish(&promoted, &documents.subscriptions)
            .evaluations
            .into_iter()
            .filter(route::Evaluation::matched)
            .map(|evaluation| evaluation.destination)
            .collect();
        assert_eq!(
            first,
            [Subscriber::WorkProcess("Order approval".to_string())]
        );
        let published = documents.processed(&order.bytes);
        assert_eq!(
            published.get("Approval"),
            Some(&xcore::ScalarValue::Text("approved".to_string()))
        );
        assert_eq!(documents.routed(&order.bytes), ["Sales orders to ERP"]);
        // Every other document goes where it went: no Work Process takes it.
        let remittance = &documents
            .of(
                "Remittance advices from Party-E",
                "remittance-advices-from-party-e",
            )
            .expect("documents")[0];
        assert!(
            documents
                .processed(&remittance.bytes)
                .get("Approval")
                .is_none()
        );
        assert_eq!(
            documents.routed(&remittance.bytes),
            ["Remittance advices to ERP"]
        );
    }
}
