//! Carried's judge: what became of each file dropped into a Receive
//! Location, and the Location's verdict from it (ADR-0028, amendment
//! 2026-11-11). A file is judged once, as it arrives: whole at the Send Port
//! its route names, at another Send Port, or with other bytes than were
//! dropped. A Location's verdict keeps the last [`KEPT`] times a file took,
//! and every file that did not arrive whole where it should.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use observe::{Health, HealthRecord};

use super::{GRACE, LOST};

/// How many of a Location's latest arrivals its fastest and median are
/// told from: ten minutes of one a second.
pub const KEPT: usize = 600;

/// What became of the files dropped into each Location, by its place.
#[derive(Default)]
pub struct Judged {
    locations: BTreeMap<usize, Judgment>,
    /// Every file routed and not yet arrived, with its Location and how
    /// long it has waited.
    waiting: Vec<(String, usize, Duration)>,
}

/// One Location's files: how long each whole one took, where they went,
/// and what went wrong.
#[derive(Default)]
struct Judgment {
    took: VecDeque<Duration>,
    delivered: u64,
    ports: BTreeSet<String>,
    elsewhere: Vec<String>,
    broken: Vec<String>,
}

impl Judged {
    /// The file `name`, dropped into the Location at `location`, arrived at
    /// the Send Port `port` where its route names `routed`: whole or not,
    /// and how long it took.
    pub fn arrived(
        &mut self,
        name: &str,
        location: usize,
        (routed, port): (Option<&str>, &str),
        (whole, took): (bool, Duration),
    ) {
        let judgment = self.locations.entry(location).or_default();
        if routed != Some(port) {
            judgment.elsewhere.push(format!(
                "{name} at {port}, routed to {}",
                routed.unwrap_or("no Send Port")
            ));
        } else if !whole {
            judgment.broken.push(format!("{name} at {port}"));
        } else {
            judgment.delivered += 1;
            judgment.ports.insert(port.to_string());
            judgment.took.push_back(took);
            if judgment.took.len() > KEPT {
                judgment.took.pop_front();
            }
        }
    }

    /// The files routed and not yet arrived, each with its Location and how
    /// long it has waited: what overdue and lost are told from.
    pub fn waiting<'a>(&mut self, files: impl Iterator<Item = (&'a str, usize, Duration)>) {
        self.waiting = files
            .map(|(name, location, waited)| (name.to_string(), location, waited))
            .collect();
    }

    /// How many files arrived whole where their route sends them, and how
    /// many routed files arrived at all.
    #[must_use]
    pub fn tally(&self) -> (u64, u64) {
        self.locations
            .values()
            .fold((0, 0), |(whole, all), judgment| {
                let wrong = (judgment.elsewhere.len() + judgment.broken.len()) as u64;
                (whole + judgment.delivered, all + judgment.delivered + wrong)
            })
    }

    /// The Location at `location`'s verdict at `scope`: Done where a file
    /// was not dropped, `problems`, arrived elsewhere or broken, or is
    /// lost; Stressed while `unrouted` names a document no Subscription
    /// takes that was dropped there this turn, or a file is overdue; Fine
    /// otherwise, saying how long its files took and where they went.
    #[must_use]
    pub fn record(
        &self,
        scope: &str,
        location: usize,
        (problems, unrouted): (&[&str], Option<&str>),
        now: i64,
    ) -> HealthRecord {
        let empty = Judgment::default();
        let judgment = self.locations.get(&location).unwrap_or(&empty);
        let mine = || self.waiting.iter().filter(move |one| one.1 == location);
        let lost: Vec<&str> = mine()
            .filter(|one| one.2 >= LOST)
            .map(|one| one.0.as_str())
            .collect();
        let overdue = mine().filter(|one| one.2 >= GRACE && one.2 < LOST).count();
        let carried = carried(judgment, mine().count());
        let named = |files: &[String]| files.iter().take(3).cloned().collect::<Vec<_>>().join("; ");
        let (health, evidence) = if !problems.is_empty() {
            (
                Health::Done,
                format!("not dropped: {}; {carried}", problems.join("; ")),
            )
        } else if !judgment.elsewhere.is_empty() {
            let count = judgment.elsewhere.len();
            let evidence = format!(
                "{count} at another Send Port than its route names: {}; {carried}",
                named(&judgment.elsewhere)
            );
            (Health::Done, evidence)
        } else if !judgment.broken.is_empty() {
            let count = judgment.broken.len();
            let evidence = format!(
                "{count} arrived with other bytes than were dropped: {}; {carried}",
                named(&judgment.broken)
            );
            (Health::Done, evidence)
        } else if !lost.is_empty() {
            let shown = lost.iter().take(3).copied().collect::<Vec<_>>().join(", ");
            (
                Health::Done,
                format!("{} lost, {shown}: {carried}", lost.len()),
            )
        } else if let Some(unrouted) = unrouted {
            let evidence = format!(
                "a document no Subscription takes, {unrouted}, was dropped this turn: the node \
                 keeps it in its Dead Message Queue, where Replay acts on it; {carried}"
            );
            (Health::Stressed, evidence)
        } else if overdue > 0 {
            (
                Health::Stressed,
                format!("{overdue} overdue past {GRACE:?}: {carried}"),
            )
        } else {
            (Health::Fine, carried)
        };
        HealthRecord {
            scope: scope.to_string(),
            health,
            severity: if health == Health::Fine { 0 } else { 60 },
            evidence,
            observed_unix_nanos: now,
        }
    }
}

/// What a Location carried, in words: how many whole, to which Send Ports,
/// and the fastest and the median of its latest arrivals.
fn carried(judgment: &Judgment, waiting: usize) -> String {
    let mut took: Vec<Duration> = judgment.took.iter().copied().collect();
    took.sort();
    let timing = took.first().map_or_else(String::new, |fastest| {
        format!(
            "; from its drop to its arrival {fastest:?} the fastest, {:?} the median",
            took[took.len() / 2]
        )
    });
    let ports: Vec<&str> = judgment.ports.iter().map(String::as_str).collect();
    let to = if ports.is_empty() {
        String::new()
    } else {
        format!(" to {}", ports.join(", "))
    };
    format!(
        "{} document(s) delivered whole{to}, {waiting} on their way{timing}",
        judgment.delivered
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Receive Location's verdict, on the test cluster's first node.
    fn scope() -> String {
        let cluster = crate::support::test_cluster();
        let node = &cluster.node(0).name;
        crate::support::scope(&format!(
            "node/{node}/receive/Sales orders from Party-A/delivery"
        ))
    }

    #[test]
    fn a_file_at_another_port_or_with_other_bytes_is_done() {
        let mut judged = Judged::default();
        let took = Duration::from_millis(40);
        judged.arrived(
            "a-0.edi",
            0,
            (Some("Sales orders to ERP"), "Sales orders to ERP"),
            (true, took),
        );
        let fine = judged.record(&scope(), 0, (&[], None), 1);
        assert_eq!(fine.health, Health::Fine, "{}", fine.evidence);
        assert!(
            fine.evidence.contains("to Sales orders to ERP"),
            "{}",
            fine.evidence
        );
        assert!(fine.evidence.contains("the median"), "{}", fine.evidence);

        judged.arrived(
            "a-1.edi",
            0,
            (Some("Sales orders to ERP"), "Invoices to Party-A"),
            (true, took),
        );
        let elsewhere = judged.record(&scope(), 0, (&[], None), 2);
        assert_eq!(elsewhere.health, Health::Done);
        assert!(
            elsewhere
                .evidence
                .contains("a-1.edi at Invoices to Party-A"),
            "{}",
            elsewhere.evidence
        );

        let mut broken = Judged::default();
        broken.arrived(
            "a-2.edi",
            3,
            (Some("Payments to Party-E"), "Payments to Party-E"),
            (false, took),
        );
        let record = broken.record(&scope(), 3, (&[], None), 3);
        assert_eq!(record.health, Health::Done);
        assert!(
            record.evidence.contains("other bytes"),
            "{}",
            record.evidence
        );
        assert_eq!(broken.tally(), (0, 1));
    }

    #[test]
    fn a_struck_turn_is_stressed_and_the_next_is_fine() {
        let mut judged = Judged::default();
        judged.arrived(
            "a-0.edi",
            1,
            (Some("P"), "P"),
            (true, Duration::from_millis(9)),
        );
        let struck = judged.record(&scope(), 1, (&[], Some("iftsta-st-9001")), 1);
        assert_eq!(struck.health, Health::Stressed);
        assert!(
            struck.evidence.contains("Dead Message Queue"),
            "{}",
            struck.evidence
        );
        let next = judged.record(&scope(), 1, (&[], None), 2);
        assert_eq!(next.health, Health::Fine, "{}", next.evidence);
    }

    #[test]
    fn a_file_waiting_past_its_grace_is_stressed_and_past_lost_done() {
        let mut judged = Judged::default();
        judged.waiting([("a-0.edi", 2, GRACE + Duration::from_secs(1))].into_iter());
        assert_eq!(
            judged.record(&scope(), 2, (&[], None), 1).health,
            Health::Stressed
        );
        judged.waiting([("a-0.edi", 2, LOST)].into_iter());
        let lost = judged.record(&scope(), 2, (&[], None), 2);
        assert_eq!(lost.health, Health::Done);
        assert!(
            lost.evidence.contains("1 lost, a-0.edi"),
            "{}",
            lost.evidence
        );
        assert_eq!(
            judged.record(&scope(), 5, (&[], None), 3).health,
            Health::Fine,
            "another Location's file is not this one's"
        );
    }
}
