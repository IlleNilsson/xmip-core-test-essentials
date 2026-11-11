//! What an `xmip-essentials-node` was told (`src/bin/node.rs`): every flag
//! named, none by position. The cluster writes the same flags as it starts
//! one (`cluster.rs`, `capability.rs`).

use std::path::PathBuf;
use std::time::Duration;

use node::Capability;

use crate::roster::Roster;
use crate::scenario;
use crate::stress::Stress;
use crate::switch;

/// What the command line said.
pub struct Arguments {
    pub name: String,
    pub shared: PathBuf,
    pub stress: Stress,
    pub rounds: u64,
    pub snapshot: PathBuf,
    pub interval: Duration,
    /// How often it beats, round or no round.
    pub beat: Duration,
    /// What this node declared (ADR-0056): its roles from `--role`, and
    /// from `--online` whether it may assume
    /// the internet (ADR-0045). Published in its own health records, so the
    /// cluster's board shows both per node.
    pub capability: Capability,
    /// Every node of the cluster and what each declared, this one among them;
    /// empty when not told.
    pub roster: Roster,
    /// The scenarios to run this node's part of; empty means every one.
    pub scenarios: Vec<String>,
}

/// What the node prints on a refused command line: every flag, and an
/// example.
pub const USAGE: &str = "usage: node --name <name> --shared <dir> --stress <level> --rounds <n> \
     --snapshot <path> [--interval-ms <ms>] [--beat-ms <ms>] [--online true|false] \
     [--role receiving,processing] [--nodes <name[=role+role],...>] \
     [--scenarios <scenarios>]\n\
     example: node --name R1 --shared shared --stress calm --rounds 0 \
     --snapshot R1.xmip --role receiving\n\
     The name is the tester's and means nothing to Xmip; --role says what the node is for.";

/// `--flag value` pairs, every required one present and well-formed.
///
/// # Errors
///
/// A flag with no value, an unknown flag, a value that does not read, a
/// required flag missing, and REFUSED for a word that is no role or no
/// scenario, naming the ones there are.
pub fn parse(args: impl Iterator<Item = String>) -> Result<Arguments, String> {
    let mut name = None;
    let mut shared = None;
    let mut stress = None;
    let mut rounds = None;
    let mut snapshot = None;
    let mut interval = Duration::from_millis(250);
    let mut beat = Duration::from_millis(100);
    let mut online = false;
    let mut role = Capability::none();
    let mut roster = Roster::default();
    let mut scenarios = Vec::new();

    let mut args = args.peekable();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--name" => name = Some(value),
            "--shared" => shared = Some(PathBuf::from(value)),
            "--stress" => {
                stress = Some(Stress::parse(&value).ok_or(format!("unknown stress {value}"))?);
            }
            "--rounds" => rounds = Some(number(&flag, &value)?),
            "--snapshot" => snapshot = Some(PathBuf::from(value)),
            "--interval-ms" => interval = Duration::from_millis(number(&flag, &value)?),
            "--beat-ms" => beat = Duration::from_millis(number(&flag, &value)?.max(1)),
            "--online" => {
                online = switch::parse(&value)
                    .ok_or(format!("--online wants true or false, not {value}"))?;
            }
            "--role" => role = Capability::parse(&value)?,
            "--nodes" => roster = Roster::parse(&value)?,
            "--scenarios" => scenarios = scenario::chosen(Some(&value))?,
            other => return Err(format!("unknown flag {other}")),
        }
    }

    Ok(Arguments {
        name: name.ok_or("--name is required")?,
        shared: shared.ok_or("--shared is required")?,
        stress: stress.ok_or("--stress is required")?,
        rounds: rounds.ok_or("--rounds is required")?,
        snapshot: snapshot.ok_or("--snapshot is required")?,
        interval,
        beat,
        capability: role.with_online(online),
        roster,
        scenarios,
    })
}

fn number(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} wants a number, not {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use configure::fixture::test_cluster;

    /// The test cluster's receiving, processing and sending node.
    fn path() -> [String; 3] {
        let cluster = test_cluster();
        ["receiving", "processing", "sending"].map(|role| cluster.with_role(role).name.clone())
    }

    fn arguments(extra: &[&str]) -> Result<Arguments, String> {
        let [name, ..] = path();
        let snapshot = format!("{name}.xmip");
        let required = [
            "--name",
            &name,
            "--shared",
            "s",
            "--stress",
            "calm",
            "--rounds",
            "1",
            "--snapshot",
            &snapshot,
        ];
        parse(required.iter().chain(extra).map(ToString::to_string))
    }

    #[test]
    fn the_scenarios_and_the_roster_are_read_and_absent_means_every_one() {
        let bare = arguments(&[]).expect("the required flags suffice");
        assert!(bare.scenarios.is_empty() && bare.roster.is_empty());
        assert_eq!(
            bare.beat,
            Duration::from_millis(100),
            "a beat a tenth of a second"
        );
        assert!(
            bare.capability.declares_no_stage(),
            "a node told nothing declares nothing and runs whole tests"
        );

        let [receiving, processing, sending] = path();
        let nodes = format!("{receiving}=receiving, {processing}=processing,{sending}=sending");
        let told = arguments(&[
            "--scenarios",
            "Round-Trip",
            "--nodes",
            &nodes,
            "--role",
            "receiving",
        ])
        .expect("all three are well formed");
        assert_eq!(told.scenarios, ["round-trip"]);
        assert_eq!(told.roster.names(), [receiving, processing, sending]);
        assert_eq!(told.capability.words(), "receiving");
        assert_eq!(
            arguments(&["--scenarios", ""])
                .expect("empty is every one")
                .scenarios,
            [] as [String; 0]
        );
    }

    #[test]
    fn an_unknown_scenario_or_role_is_refused_with_the_names_there_are() {
        let refusal = arguments(&["--scenarios", "round-trip,pingpong"])
            .err()
            .expect("pingpong is no scenario");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(
            refusal.contains("pingpong") && refusal.contains("exclusive-claim"),
            "{refusal}"
        );

        let [name, ..] = path();
        let relaying = format!("{name}=relay");
        for extra in [["--role", "relay"], ["--nodes", &relaying]] {
            let refusal = arguments(&extra)
                .err()
                .unwrap_or_else(|| panic!("{extra:?}"));
            assert!(refusal.starts_with("REFUSED"), "{refusal}");
            assert!(
                refusal.contains("relay") && refusal.contains("receiving, processing, sending"),
                "{refusal}"
            );
        }
    }
}
