//! The Essentials' Work Process, *Order approval*: its design
//! (`configuration/order-approval.bpmn`), the unit its nodes load (the
//! example `order_approval`, what `process::generate` makes of the design,
//! held to it by a test here), and what the design assigns, which the rig
//! follows a document through once the Work Process has published it
//! (`crate::documents`; ADR-0066 clause 4).

use assign::Assignment;
use process::generate::{Generated, Origin};
use process::{DesignFormat, Program, Step};

/// The design, as the developer drew it.
pub const DESIGN: &str = include_str!("../configuration/order-approval.bpmn");

/// Where the unit is and what it is called: the `[[modules]]` entry of
/// `cluster.xmip` names this module, and its library is the example
/// `order_approval`.
pub const ORIGIN: Origin<'static> = Origin {
    design: "order-approval.bpmn",
    format: process_bpmn::FORMAT,
    module: "order-approval",
    version: (0, 1, 0),
};

/// The design compiled, as a node's unit holds it.
///
/// # Errors
///
/// The compiler's refusal, in its words.
pub fn program() -> Result<Program, String> {
    process_bpmn::Bpmn
        .compile(DESIGN, &process::NoResources)
        .map_err(|refused| {
            let said: Vec<String> = refused.iter().map(ToString::to_string).collect();
            format!("{}: {}", ORIGIN.design, said.join("; "))
        })
}

/// The unit the design compiles into now: its library's source and its
/// `[[modules]]` entry.
///
/// # Errors
///
/// As [`program`].
pub fn generated() -> Result<Generated, String> {
    program().map(|program| process::generate::unit(&program, &ORIGIN))
}

/// The unit's library file, as this system names it.
#[must_use]
pub fn library_file() -> String {
    process::generate::library_file(ORIGIN.module)
}

/// Every assignment the design makes before it publishes, in its flow's
/// order from its start: what a Message it publishes carries beside what it
/// arrived with. The design is one flow with no branch, so its first
/// publish is the one it makes; a design with branches would be followed by
/// its engine, not walked.
///
/// # Errors
///
/// As [`program`], or where the flow loops before it publishes.
pub fn assignments() -> Result<Vec<Assignment>, String> {
    let program = program()?;
    let mut made = Vec::new();
    let mut at = program.starts.first().copied();
    let mut steps = 0;
    while let Some(place) = at {
        steps += 1;
        if steps > program.nodes.len() {
            return Err(format!(
                "{}: its flow loops before it publishes",
                ORIGIN.design
            ));
        }
        at = match program.node(place).map(|node| &node.step) {
            Some(Step::Start { next } | Step::Wait { next }) => Some(*next),
            Some(Step::Call { work, next }) => {
                if let process::program::Work::Assign(assigned) = work {
                    made.extend(assigned.iter().cloned());
                }
                Some(*next)
            }
            _ => None,
        };
    }
    Ok(made)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `source` with its layout taken out, as the runtime's example unit is
    /// compared: every blank, and a comma before a closing bracket.
    fn unlaid(source: &str) -> String {
        source
            .split_whitespace()
            .collect::<String>()
            .replace(",]", "]")
            .replace(",)", ")")
    }

    /// The unit the nodes load is what its design generates now, laid out
    /// as `cargo fmt` lays it, so it cannot drift from the design or from
    /// `process::generate`.
    #[test]
    fn the_unit_the_nodes_load_is_what_its_design_generates_now() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/order_approval.rs");
        let held = std::fs::read_to_string(&path).unwrap_or_default();
        let now = generated().expect("the design compiles").library;
        assert!(
            unlaid(&held) == unlaid(&now),
            "{} is not what its design generates now; generated:\n{now}",
            path.display()
        );
    }

    /// The design marks an order approved before it publishes it, and the
    /// cluster's file declares the module its unit is, under the name the
    /// generator gives it.
    #[test]
    fn an_order_is_marked_approved_and_the_cluster_declares_its_unit() {
        let made = assignments().expect("assigns");
        assert_eq!(made.len(), 1, "{made:?}");
        assert_eq!(made[0].target_key, "Approval");
        let entry = generated().expect("generated").module;
        let declared = configure::parse_toml(&format!(
            "[service]\nname = \"xmip\"\ncluster_name = \"c\"\nnode_name = \"n\"\n\n{entry}"
        ))
        .expect("the generated entry reads")
        .modules;
        let cluster: toml::Table = crate::subscribing::CLUSTER.parse().expect("TOML");
        let modules = cluster["modules"].as_array().expect("[[modules]]");
        let module = modules
            .iter()
            .find(|module| module["name"].as_str() == Some(ORIGIN.module))
            .expect("the cluster declares the unit's module");
        assert_eq!(
            module["manifest"]["entrypoint"]["library_path"].as_str(),
            declared[0].manifest.entrypoint.library_path.as_deref(),
            "the cluster names the library as the generator does"
        );
    }
}
