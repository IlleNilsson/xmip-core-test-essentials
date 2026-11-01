//! The Parties' far end, for the nodes on machines of their own (ADR-0028,
//! amendment 2026-10-22). The far end is Xmip (clause 5): no server of the
//! machines' is asked of them. The rig serves SFTP on this computer, on
//! the far end's port (`configure::placed::FAR_END_PORT`) on every address,
//! with Xmip's own server (`transport_sftp::serving_each`), and the real
//! nodes reach it as their environment bound them (`xmip-service --slice
//! --bind`): each Receive Location fetches from, and each Send Port puts
//! on, a user of its own, whose directory it is — placed over the real
//! nodes exactly as the environment placed them (`configure::placed`), so
//! the rig drops where a node fetches and reads where one puts.
//!
//! Each user logs in by the password its Location's secret holds
//! (`configure::placed::far_end_secret`). The rig mints it, sets it on the
//! node that takes the Location through that node's operate listener — the
//! node's secret store, the one place the value is kept — and the far end
//! keeps only its salted verifier, checked by the password gate
//! (`crate::login`; open problem 59). It enrolls no key, so a key is
//! refused whatever it signed. No directory is chosen by a name nothing
//! checked.
//!
//! The host's firewall must let the machines in on that port; the
//! environment's README says how, and that it needs elevation.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use configure::placed::{self, FAR_END_PORT, Placed};
use ed25519_dalek::SigningKey;
use transport::concurrent::Concurrent;
use transport_sftp::directory::{Directory, Shared};

use crate::carried::{Arrived, Binding, Spot, told};
use crate::login;
use crate::real::RealNode;
use crate::subscribing::CLUSTER;

/// How long a node that stops mid-exchange is waited on.
const PATIENCE: Duration = Duration::from_secs(10);

/// The far end being served, and the Locations placed on it.
pub struct FarEnd {
    _serving: Concurrent,
    /// Every Receive Location and Send Port placed over the real nodes,
    /// each a user's directory here.
    pub bindings: Vec<Binding>,
    /// Where it is served.
    pub address: String,
}

impl FarEnd {
    /// Serve the far end for the real nodes `real`, each Send Port's
    /// directory telling `arrived` of what a node put there, each
    /// Location's password minted and set on its node; none, where the real
    /// nodes take no Location.
    ///
    /// # Errors
    ///
    /// REFUSED, in words: a node the cluster's file does not declare, a
    /// node that did not take its Location's secret, or a port this computer
    /// will not give.
    pub fn serve(real: &[RealNode], arrived: &Arrived) -> Result<Option<Self>, String> {
        let names: Vec<String> = real.iter().map(|node| node.name.clone()).collect();
        let nodes = placed::present(CLUSTER, &names)?;
        let placements = placed::placements(CLUSTER, &nodes)?;
        if placements.is_empty() {
            return Ok(None);
        }
        let mut directories: BTreeMap<String, Shared> = BTreeMap::new();
        let mut enrolled: Vec<(String, String)> = Vec::new();
        let mut bindings = Vec::new();
        for placement in placements {
            let password = login::minted();
            real.iter()
                .find(|node| node.name == placement.node)
                .ok_or_else(|| format!("REFUSED: {} is no real node of the run", placement.node))?
                .set_password(&placed::far_end_secret(&placement), &password)?;
            enrolled.push((placement.folded(), password));
            let directory = match placement.placed {
                Placed::Receive => Directory::default(),
                Placed::Send => Directory::default().telling(told(arrived)),
            };
            let shared: Shared = Arc::new(std::sync::Mutex::new(directory));
            directories.insert(placement.folded(), Arc::clone(&shared));
            bindings.push(Binding {
                placement,
                spot: Spot::FarEnd(shared),
            });
        }
        let address = format!("0.0.0.0:{FAR_END_PORT}");
        let listener = TcpListener::bind(&address).map_err(|error| {
            format!("REFUSED: the Parties' far end cannot be served at {address}: {error}")
        })?;
        let host = SigningKey::from_bytes(&codec::random::array::<32>());
        let chosen = move |user: &str| {
            directories
                .get(user)
                .map_or_else(Shared::default, Arc::clone)
        };
        // Every Location here logs in by its password, so no key is
        // enrolled and every key is refused.
        let check = login::checking(
            enrolled
                .iter()
                .map(|(user, password)| (user.as_str(), password.as_str())),
            Vec::new(),
        );
        // The values go with the list: the far end keeps the verifiers alone.
        drop(enrolled);
        let serving = transport_sftp::serving_each(listener, host, chosen, (Some(PATIENCE), check))
            .map_err(|error| format!("REFUSED: the Parties' far end at {address}: {error}"))?;
        Ok(Some(Self {
            _serving: serving,
            bindings,
            address,
        }))
    }

    /// What the run says of it among its first lines.
    #[must_use]
    pub fn said(&self) -> String {
        format!(
            "the Parties' far end at {}, each Location placed on a real node a user of its own",
            self.address
        )
    }
}
