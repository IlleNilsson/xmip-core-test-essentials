//! A cluster judged quickly, and a node's publication read from elsewhere:
//! what the liveness tests of node processes (`tests/spawned`) stand a
//! cluster up with. Behind `test-support`: a test's, never a roll's.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::{Cluster, Liveness, Orders};

impl Cluster {
    /// [`Cluster::spawn`], judging liveness quickly enough for a test: a
    /// beat every tenth of a second, three missed, and `starting` for a node
    /// to show it is alive.
    ///
    /// # Errors
    ///
    /// When a process cannot be started.
    pub fn quick(
        binary: &Path,
        orders: &Orders,
        shared: &Path,
        snapshots: &Path,
        starting: Duration,
    ) -> io::Result<Self> {
        let liveness = Liveness {
            beat: Duration::from_millis(100),
            missed: 3,
            starting,
        };
        Self::judged(binary, orders, shared, snapshots, liveness)
    }

    /// The file the node at `place` publishes to, and its beat beside it.
    ///
    /// # Panics
    ///
    /// When the cluster has no node at `place`.
    #[must_use]
    pub fn publication(&self, place: usize) -> &Path {
        &self.nodes[place].path
    }

    /// Read the node at `place` from `path` from now on, a file the node
    /// never writes, so its beats are whatever the test writes there.
    ///
    /// # Panics
    ///
    /// When the cluster has no node at `place`.
    pub fn read_from(&mut self, place: usize, path: PathBuf) {
        self.nodes[place].path = path;
    }
}
