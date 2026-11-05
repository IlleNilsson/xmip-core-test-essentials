//! Where the executables a run spawns are: the cluster the roll starts, and
//! the node the cluster starts.
//!
//! 2026-09-19, the owner: *even clusters have to be spawned as processes
//! during tests*, so there are two to find rather than one. Both are looked
//! for the same way and both may be named outright.

use std::io;
use std::path::{Path, PathBuf};

/// The `xmip-essentials-node` binary: `XMIP_ESSENTIALS_NODE` if set, else
/// beside the current executable, else one directory up from it (a test runs
/// from `deps/`).
///
/// # Errors
///
/// When none of those is a file.
pub fn node_binary() -> io::Result<PathBuf> {
    found("XMIP_ESSENTIALS_NODE", "xmip-essentials-node")
}

/// The `xmip-essentials-cluster` binary, found the same way.
/// `XMIP_ESSENTIALS_CLUSTER` is the cluster's own name, so the override here
/// is `XMIP_ESSENTIALS_CLUSTER_BINARY`.
///
/// # Errors
///
/// When none of those is a file.
pub fn cluster_binary() -> io::Result<PathBuf> {
    found("XMIP_ESSENTIALS_CLUSTER_BINARY", "xmip-essentials-cluster")
}

/// The executable called `stem`: what `variable` names, else beside the
/// current executable, else one directory up from it.
fn found(variable: &str, stem: &str) -> io::Result<PathBuf> {
    if let Some(named) = std::env::var_os(variable) {
        return Ok(PathBuf::from(named));
    }
    let current = std::env::current_exe()?;
    let file = format!("{stem}{}", std::env::consts::EXE_SUFFIX);
    let beside = current.parent().map(|dir| dir.join(&file));
    let above = current
        .parent()
        .and_then(Path::parent)
        .map(|dir| dir.join(&file));
    [beside, above]
        .into_iter()
        .flatten()
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no {stem} beside the essentials; build it, or set {variable}"),
            )
        })
}
