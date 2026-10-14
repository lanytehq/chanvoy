//! Native-account runtime authority foundations.
//!
//! These types carry filesystem and application claims, not kernel death or
//! operational-readiness proof. Lifecycle service activation is separate from
//! this foundation; existing legacy path helpers retain their explicit role.

mod authority;
mod filesystem;
mod identity;
mod locality;
mod peer;
mod publisher;
mod record;
mod resolver;
mod socket_root;

pub use authority::{
    LeaseObservation, LifetimeLease, OperationLock, RecordSnapshot, RuntimeAuthority,
};
pub use identity::{Namespace, NativeAccount, PathBytes};
pub use peer::native_peer_pid;
pub use publisher::RecordPublisher;
pub use record::{
    child_socket_path, InodeIdentity, InstanceId, NativeBirth, NativePlatform, ProcessId,
    RuntimeBuildPin, RuntimePhase, RuntimeProcess, RuntimeRecord, RuntimeSocket,
};
pub use resolver::{
    DiscoveryInventory, LegacyInventory, MarkerInventory, ResolutionUncertainty, RuntimeBinding,
    RuntimeResolution, RuntimeResolver,
};
pub use socket_root::SocketRoot;

use thiserror::Error;

pub const RECORD_VERSION: u16 = 1;
pub const RECORD_MAX_BYTES: usize = 64 * 1024;

/// Refusals name a source or invariant and never include credentials/content.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("native runtime evidence unavailable: {0}")]
    Unavailable(&'static str),
    #[error("invalid runtime evidence: {0}")]
    Invalid(&'static str),
    #[error("runtime artifact changed: {0}")]
    Changed(&'static str),
    #[error("runtime namespace is already locked")]
    Locked,
    #[error("runtime record publication completion is uncertain")]
    PublicationUncertain,
    #[error("runtime filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
}
