//! Read-only location election. None of these observations confers native
//! process-death, idle admission, cleanup or successor-start authority.
use super::{
    InodeIdentity, Namespace, NativeAccount, PathBytes, RecordSnapshot, RuntimeAuthority,
    RuntimeError, RuntimePhase,
};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerInventory {
    Unknown,
    Absent,
    Present,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyInventory {
    Unknown,
    Absent,
    Sole(PathBuf),
    Multiple,
}
/// Complete/sole inventories must come from independently observed native
/// discovery, never a missing record/socket or a free flock alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryInventory {
    pub v1_markers: MarkerInventory,
    pub legacy: LegacyInventory,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionUncertainty {
    InventoryUnknown,
    MissingV1Record,
    MultipleLegacy,
    UnspawnedStarting,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeResolution {
    Located(RuntimeBinding),
    AbsentObservation,
    Uncertain(ResolutionUncertainty),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeBinding {
    kind: BindingKind,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum BindingKind {
    Legacy(PathBuf),
    Record {
        authority: PathBytes,
        socket: PathBuf,
        snapshot: Box<RecordSnapshot>,
        socket_root: InodeIdentity,
    },
}
impl RuntimeBinding {
    /// Explicit legacy locator. No v1 discovery, authority files, locks, hints
    /// or environment writes occur. Runtime-v1 callers use the fallible resolver
    /// after independent native discovery.
    pub fn legacy(profile: &str) -> Self {
        Self {
            kind: BindingKind::Legacy(crate::socket_path_for_profile(profile)),
        }
    }
    pub fn legacy_socket(socket: PathBuf) -> Self {
        Self {
            kind: BindingKind::Legacy(socket),
        }
    }
    pub fn socket_path(&self) -> &Path {
        match &self.kind {
            BindingKind::Legacy(path) => path,
            BindingKind::Record { socket, .. } => socket,
        }
    }
    pub fn record(&self) -> Option<&RecordSnapshot> {
        match &self.kind {
            BindingKind::Record { snapshot, .. } => Some(snapshot),
            _ => None,
        }
    }
    pub fn phase(&self) -> Option<RuntimePhase> {
        self.record().map(|r| r.record.phase)
    }
    /// Recheck this exact tuple, not another location election. Claims remain
    /// observational: peer credentials/native identity are separate checks.
    pub fn verify(&self) -> Result<(), RuntimeError> {
        let BindingKind::Record {
            authority,
            snapshot,
            socket_root,
            ..
        } = &self.kind
        else {
            return Ok(());
        };
        if snapshot.record.phase == RuntimePhase::Exited {
            return Err(RuntimeError::Unavailable("recorded exited runtime"));
        }
        let current =
            RuntimeAuthority::observe()?.ok_or(RuntimeError::Changed("authority disappeared"))?;
        if current.path().as_os_str() != authority.to_path().as_os_str()
            || !current.unchanged(snapshot)?
        {
            return Err(RuntimeError::Changed("resolved record binding"));
        }
        let socket = snapshot
            .record
            .socket
            .as_ref()
            .ok_or(RuntimeError::Invalid("resolved socket"))?;
        let path = socket.path.to_path();
        let directory = super::filesystem::PrivateDirectory::open_canonical(
            path.parent().ok_or(RuntimeError::Invalid("socket root"))?,
            snapshot.record.uid,
        )?;
        super::locality::validate_native_local_filesystem(directory.raw_fd())?;
        if directory.identity() != *socket_root {
            return Err(RuntimeError::Changed("resolved socket root identity"));
        }
        let metadata = std::fs::symlink_metadata(socket.path.to_path())?;
        if !metadata.file_type().is_socket()
            || metadata.uid() != snapshot.record.uid
            || metadata.nlink() != 1
            || metadata.dev() != socket.identity.device
            || metadata.ino() != socket.identity.inode
        {
            return Err(RuntimeError::Changed("resolved socket identity"));
        }
        current.verify()
    }
}

pub struct RuntimeResolver;
impl RuntimeResolver {
    /// Purely read-only C lookup and bounded record decode. Invalid records or
    /// native-root evidence propagate refusal, never a legacy fallback.
    pub fn resolve(
        config_root: &Path,
        profile: &str,
        inventory: &DiscoveryInventory,
    ) -> Result<RuntimeResolution, RuntimeError> {
        let account = NativeAccount::resolve()?;
        let canonical = PathBytes::from_path(&std::fs::canonicalize(config_root)?)?;
        let namespace = Namespace::derive(account.uid(), &canonical, profile)?;
        let authority = RuntimeAuthority::observe()?;
        let record = match &authority {
            Some(a) => a.read_record(&namespace)?,
            None => None,
        };
        choose(record, authority.as_ref().map(|a| a.path()), inventory)
    }
}
fn choose(
    record: Option<RecordSnapshot>,
    authority: Option<&Path>,
    inventory: &DiscoveryInventory,
) -> Result<RuntimeResolution, RuntimeError> {
    if let Some(snapshot) = record {
        let Some(socket) = snapshot.record.socket.as_ref().map(|s| s.path.to_path()) else {
            return Ok(RuntimeResolution::Uncertain(
                ResolutionUncertainty::UnspawnedStarting,
            ));
        };
        let root = super::filesystem::PrivateDirectory::open_canonical(
            socket
                .parent()
                .ok_or(RuntimeError::Invalid("socket root"))?,
            snapshot.record.uid,
        )?;
        super::locality::validate_native_local_filesystem(root.raw_fd())?;
        return Ok(RuntimeResolution::Located(RuntimeBinding {
            kind: BindingKind::Record {
                authority: PathBytes::from_path(
                    authority.ok_or(RuntimeError::Invalid("record lacks authority domain"))?,
                )?,
                socket,
                snapshot: Box::new(snapshot),
                socket_root: root.identity(),
            },
        }));
    }
    let refusal = match inventory.v1_markers {
        MarkerInventory::Present => Some(ResolutionUncertainty::MissingV1Record),
        MarkerInventory::Unknown => Some(ResolutionUncertainty::InventoryUnknown),
        MarkerInventory::Absent => None,
    };
    if let Some(reason) = refusal {
        return Ok(RuntimeResolution::Uncertain(reason));
    }
    Ok(match &inventory.legacy {
        LegacyInventory::Sole(path) => RuntimeResolution::Located(RuntimeBinding {
            kind: BindingKind::Legacy(path.clone()),
        }),
        LegacyInventory::Absent => RuntimeResolution::AbsentObservation,
        LegacyInventory::Unknown => {
            RuntimeResolution::Uncertain(ResolutionUncertainty::InventoryUnknown)
        }
        LegacyInventory::Multiple => {
            RuntimeResolution::Uncertain(ResolutionUncertainty::MultipleLegacy)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_record_with_marker_or_unknown_inventory_cannot_fall_back_to_legacy_or_absence() {
        for markers in [MarkerInventory::Unknown, MarkerInventory::Present] {
            for legacy in [
                LegacyInventory::Absent,
                LegacyInventory::Sole(PathBuf::from("/old/profile.sock")),
            ] {
                assert!(matches!(
                    choose(
                        None,
                        None,
                        &DiscoveryInventory {
                            v1_markers: markers,
                            legacy
                        }
                    )
                    .unwrap(),
                    RuntimeResolution::Uncertain(_)
                ));
            }
        }
        assert_eq!(
            choose(
                None,
                None,
                &DiscoveryInventory {
                    v1_markers: MarkerInventory::Absent,
                    legacy: LegacyInventory::Absent
                }
            )
            .unwrap(),
            RuntimeResolution::AbsentObservation
        );
        assert!(matches!(
            choose(
                None,
                None,
                &DiscoveryInventory {
                    v1_markers: MarkerInventory::Absent,
                    legacy: LegacyInventory::Multiple
                }
            )
            .unwrap(),
            RuntimeResolution::Uncertain(ResolutionUncertainty::MultipleLegacy)
        ));
        let selected = choose(
            None,
            None,
            &DiscoveryInventory {
                v1_markers: MarkerInventory::Absent,
                legacy: LegacyInventory::Sole(PathBuf::from("/observed/old.sock")),
            },
        )
        .unwrap();
        let RuntimeResolution::Located(binding) = selected else {
            panic!("sole observed legacy");
        };
        assert_eq!(binding.socket_path(), Path::new("/observed/old.sock"));
        assert!(binding.record().is_none());
    }
}
