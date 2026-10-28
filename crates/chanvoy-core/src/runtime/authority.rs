use super::filesystem::{FileFingerprint, PrivateDirectory};
use super::{
    Namespace, NativeAccount, RuntimeError, RuntimePhase, RuntimeRecord, RECORD_MAX_BYTES,
};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;

/// A native-account filesystem domain. Record claims never confer process
/// ownership or permission to retire/clean an instance.
pub struct RuntimeAuthority {
    account: NativeAccount,
    canonical_home: PathBuf,
    home: PrivateDirectory,
    base: PrivateDirectory,
    root: PrivateDirectory,
    records: Option<PrivateDirectory>,
    locks: Option<PrivateDirectory>,
}

/// Captured bytes plus descriptor/name fingerprint, for exact capture/recheck.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSnapshot {
    pub record: RuntimeRecord,
    pub(super) fingerprint: FileFingerprint,
    pub(super) bytes: Vec<u8>,
}

/// Ephemeral credential-tier handoff capture. Debug intentionally omits the
/// bootstrap, nonce and bytes. Persistent runtime records never contain them.
pub struct BootstrapSnapshot {
    pub bootstrap: super::RuntimeBootstrap,
    fingerprint: FileFingerprint,
    bytes: Vec<u8>,
}

impl RuntimeAuthority {
    /// Read-only lookup. Missing authority is only filesystem evidence; callers
    /// still reconcile native inventory/lease evidence before claiming absence.
    pub fn observe() -> Result<Option<Self>, RuntimeError> {
        Self::open(false)
    }

    /// Explicit managed initialization, after caller credential/generation
    /// gates. No invocation-specific home/state root can elect another domain.
    pub fn initialize() -> Result<Self, RuntimeError> {
        if std::env::var_os("CHANVOY_STATE_DIR").is_some() {
            return Err(RuntimeError::Invalid("state-root override unsupported"));
        }
        Self::open(true)?.ok_or(RuntimeError::Unavailable("authority initialization"))
    }

    fn open(create: bool) -> Result<Option<Self>, RuntimeError> {
        let account = NativeAccount::resolve()?;
        let canonical_home = std::fs::canonicalize(account.home())?;
        super::locality::validate_persistent_path(&canonical_home)?;
        // Refuse an unrepresentable authority before creating any directory.
        for (directory, suffix) in [("records", ".json"), ("locks", ".op.lock")] {
            super::PathBytes::from_path(
                &canonical_home
                    .join(".chanvoy/lifecycle")
                    .join(directory)
                    .join(format!("{}{suffix}", "a".repeat(64))),
            )?;
        }
        let home = PrivateDirectory::open_native_home(&canonical_home, account.uid())?;
        super::locality::validate_local_directory(home.raw_fd(), &canonical_home, &home)?;
        let Some(base) = home.child(OsStr::new(".chanvoy"), create)? else {
            return Ok(None);
        };
        super::locality::validate_local_directory(base.raw_fd(), base.path(), &home)?;
        let Some(root) = base.child(OsStr::new("lifecycle"), create)? else {
            return Ok(None);
        };
        // Submounts/aliases are checked after descriptor-relative creation/open.
        super::locality::validate_local_directory(root.raw_fd(), root.path(), &home)?;
        let records = root.child(OsStr::new("records"), create)?;
        let locks = root.child(OsStr::new("locks"), create)?;
        for dir in [&records, &locks].into_iter().flatten() {
            super::locality::validate_local_directory(dir.raw_fd(), dir.path(), &home)?;
        }
        Ok(Some(Self {
            account,
            canonical_home,
            home,
            base,
            root,
            records,
            locks,
        }))
    }

    pub fn uid(&self) -> u32 {
        self.account.uid()
    }
    pub fn path(&self) -> &std::path::Path {
        self.root.path()
    }

    pub fn verify(&self) -> Result<(), RuntimeError> {
        let account = NativeAccount::resolve()?;
        if account.uid() != self.account.uid()
            || std::fs::canonicalize(account.home())?.as_os_str() != self.canonical_home.as_os_str()
        {
            return Err(RuntimeError::Changed("native account home"));
        }
        self.home.verify_named()?;
        self.base.verify_named()?;
        self.root.verify_named()?;
        for dir in [&self.home, &self.base, &self.root] {
            super::locality::validate_local_directory(dir.raw_fd(), dir.path(), &self.home)?;
        }
        if let Some(records) = &self.records {
            records.verify_named()?;
        }
        if let Some(locks) = &self.locks {
            locks.verify_named()?;
        }
        for (name, captured) in [("records", &self.records), ("locks", &self.locks)] {
            if captured.is_none() && self.root.child(OsStr::new(name), false)?.is_some() {
                return Err(RuntimeError::Changed("authority directory inventory"));
            }
            if let Some(dir) = captured {
                super::locality::validate_local_directory(dir.raw_fd(), dir.path(), &self.home)?;
            }
        }
        Ok(())
    }

    pub fn read_record(
        &self,
        namespace: &Namespace,
    ) -> Result<Option<RecordSnapshot>, RuntimeError> {
        self.verify()?;
        let Some(dir) = &self.records else {
            return Ok(None);
        };
        let name = format!("{}.json", namespace.hex());
        let Some(file) = dir.open_file(OsStr::new(&name), libc::O_RDONLY)? else {
            self.verify()?;
            return Ok(None);
        };
        let before = FileFingerprint::of(&file.metadata()?);
        if before.size > RECORD_MAX_BYTES as u64 {
            return Err(RuntimeError::Invalid("record byte cap"));
        }
        let mut bytes = Vec::new();
        (&file)
            .take(RECORD_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > RECORD_MAX_BYTES {
            return Err(RuntimeError::Invalid("record byte cap"));
        }
        if FileFingerprint::of(&file.metadata()?) != before {
            return Err(RuntimeError::Changed("record descriptor"));
        }
        dir.verify_file_named(OsStr::new(&name), &before)?;
        self.verify()?;
        let record = RuntimeRecord::decode(&bytes, namespace, self.uid())?;
        Ok(Some(RecordSnapshot {
            record,
            fingerprint: before,
            bytes,
        }))
    }

    pub fn unchanged(&self, captured: &RecordSnapshot) -> Result<bool, RuntimeError> {
        Ok(self.read_record(&captured.record.namespace)?.as_ref() == Some(captured))
    }

    /// Open once and try the native flock backend. No explicit unlock, path
    /// deletion or holder PID claim is attached to a lock result.
    pub fn try_operation(&self, namespace: &Namespace) -> Result<OperationLock, RuntimeError> {
        let opened = self.open_lock(namespace, "op.lock", true)?;
        acquire(&opened.file)?;
        self.verify()?;
        let guard = OperationLock { opened };
        guard.verify(self)?;
        Ok(guard)
    }

    /// Launcher acquisition requires the operation guard to be held first.
    /// The lifetime inode is opened once by this holder and never re-locked.
    pub fn try_lifetime(&self, operation: &OperationLock) -> Result<LifetimeLease, RuntimeError> {
        operation.verify(self)?;
        let opened = self.open_lock(operation.namespace(), "lease", true)?;
        acquire(&opened.file)?;
        let lease = LifetimeLease { opened };
        lease.verify(self)?;
        operation.verify(self)?;
        Ok(lease)
    }

    /// A separate observer may probe an existing lease. This gives contention
    /// evidence only, with neither holder identity nor native death authority.
    pub fn observe_lease(&self, namespace: &Namespace) -> Result<LeaseObservation, RuntimeError> {
        self.verify()?;
        let Some(dir) = &self.locks else {
            return Ok(LeaseObservation::Missing);
        };
        let name = format!("{}.lease", namespace.hex());
        let Some(file) = dir.open_file(OsStr::new(&name), libc::O_RDWR)? else {
            self.verify()?;
            return Ok(LeaseObservation::Missing);
        };
        let fingerprint = FileFingerprint::of(&file.metadata()?);
        if fingerprint.size != 0 {
            return Err(RuntimeError::Invalid("nonempty lease inode"));
        }
        let result = match acquire(&file) {
            Ok(()) => LeaseObservation::Free,
            Err(RuntimeError::Locked) => LeaseObservation::Contended,
            Err(error) => return Err(error),
        };
        dir.verify_file_named(OsStr::new(&name), &fingerprint)?;
        self.verify()?;
        // A successful observer acquisition is released only by dropping this
        // observer's description, never by unlocking a holder's description.
        Ok(result)
    }

    /// Launcher revision zero is no-replace and unspawned. Existing records
    /// require a separately proven death/reaper path, not automatic overwrite.
    pub fn create_starting(
        &self,
        operation: &OperationLock,
        lease: &LifetimeLease,
        record: &RuntimeRecord,
    ) -> Result<RecordSnapshot, RuntimeError> {
        operation.verify(self)?;
        lease.verify(self)?;
        if record.namespace != *operation.namespace()
            || record.namespace != *lease.namespace()
            || record.uid != self.uid()
            || record.revision != 0
            || record.phase != RuntimePhase::Starting
        {
            return Err(RuntimeError::Invalid("launcher record ownership"));
        }
        record.validate()?;
        if std::fs::canonicalize(record.config_root.to_path())?.as_os_str()
            != record.config_root.to_path().as_os_str()
        {
            return Err(RuntimeError::Changed("loaded canonical config root"));
        }
        self.publish_record(None, record)
    }

    pub(super) fn publish_record(
        &self,
        expected: Option<&RecordSnapshot>,
        record: &RuntimeRecord,
    ) -> Result<RecordSnapshot, RuntimeError> {
        self.verify()?;
        if record.uid != self.uid() {
            return Err(RuntimeError::Invalid("record UID"));
        }
        let bytes = record.encode()?;
        if let Some(expected) = expected {
            let current = &expected.record;
            if record.namespace != current.namespace
                || record.instance_uuid != current.instance_uuid
                || record.revision != current.next_revision()?
                || record.uid != current.uid
                || record.config_root != current.config_root
                || record.profile != current.profile
            {
                return Err(RuntimeError::Invalid("record writer epoch/revision"));
            }
            if !self.unchanged(expected)? {
                return Err(RuntimeError::Changed("record revision/epoch"));
            }
        }
        let dir = self
            .records
            .as_ref()
            .ok_or(RuntimeError::Unavailable("record directory"))?;
        let name = format!("{}.json", record.namespace.hex());
        let fingerprint =
            dir.publish(OsStr::new(&name), expected.map(|r| &r.fingerprint), &bytes)?;
        self.verify()
            .map_err(|_| RuntimeError::PublicationUncertain)?;
        Ok(RecordSnapshot {
            record: record.clone(),
            fingerprint,
            bytes,
        })
    }

    pub(super) fn confirm_publication(
        &self,
        captured: &RecordSnapshot,
    ) -> Result<(), RuntimeError> {
        if !self.unchanged(captured)? {
            return Err(RuntimeError::Changed("publication reconciliation"));
        }
        let dir = self
            .records
            .as_ref()
            .ok_or(RuntimeError::Unavailable("record directory"))?;
        let name = format!("{}.json", captured.record.namespace.hex());
        let file = dir
            .open_file(OsStr::new(&name), libc::O_RDONLY)?
            .ok_or(RuntimeError::Changed("published record missing"))?;
        if FileFingerprint::of(&file.metadata()?) != captured.fingerprint {
            return Err(RuntimeError::Changed("published record identity"));
        }
        file.sync_all()
            .map_err(|_| RuntimeError::PublicationUncertain)?;
        dir.sync().map_err(|_| RuntimeError::PublicationUncertain)?;
        if !self.unchanged(captured)? {
            return Err(RuntimeError::Changed("published record changed"));
        }
        Ok(())
    }

    /// Publish an epoch-specific, no-replace bootstrap under the held operation
    /// lock. This validates file and record claims, not descriptor inheritance.
    pub fn write_bootstrap(
        &self,
        operation: &OperationLock,
        lease: &LifetimeLease,
        starting: &RecordSnapshot,
        bootstrap: &super::RuntimeBootstrap,
    ) -> Result<BootstrapSnapshot, RuntimeError> {
        operation.verify(self)?;
        lease.verify(self)?;
        if operation.namespace() != &bootstrap.namespace
            || lease.namespace() != &bootstrap.namespace
            || lease.identity() != bootstrap.lease_identity
            || starting.record.namespace != bootstrap.namespace
            || starting.record.instance_uuid != bootstrap.instance_uuid
            || starting.record.config_root != bootstrap.config_root
            || starting.record.profile != bootstrap.profile
            || starting.record.uid != bootstrap.uid
            || starting.record.revision != 0
            || starting.record.phase != RuntimePhase::Starting
            || bootstrap.uid != self.uid()
            || !self.unchanged(starting)?
        {
            return Err(RuntimeError::Changed("bootstrap launcher ownership"));
        }
        let bytes = bootstrap
            .encode()
            .map_err(|_| RuntimeError::Invalid("bootstrap contract"))?;
        let dir = self
            .records
            .as_ref()
            .ok_or(RuntimeError::Unavailable("record directory"))?;
        let name = bootstrap_name(&bootstrap.namespace, bootstrap.instance_uuid);
        let fingerprint = dir.publish(OsStr::new(&name), None, &bytes)?;
        self.verify()?;
        Ok(BootstrapSnapshot {
            bootstrap: bootstrap.clone(),
            fingerprint,
            bytes,
        })
    }

    /// Credential-tier descriptor-relative read with metadata checked before
    /// parsing. An inaccessible/changed file is never a legacy startup fallback.
    pub fn read_bootstrap(
        &self,
        namespace: &Namespace,
        instance: super::InstanceId,
    ) -> Result<BootstrapSnapshot, RuntimeError> {
        self.verify()?;
        let dir = self
            .records
            .as_ref()
            .ok_or(RuntimeError::Unavailable("record directory"))?;
        let name = bootstrap_name(namespace, instance);
        let file = dir
            .open_file(OsStr::new(&name), libc::O_RDONLY)?
            .ok_or(RuntimeError::Unavailable("bootstrap missing"))?;
        let fingerprint = FileFingerprint::of(&file.metadata()?);
        let cap = super::control::BOOTSTRAP_MAX_BYTES;
        if fingerprint.size > cap as u64 {
            return Err(RuntimeError::Invalid("bootstrap byte cap"));
        }
        let mut bytes = Vec::new();
        (&file).take(cap as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > cap || FileFingerprint::of(&file.metadata()?) != fingerprint {
            return Err(RuntimeError::Changed("bootstrap descriptor"));
        }
        dir.verify_file_named(OsStr::new(&name), &fingerprint)?;
        self.verify()?;
        let bootstrap = super::RuntimeBootstrap::decode(&bytes)
            .map_err(|_| RuntimeError::Invalid("bootstrap contract"))?;
        if bootstrap.namespace != *namespace
            || bootstrap.instance_uuid != instance
            || bootstrap.uid != self.uid()
        {
            return Err(RuntimeError::Changed("bootstrap epoch/account"));
        }
        Ok(BootstrapSnapshot {
            bootstrap,
            fingerprint,
            bytes,
        })
    }

    /// Consume only the exact handoff that passed child validation. No lease,
    /// record, listener or process is removed by this operation.
    pub fn consume_bootstrap(&self, captured: &BootstrapSnapshot) -> Result<(), RuntimeError> {
        let bootstrap = &captured.bootstrap;
        let current = self.read_bootstrap(&bootstrap.namespace, bootstrap.instance_uuid)?;
        if current.fingerprint != captured.fingerprint || current.bytes != captured.bytes {
            return Err(RuntimeError::Changed("bootstrap consumption identity"));
        }
        let dir = self
            .records
            .as_ref()
            .ok_or(RuntimeError::Unavailable("record directory"))?;
        dir.remove_file(
            OsStr::new(&bootstrap_name(
                &bootstrap.namespace,
                bootstrap.instance_uuid,
            )),
            &captured.fingerprint,
        )?;
        self.verify()
    }

    fn open_lock(
        &self,
        namespace: &Namespace,
        suffix: &str,
        create: bool,
    ) -> Result<OpenedLock, RuntimeError> {
        self.verify()?;
        let dir = self
            .locks
            .as_ref()
            .ok_or(RuntimeError::Unavailable("lock directory"))?;
        let name = format!("{}.{suffix}", namespace.hex());
        let flags = libc::O_RDWR | if create { libc::O_CREAT } else { 0 };
        let file = dir
            .open_file(OsStr::new(&name), flags)?
            .ok_or(RuntimeError::Unavailable("lock inode"))?;
        let fingerprint = FileFingerprint::of(&file.metadata()?);
        if fingerprint.size != 0 {
            return Err(RuntimeError::Invalid("nonempty lock inode"));
        }
        Ok(OpenedLock {
            file,
            name,
            namespace: namespace.clone(),
            fingerprint,
        })
    }
}

struct OpenedLock {
    file: File,
    name: String,
    namespace: Namespace,
    fingerprint: FileFingerprint,
}
pub struct OperationLock {
    opened: OpenedLock,
}
impl OperationLock {
    pub fn namespace(&self) -> &Namespace {
        &self.opened.namespace
    }
    pub fn verify(&self, authority: &RuntimeAuthority) -> Result<(), RuntimeError> {
        self.opened.verify(authority)
    }
}
impl OpenedLock {
    fn verify(&self, authority: &RuntimeAuthority) -> Result<(), RuntimeError> {
        authority.verify()?;
        let dir = authority
            .locks
            .as_ref()
            .ok_or(RuntimeError::Unavailable("lock directory"))?;
        if FileFingerprint::of(&self.file.metadata()?) != self.fingerprint {
            return Err(RuntimeError::Changed("lock descriptor"));
        }
        dir.verify_file_named(OsStr::new(&self.name), &self.fingerprint)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseObservation {
    Missing,
    Free,
    Contended,
}

fn bootstrap_name(namespace: &Namespace, instance: super::InstanceId) -> String {
    format!("{}.{}.bootstrap.json", namespace.hex(), instance.uuid())
}

/// Lifetime reference; drop closes only this reference. No explicit unlock or
/// second pathname open is implemented for a holder.
pub struct LifetimeLease {
    opened: OpenedLock,
}
impl LifetimeLease {
    /// Adopts the inherited description without opening or re-locking its path.
    /// The received descriptor is made close-on-exec before serving, so unrelated
    /// future execs do not inherit lifetime ownership.
    ///
    /// # Safety
    /// The caller must independently validate the intended-child bootstrap and
    /// prove that `descriptor` is a reference to the launcher's already-locked
    /// open description, not a separately opened descriptor. Filesystem checks
    /// below cannot prove flock ownership. A qualified handoff is mandatory.
    pub unsafe fn adopt_inherited(
        authority: &RuntimeAuthority,
        namespace: &Namespace,
        descriptor: OwnedFd,
        expected: super::InodeIdentity,
    ) -> Result<Self, RuntimeError> {
        let file = File::from(descriptor);
        let fingerprint = FileFingerprint::of(&file.metadata()?);
        if fingerprint.identity != expected || fingerprint.size != 0 {
            return Err(RuntimeError::Changed("inherited lease descriptor"));
        }
        // SAFETY: live owned fd and scalar fcntl descriptor-flag operations.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
        if flags < 0
            || unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let lease = Self {
            opened: OpenedLock {
                file,
                name: format!("{}.lease", namespace.hex()),
                namespace: namespace.clone(),
                fingerprint,
            },
        };
        lease.verify(authority)?;
        Ok(lease)
    }
    pub fn namespace(&self) -> &Namespace {
        &self.opened.namespace
    }
    pub fn verify(&self, authority: &RuntimeAuthority) -> Result<(), RuntimeError> {
        self.opened.verify(authority)
    }
    pub fn identity(&self) -> super::InodeIdentity {
        self.opened.fingerprint.identity
    }

    /// dup creates a reference to the same flock open description. CLOEXEC is
    /// initially set; qualified child bootstrap clears it only in the forked
    /// child. The parent never opens the pathname again or changes its flags.
    /// This foundation does not itself claim an exec handoff has been proven.
    pub fn duplicate_for_child(&self) -> Result<OwnedFd, RuntimeError> {
        // SAFETY: live owned descriptor, valid dup command, no pointer argument.
        let fd = unsafe { libc::fcntl(self.opened.file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: successful fcntl returned a newly owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
    pub fn descriptor(&self) -> std::os::fd::BorrowedFd<'_> {
        self.opened.file.as_fd()
    }
}
fn acquire(file: &File) -> Result<(), RuntimeError> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(RuntimeError::Locked),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::PathBytes;
    use super::*;
    use std::path::Path;

    #[test]
    fn duplicate_keeps_same_flock_description_after_holder_drop_without_unlock_or_relock() {
        // Generic persistent-base primitive proof, not real-C/bootstrap proof.
        let fixture = super::super::filesystem::tests::fixture();
        // SAFETY: pure effective UID getter.
        let uid = unsafe { libc::geteuid() };
        let dir = PrivateDirectory::open_canonical(fixture.path(), uid).unwrap();
        let namespace = Namespace::derive(
            uid,
            &PathBytes::from_path(Path::new("/owned/config")).unwrap(),
            "profile",
        )
        .unwrap();
        let name = format!("{}.lease", namespace.hex());
        let file = dir
            .open_file(
                OsStr::new(&name),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            )
            .unwrap()
            .unwrap();
        acquire(&file).unwrap();
        let fingerprint = FileFingerprint::of(&file.metadata().unwrap());
        let lease = LifetimeLease {
            opened: OpenedLock {
                file,
                name: name.clone(),
                namespace,
                fingerprint: fingerprint.clone(),
            },
        };
        let duplicate = lease.duplicate_for_child().unwrap();
        // SAFETY: F_GETFD only observes live descriptors.
        let parent_flags = unsafe { libc::fcntl(lease.descriptor().as_raw_fd(), libc::F_GETFD) };
        let child_flags = unsafe { libc::fcntl(duplicate.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(parent_flags & libc::FD_CLOEXEC, 0);
        assert_ne!(child_flags & libc::FD_CLOEXEC, 0);
        let observer = dir
            .open_file(OsStr::new(&name), libc::O_RDWR)
            .unwrap()
            .unwrap();
        assert!(matches!(acquire(&observer), Err(RuntimeError::Locked)));
        dir.verify_file_named(OsStr::new(&name), &fingerprint)
            .unwrap();
        drop(lease);
        assert!(matches!(acquire(&observer), Err(RuntimeError::Locked)));
        drop(duplicate);
        acquire(&observer).unwrap();
        assert!(dir.path().join(name).exists());
    }

    /// Run explicitly on the qualified host. This never installs/cycles an
    /// estate daemon and uses one unique fixture namespace through real C.
    #[test]
    #[ignore = "qualified normal-host native C fixture; outside sandbox write scope"]
    fn native_authority_namespace_survives_environment_changes_and_refuses_record_corruption() {
        let fixture = super::super::filesystem::tests::fixture();
        let root = std::fs::canonicalize(fixture.path()).unwrap();
        let account = NativeAccount::resolve().unwrap();
        let profile = format!("native-fixture-{}", uuid::Uuid::new_v4());
        let namespace = Namespace::derive(
            account.uid(),
            &PathBytes::from_path(&root).unwrap(),
            &profile,
        )
        .unwrap();
        let authority = RuntimeAuthority::initialize().unwrap();
        assert_eq!(
            authority.path(),
            std::fs::canonicalize(account.home())
                .unwrap()
                .join(".chanvoy/lifecycle")
        );
        let operation = authority.try_operation(&namespace).unwrap();
        let lease = authority.try_lifetime(&operation).unwrap();
        let record = RuntimeRecord {
            schema_version: 1,
            namespace: namespace.clone(),
            instance_uuid: super::super::InstanceId::new(),
            revision: 0,
            phase: RuntimePhase::Starting,
            uid: account.uid(),
            config_root: PathBytes::from_path(&root).unwrap(),
            profile: profile.clone(),
            process: None,
            socket: None,
            build: None,
            ready: false,
        };
        let starting = authority
            .create_starting(&operation, &lease, &record)
            .unwrap();
        assert_eq!(
            authority.read_record(&namespace).unwrap(),
            Some(starting.clone())
        );
        assert!(authority
            .create_starting(&operation, &lease, &record)
            .is_err());
        run_probe(
            &root,
            &profile,
            &namespace,
            authority.path(),
            "contended",
            false,
        );
        run_probe(
            &root,
            &profile,
            &namespace,
            authority.path(),
            "contended",
            true,
        );
        drop(lease);
        run_probe(&root, &profile, &namespace, authority.path(), "free", true);

        // Corrupt only this fixture's record under its retained operation lock.
        let dir = authority.records.as_ref().unwrap();
        let name = format!("{}.json", namespace.hex());
        let mut malformed = record.encode().unwrap();
        malformed.extend_from_slice(b" null");
        let fingerprint = dir
            .publish(OsStr::new(&name), Some(&starting.fingerprint), &malformed)
            .unwrap();
        assert!(authority.read_record(&namespace).is_err());
        let inventory = super::super::DiscoveryInventory {
            v1_markers: super::super::MarkerInventory::Present,
            legacy: super::super::LegacyInventory::Sole(PathBuf::from("/ignored/legacy.sock")),
        };
        assert!(super::super::RuntimeResolver::resolve(&root, &profile, &inventory).is_err());
        dir.publish(OsStr::new(&name), Some(&fingerprint), &starting.bytes)
            .unwrap();
        let restored = authority.read_record(&namespace).unwrap().unwrap();
        // No child was ever spawned, so no native-process claim exists. Leave
        // a typed Exited fixture tombstone; never unlink stable lock inodes.
        let mut exited = record;
        exited.revision = 1;
        exited.phase = RuntimePhase::Exited;
        authority.publish_record(Some(&restored), &exited).unwrap();
        operation.verify(&authority).unwrap();
        println!("native C namespace/record fixture PASS; no child/service/handoff proof claimed");
    }

    fn run_probe(
        root: &Path,
        profile: &str,
        namespace: &Namespace,
        authority: &Path,
        lease_state: &str,
        state_override: bool,
    ) {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "runtime::authority::tests::fixture_probe_entry",
                "--ignored",
                "--nocapture",
            ])
            .env("CHANVOY_FIXTURE_CONFIG", root)
            .env("CHANVOY_FIXTURE_PROFILE", profile)
            .env("CHANVOY_FIXTURE_NAMESPACE", namespace.hex())
            .env("CHANVOY_FIXTURE_C", authority)
            .env("CHANVOY_FIXTURE_LEASE", lease_state)
            .env("HOME", "/private/tmp/chanvoy-unrelated-home")
            .env("XDG_CONFIG_HOME", "/private/tmp/chanvoy-unrelated-config")
            .env("XDG_RUNTIME_DIR", "/private/tmp/chanvoy-unrelated-runtime")
            .env("TMPDIR", "/private/tmp/chanvoy-unrelated-temp");
        if state_override {
            command.env("CHANVOY_STATE_DIR", "/private/tmp/chanvoy-refused-state");
        } else {
            command.env_remove("CHANVOY_STATE_DIR");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "native fixture probe: {:?}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    #[ignore = "child helper; invoke only through the explicit native authority fixture"]
    fn fixture_probe_entry() {
        let root =
            PathBuf::from(std::env::var_os("CHANVOY_FIXTURE_CONFIG").expect("fixture config"));
        let profile = std::env::var("CHANVOY_FIXTURE_PROFILE").expect("fixture profile");
        let account = NativeAccount::resolve().unwrap();
        let namespace = Namespace::derive(
            account.uid(),
            &PathBytes::from_path(&std::fs::canonicalize(root).unwrap()).unwrap(),
            &profile,
        )
        .unwrap();
        assert_eq!(
            namespace.hex(),
            std::env::var("CHANVOY_FIXTURE_NAMESPACE").unwrap()
        );
        let authority = RuntimeAuthority::observe().unwrap().unwrap();
        assert_eq!(
            authority.path(),
            PathBuf::from(std::env::var_os("CHANVOY_FIXTURE_C").unwrap())
        );
        assert!(matches!(
            authority.try_operation(&namespace),
            Err(RuntimeError::Locked)
        ));
        let expected = match std::env::var("CHANVOY_FIXTURE_LEASE").unwrap().as_str() {
            "contended" => LeaseObservation::Contended,
            "free" => LeaseObservation::Free,
            _ => panic!("fixture lease expectation"),
        };
        assert_eq!(authority.observe_lease(&namespace).unwrap(), expected);
        let record = authority.read_record(&namespace).unwrap().unwrap();
        assert_eq!(record.record.profile, profile);
        assert_eq!(record.record.phase, RuntimePhase::Starting);
        if std::env::var_os("CHANVOY_STATE_DIR").is_some() {
            assert!(matches!(
                RuntimeAuthority::initialize(),
                Err(RuntimeError::Invalid("state-root override unsupported"))
            ));
        }
    }
    #[test]
    fn lifetime_description_survives_exec_and_parent_reference_drop() {
        use std::io::{BufRead, Write};
        use std::os::unix::process::CommandExt;
        let fixture = super::super::filesystem::tests::fixture();
        let uid = unsafe { libc::geteuid() };
        let dir = PrivateDirectory::open_canonical(fixture.path(), uid).unwrap();
        let namespace = Namespace::derive(
            uid,
            &PathBytes::from_path(fixture.path()).unwrap(),
            "exec-lease",
        )
        .unwrap();
        let name = format!("{}.lease", namespace.hex());
        let file = dir
            .open_file(
                OsStr::new(&name),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            )
            .unwrap()
            .unwrap();
        acquire(&file).unwrap();
        let lease = LifetimeLease {
            opened: OpenedLock {
                fingerprint: FileFingerprint::of(&file.metadata().unwrap()),
                file,
                name: name.clone(),
                namespace,
            },
        };
        let child_fd = lease.duplicate_for_child().unwrap();
        let raw = child_fd.as_raw_fd();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "runtime::authority::tests::inherited_lease_child_fixture",
                "--nocapture",
            ])
            .env("CHANVOY_TEST_LEASE_FD", raw.to_string())
            .env(
                "CHANVOY_TEST_LEASE_DEVICE",
                lease.identity().device.to_string(),
            )
            .env(
                "CHANVOY_TEST_LEASE_INODE",
                lease.identity().inode.to_string(),
            )
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        // SAFETY: post-fork callback uses only descriptor fcntl operations and
        // scalar state. Parent descriptor flags are never changed.
        unsafe {
            command.pre_exec(move || {
                let flags = libc::fcntl(raw, libc::F_GETFD);
                if flags < 0 || libc::fcntl(raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        drop(child_fd);
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "child ended before inherited-fd proof"
            );
            if line.contains("LEASE_INHERITED_READY") {
                break;
            }
            line.clear();
        }
        let observer = dir
            .open_file(OsStr::new(&name), libc::O_RDWR)
            .unwrap()
            .unwrap();
        assert!(matches!(acquire(&observer), Err(RuntimeError::Locked)));
        drop(lease);
        assert!(
            matches!(acquire(&observer), Err(RuntimeError::Locked)),
            "child must retain the original locked description"
        );
        child.stdin.take().unwrap().write_all(b"x").unwrap();
        assert!(child.wait().unwrap().success());
        acquire(&observer).unwrap();
    }

    #[test]
    #[ignore = "exec fixture helper, invoked only by lifetime description test"]
    fn inherited_lease_child_fixture() {
        use std::io::{Read, Write};
        // Fixture-only deadman bounds both parent-output and child-input waits.
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(10));
            std::process::exit(91);
        });
        let fd: i32 = std::env::var("CHANVOY_TEST_LEASE_FD")
            .unwrap()
            .parse()
            .unwrap();
        assert!(fd >= 3);
        // SAFETY: test launcher deliberately inherited this sole child-owned fd.
        let file = unsafe { File::from_raw_fd(fd) };
        let observed = FileFingerprint::of(&file.metadata().unwrap());
        assert_eq!(
            observed.identity.device.to_string(),
            std::env::var("CHANVOY_TEST_LEASE_DEVICE").unwrap()
        );
        assert_eq!(
            observed.identity.inode.to_string(),
            std::env::var("CHANVOY_TEST_LEASE_INODE").unwrap()
        );
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(flags >= 0);
        assert_eq!(flags & libc::FD_CLOEXEC, 0);
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) },
            0
        );
        assert_ne!(
            unsafe { libc::fcntl(fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        println!("LEASE_INHERITED_READY");
        std::io::stdout().flush().unwrap();
        let mut release = [0u8];
        std::io::stdin().read_exact(&mut release).unwrap();
        assert_eq!(release, [b'x']);
        drop(file);
    }
}
