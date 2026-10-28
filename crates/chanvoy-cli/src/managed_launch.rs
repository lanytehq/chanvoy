//! Parent-side inherited handoff. Admission/discovery must precede construction;
//! this module never interprets missing records as permission to start a child.
#![allow(dead_code)]
use chanvoy_core::runtime::control::READINESS_MAX_BYTES;
use chanvoy_core::runtime::{
    native_peer_pid, LifetimeLease, OperationLock, PathBytes, ProcessId, RecordSnapshot,
    RuntimeAuthority, RuntimeBootstrap, RuntimeBuildPin, RuntimeError, RuntimeLaunchMarker,
    RuntimeProcess, RuntimeReadiness, SocketRoot,
};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::process::Stdio;
use std::time::Instant;
use tokio::io::AsyncReadExt;

/// Keep this owner across every error and cancellation. Dropping it neither
/// signals a child nor removes evidence. Reconciliation requires independent
/// whole-process death and exact artifact checks, never just a free lease.
pub(crate) struct ManagedLaunch {
    authority: RuntimeAuthority,
    operation: OperationLock,
    lease: LifetimeLease,
    starting: RecordSnapshot,
    socket_root: SocketRoot,
    deadline: Instant,
    child: Option<tokio::process::Child>,
    readiness: Option<tokio::net::UnixStream>,
    attempted: bool,
}
impl ManagedLaunch {
    /// The caller must already have reconciled complete native discovery under
    /// this operation lock and created the exact revision-zero starting record.
    pub(crate) fn new(
        authority: RuntimeAuthority,
        operation: OperationLock,
        lease: LifetimeLease,
        starting: RecordSnapshot,
        socket_root: SocketRoot,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        operation.verify(&authority)?;
        lease.verify(&authority)?;
        starting.record.validate()?;
        if operation.namespace() != lease.namespace()
            || operation.namespace() != &starting.record.namespace
            || starting.record.revision != 0
            || !authority.unchanged(&starting)?
        {
            return Err(RuntimeError::Changed("managed launch ownership"));
        }
        socket_root.verify()?;
        check_deadline(deadline)?;
        Ok(Self {
            authority,
            operation,
            lease,
            starting,
            socket_root,
            deadline,
            child: None,
            readiness: None,
            attempted: false,
        })
    }

    /// Synchronous preparation/spawn has no cancellation point. After spawn,
    /// this owner retains both the child handle and the parent's lease reference.
    pub(crate) fn spawn(
        &mut self,
        profile: &chanvoy_core::Profile,
        identity: &chanvoy_core::Identity,
    ) -> Result<(), RuntimeError> {
        if self.attempted {
            return Err(RuntimeError::Invalid("launch already attempted"));
        }
        self.attempted = true;
        check_deadline(self.deadline)?;
        let build = chanvoy_core::resolve_host_build_info();
        require_known_build(&RuntimeBuildPin {
            version: build.version,
            commit: build.commit,
            dirty: build.dirty,
        })?;
        self.operation.verify(&self.authority)?;
        self.lease.verify(&self.authority)?;
        if !self.authority.unchanged(&self.starting)?
            || profile.name != self.starting.record.profile
        {
            return Err(RuntimeError::Changed("launch starting context"));
        }
        if identity.username != profile.bot_username
            || identity.id.is_empty()
            || std::fs::canonicalize(chanvoy_core::default_chanvoy_config_dir())?
                != self.starting.record.config_root.to_path()
        {
            return Err(RuntimeError::Changed("launch validated profile identity"));
        }
        let launcher = RuntimeProcess::observe_current()?;
        let marker = self.marker();
        let arguments = marker.arguments(launcher.executable.to_path().as_os_str())?;
        let inherited = self.lease.duplicate_for_child()?;
        let (parent, child_end) = std::os::unix::net::UnixStream::pair()?;
        parent.set_nonblocking(true)?;
        let parent = tokio::net::UnixStream::from_std(parent)?;
        let nonce = RuntimeBootstrap::fresh_nonce();
        let bootstrap = RuntimeBootstrap {
            schema_version: 1,
            kind: "runtime_bootstrap".into(),
            namespace: marker.namespace.clone(),
            instance_uuid: marker.instance_uuid,
            launcher_revision: 0,
            uid: self.authority.uid(),
            config_root: marker.config_root.clone(),
            profile: marker.profile.clone(),
            socket_root: PathBytes::from_path(self.socket_root.path())?,
            lease_descriptor: inherited.as_raw_fd(),
            lease_identity: self.lease.identity(),
            validated_username: identity.username.clone(),
            validated_user_id: identity.id.clone(),
            profile_fingerprint: chanvoy_core::compute_profile_fingerprint(profile),
            issued_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| RuntimeError::Unavailable("launch clock"))?
                .as_secs(),
            launcher_process: launcher.clone(),
            bootstrap_nonce: nonce.clone(),
        };
        self.authority
            .write_bootstrap(&self.operation, &self.lease, &self.starting, &bootstrap)?;
        let mut command = tokio::process::Command::new(&arguments[0]);
        configure_environment(
            &mut command,
            profile,
            &marker.config_root,
            &self.socket_root,
        )?;
        command
            .args(&arguments[1..])
            .env(
                super::managed_environment::LEASE_FD_ENV,
                inherited.as_raw_fd().to_string(),
            )
            .env(
                super::managed_environment::READINESS_FD_ENV,
                child_end.as_raw_fd().to_string(),
            )
            .env(super::managed_environment::NONCE_ENV, nonce)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let descriptors = [inherited.as_raw_fd(), child_end.as_raw_fd()];
        // SAFETY: the child-side closure uses only async-signal-safe syscalls.
        // Parent descriptors remain CLOEXEC. Only these two copies cross exec;
        // the operation lock does not, and the lifetime fd is never re-locked.
        unsafe {
            command.pre_exec(move || {
                for fd in descriptors {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        launcher.revalidate()?;
        check_deadline(self.deadline)?;
        self.child = Some(command.spawn()?);
        self.readiness = Some(parent);
        Ok(())
    }

    pub(crate) async fn accept(
        &mut self,
        expected_build: &RuntimeBuildPin,
    ) -> Result<RuntimeReadiness, RuntimeError> {
        check_deadline(self.deadline)?;
        require_known_build(expected_build)?;
        let stream = self
            .readiness
            .as_mut()
            .ok_or(RuntimeError::Invalid("no pending handoff"))?;
        let ready = read_readiness(stream, self.deadline).await?;
        let child = self
            .child
            .as_mut()
            .ok_or(RuntimeError::Invalid("no spawned child"))?;
        if child.try_wait()?.is_some() {
            return Err(RuntimeError::Unavailable("child exited before acceptance"));
        }
        let pid = ProcessId::new(child.id().ok_or(RuntimeError::Unavailable("child PID"))?)?;
        let native = RuntimeProcess::observe(pid)?;
        let record = self
            .authority
            .read_record(&self.starting.record.namespace)?
            .ok_or(RuntimeError::Changed("readiness record missing"))?;
        ready.check_record(&record.record)?;
        if ready.instance_uuid != self.starting.record.instance_uuid
            || ready.process != native
            || &ready.build != expected_build
            || record.record.config_root != self.starting.record.config_root
            || record.record.profile != self.starting.record.profile
        {
            return Err(RuntimeError::Changed("readiness native epoch/build"));
        }
        self.marker().check_arguments(
            native.executable.to_path().as_os_str(),
            &native.observe_arguments()?,
        )?;
        self.socket_root.verify()?;
        if self.socket_root.child_path(pid)? != ready.socket.path {
            return Err(RuntimeError::Changed("readiness socket location"));
        }
        let path = ready.socket.path.to_path();
        let before = std::fs::symlink_metadata(&path)?;
        if !before.file_type().is_socket()
            || before.uid() != native.uid
            || before.mode() & 0o777 != 0o600
            || before.dev() != ready.socket.identity.device
            || before.ino() != ready.socket.identity.inode
        {
            return Err(RuntimeError::Changed("readiness socket identity"));
        }
        let connected =
            tokio::time::timeout_at(self.deadline.into(), tokio::net::UnixStream::connect(&path))
                .await
                .map_err(|_| RuntimeError::Unavailable("readiness deadline"))??;
        if native_peer_pid(connected.as_fd())? != pid {
            return Err(RuntimeError::Changed("readiness listener peer"));
        }
        let after = std::fs::symlink_metadata(&path)?;
        if before.dev() != after.dev()
            || before.ino() != after.ino()
            || before.mode() != after.mode()
            || before.uid() != after.uid()
        {
            return Err(RuntimeError::Changed("readiness socket replaced"));
        }
        native.revalidate()?;
        self.operation.verify(&self.authority)?;
        self.lease.verify(&self.authority)?;
        if !self.authority.unchanged(&record)? {
            return Err(RuntimeError::Changed("readiness publication"));
        }
        check_deadline(self.deadline)?;
        Ok(ready)
    }
    /// Observe only the child owned by this launch, retaining both guards on
    /// timeout/cancellation. Exit status alone grants no artifact cleanup.
    pub(crate) async fn wait_child_exit(
        &mut self,
    ) -> Result<std::process::ExitStatus, RuntimeError> {
        check_deadline(self.deadline)?;
        let child = self
            .child
            .as_mut()
            .ok_or(RuntimeError::Invalid("no launched child"))?;
        let status = tokio::time::timeout_at(self.deadline.into(), child.wait())
            .await
            .map_err(|_| RuntimeError::Unavailable("launched child exit deadline"))??;
        self.operation.verify(&self.authority)?;
        self.lease.verify(&self.authority)?;
        check_deadline(self.deadline)?;
        Ok(status)
    }
    fn marker(&self) -> RuntimeLaunchMarker {
        let record = &self.starting.record;
        RuntimeLaunchMarker {
            namespace: record.namespace.clone(),
            instance_uuid: record.instance_uuid,
            config_root: record.config_root.clone(),
            profile: record.profile.clone(),
        }
    }
}
// Deliberately construct a descendant environment. Never propagate arbitrary
// agent credentials, loader injection variables, or stale handoff hints.
fn configure_environment(
    command: &mut tokio::process::Command,
    profile: &chanvoy_core::Profile,
    config_root: &PathBytes,
    socket_root: &SocketRoot,
) -> Result<(), RuntimeError> {
    command.env_clear();
    command.env(
        "HOME",
        chanvoy_core::runtime::NativeAccount::resolve()?.home(),
    );
    command.env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin");
    command.env("CHANVOY_CONFIG_DIR", config_root.to_path());
    command.env("CHANVOY_RUNTIME_DIR", socket_root.path());
    // Explicit networking/trust configuration is part of the provider setup.
    for key in TRANSPORT_ENV {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    add_profile_credential(command, profile)?;
    if let Some(reduction) = &profile.reduce {
        let family = chanvoy_core::load_profile(&reduction.use_profile)
            .map_err(|_| RuntimeError::Unavailable("reduction credential profile"))?;
        add_profile_credential(command, &family)?;
    }
    Ok(())
}
const TRANSPORT_ENV: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "RUST_LOG",
];
fn credential_key_allowed(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
        && !key.starts_with("LD_")
        && !key.starts_with("DYLD_")
        && !key.starts_with("CHANVOY_RUNTIME_")
        && ![
            "CHANVOY_CONFIG_DIR",
            "CHANVOY_STATE_DIR",
            "CHANVOY_PROFILE",
            "CHANVOY_BOOTSTRAP_NONCE",
        ]
        .contains(&key)
        && ![
            "HOME", "PATH", "TMPDIR", "TMP", "TEMP", "SHELL", "ENV", "BASH_ENV", "IFS",
        ]
        .contains(&key)
        && !TRANSPORT_ENV.contains(&key)
}
fn add_profile_credential(
    command: &mut tokio::process::Command,
    profile: &chanvoy_core::Profile,
) -> Result<(), RuntimeError> {
    if matches!(
        profile.credential_mode,
        chanvoy_core::CredentialMode::EnvFile
    ) {
        return Ok(());
    }
    if !credential_key_allowed(&profile.env_name) {
        return Err(RuntimeError::Invalid(
            "credential environment key conflicts with launch controls",
        ));
    }
    let value = std::env::var_os(&profile.env_name)
        .filter(|value| !value.is_empty())
        .ok_or(RuntimeError::Unavailable("launch profile credential"))?;
    command.env(&profile.env_name, value);
    Ok(())
}
pub(super) fn require_known_build(build: &RuntimeBuildPin) -> Result<(), RuntimeError> {
    if build.version.is_empty()
        || build.commit.len() != 40
        || !build.commit.bytes().all(|b| b.is_ascii_hexdigit())
        || build.dirty.is_none()
    {
        return Err(RuntimeError::Unavailable(
            "managed launch generation unknown",
        ));
    }
    Ok(())
}
fn check_deadline(deadline: Instant) -> Result<(), RuntimeError> {
    if Instant::now() >= deadline {
        Err(RuntimeError::Unavailable("readiness deadline"))
    } else {
        Ok(())
    }
}
async fn read_readiness(
    stream: &mut tokio::net::UnixStream,
    deadline: Instant,
) -> Result<RuntimeReadiness, RuntimeError> {
    check_deadline(deadline)?;
    let mut bytes = Vec::new();
    tokio::time::timeout_at(
        deadline.into(),
        stream
            .take(READINESS_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes),
    )
    .await
    .map_err(|_| RuntimeError::Unavailable("readiness deadline"))??;
    check_deadline(deadline)?;
    if bytes.len() > READINESS_MAX_BYTES {
        return Err(RuntimeError::Invalid("readiness byte cap"));
    }
    RuntimeReadiness::decode(&bytes).map_err(|_| RuntimeError::Invalid("readiness contract"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    const READY: &[u8] = include_bytes!("../../../schemas/runtime/control/v1/fixtures/chanvoy-runtime-control-v1/runtime_readiness-positive.json");
    fn deadline() -> Instant {
        Instant::now() + std::time::Duration::from_millis(100)
    }

    #[test]
    fn credential_keys_cannot_override_loader_or_launch_configuration() {
        for key in [
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
            "HOME",
            "PATH",
            "CHANVOY_CONFIG_DIR",
            "HTTPS_PROXY",
            "A=B",
            "1TOKEN",
            "",
        ] {
            assert!(!credential_key_allowed(key), "{key}");
        }
        assert!(credential_key_allowed("LANYTE_MM_TOKEN"));
        assert!(credential_key_allowed("CHANVOY_TOKEN"));
    }
    #[test]
    fn equal_unknown_builds_do_not_authorize_handoff() {
        let mut build = RuntimeBuildPin {
            version: "0.3.3".into(),
            commit: "unknown".into(),
            dirty: None,
        };
        assert!(require_known_build(&build).is_err());
        build.commit = "a".repeat(40);
        assert!(require_known_build(&build).is_err());
        build.dirty = Some(false);
        assert!(require_known_build(&build).is_ok());
    }
    #[tokio::test]
    async fn readiness_accepts_exact_limit_with_eof() {
        let (mut parent, mut child) = tokio::net::UnixStream::pair().unwrap();
        let mut bytes = READY.to_vec();
        bytes.resize(READINESS_MAX_BYTES, b' ');
        let sender = async {
            child.write_all(&bytes).await.unwrap();
            child.shutdown().await.unwrap();
        };
        let reader = read_readiness(&mut parent, deadline());
        let (_, result) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(sender, reader)
        })
        .await
        .unwrap();
        assert!(result.is_ok());
    }
    #[tokio::test]
    async fn readiness_requires_eof_not_just_a_complete_json_value() {
        let (mut parent, mut child) = tokio::net::UnixStream::pair().unwrap();
        child.write_all(READY).await.unwrap();
        assert!(matches!(
            read_readiness(&mut parent, deadline()).await,
            Err(RuntimeError::Unavailable("readiness deadline"))
        ));
    }
    #[tokio::test]
    async fn readiness_accepts_one_strict_document_and_rejects_trailing_payload() {
        for suffix in [&b""[..], &b"{}"[..]] {
            let (mut parent, mut child) = tokio::net::UnixStream::pair().unwrap();
            child.write_all(READY).await.unwrap();
            child.write_all(suffix).await.unwrap();
            child.shutdown().await.unwrap();
            assert_eq!(
                read_readiness(&mut parent, deadline()).await.is_ok(),
                suffix.is_empty()
            );
        }
    }
    #[tokio::test]
    async fn readiness_refuses_expired_parent_budget_even_with_buffered_ready() {
        let (mut parent, mut child) = tokio::net::UnixStream::pair().unwrap();
        child.write_all(READY).await.unwrap();
        child.shutdown().await.unwrap();
        assert!(matches!(
            read_readiness(&mut parent, Instant::now()).await,
            Err(RuntimeError::Unavailable("readiness deadline"))
        ));
    }
    #[tokio::test]
    async fn readiness_caps_bytes_without_waiting_for_peer_eof() {
        let (mut parent, mut child) = tokio::net::UnixStream::pair().unwrap();
        let sender = async {
            child
                .write_all(&vec![b' '; READINESS_MAX_BYTES + 1])
                .await
                .unwrap();
        };
        let reader = read_readiness(&mut parent, deadline());
        let (_, result) = tokio::join!(sender, reader);
        assert!(matches!(
            result,
            Err(RuntimeError::Invalid("readiness byte cap"))
        ));
    }
}
