//! Process-entry handoff capture, before any async runtime threads exist.
use super::managed_environment::{LEASE_FD_ENV, NONCE_ENV, READINESS_FD_ENV};
use super::{Cli, CliError, CommandSet, DaemonCommand};
use clap::Parser;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// Opaque invocation keeps the managed lifetime anchor until the binary exits.
pub struct PreparedInvocation {
    cli: Option<Cli>,
    managed: Option<chanvoy_daemon::PreparedManagedDaemon>,
    _lifetime: Option<chanvoy_daemon::ManagedProcessLifetime>,
    is_managed: bool,
}
impl PreparedInvocation {
    pub fn is_managed(&self) -> bool {
        self.is_managed
    }
    pub async fn run(&mut self) -> Result<(), CliError> {
        super::init_tracing();
        let cli = self
            .cli
            .take()
            .ok_or_else(|| refusal("invocation already consumed"))?;
        if let Some(managed) = self.managed.take() {
            managed.serve().await?;
            Ok(())
        } else {
            super::execute(cli).await
        }
    }
}

fn refusal(reason: &'static str) -> CliError {
    CliError::Bootstrap(reason.into())
}

/// # Safety
/// Call exactly once at the start of the binary's main function, before any
/// runtime/worker threads or other Rust descriptor owners are created. Any
/// error requires process exit. The managed success path must also terminate
/// the process while retaining PreparedInvocation through runtime teardown.
/// Descriptor numbers come only from the qualified launcher's inherited set;
/// this API must not be exposed as a late library/bootstrap fallback.
pub unsafe fn prepare_process_entry() -> Result<PreparedInvocation, CliError> {
    let advertised = [LEASE_FD_ENV, READINESS_FD_ENV, NONCE_ENV]
        .iter()
        .any(|key| std::env::var_os(key).is_some());
    let captured = if advertised {
        // Each guard is acquired/restored immediately, before looking up the
        // next descriptor or parsing configuration. Never construct two owners
        // for the same raw descriptor.
        let lease = unsafe { capture(LEASE_FD_ENV, None) }?;
        let readiness = unsafe { capture(READINESS_FD_ENV, Some(lease.as_raw_fd())) }?;
        let nonce = std::env::var(NONCE_ENV).map_err(|_| refusal("managed nonce missing"))?;
        if nonce.len() != 64
            || !nonce
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(refusal("managed nonce format"));
        }
        Some((lease, readiness, nonce))
    } else {
        None
    };
    // SAFETY: this API is called exactly once before runtime/worker threads.
    // Descriptors are now owned and CLOEXEC; descendants must not inherit stale
    // hints or the bootstrap nonce after those descriptors disappear at exec.
    for key in [LEASE_FD_ENV, READINESS_FD_ENV, NONCE_ENV] {
        unsafe { std::env::remove_var(key) };
    }
    let cli = Cli::parse();
    let marker = match &cli.command {
        CommandSet::Daemon(DaemonCommand::Serve(args)) if args.runtime_namespace.is_some() => {
            let profile = cli
                .profile
                .clone()
                .ok_or_else(|| refusal("managed child requires explicit profile"))?;
            let parse = |text: &str| serde_json::Value::String(text.to_owned());
            let namespace =
                serde_json::from_value(parse(args.runtime_namespace.as_deref().unwrap()))
                    .map_err(|_| refusal("managed namespace marker"))?;
            let instance_uuid = serde_json::from_value(parse(
                args.runtime_instance
                    .as_deref()
                    .ok_or_else(|| refusal("managed instance marker missing"))?,
            ))
            .map_err(|_| refusal("managed instance marker"))?;
            let config_root = serde_json::from_value(parse(
                args.runtime_config_root
                    .as_deref()
                    .ok_or_else(|| refusal("managed configuration marker missing"))?,
            ))
            .map_err(|_| refusal("managed configuration marker"))?;
            Some(chanvoy_core::runtime::RuntimeLaunchMarker {
                namespace,
                instance_uuid,
                config_root,
                profile,
            })
        }
        _ => None,
    };
    let managed = match (marker, captured) {
        (None, None) => None,
        (Some(marker), Some((lease, readiness, nonce))) => {
            // This child budget does not replace the parent's original deadline.
            // The parent must reject late readiness even when child checks pass.
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_secs(chanvoy_core::BOOTSTRAP_MAX_AGE_SECS);
            Some(unsafe {
                chanvoy_daemon::prepare_managed_process(marker, &nonce, lease, readiness, deadline)
            }?)
        }
        _ => {
            return Err(refusal(
                "managed entry requires matching markers and inherited handoff",
            ))
        }
    };
    let lifetime = managed
        .as_ref()
        .map(|managed| managed.retain_process_lifetime());
    let is_managed = managed.is_some();
    Ok(PreparedInvocation {
        cli: Some(cli),
        managed,
        _lifetime: lifetime,
        is_managed,
    })
}

unsafe fn capture(name: &'static str, other: Option<i32>) -> Result<OwnedFd, CliError> {
    let fd: i32 = std::env::var(name)
        .map_err(|_| refusal("managed descriptor missing"))?
        .parse()
        .map_err(|_| refusal("managed descriptor format"))?;
    if fd < 3 || other == Some(fd) {
        return Err(refusal("managed descriptor selection"));
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || flags & libc::FD_CLOEXEC != 0 {
        return Err(refusal("managed descriptor not inherited"));
    }
    // SAFETY: qualified single-thread process entry owns this inherited fd;
    // no Rust owner was created before this capture, and duplicates are refused.
    let descriptor = unsafe { OwnedFd::from_raw_fd(fd) };
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(refusal("managed descriptor CLOEXEC restoration"));
    }
    Ok(descriptor)
}
