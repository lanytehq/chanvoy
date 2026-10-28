//! Managed child-side startup, kept separate from legacy bootstrap. The only
//! unsafe boundary is adoption of a descriptor inherited by the qualified
//! launcher; matching a supplied inode number is not a substitute for that.
#![allow(dead_code)]
use crate::{native_runtime::NativeRuntime, wait_owner::WaitOwnerRegistry};
use chanvoy_core::runtime::{
    InstanceId, LifetimeLease, PathBytes, RecordPublisher, RuntimeAuthority, RuntimeBuildPin,
    RuntimeError, RuntimeLaunchMarker, RuntimeProcess, RuntimeReadiness, SocketRoot,
};
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::sync::Arc;

pub(crate) struct PreparedChild {
    publisher: Arc<RecordPublisher>,
    registry: Arc<WaitOwnerRegistry>,
    root: SocketRoot,
    process: RuntimeProcess,
    pub(crate) instance: InstanceId,
    pub(crate) user_id: String,
    deadline: std::time::Instant,
    profile_fingerprint: String,
    readiness: Option<std::os::unix::net::UnixStream>,
}
impl PreparedChild {
    /// # Safety
    /// Caller must have captured both inherited descriptors before runtime
    /// threads, verified their original clear CLOEXEC flags, restored CLOEXEC,
    /// and retained unique OwnedFd guards from that first ownership point.
    /// Caller must be the intended child of the qualified launcher, whose
    /// fork/exec path duplicated the already-held lifetime description and
    /// cleared CLOEXEC only in that child. It must transfer sole ownership of
    /// the bootstrap-named fd here. Do not call this on arbitrary env/argv
    /// input merely because descriptor metadata matches a lease inode.
    pub(crate) unsafe fn from_inherited(
        marker: &RuntimeLaunchMarker,
        nonce: &str,
        profile: &chanvoy_core::Profile,
        inherited: OwnedFd,
        readiness: OwnedFd,
        deadline: std::time::Instant,
    ) -> Result<Self, RuntimeError> {
        // Take descriptor ownership at entry, before parsing or filesystem work.
        // On every early error RAII closes this reference. No work may spawn
        // before this original inherited description becomes close-on-exec.
        let descriptor = inherited.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if descriptor < 3 || flags < 0 || flags & libc::FD_CLOEXEC == 0 {
            return Err(RuntimeError::Invalid(
                "managed lease was not inherited across exec",
            ));
        }
        if unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(RuntimeError::Unavailable(
                "managed lease CLOEXEC restoration",
            ));
        }
        let readiness_fd = readiness.as_raw_fd();
        let readiness_flags = unsafe { libc::fcntl(readiness_fd, libc::F_GETFD) };
        if readiness_fd < 3
            || readiness_fd == descriptor
            || readiness_flags < 0
            || readiness_flags & libc::FD_CLOEXEC == 0
        {
            return Err(RuntimeError::Invalid(
                "managed readiness descriptor inheritance",
            ));
        }
        if unsafe {
            libc::fcntl(
                readiness_fd,
                libc::F_SETFD,
                readiness_flags | libc::FD_CLOEXEC,
            )
        } < 0
        {
            return Err(RuntimeError::Unavailable(
                "managed readiness CLOEXEC restoration",
            ));
        }
        let readiness = std::os::unix::net::UnixStream::from(readiness);
        readiness.set_nonblocking(true)?;
        check_deadline(deadline)?;
        let authority = RuntimeAuthority::observe()?
            .ok_or(RuntimeError::Unavailable("managed bootstrap authority"))?;
        let snapshot = authority.read_bootstrap(&marker.namespace, marker.instance_uuid)?;
        let bootstrap = &snapshot.bootstrap;
        let starting = authority
            .read_record(&marker.namespace)?
            .ok_or(RuntimeError::Unavailable("managed launcher record"))?;
        let process = RuntimeProcess::observe_current()?;
        let config_root = PathBytes::from_path(&std::fs::canonicalize(
            chanvoy_core::default_chanvoy_config_dir(),
        )?)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| RuntimeError::Unavailable("bootstrap clock"))?
            .as_secs();
        bootstrap.check_context(
            &starting.record,
            &process,
            &config_root,
            profile,
            nonce,
            now,
        )?;
        if marker.config_root != config_root
            || marker.profile != profile.name
            || marker.namespace != bootstrap.namespace
            || marker.instance_uuid != bootstrap.instance_uuid
        {
            return Err(RuntimeError::Changed("managed child launch marker"));
        }
        marker.check_arguments(
            process.executable.to_path().as_os_str(),
            &process.observe_arguments()?,
        )?;
        // Reparenting before handoff is uncertainty. Parent must remain alive
        // holding its operation/lifetime references through confirmed readiness.
        if unsafe { libc::getppid() } != bootstrap.launcher_process.pid.get() as i32 {
            return Err(RuntimeError::Changed("managed bootstrap parent"));
        }
        bootstrap.launcher_process.revalidate()?;
        if chanvoy_core::runtime::native_peer_pid(readiness.as_fd())?
            != bootstrap.launcher_process.pid
        {
            return Err(RuntimeError::Changed("managed readiness parent peer"));
        }
        let root =
            SocketRoot::observe()?.ok_or(RuntimeError::Unavailable("managed socket root"))?;
        if PathBytes::from_path(root.path())? != bootstrap.socket_root {
            return Err(RuntimeError::Changed("managed bootstrap socket selection"));
        }
        root.verify()?;
        if descriptor != bootstrap.lease_descriptor {
            return Err(RuntimeError::Changed(
                "managed inherited descriptor selection",
            ));
        }
        // SAFETY: caller transfers the qualified launcher's inherited open
        // description; file metadata is corroboration, never that proof.
        let lease = unsafe {
            LifetimeLease::adopt_inherited(
                &authority,
                &marker.namespace,
                inherited,
                bootstrap.lease_identity,
            )
        }?;
        let flags = unsafe { libc::fcntl(lease.descriptor().as_raw_fd(), libc::F_GETFD) };
        if flags < 0 || flags & libc::FD_CLOEXEC == 0 {
            return Err(RuntimeError::Unavailable(
                "managed lease CLOEXEC restoration",
            ));
        }
        process.revalidate()?;
        bootstrap.launcher_process.revalidate()?;
        let user_id = bootstrap.validated_user_id.clone();
        authority.consume_bootstrap(&snapshot)?;
        let publisher =
            RecordPublisher::for_current_child(authority, lease, starting, process.clone())?;
        publisher.retry_publication()?;
        check_deadline(deadline)?;
        let publisher = Arc::new(publisher);
        let registry = Arc::new(WaitOwnerRegistry::for_runtime(
            marker.instance_uuid,
            Arc::clone(&publisher),
        ));
        Ok(Self {
            publisher,
            registry,
            root,
            process,
            instance: marker.instance_uuid,
            user_id,
            deadline,
            profile_fingerprint: chanvoy_core::compute_profile_fingerprint(profile),
            readiness: Some(readiness),
        })
    }

    pub(crate) fn socket_path(&self) -> Result<std::path::PathBuf, RuntimeError> {
        Ok(self.root.child_path(self.process.pid)?.to_path())
    }

    /// Call after the real listener is bound and local application recovery is
    /// complete. The returned owner must outlive every runtime task and drain.
    async fn publish_running(
        &self,
        registry: Arc<WaitOwnerRegistry>,
        profile: &chanvoy_core::Profile,
        listener: &tokio::net::UnixListener,
    ) -> Result<(Arc<NativeRuntime>, RuntimeReadiness), RuntimeError> {
        check_deadline(self.deadline)?;
        self.process.revalidate()?;
        let path = self.socket_path()?;
        let socket =
            crate::native_listener::prove(listener, &path, &self.process, self.deadline).await?;
        let build = chanvoy_core::resolve_host_build_info();
        let build = RuntimeBuildPin {
            version: build.version,
            commit: build.commit,
            dirty: build.dirty,
        };
        self.publisher
            .publish_running(socket.clone(), build.clone())?;
        let view = self.publisher.inspect_publication()?;
        if !view.confirmed {
            return Err(RuntimeError::PublicationUncertain);
        }
        let readiness = RuntimeReadiness {
            schema_version: 1,
            kind: "runtime_readiness".into(),
            namespace: view.logical.namespace.clone(),
            instance_uuid: self.instance,
            revision: view.logical.revision,
            process: self.process.clone(),
            socket,
            build,
            handoff_validated: true,
            listener_bound: true,
            publication_confirmed: true,
            ready: true,
        };
        readiness.check_record(&view.logical)?;
        // Startup keeps its publisher reference on every error, including a
        // late or uncertain publication. The bound bundle owns listener+guard.
        let root =
            SocketRoot::observe()?.ok_or(RuntimeError::Unavailable("managed socket root"))?;
        if root.path() != self.root.path() {
            return Err(RuntimeError::Changed("managed startup socket root"));
        }
        let native =
            NativeRuntime::for_child(registry, Arc::clone(&self.publisher), root, profile)?;
        check_deadline(self.deadline)?;
        Ok((Arc::new(native), readiness))
    }
}

fn check_deadline(deadline: std::time::Instant) -> Result<(), RuntimeError> {
    if std::time::Instant::now() >= deadline {
        Err(RuntimeError::Unavailable("managed startup deadline"))
    } else {
        Ok(())
    }
}

/// Field order closes the actual listener before releasing the startup lease.
/// Serving tasks receive NativeRuntime references and retain that same lease
/// until their own drains finish; no second lease pathname open is involved.
pub(crate) struct BoundChild {
    listener: tokio::net::UnixListener,
    readiness: tokio::sync::Mutex<tokio::net::UnixStream>,
    startup: PreparedChild,
}
/// Pre-runtime socket bundle: constructed under the qualified launcher's
/// restrictive socket umask, before any asynchronous runtime threads exist.
pub(crate) struct BoundBeforeRuntime {
    listener: std::os::unix::net::UnixListener,
    startup: PreparedChild,
}
impl PreparedChild {
    /// Requires the single-thread launch entry's socket umask (0177). This
    /// method never toggles a process-global mask or repairs a pathname mode.
    pub(crate) fn bind_before_runtime(self) -> Result<BoundBeforeRuntime, RuntimeError> {
        check_deadline(self.deadline)?;
        let path = self.socket_path()?;
        let listener = std::os::unix::net::UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        // Only observe. A bad mode or a substituted path is a refusal; never
        // chmod a possibly foreign artifact to make the check pass.
        crate::native_listener::capture_path(&path, self.process.uid)?;
        Ok(BoundBeforeRuntime {
            listener,
            startup: self,
        })
    }
}
impl BoundBeforeRuntime {
    pub(crate) fn registry(&self) -> Arc<WaitOwnerRegistry> {
        Arc::clone(&self.startup.registry)
    }
    pub(crate) fn register(mut self) -> Result<BoundChild, RuntimeError> {
        let readiness = tokio::net::UnixStream::from_std(
            self.startup
                .readiness
                .take()
                .ok_or(RuntimeError::Invalid("managed readiness owner"))?,
        )?;
        let listener = tokio::net::UnixListener::from_std(self.listener)?;
        Ok(BoundChild {
            listener,
            readiness: tokio::sync::Mutex::new(readiness),
            startup: self.startup,
        })
    }
}
impl BoundChild {
    pub(crate) async fn send_readiness(
        &self,
        ready: &RuntimeReadiness,
    ) -> Result<(), RuntimeError> {
        use tokio::io::AsyncWriteExt;
        check_deadline(self.startup.deadline)?;
        let bytes = ready
            .encode()
            .map_err(|_| RuntimeError::Invalid("managed readiness contract"))?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(self.startup.deadline),
            async {
                let mut stream = self.readiness.lock().await;
                stream.write_all(&bytes).await?;
                // EOF delimits this one non-RPC document; no newline/RPC fallback.
                stream.shutdown().await
            },
        )
        .await
        .map_err(|_| RuntimeError::Unavailable("managed readiness deadline"))??;
        check_deadline(self.startup.deadline)
    }

    pub(crate) fn user_id(&self) -> &str {
        &self.startup.user_id
    }
    pub(crate) fn check_profile(
        &self,
        profile: &chanvoy_core::Profile,
    ) -> Result<(), RuntimeError> {
        if chanvoy_core::compute_profile_fingerprint(profile) != self.startup.profile_fingerprint {
            return Err(RuntimeError::Changed("managed startup profile"));
        }
        check_deadline(self.startup.deadline)
    }

    pub(crate) fn registry(&self) -> Arc<WaitOwnerRegistry> {
        Arc::clone(&self.startup.registry)
    }
    pub(crate) fn listener(&self) -> &tokio::net::UnixListener {
        &self.listener
    }
    pub(crate) async fn publish_running(
        &self,
        profile: &chanvoy_core::Profile,
    ) -> Result<(Arc<NativeRuntime>, RuntimeReadiness), RuntimeError> {
        self.check_profile(profile)?;
        self.startup
            .publish_running(self.registry(), profile, &self.listener)
            .await
    }
}
