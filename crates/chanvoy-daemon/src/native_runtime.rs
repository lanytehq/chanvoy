//! Native revalidation around application retirement. Startup must retain this
//! owner through serving and all shutdown drains, so its lifetime lease cannot
//! disappear while provider/listener tasks still own runtime resources.
#![allow(dead_code)]
use crate::retirement_control::{ResultFacts, RetirementControl};
use crate::wait_owner::WaitOwnerRegistry;
use chanvoy_core::runtime::{
    control::ControlDocument, InodeIdentity, PathBytes, RecordPublisher, RuntimeError,
    RuntimeLaunchMarker, RuntimeProcess, RuntimeRecord, SocketRoot,
};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::sync::Arc;

pub(crate) struct NativeRuntime {
    control: RetirementControl<Arc<RecordPublisher>>,
    process: RuntimeProcess,
    socket_root: SocketRoot,
    config_root: PathBytes,
    config_identity: InodeIdentity,
    marker: RuntimeLaunchMarker,
    profile_fingerprint: String,
}

impl NativeRuntime {
    /// Called after qualified bootstrap, child publication and listener bind.
    /// There is no alternate constructor accepting wire readiness as evidence.
    pub(crate) fn for_child(
        registry: Arc<WaitOwnerRegistry>,
        publisher: Arc<RecordPublisher>,
        socket_root: SocketRoot,
        profile: &chanvoy_core::Profile,
    ) -> Result<Self, RuntimeError> {
        if !registry.retains_publisher(&publisher) {
            return Err(RuntimeError::Invalid("runtime arena lifetime ownership"));
        }
        let view = publisher.inspect_publication()?;
        let record = &view.logical;
        if profile.name != record.profile
            || std::fs::canonicalize(chanvoy_core::default_chanvoy_config_dir())?
                != record.config_root.to_path()
        {
            return Err(RuntimeError::Changed("runtime loaded configuration"));
        }
        let process = RuntimeProcess::observe_current()?;
        if record.process.as_ref() != Some(&process) {
            return Err(RuntimeError::Changed("runtime child process"));
        }
        let marker = RuntimeLaunchMarker {
            namespace: record.namespace.clone(),
            instance_uuid: record.instance_uuid,
            config_root: record.config_root.clone(),
            profile: record.profile.clone(),
        };
        marker.check_arguments(
            process.executable.to_path().as_os_str(),
            &process.observe_arguments()?,
        )?;
        let config_identity = config_identity(&record.config_root, record.uid)?;
        check_socket(&socket_root, record)?;
        Ok(Self {
            process,
            config_root: record.config_root.clone(),
            config_identity,
            marker,
            profile_fingerprint: chanvoy_core::compute_profile_fingerprint(profile),
            control: RetirementControl::for_child(registry, publisher)?,
            socket_root,
        })
    }

    pub(crate) fn attempt(
        &self,
        request: &ControlDocument,
        peer: Option<&RuntimeProcess>,
    ) -> Result<ResultFacts, RuntimeError> {
        self.control.attempt(request, |record| {
            // Native socket peer identity was captured before dispatch and is
            // revalidated for every attempt. A same-UID claim is insufficient.
            let peer = peer.ok_or(RuntimeError::Unavailable("control peer identity"))?;
            peer.revalidate()?;
            self.process.revalidate()?;
            self.marker.check_arguments(
                self.process.executable.to_path().as_os_str(),
                &self.process.observe_arguments()?,
            )?;
            if peer.uid != self.process.uid
                || peer.executable != self.process.executable
                || peer.pid_namespace != self.process.pid_namespace
                || peer.mount_namespace != self.process.mount_namespace
                || record.process.as_ref() != Some(&self.process)
                || record.config_root != self.config_root
                || config_identity(&self.config_root, self.process.uid)? != self.config_identity
            {
                return Err(RuntimeError::Changed("runtime control native binding"));
            }
            if std::fs::canonicalize(chanvoy_core::default_chanvoy_config_dir())?
                != self.config_root.to_path()
            {
                return Err(RuntimeError::Changed("runtime selected configuration"));
            }
            let profile = chanvoy_core::load_profile(&record.profile)
                .map_err(|_| RuntimeError::Unavailable("runtime profile read"))?;
            if chanvoy_core::compute_profile_fingerprint(&profile) != self.profile_fingerprint {
                return Err(RuntimeError::Changed("runtime profile fingerprint"));
            }
            check_socket(&self.socket_root, record)
        })
    }
}

fn config_identity(path: &PathBytes, uid: u32) -> Result<InodeIdentity, RuntimeError> {
    let path = path.to_path();
    if std::fs::canonicalize(&path)?.as_os_str() != path.as_os_str() {
        return Err(RuntimeError::Changed("runtime configuration root path"));
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o022 != 0 {
        return Err(RuntimeError::Invalid(
            "runtime configuration root owner/mode",
        ));
    }
    Ok(InodeIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn check_socket(root: &SocketRoot, record: &RuntimeRecord) -> Result<(), RuntimeError> {
    root.verify()?;
    let socket = record
        .socket
        .as_ref()
        .ok_or(RuntimeError::Invalid("runtime socket absent"))?;
    let process = record
        .process
        .as_ref()
        .ok_or(RuntimeError::Invalid("runtime process absent"))?;
    if socket.path != root.child_path(process.pid)? {
        return Err(RuntimeError::Changed("runtime socket locator"));
    }
    let metadata = std::fs::symlink_metadata(socket.path.to_path())?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != record.uid
        || metadata.mode() & 0o777 != 0o600
        || metadata.nlink() != 1
        || metadata.dev() != socket.identity.device
        || metadata.ino() != socket.identity.inode
    {
        return Err(RuntimeError::Changed("runtime socket inode/owner/mode"));
    }
    root.verify()
}
