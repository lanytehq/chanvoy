//! Confirm termination before releasing a predecessor's runtime state.
use std::io::Read;
use std::os::unix::{
    fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
    io::{AsRawFd, FromRawFd, OwnedFd},
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    device: u64,
    inode: u64,
    modified_secs: i64,
    modified_nanos: i64,
    size: u64,
}

fn stamp(path: &Path) -> Result<Option<FileStamp>, String> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("runtime metadata unreadable".into()),
    };
    if meta.uid() != unsafe { libc::geteuid() } {
        return Err("runtime owner unconfirmed".into());
    }
    Ok(Some(FileStamp {
        device: meta.dev(),
        inode: meta.ino(),
        modified_secs: meta.mtime(),
        modified_nanos: meta.mtime_nsec(),
        size: meta.len(),
    }))
}

fn read_pid(path: &Path) -> Result<Option<u32>, String> {
    let Some(before) = stamp(path)? else {
        return Ok(None);
    };
    if !std::fs::symlink_metadata(path)
        .map_err(|_| "pid metadata unreadable")?
        .is_file()
    {
        return Err("pid path is not a regular file".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "pid file unreadable")?;
    let meta = file.metadata().map_err(|_| "pid descriptor unreadable")?;
    if meta.dev() != before.device || meta.ino() != before.inode {
        return Err("pid file changed".into());
    }
    let mut body = String::new();
    file.take(65)
        .read_to_string(&mut body)
        .map_err(|_| "pid file unreadable")?;
    if body.len() > 64 || stamp(path)?.as_ref() != Some(&before) {
        return Err("pid file changed or oversized".into());
    }
    let pid = body.trim().parse::<u32>().map_err(|_| "pid file invalid")?;
    if pid == 0 || pid > i32::MAX as u32 {
        return Err("pid outside positive process range".into());
    }
    Ok(Some(pid))
}

struct NativeControl {
    profile: String,
    pid: u32,
    pid_path: PathBuf,
    socket_path: PathBuf,
    /// None when the pid came from the socket peer because no pid file
    /// exists; the pid path must then stay absent until cleanup.
    pid_stamp: Option<FileStamp>,
    socket_stamp: Option<FileStamp>,
    captured: ProcessObservation,
    captured_birth: Option<u64>,
    exit_watch: Option<ExitWatch>,
    death_seen: Mutex<Option<Instant>>,
    /// The pid file was written before the current boot. Its pid is released
    /// only when the current holder is provably not this profile's daemon.
    pre_boot_record: bool,
    /// A pre-boot record whose pid is now held under another account; that
    /// holder is never inspected or signalled.
    foreign_holder: bool,
}

/// Read-only binding for a daemon observation. This carries no signaling or
/// cleanup authority and does not require a pid file to inspect a live peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReuseCandidate {
    binding: chanvoy_core::runtime::RuntimeBinding,
    socket: FileStamp,
    pid: u32,
    birth: u64,
    uid: u32,
    native: Option<ProcessIdentity>,
}

impl ReuseCandidate {
    pub(super) async fn capture(profile: &str) -> Option<Self> {
        Self::capture_binding(chanvoy_core::runtime::RuntimeBinding::legacy(profile))
            .await
            .ok()
            .flatten()
    }

    #[cfg(test)]
    async fn capture_path(path: &Path) -> Option<Self> {
        Self::capture_binding(chanvoy_core::runtime::RuntimeBinding::legacy_socket(
            path.to_path_buf(),
        ))
        .await
        .ok()
        .flatten()
    }

    pub(super) async fn capture_binding(
        binding: chanvoy_core::runtime::RuntimeBinding,
    ) -> Result<Option<Self>, chanvoy_core::runtime::RuntimeError> {
        binding.verify()?;
        let observed = Self::capture_bound_inner(binding.clone()).await;
        binding.verify()?;
        Ok(observed)
    }

    async fn capture_bound_inner(binding: chanvoy_core::runtime::RuntimeBinding) -> Option<Self> {
        let path = binding.socket_path();
        let before = stamp(path).ok()??;
        if !std::fs::symlink_metadata(path)
            .ok()?
            .file_type()
            .is_socket()
        {
            return None;
        }
        let SocketOwner::Live(pid) = peer_owner(path, Duration::from_millis(750)).await else {
            return None;
        };
        let birth = native_birth(pid)?;
        let uid = native_owner(pid)?;
        // SAFETY: geteuid has no pointer arguments or preconditions and only
        // observes this process's effective user identity.
        if uid != unsafe { libc::geteuid() }
            || native_terminated(pid)
            || native_birth(pid) != Some(birth)
            || native_owner(pid) != Some(uid)
            || stamp(path).ok()?.as_ref() != Some(&before)
        {
            return None;
        }
        let native = if let Some(snapshot) = binding.record() {
            let record = &snapshot.record;
            if record.phase != chanvoy_core::runtime::RuntimePhase::Running {
                return None;
            }
            let process = record.process.as_ref()?;
            if process.pid.get() != pid
                || process.uid != uid
                || birth_claim(&process.birth)? != birth
            {
                return None;
            }
            let ProcessObservation::Alive(identity) = observe_pid(pid) else {
                return None;
            };
            if Path::new(&identity.executable) != process.executable.to_path()
                || unique_argument(&identity.args, "--runtime-namespace")
                    != Some(record.namespace.hex())
                || unique_argument(&identity.args, "--runtime-instance")
                    != Some(record.instance_uuid.uuid().to_string().as_str())
                || !namespace_claims_match(process)
            {
                return None;
            }
            Some(identity)
        } else {
            None
        };
        if native_birth(pid) != Some(birth)
            || native_owner(pid) != Some(uid)
            || native_terminated(pid)
            || stamp(path).ok()?.as_ref() != Some(&before)
        {
            return None;
        }
        Some(Self {
            binding,
            socket: before,
            pid,
            birth,
            uid,
            native,
        })
    }

    pub(super) async fn unchanged(&self) -> bool {
        Self::capture_binding(self.binding.clone())
            .await
            .ok()
            .flatten()
            .as_ref()
            == Some(self)
    }

    pub(super) fn binding(&self) -> &chanvoy_core::runtime::RuntimeBinding {
        &self.binding
    }

    pub(super) fn terminated(&self) -> bool {
        native_birth(self.pid).is_some_and(|birth| birth != self.birth)
            || native_terminated(self.pid)
            || matches!(death_or_unknown(self.pid), ProcessObservation::Dead)
    }
}

fn unique_argument<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let mut result = None;
    for (index, arg) in args.iter().enumerate() {
        if arg == flag {
            if result.is_some() {
                return None;
            }
            result = Some(args.get(index + 1)?.as_str());
        } else if arg.starts_with(&format!("{flag}=")) {
            return None;
        }
    }
    result.filter(|v| !v.is_empty() && !v.starts_with('-'))
}
fn birth_claim(birth: &chanvoy_core::runtime::NativeBirth) -> Option<u64> {
    match birth {
        chanvoy_core::runtime::NativeBirth::Linux { clock_ticks } => Some(*clock_ticks),
        chanvoy_core::runtime::NativeBirth::MacOs {
            seconds,
            microseconds,
        } => seconds
            .checked_mul(1_000_000)?
            .checked_add(u64::from(*microseconds)),
    }
}
fn namespace_claims_match(process: &chanvoy_core::runtime::RuntimeProcess) -> bool {
    #[cfg(target_os = "linux")]
    {
        if !matches!(
            process.birth,
            chanvoy_core::runtime::NativeBirth::Linux { .. }
        ) {
            return false;
        }
        for (name, expected) in [
            ("pid", process.pid_namespace),
            ("mnt", process.mount_namespace),
        ] {
            let Some(expected) = expected else {
                return false;
            };
            let Ok(meta) = std::fs::metadata(format!("/proc/{}/ns/{name}", process.pid.get()))
            else {
                return false;
            };
            if meta.dev() != expected.device || meta.ino() != expected.inode {
                return false;
            }
        }
        true
    }
    #[cfg(target_os = "macos")]
    {
        matches!(
            process.birth,
            chanvoy_core::runtime::NativeBirth::MacOs { .. }
        ) && process.pid_namespace.is_none()
            && process.mount_namespace.is_none()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = process;
        false
    }
}

/// The parent's Child handle supplies death proof; these stamps separately
/// govern its failed-start runtime cleanup. Capture before finalization.
pub(super) struct FailedChildRuntime {
    profile: String,
    pid_path: PathBuf,
    socket_path: PathBuf,
    pid_stamp: Option<FileStamp>,
    socket_stamp: Option<FileStamp>,
    pid: u32,
}

impl FailedChildRuntime {
    pub(super) fn capture(profile: &str, pid: u32) -> Result<Self, String> {
        let pid_path = chanvoy_core::pid_path_for_profile(profile);
        let socket_path = chanvoy_core::socket_path_for_profile(profile);
        let pid_stamp = stamp(&pid_path)?;
        let socket_stamp = stamp(&socket_path)?;
        if pid_stamp.is_some() && read_pid(&pid_path)? != Some(pid) {
            return Err("failed-child pid identity unconfirmed".into());
        }
        if socket_stamp.is_some() && pid_stamp.is_none() {
            return Err("failed-child socket identity unconfirmed".into());
        }
        if socket_stamp.is_some()
            && !std::fs::symlink_metadata(&socket_path)
                .map_err(|_| "failed-child socket metadata unconfirmed")?
                .file_type()
                .is_socket()
        {
            return Err("failed-child socket path is not a socket".into());
        }
        Ok(Self {
            profile: profile.to_string(),
            pid_path,
            socket_path,
            pid_stamp,
            socket_stamp,
            pid,
        })
    }

    fn matches(&self) -> Result<bool, String> {
        Ok(stamp(&self.pid_path)? == self.pid_stamp
            && stamp(&self.socket_path)? == self.socket_stamp
            && (self.pid_stamp.is_none() || read_pid(&self.pid_path)? == Some(self.pid)))
    }

    /// Called only after the owned Child has supplied confirmed termination.
    pub(super) async fn cleanup_after_exit(&self) -> Result<(), String> {
        if !self.matches()? {
            return Err("failed-child runtime changed".into());
        }
        let absence = chanvoy_core::startup::bounded(
            chanvoy_core::startup::Phase::ChildSocketAbsence,
            &self.profile,
            chanvoy_core::startup::LOCAL_PING_BUDGET,
            async {
                match peer_owner(&self.socket_path, Duration::from_millis(750)).await {
                    SocketOwner::Absent => Ok(()),
                    _ => Err(()),
                }
            },
            |_| chanvoy_core::startup::Outcome::LocalUnconfirmed,
        )
        .await;
        if !self.matches()? || absence.is_err() {
            return Err("failed-child socket absence unconfirmed".into());
        }
        if self.socket_stamp.is_some() {
            if !self.matches()? {
                return Err("failed-child runtime changed".into());
            }
            std::fs::remove_file(&self.socket_path)
                .map_err(|_| "failed-child socket cleanup unconfirmed")?;
        }
        if self.pid_stamp.is_some() {
            if stamp(&self.socket_path)?.is_some()
                || stamp(&self.pid_path)? != self.pid_stamp
                || read_pid(&self.pid_path)? != Some(self.pid)
            {
                return Err("failed-child runtime changed".into());
            }
            std::fs::remove_file(&self.pid_path)
                .map_err(|_| "failed-child pid cleanup unconfirmed")?;
        }
        Ok(())
    }
}

/// Kernel exit evidence stays usable when an unreaped process no longer exposes
/// executable metadata. It observes termination; signal ownership still needs
/// the separately revalidated process and socket identity.
struct ExitWatch {
    fd: OwnedFd,
    pid: u32,
    exited: AtomicBool,
}

impl ExitWatch {
    fn new(pid: u32) -> Result<Option<Self>, String> {
        if matches!(death_or_unknown(pid), ProcessObservation::Dead) {
            return Ok(None);
        }
        #[cfg(target_os = "macos")]
        {
            // Registering NOTE_EXIT for an already-exited unreaped process
            // can fail. Fresh BSD zombie proof needs no new monitor; capture
            // still revalidates death and birth around this decision.
            if native_terminated(pid) {
                return Ok(None);
            }
            let fd = unsafe { libc::kqueue() };
            if fd < 0 {
                let error = std::io::Error::last_os_error();
                tracing::debug!(stage = "exit-observer-create", pid, outcome = ?error.kind());
                return Err("kernel exit monitor unavailable".into());
            }
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
                let error = std::io::Error::last_os_error();
                tracing::debug!(stage = "exit-observer-descriptor", pid, outcome = ?error.kind());
                return Err("kernel exit monitor descriptor setup failed".into());
            }
            let event = libc::kevent {
                ident: pid as libc::uintptr_t,
                filter: libc::EVFILT_PROC,
                flags: libc::EV_ADD | libc::EV_ONESHOT,
                fflags: libc::NOTE_EXIT,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            if unsafe {
                libc::kevent(
                    fd.as_raw_fd(),
                    &event,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            } < 0
            {
                let error = std::io::Error::last_os_error();
                tracing::debug!(stage = "exit-observer-registration", pid, outcome = ?error.kind());
                if native_terminated(pid)
                    || matches!(death_or_unknown(pid), ProcessObservation::Dead)
                {
                    return Ok(None);
                }
                return Err("kernel exit monitor registration failed".into());
            }
            Ok(Some(Self {
                fd,
                pid,
                exited: AtomicBool::new(false),
            }))
        }
        #[cfg(target_os = "linux")]
        {
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
            if fd < 0 {
                let error = std::io::Error::last_os_error();
                tracing::debug!(stage = "exit-observer-create", pid, outcome = ?error.kind());
                return Err("kernel exit monitor unavailable".into());
            }
            Ok(Some(Self {
                fd: unsafe { OwnedFd::from_raw_fd(fd as i32) },
                pid,
                exited: AtomicBool::new(false),
            }))
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err("kernel exit monitor unsupported".into())
        }
    }

    fn exited(&self) -> Result<bool, String> {
        if self.exited.load(Ordering::Acquire) {
            return Ok(true);
        }
        #[cfg(target_os = "macos")]
        let exited = {
            let mut event: libc::kevent = unsafe { std::mem::zeroed() };
            let zero = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            let count = unsafe {
                libc::kevent(
                    self.fd.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &mut event,
                    1,
                    &zero,
                )
            };
            if count < 0 || (count > 0 && event.flags & libc::EV_ERROR != 0) {
                return Err("kernel exit observation unknown".into());
            }
            count > 0 && event.filter == libc::EVFILT_PROC && event.fflags & libc::NOTE_EXIT != 0
        };
        #[cfg(target_os = "linux")]
        let exited = {
            let mut fd = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let count = unsafe { libc::poll(&mut fd, 1, 0) };
            if count < 0 || fd.revents & (libc::POLLNVAL | libc::POLLERR) != 0 {
                return Err("kernel exit observation unknown".into());
            }
            count > 0 && fd.revents & (libc::POLLIN | libc::POLLHUP) != 0
        };
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let exited = false;
        if exited {
            self.exited.store(true, Ordering::Release);
        }
        Ok(exited)
    }

    /// SIGKILL the process this watch was registered on, never a later holder
    /// of its pid. Nothing is sent once the registered exit is observed.
    /// Linux signals through the pidfd. macOS has no process handle; an
    /// unexited process keeps its pid, leaving just the gap between the exit
    /// check and the kill call.
    fn force_kill(&self) -> Result<(), String> {
        if self.exited()? {
            return Err("process exited before force signal".into());
        }
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.fd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0u32,
            )
        };
        #[cfg(target_os = "macos")]
        let result = unsafe { libc::kill(self.pid as libc::pid_t, libc::SIGKILL) };
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let result: i32 = {
            return Err("bound force signal unsupported".into());
        };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            tracing::debug!(stage = "bound-force-signal", pid = self.pid, outcome = ?error.kind());
            return Err(format!("bound force signal failed: {:?}", error.kind()));
        }
        tracing::debug!(
            stage = "bound-force-signal",
            pid = self.pid,
            outcome = "sent"
        );
        Ok(())
    }
}

const PIDLESS_SOCKET: &str = "socket exists without a regular readable pid file; state retained";

/// A pid file whose mtime precedes the observed process start by more than
/// this cannot describe that process: a daemon writes its pid file after it
/// starts. The slack absorbs start-time rounding across platforms.
const STALE_RECORD_SLACK_MS: i128 = 2_000;

fn stamp_ms(stamp: &FileStamp) -> i128 {
    (stamp.modified_secs as i128) * 1000 + (stamp.modified_nanos as i128) / 1_000_000
}

/// The daemon runs as the invoking user and never changes account, so a
/// holder positively read as another account cannot be the recorded daemon.
/// A denied or unavailable read is not evidence either way.
fn held_by_other_user(pid: u32) -> bool {
    // SAFETY: geteuid has no arguments or preconditions.
    account_of(pid).is_some_and(|uid| uid != unsafe { libc::geteuid() })
}

/// Darwin `struct proc_bsdshortinfo` from `<sys/proc_info.h>`; the locked
/// libc release does not declare it.
#[cfg(target_os = "macos")]
#[repr(C)]
struct ProcBsdShortInfo {
    pid: u32,
    ppid: u32,
    pgid: u32,
    status: u32,
    comm: [libc::c_char; 16],
    flags: u32,
    uid: libc::uid_t,
    gid: libc::gid_t,
    ruid: libc::uid_t,
    rgid: libc::gid_t,
    svuid: libc::uid_t,
    svgid: libc::gid_t,
    reserved: u32,
}

#[cfg(target_os = "macos")]
const PROC_PIDT_SHORTBSDINFO: libc::c_int = 13;

#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<ProcBsdShortInfo>() == 64);

#[cfg(target_os = "macos")]
fn account_of(pid: u32) -> Option<u32> {
    if pid == 0 || pid > i32::MAX as u32 {
        return None;
    }
    // The short BSD flavor is readable across accounts, unlike the full one.
    // SAFETY: an all-zero bit pattern is valid for this plain C struct.
    let mut info: ProcBsdShortInfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    // SAFETY: the buffer pointer and size describe `info` exactly.
    let read = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            PROC_PIDT_SHORTBSDINFO,
            1,
            (&mut info as *mut ProcBsdShortInfo).cast(),
            size,
        )
    };
    (read == size && info.pid == pid).then_some(info.uid)
}

#[cfg(target_os = "linux")]
fn account_of(pid: u32) -> Option<u32> {
    native_owner(pid)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn account_of(_: u32) -> Option<u32> {
    None
}

fn written_before_boot(stamp: &FileStamp) -> bool {
    boot_time_ms().is_some_and(|boot| stamp_ms(stamp) + STALE_RECORD_SLACK_MS < boot)
}

#[cfg(target_os = "macos")]
fn boot_time_ms() -> Option<i128> {
    let mut boot: libc::timeval = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of_val(&boot);
    // SAFETY: the output buffer and its length describe `boot` exactly; no
    // new value is written.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.boottime".as_ptr(),
            (&mut boot as *mut libc::timeval).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || size != std::mem::size_of_val(&boot) || boot.tv_sec <= 0 {
        return None;
    }
    Some(i128::from(boot.tv_sec) * 1000 + i128::from(boot.tv_usec) / 1000)
}

#[cfg(target_os = "linux")]
fn boot_time_ms() -> Option<i128> {
    std::fs::read_to_string("/proc/stat")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?
        .trim()
        .parse::<i128>()
        .ok()
        .filter(|secs| *secs > 0)
        .map(|secs| secs * 1000)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn boot_time_ms() -> Option<i128> {
    None
}

impl NativeControl {
    fn capture(
        profile: &str,
        pid_path: PathBuf,
        socket_path: PathBuf,
    ) -> Result<Option<Self>, String> {
        let Some(pid) = read_pid(&pid_path)? else {
            return if stamp(&socket_path)?.is_none() {
                Ok(None)
            } else {
                Err(PIDLESS_SOCKET.into())
            };
        };
        let pid_stamp = stamp(&pid_path)?.ok_or("pid file changed")?;
        Self::capture_pid(profile, pid, pid_path, socket_path, Some(pid_stamp)).map(Some)
    }

    /// Pid-file capture, falling back to the socket peer when the pid file is
    /// absent but the socket exists. The peer pid is neither birth nor
    /// liveness proof, so the fallback requires a live same-user process,
    /// re-reads the peer after the identity read, and leaves ownership to
    /// `owned_identity` with the socket inode standing in for the pid file.
    async fn capture_with_peer(
        profile: &str,
        pid_path: PathBuf,
        socket_path: PathBuf,
    ) -> Result<Option<Self>, String> {
        if read_pid(&pid_path)?.is_some() {
            return Self::capture(profile, pid_path, socket_path);
        }
        let Some(socket_stamp) = stamp(&socket_path)? else {
            return Ok(None);
        };
        if !std::fs::symlink_metadata(&socket_path)
            .map_err(|_| "socket metadata unreadable")?
            .file_type()
            .is_socket()
        {
            return Err("runtime socket path is not a socket; state retained".into());
        }
        let SocketOwner::Live(pid) = peer_owner(&socket_path, Duration::from_millis(750)).await
        else {
            return Err(PIDLESS_SOCKET.into());
        };
        let control = Self::capture_pid(profile, pid, pid_path, socket_path, None)?;
        if control.socket_stamp.as_ref() != Some(&socket_stamp)
            || !matches!(control.captured, ProcessObservation::Alive(_))
        {
            return Err(
                "socket peer identity unconfirmed without a pid file; state retained".into(),
            );
        }
        let owner = peer_owner(&control.socket_path, Duration::from_millis(750)).await;
        if !control.peer_unchanged(owner) {
            return Err("socket peer changed during identity capture; state retained".into());
        }
        Ok(Some(control))
    }

    /// The listener still belongs to the same process after its identity was
    /// read, so a reused pid cannot inherit the earlier observation.
    fn peer_unchanged(&self, owner: SocketOwner) -> bool {
        owner == SocketOwner::Live(self.pid)
            && native_birth(self.pid) == self.captured_birth
            && self.runtime_matches(false)
    }

    fn capture_pid(
        profile: &str,
        pid: u32,
        pid_path: PathBuf,
        socket_path: PathBuf,
        pid_stamp: Option<FileStamp>,
    ) -> Result<Self, String> {
        let socket_stamp = stamp(&socket_path)?;
        if socket_stamp.is_some()
            && !std::fs::symlink_metadata(&socket_path)
                .map_err(|_| "socket metadata unreadable")?
                .file_type()
                .is_socket()
        {
            return Err("runtime socket path is not a socket; state retained".into());
        }
        let pre_boot_record = pid_stamp.as_ref().is_some_and(written_before_boot);
        if pre_boot_record && held_by_other_user(pid) {
            let control = Self {
                profile: profile.into(),
                pid,
                pid_path,
                socket_path,
                pid_stamp,
                socket_stamp,
                captured: ProcessObservation::Dead,
                captured_birth: None,
                exit_watch: None,
                death_seen: Mutex::new(None),
                pre_boot_record: true,
                foreign_holder: true,
            };
            if !control.runtime_matches(false) {
                return Err("runtime changed during capture; state retained".into());
            }
            return Ok(control);
        }
        let captured_birth = native_birth(pid);
        let (captured, exit_watch) = capture_monitor(|| observe_pid(pid), || ExitWatch::new(pid))?;
        let after = observe_pid(pid);
        if native_birth(pid).is_some_and(|birth| Some(birth) != captured_birth) {
            return Err("process birth changed around exit monitor; state retained".into());
        }
        if let ProcessObservation::Alive(identity) = &captured {
            if let Some(birth) = native_birth(pid) {
                if birth != identity.birth {
                    return Err("process birth changed around exit monitor; state retained".into());
                }
            } else if after != ProcessObservation::Dead {
                return Err("process birth unavailable around exit monitor; state retained".into());
            }
        }
        let control = Self {
            profile: profile.into(),
            pid,
            pid_path,
            socket_path,
            pid_stamp,
            socket_stamp,
            captured,
            captured_birth,
            exit_watch,
            death_seen: Mutex::new(None),
            pre_boot_record,
            foreign_holder: false,
        };
        if !control.runtime_matches(false) {
            return Err("runtime changed around exit monitor; state retained".into());
        }
        Ok(control)
    }

    /// The process must have started no later than its runtime record was
    /// written: the pid file, or the socket inode when no pid file exists.
    fn owned_identity(&self, identity: &ProcessIdentity) -> bool {
        let Ok(exe) = std::env::current_exe().and_then(std::fs::canonicalize) else {
            return false;
        };
        let Some(record) = self.pid_stamp.as_ref().or(self.socket_stamp.as_ref()) else {
            return false;
        };
        identity.executable == exe.to_string_lossy()
            && self.serves_profile(identity)
            && stamp_ms(record) >= i128::from(identity.start_ms)
    }

    /// Arguments of a `daemon serve` for this profile.
    fn serves_profile(&self, identity: &ProcessIdentity) -> bool {
        let explicit_profile = identity
            .args
            .windows(2)
            .any(|a| a[0] == "--profile" && a[1] == self.profile)
            || identity
                .args
                .iter()
                .any(|a| a == &format!("--profile={}", self.profile));
        let serve = identity
            .args
            .windows(2)
            .any(|a| a[0] == "daemon" && a[1] == "serve");
        explicit_profile && serve
    }

    /// A pid record is stale only when timing says it predates the current
    /// holder AND the holder is not a daemon at all. Timing alone is not death
    /// proof: clocks step and mtimes can be restored. Any `daemon serve`
    /// holder may serve this profile through `CHANVOY_PROFILE`, so it falls
    /// through to the ownership refusal instead.
    fn record_released(&self, identity: &ProcessIdentity) -> bool {
        let Some(record) = self.pid_stamp.as_ref() else {
            return false;
        };
        let predates = i128::from(identity.start_ms) > stamp_ms(record) + STALE_RECORD_SLACK_MS;
        (predates || self.pre_boot_record) && !maybe_daemon_serve(identity)
    }
}

/// Whether a process could be a `daemon serve` of any profile. Global options
/// may sit between the two words, so any argument order counts. This only
/// blocks releasing a record; signalling still requires `owned_identity`.
fn maybe_daemon_serve(identity: &ProcessIdentity) -> bool {
    let has = |word: &str| identity.args.iter().skip(1).any(|a| a == word);
    has("daemon") && has("serve")
}

fn capture_monitor<W>(
    mut observe: impl FnMut() -> ProcessObservation,
    register: impl FnOnce() -> Result<W, String>,
) -> Result<(ProcessObservation, W), String> {
    let before = observe();
    if before == ProcessObservation::Unknown {
        return Err("process identity unavailable before exit monitor; state retained".into());
    }
    let watch = register()?;
    validate_monitor_identity(&before, &observe())?;
    Ok((before, watch))
}

fn validate_monitor_identity(
    before: &ProcessObservation,
    after: &ProcessObservation,
) -> Result<(), String> {
    match (before, after) {
        (ProcessObservation::Alive(before), ProcessObservation::Alive(after))
            if before == after =>
        {
            Ok(())
        }
        (ProcessObservation::Alive(_), ProcessObservation::Dead)
        | (ProcessObservation::Dead, ProcessObservation::Dead) => Ok(()),
        _ => Err("process changed or became unknown around exit monitor; state retained".into()),
    }
}

#[cfg(target_os = "macos")]
fn native_bsd_info(pid: u32) -> Option<libc::proc_bsdinfo> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let read = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            // This flavor's nonzero argument includes unreaped zombies, so
            // native birth evidence remains available after NOTE_EXIT.
            1,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if read != size {
        return None;
    }
    Some(info)
}

#[cfg(target_os = "macos")]
fn native_birth(pid: u32) -> Option<u64> {
    let info = native_bsd_info(pid)?;
    info.pbi_start_tvsec
        .checked_mul(1_000_000)?
        .checked_add(info.pbi_start_tvusec)
}

#[cfg(target_os = "macos")]
fn native_terminated(pid: u32) -> bool {
    let confirmed = native_bsd_info(pid).is_some_and(|info| info.pbi_status == libc::SZOMB);
    if confirmed {
        tracing::debug!(
            stage = "process-exit",
            pid,
            outcome = "macos-zombie-exit-confirmed"
        );
    }
    confirmed
}
#[cfg(target_os = "linux")]
fn native_terminated(pid: u32) -> bool {
    // A leader can be a zombie while another thread still owns descriptors.
    // A process pidfd (flags 0) becomes ready only after the final thread exits.
    // Fresh proof also supports a fully exited, unreaped predecessor captured
    // without an earlier monitor. Recheck birth around observer registration.
    let Some(birth) = native_birth(pid) else {
        return false;
    };
    let Ok(Some(watch)) = ExitWatch::new(pid) else {
        tracing::debug!(
            stage = "process-exit",
            pid,
            outcome = "whole-process-observer-unavailable"
        );
        return false;
    };
    let confirmed = matches!(watch.exited(), Ok(true)) && native_birth(pid) == Some(birth);
    tracing::debug!(
        stage = "process-exit",
        pid,
        outcome = if confirmed {
            "whole-process-exit-confirmed"
        } else {
            "whole-process-exit-unconfirmed"
        }
    );
    confirmed
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn native_terminated(_: u32) -> bool {
    false
}

#[cfg(target_os = "linux")]
fn native_birth(pid: u32) -> Option<u64> {
    let body = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    linux_birth_from_stat(&body)
}

#[cfg(any(target_os = "linux", test))]
fn linux_birth_from_stat(body: &str) -> Option<u64> {
    // The parenthesized comm may itself contain spaces or closing parentheses.
    body.rsplit_once(") ")?
        .1
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn native_birth(_: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "macos")]
fn native_owner(pid: u32) -> Option<u32> {
    Some(native_bsd_info(pid)?.pbi_uid)
}

#[cfg(target_os = "linux")]
fn native_owner(pid: u32) -> Option<u32> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn native_owner(_: u32) -> Option<u32> {
    None
}

fn observe_pid(pid: u32) -> ProcessObservation {
    if pid == 0 || pid > i32::MAX as u32 {
        return ProcessObservation::Unknown;
    }
    if unsafe { libc::kill(pid as i32, 0) } != 0 {
        let error = std::io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            tracing::debug!(stage = "process-liveness", pid, outcome = "pid-absent");
            ProcessObservation::Dead
        } else {
            tracing::debug!(stage = "pid-probe", pid, outcome = ?error.kind());
            ProcessObservation::Unknown
        };
    }
    let Some(birth) = native_birth(pid) else {
        return death_or_unknown(pid);
    };
    let Some(uid) = native_owner(pid).filter(|uid| *uid == unsafe { libc::geteuid() }) else {
        return ProcessObservation::Unknown;
    };
    // Termination needs platform-specific proof before executable metadata
    // disappears: macOS zombie state or Linux whole-process pidfd readiness.
    if native_terminated(pid) {
        return ProcessObservation::Dead;
    }
    let Ok(info) = sysprims_proc::get_process(pid) else {
        return death_or_unknown(pid);
    };
    if native_birth(pid) != Some(birth) || native_owner(pid) != Some(uid) {
        return death_or_unknown(pid);
    }
    if info.state == sysprims_proc::ProcessState::Zombie {
        #[cfg(target_os = "linux")]
        {
            tracing::debug!(
                stage = "process-liveness",
                pid,
                outcome = "leader-zombie-unconfirmed"
            );
            return ProcessObservation::Unknown;
        }
        #[cfg(not(target_os = "linux"))]
        return ProcessObservation::Dead;
    }
    if info.cmdline.is_empty() {
        tracing::debug!(
            stage = "process-identity-observation",
            pid,
            outcome = "arguments-unavailable"
        );
    }
    identity_from_metadata(
        pid,
        uid,
        birth,
        info.start_time_unix_ms,
        info.exe_path,
        info.cmdline,
    )
    .map(ProcessObservation::Alive)
    .unwrap_or_else(|| death_or_unknown(pid))
}

/// Best-effort metadata can disappear during exit. An incomplete read cannot
/// establish either ownership or identity drift; death needs separate proof.
fn identity_from_metadata(
    pid: u32,
    uid: u32,
    birth: u64,
    start_ms: Option<u64>,
    executable: Option<String>,
    args: Vec<String>,
) -> Option<ProcessIdentity> {
    let start_ms = start_ms?;
    let executable = executable?;
    if executable.is_empty() || executable.ends_with(" (deleted)") || args.is_empty() {
        return None;
    }
    Some(ProcessIdentity {
        pid,
        uid,
        birth,
        start_ms,
        executable,
        args,
    })
}

/// A process may exit during metadata collection. Only a fresh ESRCH proves
/// that race benign; any other inspection failure remains unknown.
fn death_or_unknown(pid: u32) -> ProcessObservation {
    if unsafe { libc::kill(pid as i32, 0) } != 0
        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    {
        ProcessObservation::Dead
    } else {
        ProcessObservation::Unknown
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SocketUnknown {
    Connect(std::io::ErrorKind),
    Credentials(std::io::ErrorKind),
    ConnectTimeout,
    ProbeTimeout,
    CredentialSizeInvalid,
    CredentialPidInvalid,
    #[cfg(target_os = "linux")]
    CredentialOwnerMismatch,
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SocketOwner {
    // This is returned peer credential metadata, not fresh liveness proof.
    Live(u32),
    Absent,
    Unknown(SocketUnknown),
}

impl SocketOwner {
    fn receipt(self) -> String {
        match self {
            Self::Live(pid) => format!("Live({pid})"),
            Self::Absent => "Absent".into(),
            Self::Unknown(reason) => format!("Unknown({reason:?})"),
        }
    }
}

async fn peer_owner(socket: &Path, budget: Duration) -> SocketOwner {
    peer_stream(socket, budget).await.0
}

/// Connect once and read that connection's kernel peer credentials. The
/// stream is returned only with a `Live` owner, so a caller can keep talking
/// to exactly the listener it checked.
async fn peer_stream(
    socket: &Path,
    budget: Duration,
) -> (SocketOwner, Option<tokio::net::UnixStream>) {
    let stream = match tokio::time::timeout(budget, tokio::net::UnixStream::connect(socket)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            tracing::debug!(stage = "socket-connect", outcome = ?error.kind());
            return (SocketOwner::Absent, None);
        }
        Ok(Err(error)) => {
            tracing::debug!(stage = "socket-connect", outcome = ?error.kind());
            return (
                SocketOwner::Unknown(SocketUnknown::Connect(error.kind())),
                None,
            );
        }
        Err(_) => {
            tracing::debug!(stage = "socket-connect", outcome = "connect-timeout");
            return (SocketOwner::Unknown(SocketUnknown::ConnectTimeout), None);
        }
    };
    #[cfg(target_os = "macos")]
    let owner = {
        let mut pid: libc::pid_t = 0;
        let mut size = std::mem::size_of_val(&pid) as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&mut pid as *mut libc::pid_t).cast(),
                &mut size,
            )
        };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            SocketOwner::Unknown(SocketUnknown::Credentials(error.kind()))
        } else if size as usize != std::mem::size_of_val(&pid) {
            SocketOwner::Unknown(SocketUnknown::CredentialSizeInvalid)
        } else if pid <= 0 {
            SocketOwner::Unknown(SocketUnknown::CredentialPidInvalid)
        } else {
            SocketOwner::Live(pid as u32)
        }
    };
    #[cfg(target_os = "linux")]
    let owner = {
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut size = std::mem::size_of_val(&cred) as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut size,
            )
        };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            SocketOwner::Unknown(SocketUnknown::Credentials(error.kind()))
        } else if size as usize != std::mem::size_of_val(&cred) {
            SocketOwner::Unknown(SocketUnknown::CredentialSizeInvalid)
        } else if cred.pid <= 0 {
            SocketOwner::Unknown(SocketUnknown::CredentialPidInvalid)
        } else if cred.uid != unsafe { libc::geteuid() } {
            SocketOwner::Unknown(SocketUnknown::CredentialOwnerMismatch)
        } else {
            SocketOwner::Live(cred.pid as u32)
        }
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let owner = SocketOwner::Unknown(SocketUnknown::Unsupported);
    tracing::debug!(stage = "socket-credential-query", outcome = %owner.receipt());
    match owner {
        SocketOwner::Live(_) => (owner, Some(stream)),
        _ => (owner, None),
    }
}

impl NativeControl {
    fn observe_process(&self) -> ProcessObservation {
        if self.foreign_holder {
            // Recheck the account boundary on every observation.
            return if held_by_other_user(self.pid)
                || death_or_unknown(self.pid) == ProcessObservation::Dead
            {
                ProcessObservation::Dead
            } else {
                ProcessObservation::Unknown
            };
        }
        if native_birth(self.pid).is_some_and(|birth| Some(birth) != self.captured_birth) {
            return ProcessObservation::Unknown;
        }
        let current = observe_pid(self.pid);
        match (&self.captured, &current) {
            (ProcessObservation::Alive(expected), ProcessObservation::Alive(actual))
                if expected != actual =>
            {
                tracing::debug!(
                    stage = "process-identity-revalidation",
                    pid = self.pid,
                    outcome = identity_difference(expected, actual)
                );
                return current;
            }
            (ProcessObservation::Dead, ProcessObservation::Alive(_)) => {
                return ProcessObservation::Unknown
            }
            _ => {}
        }
        if let Some(watch) = &self.exit_watch {
            match watch.exited() {
                Ok(true) => {
                    tracing::debug!(
                        stage = "registered-exit-observer",
                        pid = self.pid,
                        outcome = "whole-process-exit-confirmed"
                    );
                    if current == ProcessObservation::Dead {
                        return current;
                    }
                    let ProcessObservation::Alive(expected) = &self.captured else {
                        return ProcessObservation::Unknown;
                    };
                    return if native_birth(self.pid) == Some(expected.birth) {
                        ProcessObservation::Dead
                    } else {
                        ProcessObservation::Unknown
                    };
                }
                Err(_) => return ProcessObservation::Unknown,
                Ok(false) => {}
            }
        }
        if let ProcessObservation::Alive(identity) = &current {
            if self.record_released(identity) {
                tracing::debug!(
                    stage = "process-liveness",
                    pid = self.pid,
                    outcome = "pid-record-predates-process"
                );
                return ProcessObservation::Dead;
            }
        }
        current
    }
}

impl ProcessControl for NativeControl {
    type Peer = tokio::net::UnixStream;
    fn observe(&self) -> ProcessObservation {
        let observation = self.observe_process();
        if observation == ProcessObservation::Dead {
            let Ok(mut seen) = self.death_seen.lock() else {
                return ProcessObservation::Unknown;
            };
            seen.get_or_insert_with(Instant::now);
        }
        observation
    }
    fn runtime_matches(&self, allow_absent: bool) -> bool {
        let matches = |path: &Path, expected: Option<&FileStamp>| match stamp(path) {
            Ok(actual) => actual.as_ref() == expected || (allow_absent && actual.is_none()),
            Err(_) => false,
        };
        matches(&self.pid_path, self.pid_stamp.as_ref())
            && matches(&self.socket_path, self.socket_stamp.as_ref())
            && match read_pid(&self.pid_path) {
                Ok(Some(pid)) => pid == self.pid,
                Ok(None) => allow_absent || self.pid_stamp.is_none(),
                // The predecessor may unlink its own PID file between these
                // reads during graceful exit. Only freshly confirmed absence
                // is benign; unreadable or changed existing files still refuse.
                Err(_) => allow_absent && matches!(stamp(&self.pid_path), Ok(None)),
            }
    }
    async fn peer_matches(&self) -> bool {
        peer_owner(&self.socket_path, Duration::from_millis(750)).await
            == SocketOwner::Live(self.pid)
    }
    async fn bind_peer(&self) -> Option<Self::Peer> {
        match peer_stream(&self.socket_path, Duration::from_millis(750)).await {
            (SocketOwner::Live(pid), stream) if pid == self.pid => stream,
            _ => None,
        }
    }
    async fn socket_owner(&self, budget: Duration) -> SocketOwner {
        peer_owner(&self.socket_path, budget).await
    }
    fn leftover_deadline(&self, pid: u32, window: Duration) -> Option<Instant> {
        #[cfg(target_os = "linux")]
        {
            let ProcessObservation::Alive(identity) = &self.captured else {
                return None;
            };
            if pid != self.pid
                || self.captured_birth != Some(identity.birth)
                || !self.owned_identity(identity)
                || !matches!(self.exit_watch.as_ref()?.exited(), Ok(true))
            {
                return None;
            }
            match native_birth(self.pid) {
                Some(birth) if birth == identity.birth => {}
                None if death_or_unknown(self.pid) == ProcessObservation::Dead => {}
                _ => return None,
            }
            (*self.death_seen.lock().ok()?).map(|seen| seen + window)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (pid, window);
            None
        }
    }
    async fn shutdown(&self, peer: Self::Peer) -> Result<(), String> {
        chanvoy_daemon::stop_over(peer)
            .await
            .map_err(|e| e.to_string())
    }
    fn force_signal(&self) -> Result<(), String> {
        match &self.exit_watch {
            Some(watch) => watch.force_kill(),
            None => Err("no kernel handle for the captured process; force signal withheld".into()),
        }
    }
    fn cleanup(&self) -> Result<(), String> {
        for path in [&self.socket_path, &self.pid_path] {
            if self.observe() != ProcessObservation::Dead || !self.runtime_matches(true) {
                return Err(
                    "death or runtime identity changed before cleanup; state retained".into(),
                );
            }
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err("runtime cleanup failed".into()),
            }
        }
        Ok(())
    }
}

pub(super) async fn stop(profile: &str) -> Result<(), String> {
    stop_at(
        profile,
        chanvoy_core::pid_path_for_profile(profile),
        chanvoy_core::socket_path_for_profile(profile),
    )
    .await
}

async fn stop_at(profile: &str, pid_path: PathBuf, socket_path: PathBuf) -> Result<(), String> {
    let Some(control) = NativeControl::capture_with_peer(profile, pid_path, socket_path).await?
    else {
        return Ok(());
    };
    let result = match control.observe() {
        ProcessObservation::Dead => cleanup_dead(&control).await,
        ProcessObservation::Unknown => {
            Err("process liveness or birth identity unknown; state retained".into())
        }
        ProcessObservation::Alive(identity) if control.owned_identity(&identity) => {
            stop_confirmed(
                &control,
                &identity,
                StopBudget {
                    rpc: Duration::from_secs(3),
                    interval: Duration::from_millis(250),
                    polls: 20,
                },
            )
            .await
        }
        ProcessObservation::Alive(_) => {
            Err("daemon process ownership unconfirmed; state retained".into())
        }
    };
    result.map_err(|e| {
        format!(
            "pid {}{}: {e}; attempted bounded shutdown/confirmed-stop assessment",
            control.pid,
            if control.pid_stamp.is_none() {
                " (socket peer)"
            } else {
                ""
            }
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessIdentity {
    pid: u32,
    uid: u32,
    start_ms: u64,
    /// Native start ticks on Linux; start microseconds on macOS.
    birth: u64,
    executable: String,
    args: Vec<String>,
}

fn identity_difference(expected: &ProcessIdentity, actual: &ProcessIdentity) -> &'static str {
    if expected.pid != actual.pid {
        "pid-changed"
    } else if expected.uid != actual.uid {
        "owner-changed"
    } else if expected.birth != actual.birth {
        "birth-changed"
    } else if expected.start_ms != actual.start_ms {
        "start-time-changed"
    } else if expected.executable != actual.executable {
        "executable-changed"
    } else if actual.args.is_empty() {
        "arguments-unavailable"
    } else {
        "arguments-changed"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProcessObservation {
    Alive(ProcessIdentity),
    Dead,
    Unknown,
}

trait ProcessControl {
    /// A connection whose kernel peer is the captured process.
    type Peer;
    fn observe(&self) -> ProcessObservation;
    fn runtime_matches(&self, allow_absent: bool) -> bool;
    async fn peer_matches(&self) -> bool;
    async fn bind_peer(&self) -> Option<Self::Peer>;
    async fn socket_owner(&self, budget: Duration) -> SocketOwner;
    fn leftover_deadline(&self, pid: u32, window: Duration) -> Option<Instant>;
    async fn shutdown(&self, peer: Self::Peer) -> Result<(), String>;
    fn force_signal(&self) -> Result<(), String>;
    fn cleanup(&self) -> Result<(), String>;
}

#[derive(Clone, Copy)]
struct StopBudget {
    rpc: Duration,
    interval: Duration,
    polls: usize,
}

fn revalidate(
    control: &impl ProcessControl,
    expected: &ProcessIdentity,
    allow_absent: bool,
) -> Result<bool, String> {
    if !control.runtime_matches(allow_absent) {
        return Err("runtime identity changed; termination unconfirmed; state retained".into());
    }
    match control.observe() {
        ProcessObservation::Dead => Ok(false),
        ProcessObservation::Alive(actual) if actual == *expected => Ok(true),
        ProcessObservation::Alive(_) => {
            Err("process identity changed; termination unconfirmed; state retained".into())
        }
        ProcessObservation::Unknown => {
            Err("process liveness unknown; termination unconfirmed; state retained".into())
        }
    }
}

#[derive(Clone, Copy)]
struct SocketPollBudget {
    total: Duration,
    interval: Duration,
    attempts: usize,
}

async fn cleanup_dead(control: &impl ProcessControl) -> Result<(), String> {
    cleanup_dead_with(
        control,
        SocketPollBudget {
            total: Duration::from_millis(750),
            interval: Duration::from_millis(50),
            attempts: 5,
        },
    )
    .await
}

fn revalidate_dead(control: &impl ProcessControl) -> Result<(), String> {
    if !control.runtime_matches(true) || control.observe() != ProcessObservation::Dead {
        return Err(
            "death or runtime identity changed before socket observation; state retained".into(),
        );
    }
    Ok(())
}

async fn cleanup_dead_with(
    control: &impl ProcessControl,
    budget: SocketPollBudget,
) -> Result<(), String> {
    let mut deadline = Instant::now() + budget.total;
    let mut last = SocketOwner::Unknown(SocketUnknown::ProbeTimeout);
    for attempt in 0..budget.attempts {
        revalidate_dead(control)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        last = match tokio::time::timeout_at(deadline, control.socket_owner(remaining)).await {
            Ok(owner) => owner,
            Err(_) => SocketOwner::Unknown(SocketUnknown::ProbeTimeout),
        };
        if Instant::now() >= deadline {
            last = SocketOwner::Unknown(SocketUnknown::ProbeTimeout);
        }
        tracing::debug!(stage = "post-death-socket-owner", outcome = %last.receipt());
        revalidate_dead(control)?;
        match last {
            SocketOwner::Absent => return control.cleanup(),
            SocketOwner::Unknown(_) => break,
            SocketOwner::Live(pid) => {
                let Some(eligible_until) = control.leftover_deadline(pid, budget.total) else {
                    break;
                };
                deadline = deadline.min(eligible_until);
                if attempt + 1 == budget.attempts || Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep_until((Instant::now() + budget.interval).min(deadline)).await;
            }
        }
    }
    Err(format!(
        "socket owner not confirmed absent ({}); state retained",
        last.receipt()
    ))
}

async fn death_within(
    control: &impl ProcessControl,
    expected: &ProcessIdentity,
    budget: StopBudget,
) -> Result<bool, String> {
    for _ in 0..budget.polls {
        if !control.runtime_matches(true) {
            return Err(
                "runtime identity changed while observing termination; state retained".into(),
            );
        }
        match control.observe() {
            ProcessObservation::Dead => return Ok(true),
            ProcessObservation::Alive(actual) if actual != *expected => {
                return Err(
                    "process identity changed while observing termination; state retained".into(),
                )
            }
            // An exit can briefly make process metadata unavailable. Keep
            // observing within the same budget; never infer death or signal
            // while the outcome is unknown.
            ProcessObservation::Alive(_) | ProcessObservation::Unknown => {}
        }
        tokio::time::sleep(budget.interval).await;
    }
    Ok(!revalidate(control, expected, true)?)
}

async fn stop_confirmed(
    control: &impl ProcessControl,
    expected: &ProcessIdentity,
    budget: StopBudget,
) -> Result<(), String> {
    // Check before either kind of signal, including the graceful shutdown RPC.
    if !revalidate(control, expected, false)? {
        return cleanup_dead(control).await;
    }
    // The shutdown request travels on the connection whose peer was checked.
    let peer = match control.bind_peer().await {
        Some(peer) if revalidate(control, expected, false)? => peer,
        _ => {
            return Err(
                "socket process ownership unconfirmed before shutdown; state retained".into(),
            )
        }
    };
    let _ = tokio::time::timeout(budget.rpc, control.shutdown(peer)).await;
    if death_within(control, expected, budget).await? {
        return cleanup_dead(control).await;
    }
    if !revalidate(control, expected, false)? {
        return cleanup_dead(control).await;
    }
    if !control.peer_matches().await || !revalidate(control, expected, false)? {
        return Err(
            "socket process ownership unconfirmed before force signal; state retained".into(),
        );
    }
    if let Err(error) = control.force_signal() {
        if !revalidate(control, expected, true)? {
            return cleanup_dead(control).await;
        }
        return Err(format!(
            "force signal failed ({error}); termination unconfirmed; state retained"
        ));
    }
    if death_within(control, expected, budget).await? {
        cleanup_dead(control).await
    } else {
        Err("force signal attempted; process still alive after grace; termination unconfirmed; state retained".into())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_markers_are_unique_explicit_and_birth_encoding_never_wraps() {
        use super::{birth_claim, unique_argument};
        let args = vec![
            "chanvoy".into(),
            "--runtime-namespace".into(),
            "abc".into(),
            "--runtime-instance".into(),
            "def".into(),
        ];
        assert_eq!(unique_argument(&args, "--runtime-namespace"), Some("abc"));
        for bad in [
            vec!["--runtime-namespace".into()],
            vec!["--runtime-namespace".into(), "--runtime-instance".into()],
            vec!["--runtime-namespace=abc".into()],
            vec![
                "--runtime-namespace".into(),
                "abc".into(),
                "--runtime-namespace".into(),
                "abc".into(),
            ],
        ] {
            assert!(unique_argument(&bad, "--runtime-namespace").is_none());
        }
        assert_eq!(
            birth_claim(&chanvoy_core::runtime::NativeBirth::MacOs {
                seconds: 1,
                microseconds: 2
            }),
            Some(1_000_002)
        );
        assert!(birth_claim(&chanvoy_core::runtime::NativeBirth::MacOs {
            seconds: u64::MAX,
            microseconds: 2
        })
        .is_none());
    }
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[tokio::test]
    async fn read_only_candidate_binds_birth_uid_and_socket_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("candidate.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let before = super::ReuseCandidate::capture_path(&path)
            .await
            .expect("owned native listener evidence");
        assert_eq!(before.pid, std::process::id());
        assert_eq!(
            super::ReuseCandidate::capture_path(&path).await.as_ref(),
            Some(&before)
        );
        assert!(!before.terminated());
        assert!(before.unchanged().await);
        assert_eq!(before.binding().socket_path(), path);

        // Simulate an external replacement of this test-owned endpoint. The
        // old listener stays open, so this is not a pathname-only death proof.
        std::fs::remove_file(&path).unwrap();
        let replacement = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let after = super::ReuseCandidate::capture_path(&path).await.unwrap();
        assert_ne!(before, after);
        assert_eq!(
            (before.pid, before.birth, before.uid),
            (after.pid, after.birth, after.uid)
        );
        assert!(!before.terminated());
        drop((replacement, listener));
    }

    #[tokio::test]
    async fn read_only_candidate_refuses_non_socket_and_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let regular = dir.path().join("regular");
        std::fs::write(&regular, b"synthetic").unwrap();
        assert!(super::ReuseCandidate::capture_path(&regular)
            .await
            .is_none());
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&regular, &link).unwrap();
        assert!(super::ReuseCandidate::capture_path(&link).await.is_none());
    }

    fn failed_child_fixture(dir: &std::path::Path, pid: u32) -> super::FailedChildRuntime {
        let pid_path = dir.join("synthetic.pid");
        let socket_path = dir.join("synthetic.sock");
        std::fs::write(&pid_path, pid.to_string()).unwrap();
        std::fs::set_permissions(
            &pid_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();
        super::FailedChildRuntime {
            profile: "synthetic".into(),
            pid_stamp: super::stamp(&pid_path).unwrap(),
            socket_stamp: super::stamp(&socket_path).unwrap(),
            pid_path,
            socket_path,
            pid,
        }
    }

    #[tokio::test]
    async fn failed_child_cleanup_retains_a_live_listener() {
        let dir = tempfile::tempdir().unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(dir.path().join("synthetic.sock")).unwrap();
        let runtime = failed_child_fixture(dir.path(), 4242);
        assert!(runtime.cleanup_after_exit().await.is_err());
        assert!(runtime.pid_path.exists() && runtime.socket_path.exists());
        drop(listener);
    }

    #[tokio::test]
    async fn failed_child_cleanup_rejects_replaced_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(dir.path().join("synthetic.sock")).unwrap();
        drop(listener);
        let runtime = failed_child_fixture(dir.path(), 4242);
        std::fs::write(&runtime.pid_path, "42424242").unwrap();
        assert!(runtime.cleanup_after_exit().await.is_err());
        assert_eq!(
            std::fs::read_to_string(&runtime.pid_path).unwrap(),
            "42424242"
        );
        assert!(runtime.socket_path.exists());
    }

    #[tokio::test]
    async fn failed_child_cleanup_requires_positive_socket_absence() {
        if socket_fixture_in_child(
            "lifecycle::tests::failed_child_cleanup_requires_positive_socket_absence",
        )
        .await
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(dir.path().join("synthetic.sock")).unwrap();
        drop(listener);
        let runtime = failed_child_fixture(dir.path(), 4242);
        runtime.cleanup_after_exit().await.unwrap();
        assert!(!runtime.pid_path.exists() && !runtime.socket_path.exists());
    }

    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    #[test]
    fn linux_birth_keeps_ticks_and_handles_parenthesized_names() {
        let stat = |ticks: u64| {
            let mut fields = vec!["0".to_string(); 20];
            fields[0] = "S".into();
            fields[19] = ticks.to_string();
            format!("1234 (a name ) with parentheses) {}", fields.join(" "))
        };
        assert_eq!(linux_birth_from_stat(&stat(123456)), Some(123456));
        assert_eq!(linux_birth_from_stat(&stat(123457)), Some(123457));
        assert_ne!(
            linux_birth_from_stat(&stat(123456)),
            linux_birth_from_stat(&stat(123457)),
            "adjacent native ticks must not collapse to the same rounded second"
        );
        assert_eq!(linux_birth_from_stat("1234 (partial) S 1 2"), None);
        assert_eq!(linux_birth_from_stat("invalid stat"), None);
    }

    struct OwnedChild(std::process::Child);
    impl Drop for OwnedChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn child() -> OwnedChild {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("chanvoy_cli::lifecycle=debug")
            .with_ansi(false)
            .without_time()
            .with_writer(std::io::stderr)
            .try_init();
        OwnedChild(
            std::process::Command::new("sleep")
                .arg("30")
                .spawn()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn kernel_exit_observer_confirms_owned_child_before_reaping() {
        let mut child = child();
        let watch = ExitWatch::new(child.0.id())
            .unwrap()
            .expect("owned process exit watch");
        assert!(!watch.exited().unwrap());
        child.0.kill().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !watch.exited().unwrap() {
            assert!(
                std::time::Instant::now() < deadline,
                "owned-child kernel exit proof blocked"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            watch.exited().unwrap(),
            "exit evidence remains latched after consumption"
        );
        assert_eq!(observe_pid(child.0.id()), ProcessObservation::Dead);
        // A fresh capture of an unreaped fully exited process must still allow
        // stale PID cleanup, with independent positive socket absence proof.
        let root = tempfile::tempdir().unwrap();
        let pid_file = root.path().join("unreaped.pid");
        std::fs::write(&pid_file, child.0.id().to_string()).unwrap();
        let control = NativeControl::capture(
            "unreaped",
            pid_file.clone(),
            root.path().join("absent.sock"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(control.observe(), ProcessObservation::Dead);
        let first_death = *control.death_seen.lock().unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(control.observe(), ProcessObservation::Dead);
        assert_eq!(
            *control.death_seen.lock().unwrap(),
            first_death,
            "re-observation must not renew the post-death retry window"
        );
        assert!(
            control
                .leftover_deadline(child.0.id(), Duration::from_millis(750))
                .is_none(),
            "a cold dead capture cannot claim a recent owned daemon leftover"
        );
        cleanup_dead(&control).await.unwrap();
        assert!(!pid_file.exists());
        child.0.wait().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn exited_leader_with_live_worker_is_not_process_death() {
        use std::io::{BufRead, BufReader, Write};
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("owned-leader");
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/leader_exit.c");
        assert!(std::process::Command::new("cc")
            .arg("-pthread")
            .arg(fixture)
            .arg("-o")
            .arg(&executable)
            .status()
            .expect("owned Linux fixture compiler")
            .success());
        let socket = root.path().join("owned.sock");
        let pid_file = root.path().join("owned.pid");
        let mut child = OwnedChild(
            std::process::Command::new(executable)
                .arg(&socket)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let mut ready = String::new();
        BufReader::new(child.0.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "ready");
        std::fs::write(&pid_file, child.0.id().to_string()).unwrap();
        let control = NativeControl::capture("owned", pid_file.clone(), socket.clone())
            .unwrap()
            .unwrap();
        let ProcessObservation::Alive(identity) = control.observe() else {
            panic!("owned Linux process inspection blocked or incomplete");
        };
        let watch = control
            .exit_watch
            .as_ref()
            .expect("registered process pidfd");
        child.0.stdin.take().unwrap().write_all(b"x").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let stat = std::fs::read_to_string(format!("/proc/{}/stat", child.0.id())).unwrap();
            if stat.rsplit_once(") ").unwrap().1.starts_with("Z ") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "leader exit transition"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!watch.exited().unwrap(), "worker still holds listener");
        assert!(control.peer_matches().await, "kernel peer remains present");
        assert_eq!(
            control.observe(),
            ProcessObservation::Unknown,
            "a leader Zombie must not override a not-ready whole-process pidfd"
        );
        assert!(stop_confirmed(&control, &identity, budget()).await.is_err());
        assert!(pid_file.exists() && socket.exists());
        assert!(
            !watch.exited().unwrap(),
            "uncertainty must not signal the worker"
        );
        child.0.kill().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !watch.exited().unwrap() {
            assert!(
                std::time::Instant::now() < deadline,
                "whole-process exit transition"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            control.observe(),
            ProcessObservation::Dead,
            "kernel exit proves an unreaped whole process dead"
        );
        cleanup_dead(&control).await.unwrap();
        assert!(!pid_file.exists() && !socket.exists());
        child.0.wait().unwrap();
    }

    /// A just-spawned child can still be exec-ing when it is first inspected:
    /// between fork and exec it shows this test binary's identity, and
    /// production treats the transition as unknown or changed. Fixtures wait,
    /// within a bound, for the post-exec identity rather than racing that window.
    async fn capture_live(pid: &Path, socket: &Path) -> (NativeControl, ProcessIdentity) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let this_binary = std::env::current_exe()
            .and_then(std::fs::canonicalize)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        loop {
            let last = match NativeControl::capture("test", pid.to_path_buf(), socket.to_path_buf())
            {
                Ok(Some(control)) => match control.observe() {
                    ProcessObservation::Alive(identity) if identity.executable != this_binary => {
                        return (control, identity)
                    }
                    other => format!("{other:?}"),
                },
                other => format!("{:?}", other.map(|c| c.map(|c| c.pid))),
            };
            assert!(
                std::time::Instant::now() < deadline,
                "owned-child native process inspection blocked or incomplete: {last}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn socket_absent_with_live_owned_pid_is_not_stop_success() {
        let child = child();
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        std::fs::write(&pid, child.0.id().to_string()).unwrap();
        let (control, identity) = capture_live(&pid, &socket).await;
        assert!(stop_confirmed(&control, &identity, budget()).await.is_err());
        assert!(pid.exists());
        assert!(matches!(control.observe(), ProcessObservation::Alive(_)));
    }

    #[tokio::test]
    async fn dead_pid_does_not_sweep_a_live_peers_socket() {
        if socket_fixture_in_child("lifecycle::tests::dead_pid_does_not_sweep_a_live_peers_socket")
            .await
        {
            return;
        }
        assert_socket_handle_cleanup(false).await;
    }

    #[tokio::test]
    async fn closing_one_listener_descriptor_retains_live_peer() {
        if socket_fixture_in_child(
            "lifecycle::tests::closing_one_listener_descriptor_retains_live_peer",
        )
        .await
        {
            return;
        }
        assert_socket_handle_cleanup(true).await;
    }

    // Bind after exec in a single-test process, so parallel test spawns cannot
    // inherit this fixture's listener during their fork-to-exec window.
    async fn socket_fixture_in_child(test: &str) -> bool {
        const FLAG: &str = "CHANVOY_SOCKET_OWNER_TEST";
        if std::env::var(FLAG).as_deref() == Ok(test) {
            return false;
        }
        let child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(FLAG, test)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
            .await
            .expect("owned socket fixture deadline")
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(stdout.contains(&format!("test {test} ... ok")));
        true
    }

    async fn assert_socket_handle_cleanup(retain_descriptor: bool) {
        let mut child = child();
        let child_pid = child.0.id();
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        std::fs::write(&pid, child_pid.to_string()).unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let control = NativeControl::capture("test", pid.clone(), socket.clone())
            .unwrap()
            .unwrap();
        assert_eq!(control.observe(), ProcessObservation::Dead);
        assert!(control
            .leftover_deadline(child_pid, Duration::from_millis(750))
            .is_none());
        let refusal = cleanup_dead(&control).await.unwrap_err();
        assert!(refusal.contains(&format!("Live({})", std::process::id())));
        assert!(pid.exists() && socket.exists());
        let retained = retain_descriptor.then(|| listener.try_clone().unwrap());
        drop(listener);
        if let Some(retained) = retained {
            let refusal = cleanup_dead(&control).await.unwrap_err();
            assert!(refusal.contains(&format!("Live({})", std::process::id())));
            assert!(pid.exists() && socket.exists());
            drop(retained);
        }
        let cleanup = cleanup_dead(&control).await;
        assert!(cleanup.is_ok(), "post-drop cleanup refused: {cleanup:?}");
        assert!(!pid.exists() && !socket.exists());
    }

    fn set_mtime(path: &Path, at: std::time::SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(at)
            .unwrap();
    }

    #[tokio::test]
    async fn pid_record_older_than_its_process_is_released_without_a_signal() {
        let child = child();
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        std::fs::write(&pid, child.0.id().to_string()).unwrap();
        let (_, identity) = capture_live(&pid, &socket).await;
        // Written well before this process started, but after boot.
        let start = std::time::UNIX_EPOCH + Duration::from_millis(identity.start_ms);
        let boot = boot_time_ms().expect("boot time") as u64;
        let earlier = (start - Duration::from_secs(60))
            .max(std::time::UNIX_EPOCH + Duration::from_millis(boot + 10_000));
        assert!(
            earlier + Duration::from_secs(3) < start,
            "child too close to boot"
        );
        set_mtime(&pid, earlier);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let control = loop {
            match NativeControl::capture("test", pid.clone(), socket.clone()) {
                Ok(Some(control)) if control.observe() == ProcessObservation::Dead => {
                    break control
                }
                other => {
                    let last = format!("{:?}", other.map(|c| c.map(|c| c.observe())));
                    assert!(std::time::Instant::now() < deadline, "{last}");
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(!control.pre_boot_record);
        stop_at("test", pid.clone(), socket).await.unwrap();
        assert!(!pid.exists(), "stale record released");
        assert!(
            matches!(observe_pid(child.0.id()), ProcessObservation::Alive(_)),
            "the process now holding the pid is never signalled"
        );
    }

    #[tokio::test]
    async fn old_record_held_by_a_daemon_shaped_process_is_not_released() {
        // Timing is not death proof: a `daemon serve` holder keeps its record
        // and reaches the ownership refusal, including one whose profile came
        // from `CHANVOY_PROFILE` rather than its arguments.
        for args in [
            &["--profile", "test", "daemon", "serve"][..],
            &["daemon", "serve"],
            &["daemon", "--profile", "test", "serve"],
            &["daemon", "--profile=test", "serve"],
            &["--profile", "test", "daemon", "--json", "serve"],
        ] {
            old_record_holder_is_refused(args).await;
        }
    }

    async fn old_record_holder_is_refused(holder_args: &[&str]) {
        let _ = child();
        let holder = OwnedChild(
            std::process::Command::new("/bin/sh")
                .args(["-c", "sleep 30; :", "sh"])
                .args(holder_args)
                .spawn()
                .unwrap(),
        );
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        std::fs::write(&pid, holder.0.id().to_string()).unwrap();
        let (_, identity) = capture_live(&pid, &socket).await;
        assert!(
            identity.args.iter().any(|a| a == "serve"),
            "{:?}",
            identity.args
        );
        for at in [
            std::time::UNIX_EPOCH + Duration::from_secs(1_000),
            std::time::UNIX_EPOCH + Duration::from_millis(identity.start_ms)
                - Duration::from_secs(30),
        ] {
            set_mtime(&pid, at);
            capture_live(&pid, &socket).await;
            // Retry only transient inspection refusals; any success fails.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let refusal = loop {
                let refusal = stop_at("test", pid.clone(), socket.clone())
                    .await
                    .unwrap_err();
                if !refusal.contains("unavailable") && !refusal.contains("unknown") {
                    break refusal;
                }
                assert!(std::time::Instant::now() < deadline, "{refusal}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            assert!(refusal.contains("ownership unconfirmed"), "{refusal}");
            assert!(pid.exists());
        }
        // Never signalled: still running, not merely inspectable.
        let mut holder = holder;
        assert!(
            holder.0.try_wait().unwrap().is_none(),
            "holder was signalled"
        );
    }

    #[test]
    fn other_account_requires_a_positive_uid_read() {
        assert!(!held_by_other_user(std::process::id()));
        assert!(!held_by_other_user(0));
        assert!(!held_by_other_user(u32::MAX));
        // SAFETY: geteuid has no arguments or preconditions.
        if unsafe { libc::geteuid() } != 0 {
            assert_eq!(account_of(1), Some(0), "init is readable across accounts");
            assert!(held_by_other_user(1));
        }
    }

    #[tokio::test]
    async fn pid_record_from_before_boot_is_released_only_with_socket_absence() {
        if socket_fixture_in_child(
            "lifecycle::tests::pid_record_from_before_boot_is_released_only_with_socket_absence",
        )
        .await
        {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        // A pid that is live now but cannot be the recorded daemon: this
        // process, which may have no daemon arguments at all.
        std::fs::write(&pid, std::process::id().to_string()).unwrap();
        set_mtime(&pid, std::time::UNIX_EPOCH + Duration::from_secs(1_000));
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let control = NativeControl::capture("test", pid.clone(), socket.clone())
            .unwrap()
            .unwrap();
        assert!(control.pre_boot_record);
        assert_eq!(control.observe(), ProcessObservation::Dead);
        let refusal = stop_at("test", pid.clone(), socket.clone())
            .await
            .unwrap_err();
        assert!(refusal.contains("state retained"), "{refusal}");
        assert!(
            pid.exists() && socket.exists(),
            "a live listener keeps both"
        );
        drop(listener);
        stop_at("test", pid.clone(), socket.clone()).await.unwrap();
        assert!(!pid.exists() && !socket.exists());
    }

    #[tokio::test]
    async fn pidless_socket_owned_by_another_program_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let before = std::fs::symlink_metadata(&socket).unwrap().ino();
        let control = NativeControl::capture_with_peer("test", pid.clone(), socket.clone())
            .await
            .unwrap()
            .expect("peer fallback capture");
        assert_eq!(control.pid, std::process::id());
        assert!(control.pid_stamp.is_none());
        let ProcessObservation::Alive(identity) = control.observe() else {
            panic!("own process inspection blocked; host proof required");
        };
        assert!(
            !control.owned_identity(&identity),
            "this test binary is not `daemon serve --profile test`"
        );
        let refusal = stop_at("test", pid.clone(), socket.clone())
            .await
            .unwrap_err();
        assert!(refusal.contains("(socket peer)"), "{refusal}");
        assert!(refusal.contains("ownership unconfirmed"), "{refusal}");
        assert_eq!(std::fs::symlink_metadata(&socket).unwrap().ino(), before);
        assert!(!pid.exists());
    }

    #[tokio::test]
    async fn pidless_capture_refuses_a_pid_file_that_appears_later() {
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let control = NativeControl::capture_with_peer("test", pid.clone(), socket.clone())
            .await
            .unwrap()
            .unwrap();
        assert!(control.runtime_matches(false));
        assert!(control.peer_unchanged(SocketOwner::Live(control.pid)));
        for changed in [
            SocketOwner::Live(control.pid + 1),
            SocketOwner::Absent,
            SocketOwner::Unknown(SocketUnknown::ConnectTimeout),
        ] {
            assert!(!control.peer_unchanged(changed), "{changed:?}");
        }
        std::fs::write(&pid, std::process::id().to_string()).unwrap();
        assert!(!control.runtime_matches(false));
        assert!(!control.runtime_matches(true));
    }

    #[tokio::test]
    async fn dead_listener_without_pid_file_is_still_refused() {
        if socket_fixture_in_child(
            "lifecycle::tests::dead_listener_without_pid_file_is_still_refused",
        )
        .await
        {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        let refusal = stop_at("test", pid.clone(), socket.clone())
            .await
            .unwrap_err();
        assert!(refusal.contains("without a regular readable pid file"));
        assert!(socket.exists() && !pid.exists());
    }

    #[test]
    fn nonregular_invalid_and_replaced_pid_files_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        for invalid in ["0", "4294967295", "not-a-pid"] {
            std::fs::write(&pid, invalid).unwrap();
            assert!(NativeControl::capture("test", pid.clone(), socket.clone()).is_err());
        }
        std::fs::remove_file(&pid).unwrap();
        let fifo = std::ffi::CString::new(pid.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(NativeControl::capture("test", pid.clone(), socket.clone()).is_err());
        std::fs::remove_file(&pid).unwrap();
        std::fs::write(&pid, std::process::id().to_string()).unwrap();
        let control = NativeControl::capture("test", pid.clone(), socket)
            .unwrap()
            .unwrap();
        let replacement = root.path().join("replacement.pid");
        std::fs::write(&replacement, std::process::id().to_string()).unwrap();
        std::fs::rename(replacement, &pid).unwrap();
        assert!(!control.runtime_matches(false));
        assert!(!control.runtime_matches(true));
    }

    struct Fake {
        observations: RefCell<VecDeque<ProcessObservation>>,
        last: RefCell<ProcessObservation>,
        runtime: Cell<bool>,
        change_on_shutdown: bool,
        change_on_signal: bool,
        die_on_signal: bool,
        signal_error: Option<&'static str>,
        shutdowns: Cell<usize>,
        signals: Cell<usize>,
        cleanups: Cell<usize>,
        owners: RefCell<VecDeque<SocketOwner>>,
        last_owner: Cell<SocketOwner>,
        owner_calls: Cell<usize>,
        leftover_birth: Option<u64>,
        leftover_until: Instant,
        change_on_peer: bool,
        alive_on_peer: bool,
        pending_peer: bool,
        peer_unbound: bool,
    }
    fn identity() -> ProcessIdentity {
        ProcessIdentity {
            pid: 1234,
            uid: 1000,
            start_ms: 1000,
            birth: 1000000,
            executable: "/synthetic/chanvoy".into(),
            args: vec!["daemon".into(), "serve".into()],
        }
    }
    #[test]
    fn process_identity_requires_complete_metadata() {
        let expected = identity();
        let read = |start, executable, args| {
            identity_from_metadata(
                expected.pid,
                expected.uid,
                expected.birth,
                start,
                executable,
                args,
            )
        };
        assert_eq!(
            read(
                Some(expected.start_ms),
                Some(expected.executable.clone()),
                expected.args.clone()
            ),
            Some(expected.clone())
        );
        assert_eq!(
            read(
                Some(expected.start_ms),
                Some(expected.executable.clone()),
                vec![]
            ),
            None
        );
        assert_eq!(
            read(
                None,
                Some(expected.executable.clone()),
                expected.args.clone()
            ),
            None
        );
        for executable in [
            None,
            Some(String::new()),
            Some("/synthetic/chanvoy (deleted)".into()),
        ] {
            assert_eq!(
                read(Some(expected.start_ms), executable, expected.args.clone()),
                None
            );
        }
        let changed = read(
            Some(expected.start_ms),
            Some(expected.executable.clone()),
            vec!["other".into()],
        )
        .unwrap();
        assert_ne!(changed, expected);
        assert_eq!(
            identity_difference(&expected, &changed),
            "arguments-changed"
        );
    }

    #[tokio::test]
    async fn incomplete_shutdown_metadata_requires_confirmed_death() {
        let expected = identity();
        let incomplete = identity_from_metadata(
            expected.pid,
            expected.uid,
            expected.birth,
            Some(expected.start_ms),
            Some(expected.executable.clone()),
            vec![],
        )
        .map(ProcessObservation::Alive)
        .unwrap_or(ProcessObservation::Unknown);
        for confirmed in [false, true] {
            let mut observations = vec![
                ProcessObservation::Alive(expected.clone()),
                ProcessObservation::Alive(expected.clone()),
                incomplete.clone(),
            ];
            if confirmed {
                observations.push(ProcessObservation::Dead);
            }
            let f = fake(observations);
            let result = stop_confirmed(
                &f,
                &expected,
                StopBudget {
                    polls: 2,
                    ..budget()
                },
            )
            .await;
            assert_eq!(result.is_ok(), confirmed);
            assert_eq!(f.shutdowns.get(), 1);
            assert_eq!(f.signals.get(), 0);
            assert_eq!(f.cleanups.get(), usize::from(confirmed));
        }
        let mut changed = expected.clone();
        changed.args = vec!["other".into()];
        let f = fake(vec![
            ProcessObservation::Alive(expected.clone()),
            ProcessObservation::Alive(expected.clone()),
            ProcessObservation::Alive(changed),
            ProcessObservation::Dead,
        ]);
        assert!(stop_confirmed(&f, &expected, budget()).await.is_err());
        assert_eq!((f.signals.get(), f.cleanups.get()), (0, 0));
    }
    #[test]
    fn monitor_registration_never_adopts_changed_or_unknown_identity() {
        let mut reused = identity();
        reused.birth += 1;
        for after in [
            ProcessObservation::Unknown,
            ProcessObservation::Alive(reused),
        ] {
            let mut observations = VecDeque::from([ProcessObservation::Alive(identity()), after]);
            assert!(capture_monitor(|| observations.pop_front().unwrap(), || Ok(())).is_err());
        }
        for error in ["registration denied", "unsupported kernel"] {
            assert!(capture_monitor(
                || ProcessObservation::Alive(identity()),
                || Err::<(), _>(error.into())
            )
            .is_err());
        }
        let mut observations = VecDeque::from([
            ProcessObservation::Dead,
            ProcessObservation::Alive(identity()),
        ]);
        assert!(capture_monitor(|| observations.pop_front().unwrap(), || Ok(())).is_err());
    }

    fn socket_budget() -> SocketPollBudget {
        SocketPollBudget {
            total: Duration::from_secs(1),
            interval: Duration::ZERO,
            attempts: 3,
        }
    }
    fn dead_with_owners(owners: Vec<SocketOwner>) -> Fake {
        let mut f = fake(vec![ProcessObservation::Dead]);
        f.owners = RefCell::new(owners.into());
        f.leftover_birth = Some(identity().birth);
        f
    }
    fn assert_retained(f: &Fake) {
        assert_eq!(
            (f.shutdowns.get(), f.signals.get(), f.cleanups.get()),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn same_birth_leftover_needs_positive_absence_within_attempt_bound() {
        let f = dead_with_owners(vec![SocketOwner::Live(identity().pid), SocketOwner::Absent]);
        cleanup_dead_with(&f, socket_budget()).await.unwrap();
        assert_eq!((f.owner_calls.get(), f.cleanups.get()), (2, 1));
        assert_eq!((f.shutdowns.get(), f.signals.get()), (0, 0));
        let f = dead_with_owners(vec![SocketOwner::Live(identity().pid)]);
        let err = cleanup_dead_with(&f, socket_budget()).await.unwrap_err();
        assert!(err.contains("Live(1234)"));
        assert_eq!(f.owner_calls.get(), 3);
        assert_retained(&f);
    }

    #[tokio::test]
    async fn unknown_owner_is_classified_and_never_retried_as_leftover() {
        for reason in [
            SocketUnknown::Connect(std::io::ErrorKind::PermissionDenied),
            SocketUnknown::Credentials(std::io::ErrorKind::InvalidInput),
            SocketUnknown::ConnectTimeout,
            SocketUnknown::CredentialSizeInvalid,
            SocketUnknown::CredentialPidInvalid,
        ] {
            let f = dead_with_owners(vec![SocketOwner::Unknown(reason), SocketOwner::Absent]);
            let err = cleanup_dead_with(&f, socket_budget()).await.unwrap_err();
            assert!(err.contains(&SocketOwner::Unknown(reason).receipt()));
            assert_eq!(f.owner_calls.get(), 1);
            assert_retained(&f);
        }
    }

    #[tokio::test]
    async fn foreign_peer_birth_and_expired_window_retain_runtime() {
        for (pid, birth, expired) in [
            (4321, Some(identity().birth), false),
            (1234, Some(identity().birth + 1), false),
            (1234, None, false),
            (1234, Some(identity().birth), true),
        ] {
            let mut f = dead_with_owners(vec![SocketOwner::Live(pid), SocketOwner::Absent]);
            f.leftover_birth = birth;
            if expired {
                f.leftover_until = Instant::now() - Duration::from_millis(1);
                f.owners = RefCell::new(VecDeque::from([
                    SocketOwner::Live(pid),
                    SocketOwner::Live(pid),
                ]));
            }
            assert!(cleanup_dead_with(&f, socket_budget()).await.is_err());
            assert_eq!(f.owner_calls.get(), 1);
            assert_retained(&f);
            if expired {
                assert!(
                    cleanup_dead_with(&f, socket_budget()).await.is_err(),
                    "another cleanup call must not renew the post-death window"
                );
                assert_eq!(f.owner_calls.get(), 2);
                assert_retained(&f);
            }
        }
    }

    #[tokio::test]
    async fn socket_probe_revalidates_death_and_runtime_before_cleanup_and_retry() {
        for owner in [SocketOwner::Absent, SocketOwner::Live(identity().pid)] {
            for change_runtime in [true, false] {
                let mut f = dead_with_owners(vec![owner]);
                f.change_on_peer = change_runtime;
                f.alive_on_peer = !change_runtime;
                assert!(cleanup_dead_with(&f, socket_budget()).await.is_err());
                assert_eq!(f.owner_calls.get(), 1);
                assert_retained(&f);
            }
        }
        let mut reused = identity();
        reused.birth += 1;
        let mut f = dead_with_owners(vec![SocketOwner::Live(identity().pid), SocketOwner::Absent]);
        f.observations = RefCell::new(VecDeque::from([
            ProcessObservation::Dead,
            ProcessObservation::Dead,
            ProcessObservation::Alive(reused),
        ]));
        assert!(cleanup_dead_with(&f, socket_budget()).await.is_err());
        assert_eq!(f.owner_calls.get(), 1);
        assert_retained(&f);
    }

    #[tokio::test]
    async fn socket_connect_cannot_outlive_shared_deadline() {
        let mut f = dead_with_owners(vec![SocketOwner::Absent]);
        f.pending_peer = true;
        let budget = SocketPollBudget {
            total: Duration::from_millis(10),
            interval: Duration::ZERO,
            attempts: 3,
        };
        let err = cleanup_dead_with(&f, budget).await.unwrap_err();
        assert!(err.contains("Unknown(ProbeTimeout)"));
        assert_eq!(f.owner_calls.get(), 1);
        assert_retained(&f);
    }

    #[test]
    fn process_ownership_requires_executable_path_and_explicit_profile() {
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("own.pid");
        std::fs::write(&pid, std::process::id().to_string()).unwrap();
        let control = NativeControl::capture("own", pid, root.path().join("own.sock"))
            .unwrap()
            .unwrap();
        let ProcessObservation::Alive(mut identity) = control.observe() else {
            panic!("owned process inspection blocked; host proof required");
        };
        identity.args = vec![
            "--profile".into(),
            "own".into(),
            "daemon".into(),
            "serve".into(),
        ];
        assert!(
            control.owned_identity(&identity),
            "same canonical executable path is eligible, independent of loaded binary bytes"
        );
        identity.executable.push_str(" (deleted)");
        assert!(!control.owned_identity(&identity));
        identity.executable = "/synthetic/older-path/chanvoy".into();
        assert!(!control.owned_identity(&identity));
    }
    fn fake(observations: Vec<ProcessObservation>) -> Fake {
        Fake {
            observations: RefCell::new(observations.into()),
            last: RefCell::new(ProcessObservation::Alive(identity())),
            runtime: Cell::new(true),
            change_on_shutdown: false,
            change_on_signal: false,
            die_on_signal: false,
            signal_error: None,
            shutdowns: Cell::new(0),
            signals: Cell::new(0),
            cleanups: Cell::new(0),
            owners: RefCell::new(VecDeque::new()),
            last_owner: Cell::new(SocketOwner::Absent),
            owner_calls: Cell::new(0),
            leftover_birth: None,
            leftover_until: Instant::now() + Duration::from_secs(1),
            change_on_peer: false,
            alive_on_peer: false,
            pending_peer: false,
            peer_unbound: false,
        }
    }
    fn budget() -> StopBudget {
        StopBudget {
            rpc: Duration::from_millis(10),
            interval: Duration::ZERO,
            polls: 1,
        }
    }
    impl ProcessControl for Fake {
        type Peer = ();
        fn observe(&self) -> ProcessObservation {
            if let Some(next) = self.observations.borrow_mut().pop_front() {
                *self.last.borrow_mut() = next;
            }
            self.last.borrow().clone()
        }
        fn runtime_matches(&self, _: bool) -> bool {
            self.runtime.get()
        }
        async fn peer_matches(&self) -> bool {
            true
        }
        async fn bind_peer(&self) -> Option<()> {
            (!self.peer_unbound).then_some(())
        }
        async fn socket_owner(&self, _: Duration) -> SocketOwner {
            self.owner_calls.set(self.owner_calls.get() + 1);
            if self.pending_peer {
                std::future::pending::<()>().await;
            }
            if self.change_on_peer {
                self.runtime.set(false);
            }
            if self.alive_on_peer {
                *self.last.borrow_mut() = ProcessObservation::Alive(identity());
            }
            if let Some(owner) = self.owners.borrow_mut().pop_front() {
                self.last_owner.set(owner);
            }
            self.last_owner.get()
        }
        fn leftover_deadline(&self, pid: u32, _: Duration) -> Option<Instant> {
            (pid == identity().pid && self.leftover_birth == Some(identity().birth))
                .then_some(self.leftover_until)
        }
        async fn shutdown(&self, _: ()) -> Result<(), String> {
            self.shutdowns.set(self.shutdowns.get() + 1);
            if self.change_on_shutdown {
                self.runtime.set(false);
            }
            Ok(())
        }
        fn force_signal(&self) -> Result<(), String> {
            self.signals.set(self.signals.get() + 1);
            if self.change_on_signal {
                self.runtime.set(false);
            }
            if self.die_on_signal {
                *self.last.borrow_mut() = ProcessObservation::Dead;
            }
            self.signal_error.map(|e| Err(e.into())).unwrap_or(Ok(()))
        }
        fn cleanup(&self) -> Result<(), String> {
            self.cleanups.set(self.cleanups.get() + 1);
            Ok(())
        }
    }

    #[tokio::test]
    async fn unbound_peer_never_requests_shutdown_or_signals() {
        let mut f = fake(vec![]);
        f.peer_unbound = true;
        let refusal = stop_confirmed(&f, &identity(), budget()).await.unwrap_err();
        assert!(refusal.contains("before shutdown"), "{refusal}");
        assert_eq!(
            (f.shutdowns.get(), f.signals.get(), f.cleanups.get()),
            (0, 0, 0)
        );
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn bound_force_signal_reaches_only_the_registered_process() {
        let mut live = child();
        let watch = ExitWatch::new(live.0.id()).unwrap().expect("live child");
        watch.force_kill().unwrap();
        let status = live.0.wait().unwrap();
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(libc::SIGKILL)
        );

        // An exited process is never signalled. Unreaped, it still holds its
        // pid and a numeric kill would succeed; once reaped the pid is free
        // for reuse and a numeric kill could reach a later holder.
        let mut gone = child();
        let watch = ExitWatch::new(gone.0.id()).unwrap().expect("live child");
        gone.0.kill().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !watch.exited().unwrap() {
            assert!(std::time::Instant::now() < deadline, "exit observation");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            watch.force_kill().unwrap_err(),
            "process exited before force signal"
        );
        gone.0.wait().unwrap();
        assert!(watch.force_kill().is_err());
    }

    #[tokio::test]
    async fn shutdown_reaches_the_checked_listener_not_a_replacement() {
        let root = tempfile::tempdir().unwrap();
        let pid = root.path().join("test.pid");
        let socket = root.path().join("test.sock");
        let checked = tokio::net::UnixListener::bind(&socket).unwrap();
        let control = NativeControl::capture_with_peer("test", pid, socket.clone())
            .await
            .unwrap()
            .expect("peer fallback capture");
        let peer = control.bind_peer().await.expect("peer is this process");
        let mut other =
            NativeControl::capture_with_peer("test", root.path().join("test.pid"), socket.clone())
                .await
                .unwrap()
                .unwrap();
        other.pid += 1;
        assert!(
            other.bind_peer().await.is_none(),
            "a peer that is not the captured pid is never bound"
        );

        // A different listener takes the path after the peer check.
        std::fs::remove_file(&socket).unwrap();
        let replacement = tokio::net::UnixListener::bind(&socket).unwrap();

        let serve = async {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
            // Earlier credential probes are queued first and close unread.
            let (mut accepted, line) = loop {
                let (mut accepted, _) = checked.accept().await.unwrap();
                let mut line = String::new();
                tokio::io::BufReader::new(&mut accepted)
                    .read_line(&mut line)
                    .await
                    .unwrap();
                if !line.is_empty() {
                    break (accepted, line);
                }
            };
            assert!(line.contains("\"shutdown\""), "{line}");
            accepted
                .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"00000000-0000-0000-0000-000000000000\",\"result\":{\"stopping\":true}}\n")
                .await
                .unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(control.shutdown(peer), serve)
        })
        .await
        .expect("shutdown must reach the checked listener");
        result.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), replacement.accept())
                .await
                .is_err(),
            "the replacement listener must not be contacted"
        );
    }

    #[tokio::test]
    async fn unknown_and_reused_pid_never_signal_or_cleanup() {
        let mut reused = identity();
        reused.birth += 1;
        for observation in [
            ProcessObservation::Unknown,
            ProcessObservation::Alive(reused),
        ] {
            let f = fake(vec![observation]);
            assert!(stop_confirmed(&f, &identity(), budget()).await.is_err());
            assert_eq!(
                (f.shutdowns.get(), f.signals.get(), f.cleanups.get()),
                (0, 0, 0)
            );
        }
    }

    #[tokio::test]
    async fn failure_to_signal_and_survivor_keep_runtime() {
        for error in [None, Some("denied"), Some("command failed to launch")] {
            let mut f = fake(vec![]);
            f.signal_error = error;
            assert!(stop_confirmed(&f, &identity(), budget()).await.is_err());
            assert_eq!((f.signals.get(), f.cleanups.get()), (1, 0));
        }
    }

    #[tokio::test]
    async fn unknown_after_shutdown_does_not_force_signal_or_cleanup() {
        let f = fake(vec![
            ProcessObservation::Alive(identity()),
            ProcessObservation::Alive(identity()),
            ProcessObservation::Unknown,
        ]);
        assert!(stop_confirmed(&f, &identity(), budget()).await.is_err());
        assert_eq!(
            (f.shutdowns.get(), f.signals.get(), f.cleanups.get()),
            (1, 0, 0)
        );
    }

    #[tokio::test]
    async fn death_is_required_and_runtime_rechecked_before_cleanup() {
        let mut f = fake(vec![]);
        f.die_on_signal = true;
        assert!(stop_confirmed(&f, &identity(), budget()).await.is_ok());
        assert_eq!((f.signals.get(), f.cleanups.get()), (1, 1));
        let mut f = fake(vec![]);
        f.die_on_signal = true;
        f.change_on_signal = true;
        assert!(stop_confirmed(&f, &identity(), budget()).await.is_err());
        assert_eq!(f.cleanups.get(), 0);
        let mut f = fake(vec![]);
        f.change_on_shutdown = true;
        assert!(stop_confirmed(&f, &identity(), budget()).await.is_err());
        assert_eq!((f.signals.get(), f.cleanups.get()), (0, 0));
    }

    #[tokio::test]
    async fn identity_change_between_graceful_and_force_signal_is_refused() {
        let mut reused = identity();
        reused.birth += 1;
        let f = fake(vec![
            ProcessObservation::Alive(identity()),
            ProcessObservation::Alive(identity()),
            ProcessObservation::Alive(identity()),
            ProcessObservation::Alive(reused),
        ]);
        assert!(stop_confirmed(&f, &identity(), budget()).await.is_err());
        assert_eq!(
            (f.shutdowns.get(), f.signals.get(), f.cleanups.get()),
            (1, 0, 0)
        );
    }
}
