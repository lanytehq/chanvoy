//! Read-only native process identity for lifecycle binding. These observations
//! never establish whole-process death, lease ownership, or restart authority.
use super::{NativeBirth, PathBytes, ProcessId, RuntimeError, RuntimeProcess};

impl RuntimeProcess {
    /// Capture identity twice around native reads. A PID alone, a partially
    /// readable process, or changing evidence is never a usable identity.
    pub fn observe(pid: ProcessId) -> Result<Self, RuntimeError> {
        let first = observe_once(pid)?;
        let second = observe_once(pid)?;
        if first != second {
            return Err(RuntimeError::Changed("native process identity"));
        }
        Ok(second)
    }

    pub fn observe_current() -> Result<Self, RuntimeError> {
        Self::observe(ProcessId::new(std::process::id())?)
    }

    pub fn revalidate(&self) -> Result<(), RuntimeError> {
        if Self::observe(self.pid)? != *self {
            return Err(RuntimeError::Changed("bound native process"));
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn observe_once(pid: ProcessId) -> Result<RuntimeProcess, RuntimeError> {
    use std::os::unix::ffi::OsStrExt;
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    // SAFETY: positive representable PID and fully sized writable native buffer.
    let read = unsafe {
        *libc::__error() = 0;
        libc::proc_pidinfo(
            pid.get() as i32,
            libc::PROC_PIDTBSDINFO,
            1,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    let errno = std::io::Error::last_os_error();
    if read != size {
        return Err(
            if read <= 0 && matches!(errno.raw_os_error(), Some(libc::EPERM | libc::EACCES)) {
                RuntimeError::Unavailable("native process birth/owner permission denied")
            } else {
                RuntimeError::Unavailable("native process birth/owner incomplete")
            },
        );
    }
    if info.pbi_pid != pid.get() || info.pbi_start_tvusec >= 1_000_000 {
        return Err(RuntimeError::Invalid("native process birth/PID"));
    }
    let mut path = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: positive PID and writable buffer whose length is supplied.
    let read = unsafe {
        *libc::__error() = 0;
        libc::proc_pidpath(
            pid.get() as i32,
            path.as_mut_ptr().cast(),
            path.len() as u32,
        )
    };
    let errno = std::io::Error::last_os_error();
    if read <= 0 || read as usize >= path.len() {
        return Err(
            if read <= 0 && matches!(errno.raw_os_error(), Some(libc::EPERM | libc::EACCES)) {
                RuntimeError::Unavailable("native executable path permission denied")
            } else {
                RuntimeError::Unavailable("native executable path incomplete")
            },
        );
    }
    let end = path
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(RuntimeError::Invalid("native executable terminator"))?;
    if end == 0 {
        return Err(RuntimeError::Unavailable("native executable path"));
    }
    let executable = std::path::Path::new(std::ffi::OsStr::from_bytes(&path[..end]));
    Ok(RuntimeProcess {
        pid,
        uid: info.pbi_uid,
        birth: NativeBirth::MacOs {
            seconds: info.pbi_start_tvsec,
            microseconds: info.pbi_start_tvusec as u32,
        },
        executable: PathBytes::from_path(executable)?,
        pid_namespace: None,
        mount_namespace: None,
    })
}

#[cfg(target_os = "linux")]
fn observe_once(pid: ProcessId) -> Result<RuntimeProcess, RuntimeError> {
    use super::InodeIdentity;
    use std::os::unix::fs::MetadataExt;
    let root = std::path::PathBuf::from(format!("/proc/{}", pid.get()));
    let read = |name: &str| -> Result<String, RuntimeError> {
        use std::io::Read;
        let file = std::fs::File::open(root.join(name))?;
        let mut bytes = Vec::new();
        file.take(65_537).read_to_end(&mut bytes)?;
        if bytes.len() > 65_536 {
            return Err(RuntimeError::Invalid("native process metadata size"));
        }
        String::from_utf8(bytes)
            .map_err(|_| RuntimeError::Invalid("native process metadata encoding"))
    };
    let clock_ticks = linux_birth(&read("stat")?, pid)?;
    let uid = linux_effective_uid(&read("status")?)?;
    let executable = std::fs::read_link(root.join("exe"))?;
    use std::os::unix::ffi::OsStrExt;
    if executable.as_os_str().as_bytes().ends_with(b" (deleted)") {
        return Err(RuntimeError::Unavailable("deleted native executable"));
    }
    let namespace = |name: &str| -> Result<InodeIdentity, RuntimeError> {
        let meta = std::fs::metadata(root.join("ns").join(name))?;
        if meta.ino() == 0 {
            return Err(RuntimeError::Invalid("native namespace identity"));
        }
        Ok(InodeIdentity {
            device: meta.dev(),
            inode: meta.ino(),
        })
    };
    Ok(RuntimeProcess {
        pid,
        uid,
        birth: NativeBirth::Linux { clock_ticks },
        executable: PathBytes::from_path(&executable)?,
        pid_namespace: Some(namespace("pid")?),
        mount_namespace: Some(namespace("mnt")?),
    })
}

#[cfg(any(target_os = "linux", test))]
fn linux_birth(stat: &str, pid: ProcessId) -> Result<u64, RuntimeError> {
    let (head, tail) = stat
        .rsplit_once(") ")
        .ok_or(RuntimeError::Invalid("native stat delimiter"))?;
    let (observed, _) = head
        .split_once(" (")
        .ok_or(RuntimeError::Invalid("native stat PID"))?;
    if observed.parse::<u32>().ok() != Some(pid.get()) {
        return Err(RuntimeError::Changed("native stat PID"));
    }
    tail.split_whitespace()
        .nth(19)
        .and_then(|value| value.parse().ok())
        .ok_or(RuntimeError::Invalid("native stat birth"))
}

#[cfg(any(target_os = "linux", test))]
pub(super) fn linux_effective_uid(status: &str) -> Result<u32, RuntimeError> {
    let mut rows = status.lines().filter_map(|line| line.strip_prefix("Uid:"));
    let row = rows.next().ok_or(RuntimeError::Invalid("native UID row"))?;
    if rows.next().is_some() {
        return Err(RuntimeError::Invalid("duplicate native UID row"));
    }
    let ids: Vec<_> = row.split_whitespace().collect();
    if ids.len() != 4 || ids.iter().any(|id| id.parse::<u32>().is_err()) {
        return Err(RuntimeError::Invalid("native UID values"));
    }
    ids[1]
        .parse()
        .map_err(|_| RuntimeError::Invalid("native effective UID"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn observe_once(_: ProcessId) -> Result<RuntimeProcess, RuntimeError> {
    Err(RuntimeError::Unavailable("native process platform"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_birth_checks_pid_and_preserves_parenthesized_command() {
        let pid = ProcessId::new(42).unwrap();
        let tail = [vec!["S"], vec!["0"; 18], vec!["123456"]]
            .concat()
            .join(" ");
        assert_eq!(
            linux_birth(&format!("42 (a ) tricky (command)) {tail}"), pid).unwrap(),
            123456
        );
        assert!(linux_birth(&format!("43 (a) {tail}"), pid).is_err());
        assert!(linux_birth("42 (short) S 0", pid).is_err());
        assert!(linux_birth("42 missing parentheses", pid).is_err());
    }

    #[test]
    fn linux_uid_requires_one_complete_row_and_uses_effective_identity() {
        assert_eq!(
            linux_effective_uid("Name:\ttest\nUid:\t10 20 30 40\n").unwrap(),
            20
        );
        for bad in [
            "",
            "Uid: 10 20",
            "Uid: 10 20 x 40",
            "Uid: 1 2 3 4\nUid: 1 2 3 4",
        ] {
            assert!(linux_effective_uid(bad).is_err());
        }
    }

    #[test]
    #[ignore = "native process inspection must be permitted"]
    fn current_process_identity_round_trips() {
        let process = RuntimeProcess::observe_current().unwrap();
        assert_eq!(process.pid.get(), std::process::id());
        assert_eq!(process.uid, unsafe { libc::geteuid() });
        process.revalidate().unwrap();
        let mut wrong = process.clone();
        wrong.executable =
            PathBytes::from_path(std::path::Path::new("/not-the-current-executable")).unwrap();
        assert!(wrong.revalidate().is_err());
    }
}

/// Stage-one identity for discovery relevance only. It cannot authorize a
/// runtime candidate, retirement, or listener exclusion without full binding.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct RelevanceIdentity {
    pub pid: ProcessId,
    pub uid: u32,
    pub birth: NativeBirth,
    pub pid_namespace: Option<super::InodeIdentity>,
    pub mount_namespace: Option<super::InodeIdentity>,
}
impl RelevanceIdentity {
    pub fn observe(pid: ProcessId) -> Result<Self, RuntimeError> {
        let first = relevance_once(pid)?;
        let second = relevance_once(pid)?;
        if first != second {
            return Err(RuntimeError::Changed("native relevance identity"));
        }
        Ok(second)
    }
    pub fn revalidate(&self) -> Result<(), RuntimeError> {
        if Self::observe(self.pid)? != *self {
            return Err(RuntimeError::Changed("native relevance identity"));
        }
        Ok(())
    }
    pub fn matches_full(&self, full: &RuntimeProcess) -> bool {
        self.pid == full.pid
            && self.uid == full.uid
            && self.birth == full.birth
            && self.pid_namespace == full.pid_namespace
            && self.mount_namespace == full.mount_namespace
    }
}
#[cfg(target_os = "macos")]
fn relevance_once(pid: ProcessId) -> Result<RelevanceIdentity, RuntimeError> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let read = unsafe {
        libc::proc_pidinfo(
            pid.get() as i32,
            libc::PROC_PIDTBSDINFO,
            1,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if read != size {
        return Err(RuntimeError::Unavailable(
            "native relevance birth/owner unreadable",
        ));
    }
    if info.pbi_pid != pid.get() || info.pbi_start_tvusec >= 1_000_000 {
        return Err(RuntimeError::Invalid("native relevance PID/birth"));
    }
    Ok(RelevanceIdentity {
        pid,
        uid: info.pbi_uid,
        birth: NativeBirth::MacOs {
            seconds: info.pbi_start_tvsec,
            microseconds: info.pbi_start_tvusec as u32,
        },
        pid_namespace: None,
        mount_namespace: None,
    })
}
#[cfg(target_os = "linux")]
fn relevance_once(pid: ProcessId) -> Result<RelevanceIdentity, RuntimeError> {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let root = std::path::PathBuf::from(format!("/proc/{}", pid.get()));
    let read = |name: &str| -> Result<String, RuntimeError> {
        let mut bytes = Vec::new();
        std::fs::File::open(root.join(name))?
            .take(65_537)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 65_536 {
            return Err(RuntimeError::Unavailable("native relevance metadata cap"));
        }
        String::from_utf8(bytes)
            .map_err(|_| RuntimeError::Invalid("native relevance metadata encoding"))
    };
    let namespace = |name: &str| -> Result<super::InodeIdentity, RuntimeError> {
        let m = std::fs::metadata(root.join("ns").join(name))?;
        if m.ino() == 0 {
            return Err(RuntimeError::Invalid("native relevance namespace"));
        }
        Ok(super::InodeIdentity {
            device: m.dev(),
            inode: m.ino(),
        })
    };
    Ok(RelevanceIdentity {
        pid,
        uid: linux_effective_uid(&read("status")?)?,
        birth: NativeBirth::Linux {
            clock_ticks: linux_birth(&read("stat")?, pid)?,
        },
        pid_namespace: Some(namespace("pid")?),
        mount_namespace: Some(namespace("mnt")?),
    })
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn relevance_once(_: ProcessId) -> Result<RelevanceIdentity, RuntimeError> {
    Err(RuntimeError::Unavailable("native relevance platform"))
}
