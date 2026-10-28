//! Restrict discovery to an unobscured procfs view on a qualified native host.
//! These checks detect unsupported views; they cannot certify host provenance
//! from inside a container. Native-host deployment qualification remains required.
use super::RuntimeError;
#[cfg(target_os = "linux")]
use super::{ProcessId, MAX_PIDS};
#[cfg(target_os = "linux")]
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[cfg(target_os = "linux")]
fn bounded(path: &std::path::Path, cap: usize) -> Result<Vec<u8>, RuntimeError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut bytes = Vec::new();
    file.take(cap as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > cap {
        return Err(RuntimeError::Unavailable("procfs metadata truncated"));
    }
    Ok(bytes)
}
#[cfg(target_os = "linux")]
fn visibility() -> Result<(std::fs::File, Vec<u8>), RuntimeError> {
    let root = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open("/proc")?;
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatfs(root.as_raw_fd(), &mut fs) } != 0
        || fs.f_type != libc::PROC_SUPER_MAGIC
    {
        return Err(RuntimeError::Unavailable("native inventory procfs type"));
    }
    let mountinfo = bounded(
        std::path::Path::new("/proc/self/mountinfo"),
        4 * 1024 * 1024,
    )?;
    validate_mount_view(&mountinfo)?;
    for kind in ["pid", "mnt"] {
        let ours = std::fs::metadata(format!("/proc/self/ns/{kind}"))?;
        let init = std::fs::metadata(format!("/proc/1/ns/{kind}"))?;
        if (ours.dev(), ours.ino()) != (init.dev(), init.ino()) {
            return Err(RuntimeError::Unavailable("native inventory namespace view"));
        }
    }
    let current = root.metadata()?;
    let named = std::fs::symlink_metadata("/proc")?;
    if (current.dev(), current.ino()) != (named.dev(), named.ino()) {
        return Err(RuntimeError::Changed("native inventory procfs root"));
    }
    Ok((root, mountinfo))
}
#[cfg(target_os = "linux")]
pub(super) fn enumerate() -> Result<Vec<ProcessId>, RuntimeError> {
    let (root, before) = visibility()?;
    // fdopendir takes ownership of a duplicate of the validated directory,
    // rather than resolving /proc again between validation and iteration.
    let duplicate = unsafe { libc::fcntl(root.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(error.into());
    }
    struct Directory(*mut libc::DIR);
    impl Drop for Directory {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let directory = Directory(directory);
    let mut pids = Vec::new();
    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(directory.0) };
        if entry.is_null() {
            let errno = std::io::Error::last_os_error();
            if errno.raw_os_error() != Some(0) {
                return Err(errno.into());
            }
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        let bytes = name.to_bytes();
        if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
            continue;
        }
        let pid = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or(RuntimeError::Invalid("native inventory PID name"))?;
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::fstatat(
                root.as_raw_fd(),
                name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(RuntimeError::Unavailable(
                "native inventory obscured process",
            ));
        }
        pids.push(ProcessId::new(pid)?);
        if pids.len() >= MAX_PIDS {
            return Err(RuntimeError::Unavailable("native inventory truncated"));
        }
    }
    let (after_root, after) = visibility()?;
    let old = root.metadata()?;
    let new = after_root.metadata()?;
    if before != after || (old.dev(), old.ino()) != (new.dev(), new.ino()) {
        return Err(RuntimeError::Changed("native inventory procfs mounts"));
    }
    Ok(pids)
}
#[cfg(target_os = "linux")]
pub(super) fn owner(pid: ProcessId) -> Result<u32, RuntimeError> {
    let bytes = bounded(
        &std::path::PathBuf::from(format!("/proc/{}/status", pid.get())),
        65_536,
    )?;
    let status = std::str::from_utf8(&bytes)
        .map_err(|_| RuntimeError::Invalid("native inventory status encoding"))?;
    super::super::native_process::linux_effective_uid(status)
}
fn validate_mount_view(bytes: &[u8]) -> Result<(), RuntimeError> {
    let mut proc_root = 0;
    for line in bytes.split(|b| *b == b'\n').filter(|s| !s.is_empty()) {
        let f: Vec<_> = line.split(|b| *b == b' ').collect();
        let sep = f
            .iter()
            .position(|s| *s == b"-")
            .ok_or(RuntimeError::Unavailable("procfs mount fields"))?;
        if sep < 6 || f.len() != sep + 4 {
            return Err(RuntimeError::Unavailable("procfs mount fields"));
        }
        if f[4] == b"/proc" {
            proc_root += 1;
            if f[3] != b"/" || f[sep + 1] != b"proc" {
                return Err(RuntimeError::Unavailable("partial procfs root"));
            }
            for opts in [f[5], f[sep + 3]] {
                for option in opts.split(|b| *b == b',') {
                    if (option.starts_with(b"hidepid=") && option != b"hidepid=0")
                        || option.starts_with(b"subset=")
                    {
                        return Err(RuntimeError::Unavailable("restricted procfs visibility"));
                    }
                }
            }
        } else if let Some(rest) = f[4].strip_prefix(b"/proc/") {
            // Any overlay on a process view or on the namespace/metadata
            // selectors makes inventory incomplete. Other procfs auxiliaries
            // (e.g. binfmt_misc) do not cover PID directories.
            let first = rest.split(|b| *b == b'/').next().unwrap_or_default();
            if first == b"self"
                || first == b"thread-self"
                || first.first().is_some_and(u8::is_ascii_digit)
            {
                return Err(RuntimeError::Unavailable("overlaid procfs process view"));
            }
        }
    }
    if proc_root != 1 {
        return Err(RuntimeError::Unavailable("procfs mount root ambiguous"));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::validate_mount_view;
    #[test]
    fn procfs_visibility_refuses_hidden_partial_and_overlaid_views() {
        let full = b"22 1 0:5 / /proc rw,nosuid - proc proc rw\n";
        assert!(validate_mount_view(full).is_ok());
        for bad in [
            "22 1 0:5 / /proc rw - proc proc rw,hidepid=2\n",
            "22 1 0:5 / /proc rw - proc proc rw,subset=pid\n",
            "22 1 0:5 /part /proc rw - proc proc rw\n",
            "22 1 0:5 / /proc rw - tmpfs tmpfs rw\n",
            "22 1 0:5 / /proc rw - proc proc rw\n23 22 0:6 / /proc/123 rw - tmpfs tmpfs rw\n",
            "22 1 0:5 / /proc rw - proc proc rw\n23 22 0:6 / /proc/self rw - tmpfs tmpfs rw\n",
        ] {
            assert!(validate_mount_view(bad.as_bytes()).is_err());
        }
    }
}
