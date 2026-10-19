//! Linux mount-provider coordinates, also parsed in deterministic tests.
use super::{validate_persistent_path, RuntimeError};
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Mount {
    id: u64,
    major: u64,
    minor: u64,
    root: PathBuf,
    point: PathBuf,
}

fn number(bytes: &[u8]) -> Result<u64, RuntimeError> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(RuntimeError::Unavailable("mount numeric field"));
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(RuntimeError::Unavailable("mount numeric range"))
}
fn path(bytes: &[u8]) -> Result<PathBuf, RuntimeError> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] != b'\\' {
            out.push(bytes[pos]);
            pos += 1;
            continue;
        }
        let escape = bytes
            .get(pos + 1..pos + 4)
            .ok_or(RuntimeError::Unavailable("mount pathname escape"))?;
        let decoded = match escape {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => return Err(RuntimeError::Unavailable("mount pathname escape")),
        };
        out.push(decoded);
        pos += 4;
    }
    if out.contains(&0) {
        return Err(RuntimeError::Unavailable("mount pathname NUL"));
    }
    let path = PathBuf::from(OsString::from_vec(out));
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(RuntimeError::Unavailable("mount pathname shape"));
    }
    Ok(path)
}
fn parse(bytes: &[u8]) -> Result<Vec<Mount>, RuntimeError> {
    let mut mounts = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for line in bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
        let fields: Vec<_> = line
            .split(u8::is_ascii_whitespace)
            .filter(|f| !f.is_empty())
            .collect();
        let separator = fields
            .iter()
            .position(|f| *f == b"-")
            .ok_or(RuntimeError::Unavailable("mount separator"))?;
        if separator < 6 || fields.len() != separator + 4 {
            return Err(RuntimeError::Unavailable("mount fields"));
        }
        let device: Vec<_> = fields[2].split(|b| *b == b':').collect();
        if device.len() != 2 {
            return Err(RuntimeError::Unavailable("mount device"));
        }
        let id = number(fields[0])?;
        number(fields[1])?;
        if id == 0 || !ids.insert(id) {
            return Err(RuntimeError::Unavailable("mount identity"));
        }
        mounts.push(Mount {
            id,
            major: number(device[0])?,
            minor: number(device[1])?,
            root: path(fields[3])?,
            point: path(fields[4])?,
        });
    }
    if mounts.is_empty() {
        return Err(RuntimeError::Unavailable("empty mount inventory"));
    }
    Ok(mounts)
}
fn map<'a>(
    mounts: &'a [Mount],
    id: u64,
    path: &Path,
) -> Result<(&'a Mount, PathBuf), RuntimeError> {
    let mount = mounts
        .iter()
        .find(|m| m.id == id)
        .ok_or(RuntimeError::Unavailable(
            "descriptor mount absent from inventory",
        ))?;
    let relative = path
        .strip_prefix(&mount.point)
        .map_err(|_| RuntimeError::Changed("descriptor mount pathname"))?;
    Ok((mount, mount.root.join(relative)))
}
fn reject_alias(mapped: &Path, mapped_home: &Path) -> Result<(), RuntimeError> {
    validate_persistent_path(mapped)?;
    for cache in [".cache", ".cargo", "Library/Caches"] {
        if mapped.starts_with(mapped_home.join(cache)) {
            return Err(RuntimeError::Invalid("cache-backed authority alias"));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn reconcile(
    fd: i32,
    path: &Path,
    home_fd: i32,
    home: &Path,
) -> Result<(), RuntimeError> {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;
    fn proc_bytes(path: &Path, cap: u64) -> Result<Vec<u8>, RuntimeError> {
        let file = std::fs::File::open(path)?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: live read-only descriptor and writable stat buffer.
        if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: initialized by successful fstatfs.
        if unsafe { stat.assume_init() }.f_type != libc::PROC_SUPER_MAGIC {
            return Err(RuntimeError::Unavailable("kernel procfs source"));
        }
        let mut bytes = Vec::new();
        file.take(cap + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > cap {
            return Err(RuntimeError::Unavailable("kernel inventory byte cap"));
        }
        Ok(bytes)
    }
    fn mount_id(fd: i32) -> Result<u64, RuntimeError> {
        let bytes = proc_bytes(
            &PathBuf::from(format!("/proc/{}/fdinfo/{fd}", std::process::id())),
            8192,
        )?;
        let mut ids = bytes
            .split(|b| *b == b'\n')
            .filter_map(|line| line.strip_prefix(b"mnt_id:").map(|v| v.trim_ascii()));
        let id = number(
            ids.next()
                .ok_or(RuntimeError::Unavailable("descriptor mount id"))?,
        )?;
        if ids.next().is_some() {
            return Err(RuntimeError::Unavailable("duplicate descriptor mount id"));
        }
        Ok(id)
    }
    fn device(fd: i32) -> Result<(u64, u64), RuntimeError> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fd is borrowed from a live directory, output is writable.
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: initialized by successful fstat.
        let stat = unsafe { stat.assume_init() };
        Ok((
            libc::major(stat.st_dev) as u64,
            libc::minor(stat.st_dev) as u64,
        ))
    }
    let bytes = proc_bytes(
        &PathBuf::from(format!("/proc/{}/mountinfo", std::process::id())),
        4 * 1024 * 1024,
    )?;
    let mounts = parse(&bytes)?;
    let target_id = mount_id(fd)?;
    let home_id = mount_id(home_fd)?;
    let (target, mapped) = map(&mounts, target_id, path)?;
    let (home_mount, mapped_home) = map(&mounts, home_id, home)?;
    let target_device = device(fd)?;
    let home_device = device(home_fd)?;
    if target_device != (target.major, target.minor)
        || home_device != (home_mount.major, home_mount.minor)
    {
        return Err(RuntimeError::Changed("descriptor mount device"));
    }
    if target_device != home_device {
        return Err(RuntimeError::Unavailable(
            "authority submount provider cannot be reconciled with native home",
        ));
    }
    reject_alias(&mapped, &mapped_home)?;
    // Re-read mount inventory and descriptor ids so an in-flight remount is not
    // silently accepted as the captured provider coordinate.
    if proc_bytes(
        &PathBuf::from(format!("/proc/{}/mountinfo", std::process::id())),
        4 * 1024 * 1024,
    )? != bytes
        || mount_id(fd)? != target_id
        || mount_id(home_fd)? != home_id
    {
        return Err(RuntimeError::Changed("mount inventory"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bind_alias_uses_provider_root_not_display_path_and_refuses_temporary_cache() {
        let mounts = parse(b"1 0 8:1 / / rw - ext4 /dev/sda rw\n2 1 8:1 /tmp/source /home/user/.chanvoy rw - ext4 /dev/sda rw\n3 1 8:1 /home/user/.cache/source /home/user/cache-alias rw - ext4 /dev/sda rw\n").unwrap();
        let (_, home) = map(&mounts, 1, Path::new("/home/user")).unwrap();
        for (id, p) in [
            (2, "/home/user/.chanvoy/lifecycle"),
            (3, "/home/user/cache-alias/lifecycle"),
        ] {
            let (_, mapped) = map(&mounts, id, Path::new(p)).unwrap();
            assert!(reject_alias(&mapped, &home).is_err());
        }
        assert!(map(&mounts, 999, Path::new("/home/user")).is_err());
        assert!(map(&mounts, 2, Path::new("/other/path")).is_err());
    }
    #[test]
    fn mount_paths_are_lossless_and_malformed_or_duplicate_sources_are_unknown() {
        let mounts = parse(b"1 0 8:1 / /owned\\040root rw shared:4 - ext4 /dev/x rw\n").unwrap();
        assert_eq!(mounts[0].point, Path::new("/owned root"));
        assert_eq!((mounts[0].major, mounts[0].minor), (8, 1));
        for bad in [
            b"1 0 8:1 / / rw\n".as_slice(),
            b"1 0 8:1 / / rw - ext4 /dev/x rw\n1 0 8:1 / / rw - ext4 /dev/x rw\n",
            b"1 0 8:1 / /bad\\777 rw - ext4 /dev/x rw\n",
        ] {
            assert!(parse(bad).is_err());
        }
        assert_eq!(
            path(b"/owned/\xff").unwrap(),
            PathBuf::from(OsString::from_vec(b"/owned/\xff".to_vec()))
        );
    }
}
