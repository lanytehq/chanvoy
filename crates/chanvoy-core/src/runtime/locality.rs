use super::filesystem::PrivateDirectory;
use super::RuntimeError;
use std::path::Path;
#[cfg(any(target_os = "linux", test))]
mod mounts;

pub(super) fn validate_persistent_path(path: &Path) -> Result<(), RuntimeError> {
    if !path.is_absolute() {
        return Err(RuntimeError::Invalid("absolute persistent root required"));
    }
    for prefix in [
        "/tmp",
        "/private/tmp",
        "/var/tmp",
        "/private/var/tmp",
        "/var/folders",
        "/private/var/folders",
        "/run",
        "/var/run",
        "/private/var/run",
    ] {
        if path.starts_with(prefix) {
            return Err(RuntimeError::Invalid("reapable authority ancestry"));
        }
    }
    let components: Vec<_> = path
        .components()
        .filter_map(|c| {
            if let std::path::Component::Normal(n) = c {
                Some(n)
            } else {
                None
            }
        })
        .collect();
    // Known per-user caches, preserving arbitrary login names themselves.
    let cache = (components
        .first()
        .is_some_and(|c| *c == "home" || *c == "Users")
        && components
            .get(2)
            .is_some_and(|c| *c == ".cache" || *c == ".cargo"))
        || (components.first().is_some_and(|c| *c == "root")
            && components
                .get(1)
                .is_some_and(|c| *c == ".cache" || *c == ".cargo"))
        || components
            .windows(2)
            .any(|pair| pair[0] == "Library" && pair[1] == "Caches");
    if cache {
        return Err(RuntimeError::Invalid("cache authority ancestry"));
    }
    Ok(())
}

pub(super) fn validate_local_directory(
    fd: i32,
    path: &Path,
    home: &PrivateDirectory,
) -> Result<(), RuntimeError> {
    validate_persistent_path(path)?;
    validate_native_local_filesystem(fd)?;
    #[cfg(target_os = "linux")]
    mounts::reconcile(fd, path, home.raw_fd(), home.path())?;
    #[cfg(not(target_os = "linux"))]
    let _ = home;
    Ok(())
}

/// Socket roots may be reaped, but must still be proved native/local. This
/// checker does not grant persistent-authority status to socket-only roots.
pub(super) fn validate_native_local_filesystem(fd: i32) -> Result<(), RuntimeError> {
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: fd is a live directory descriptor and stat is writable.
    if unsafe { libc::fstatfs(fd, stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful fstatfs initialized the output.
    let stat = unsafe { stat.assume_init() };
    #[cfg(target_os = "macos")]
    {
        if stat.f_flags & libc::MNT_LOCAL as u32 == 0 {
            return Err(RuntimeError::Unavailable("non-local authority filesystem"));
        }
        // Unknown local providers (including null/bind-like filesystem layers)
        // are not proved persistent just because MNT_LOCAL is set.
        let name: Vec<u8> = stat
            .f_fstypename
            .iter()
            .map(|b| *b as u8)
            .take_while(|b| *b != 0)
            .collect();
        if name != b"apfs" && name != b"hfs" {
            return Err(RuntimeError::Unavailable(
                "macOS authority filesystem provider unconfirmed",
            ));
        }
    }
    #[cfg(target_os = "linux")]
    {
        if ![
            libc::EXT4_SUPER_MAGIC,
            libc::XFS_SUPER_MAGIC,
            libc::BTRFS_SUPER_MAGIC,
            libc::F2FS_SUPER_MAGIC,
        ]
        .contains(&stat.f_type)
        {
            return Err(RuntimeError::Unavailable(
                "authority filesystem locality unconfirmed",
            ));
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        return Err(RuntimeError::Unavailable("unsupported native filesystem"));
    }
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persistent_selector_classifies_canonical_temp_cache_aliases_without_reclassifying_login_names(
    ) {
        for path in [
            "/tmp/home",
            "/private/tmp/home",
            "/var/tmp/home",
            "/private/var/folders/x/0",
            "/run/home",
            "/Users/user/.cache/home",
            "/Users/user/.cargo/home",
            "/home/user/.cache/home",
            "/root/.cargo/home",
            "/Users/user/Library/Caches/home",
        ] {
            assert!(validate_persistent_path(Path::new(path)).is_err(), "{path}");
        }
        for path in [
            "/Users/.cache",
            "/home/.cargo",
            "/Users/user",
            "/srv/owned/home",
        ] {
            validate_persistent_path(Path::new(path)).unwrap();
        }
        let fixture = super::super::filesystem::tests::fixture();
        let alias = fixture.path().join("home-alias");
        std::os::unix::fs::symlink("/private/tmp", &alias).unwrap();
        #[cfg(target_os = "macos")]
        assert!(validate_persistent_path(&std::fs::canonicalize(alias).unwrap()).is_err());
        #[cfg(not(target_os = "macos"))]
        {
            std::fs::remove_file(&alias).unwrap();
            std::os::unix::fs::symlink("/tmp", &alias).unwrap();
            assert!(validate_persistent_path(&std::fs::canonicalize(alias).unwrap()).is_err());
        }
    }
}
