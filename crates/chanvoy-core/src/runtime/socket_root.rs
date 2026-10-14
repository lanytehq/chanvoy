//! Native socket-only root selection. It cannot elect lifecycle authority C.
use super::filesystem::PrivateDirectory;
use super::{child_socket_path, NativePlatform, PathBytes, ProcessId, RuntimeError};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

pub struct SocketRoot {
    directory: PrivateDirectory,
    platform: NativePlatform,
    uid: u32,
}
impl SocketRoot {
    /// Explicit managed initialization. Existing override roots must already
    /// satisfy the private native-account contract; no broad root is repaired.
    pub fn initialize() -> Result<Self, RuntimeError> {
        if std::env::var_os("CHANVOY_STATE_DIR").is_some() {
            return Err(RuntimeError::Invalid("state-root override unsupported"));
        }
        Self::select(true)?.ok_or(RuntimeError::Unavailable("socket root"))
    }
    pub fn observe() -> Result<Option<Self>, RuntimeError> {
        Self::select(false)
    }
    fn select(create: bool) -> Result<Option<Self>, RuntimeError> {
        let account = super::NativeAccount::resolve()?;
        let platform = NativePlatform::current()?;
        let directory = if let Some(override_path) = std::env::var_os("CHANVOY_RUNTIME_DIR") {
            let path = PathBuf::from(override_path);
            // Explicit override means an existing absolute, canonical, private
            // account directory. Missing/uncertain override never falls back.
            PathBytes::from_path(&path)?;
            let canonical = std::fs::canonicalize(&path)?;
            if canonical.as_os_str() != path.as_os_str() {
                return Err(RuntimeError::Invalid("noncanonical socket override"));
            }
            PrivateDirectory::open_canonical(&canonical, account.uid())?
        } else {
            let parent_path = native_parent()?;
            let parent_path = std::fs::canonicalize(parent_path)?;
            let parent = open_socket_parent(&parent_path, account.uid(), platform)?;
            super::locality::validate_native_local_filesystem(parent.raw_fd())?;
            let name = match platform {
                NativePlatform::MacOs => "chanvoy".to_string(),
                NativePlatform::Linux => format!("chanvoy-{}", account.uid()),
            };
            child_socket_path(
                &parent.path().join(&name).join("r"),
                ProcessId::new(i32::MAX as u32)?,
                platform,
            )?;
            let Some(base) = parent.child(OsStr::new(&name), create)? else {
                return Ok(None);
            };
            super::locality::validate_native_local_filesystem(base.raw_fd())?;
            let Some(directory) = base.child(OsStr::new("r"), create)? else {
                return Ok(None);
            };
            directory
        };
        super::locality::validate_native_local_filesystem(directory.raw_fd())?;
        child_socket_path(directory.path(), ProcessId::new(i32::MAX as u32)?, platform)?;
        Ok(Some(Self {
            directory,
            platform,
            uid: account.uid(),
        }))
    }
    pub fn path(&self) -> &Path {
        self.directory.path()
    }
    pub fn child_path(&self, pid: ProcessId) -> Result<PathBytes, RuntimeError> {
        self.verify()?;
        child_socket_path(self.path(), pid, self.platform)
    }
    pub fn verify(&self) -> Result<(), RuntimeError> {
        if super::NativeAccount::resolve()?.uid() != self.uid {
            return Err(RuntimeError::Changed("socket account UID"));
        }
        self.directory.verify_named()?;
        super::locality::validate_native_local_filesystem(self.directory.raw_fd())
    }
}

fn open_socket_parent(
    path: &Path,
    uid: u32,
    platform: NativePlatform,
) -> Result<PrivateDirectory, RuntimeError> {
    match platform {
        NativePlatform::MacOs => PrivateDirectory::open_canonical(path, uid),
        NativePlatform::Linux => PrivateDirectory::open_system_temporary_parent(path, uid),
    }
}

fn native_parent() -> Result<PathBuf, RuntimeError> {
    #[cfg(target_os = "linux")]
    {
        Ok(PathBuf::from("/var/tmp"))
    }
    #[cfg(target_os = "macos")]
    {
        // macOS native local selector (local0), not the temp/cache selectors.
        // It has no invocation HOME/TMPDIR fallback.
        // SAFETY: null/zero asks confstr for the required bounded allocation.
        let size = unsafe { libc::confstr(libc::_CS_DARWIN_USER_DIR, std::ptr::null_mut(), 0) };
        if size == 0 || size > 4097 {
            return Err(RuntimeError::Unavailable("native local socket selector"));
        }
        let mut bytes = vec![0u8; size];
        // SAFETY: writable buffer advertised at its actual length.
        let actual = unsafe {
            libc::confstr(
                libc::_CS_DARWIN_USER_DIR,
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if actual != size || bytes.last() != Some(&0) || bytes[..size - 1].contains(&0) {
            return Err(RuntimeError::Changed("native local socket selector"));
        }
        bytes.pop();
        use std::os::unix::ffi::OsStringExt;
        let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
        PathBytes::from_path(&path)?;
        Ok(path)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(RuntimeError::Unavailable("native socket selector"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "qualified normal-host default native R fixture; no socket/service bind"]
    fn native_socket_root_uses_account_local_selector_and_worst_case_budget() {
        assert!(
            std::env::var_os("CHANVOY_RUNTIME_DIR").is_none(),
            "qualification must select the actual native default R"
        );
        assert!(
            std::env::var_os("CHANVOY_STATE_DIR").is_none(),
            "managed override refusal is covered separately"
        );
        let root = SocketRoot::initialize().unwrap();
        let account = super::super::NativeAccount::resolve().unwrap();
        let parent = std::fs::canonicalize(native_parent().unwrap()).unwrap();
        let name = match NativePlatform::current().unwrap() {
            NativePlatform::MacOs => "chanvoy".to_string(),
            NativePlatform::Linux => format!("chanvoy-{}", account.uid()),
        };
        assert_eq!(root.path(), parent.join(name).join("r"));
        root.child_path(ProcessId::new(i32::MAX as u32).unwrap())
            .unwrap();
        root.verify().unwrap();
        println!("native default R selection/private-root/byte-budget PASS; no child socket/handoff claim");
    }
}
