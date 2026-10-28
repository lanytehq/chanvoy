//! Descriptor-relative filesystem primitives for native runtime authority.
//! The generic directory primitive does not elect an account coordination root.
use super::{InodeIdentity, RuntimeError};
use std::ffi::{CString, OsStr};
use std::fs::{File, Metadata};
use std::io::Write;
use std::os::unix::{
    ffi::OsStrExt,
    fs::MetadataExt,
    io::{AsRawFd, FromRawFd},
};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileFingerprint {
    pub identity: InodeIdentity,
    pub uid: u32,
    pub mode: u32,
    pub size: u64,
    pub modified_secs: i64,
    pub modified_nanos: i64,
}
impl FileFingerprint {
    pub fn of(meta: &Metadata) -> Self {
        Self {
            identity: InodeIdentity {
                device: meta.dev(),
                inode: meta.ino(),
            },
            uid: meta.uid(),
            mode: meta.mode(),
            size: meta.len(),
            modified_secs: meta.mtime(),
            modified_nanos: meta.mtime_nsec(),
        }
    }
}

pub(super) struct PrivateDirectory {
    file: File,
    path: PathBuf,
    uid: u32,
    identity: InodeIdentity,
    policy: DirectoryPolicy,
}
#[derive(Clone, Copy)]
enum DirectoryPolicy {
    Private,
    NativeHome,
    NativeSocketParent,
    SystemTemporary,
}
impl PrivateDirectory {
    /// Walk a previously canonicalized absolute path with non-following opens.
    /// Trusted ancestry may be system-owned; the final dedicated directory must
    /// be account-owned and exactly private. No permissions are repaired.
    pub fn open_canonical(path: &Path, uid: u32) -> Result<Self, RuntimeError> {
        Self::open_directory(path, uid, DirectoryPolicy::Private)
    }

    pub fn open_native_home(path: &Path, uid: u32) -> Result<Self, RuntimeError> {
        Self::open_directory(path, uid, DirectoryPolicy::NativeHome)
    }

    /// Darwin's native per-account selector is system-managed ancestry, not
    /// a dedicated private child. Stock 0755 is safe when account-owned and
    /// protected from other-account writes; never repair its permissions.
    pub fn open_native_socket_parent(path: &Path, uid: u32) -> Result<Self, RuntimeError> {
        Self::open_directory(path, uid, DirectoryPolicy::NativeSocketParent)
    }

    pub fn open_system_temporary_parent(path: &Path, uid: u32) -> Result<Self, RuntimeError> {
        Self::open_directory(path, uid, DirectoryPolicy::SystemTemporary)
    }

    fn open_directory(
        path: &Path,
        uid: u32,
        policy: DirectoryPolicy,
    ) -> Result<Self, RuntimeError> {
        if !path.is_absolute() || std::fs::canonicalize(path)?.as_os_str() != path.as_os_str() {
            return Err(RuntimeError::Invalid("canonical directory required"));
        }
        let mut dir = open_root()?;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            match component {
                Component::RootDir => continue,
                Component::Normal(name) => {
                    let next = open_at(&dir, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
                    let meta = next.metadata()?;
                    if components.peek().is_none() {
                        validate_selected_directory(&meta, uid, policy)?;
                    } else {
                        validate_ancestor(&meta, uid)?;
                    }
                    dir = next;
                }
                _ => return Err(RuntimeError::Invalid("noncanonical directory component")),
            }
        }
        let meta = dir.metadata()?;
        validate_selected_directory(&meta, uid, policy)?;
        Ok(Self {
            file: dir,
            path: path.to_path_buf(),
            uid,
            identity: InodeIdentity {
                device: meta.dev(),
                inode: meta.ino(),
            },
            policy,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn identity(&self) -> InodeIdentity {
        self.identity
    }

    pub fn verify_named(&self) -> Result<(), RuntimeError> {
        let current = Self::open_directory(&self.path, self.uid, self.policy)?;
        if current.identity != self.identity {
            return Err(RuntimeError::Changed("directory identity"));
        }
        Ok(())
    }

    pub fn child(&self, name: &OsStr, create: bool) -> Result<Option<Self>, RuntimeError> {
        validate_leaf(name)?;
        self.verify_named()?;
        let mut created = false;
        let file = match open_at(&self.file, name, libc::O_RDONLY | libc::O_DIRECTORY, 0) {
            Ok(file) => file,
            Err(RuntimeError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound && create =>
            {
                let name_c = CString::new(name.as_bytes())
                    .map_err(|_| RuntimeError::Invalid("directory name"))?;
                // SAFETY: parent is a live directory descriptor; name is one
                // NUL-terminated component. mkdirat cannot escape this parent.
                let result =
                    unsafe { libc::mkdirat(self.file.as_raw_fd(), name_c.as_ptr(), 0o700) };
                if result != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(error.into());
                    }
                } else {
                    created = true;
                }
                open_at(&self.file, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?
            }
            Err(RuntimeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                self.verify_named()?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let meta = file.metadata()?;
        validate_private_directory(&meta, self.uid)?;
        self.verify_named()?;
        let child = Self {
            file,
            path: self.path.join(name),
            uid: self.uid,
            identity: InodeIdentity {
                device: meta.dev(),
                inode: meta.ino(),
            },
            policy: DirectoryPolicy::Private,
        };
        child.verify_named()?;
        if created {
            child.sync()?;
            self.sync()?;
        }
        Ok(Some(child))
    }

    pub fn open_file(&self, name: &OsStr, flags: i32) -> Result<Option<File>, RuntimeError> {
        validate_leaf(name)?;
        self.verify_named()?;
        let file = match open_at(&self.file, name, flags, 0o600) {
            Ok(file) => file,
            Err(RuntimeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                self.verify_named()?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        validate_private_file(&file.metadata()?, self.uid)?;
        self.verify_named()?;
        self.verify_file_named(name, &FileFingerprint::of(&file.metadata()?))?;
        Ok(Some(file))
    }

    // libc stat field widths differ across the supported Unix ABIs.
    #[allow(clippy::unnecessary_cast)]
    pub fn verify_file_named(
        &self,
        name: &OsStr,
        expected: &FileFingerprint,
    ) -> Result<(), RuntimeError> {
        validate_leaf(name)?;
        let name =
            CString::new(name.as_bytes()).map_err(|_| RuntimeError::Invalid("entry name"))?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: live directory, single terminated component and writable stat.
        // A named stat does not open the inode again: lease holders must retain
        // their sole open description without a second pathname open.
        let result = unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: successful fstatat initialized the supplied stat structure.
        let stat = unsafe { stat.assume_init() };
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG
            || stat.st_uid != self.uid
            || stat.st_mode as u32 & 0o7777 != 0o600
            || stat.st_nlink != 1
        {
            return Err(RuntimeError::Invalid(
                "named file is not private regular authority",
            ));
        }
        // Descriptor fingerprints additionally fence modification times before
        // and after reads/writes; this check binds that descriptor to its name.
        if stat.st_dev as u64 != expected.identity.device
            || stat.st_ino as u64 != expected.identity.inode
            || stat.st_uid != expected.uid
            || stat.st_mode as u32 != expected.mode
            || stat.st_size < 0
            || stat.st_size as u64 != expected.size
        {
            return Err(RuntimeError::Changed("named file identity"));
        }
        self.verify_named()
    }

    pub fn raw_fd(&self) -> i32 {
        self.file.as_raw_fd()
    }
    pub fn sync(&self) -> Result<(), RuntimeError> {
        self.file.sync_all().map_err(Into::into)
    }

    /// Synchronous publication: the caller retains mutation ownership through
    /// all writes, replacement and directory sync. Once a namespace mutation
    /// begins, any failure is uncertainty, never a claim of rollback.
    /// Remove only an exact captured private regular artifact. The owning
    /// lifecycle operation/publisher must exclude cooperating writers.
    pub fn remove_file(
        &self,
        name: &OsStr,
        expected: &FileFingerprint,
    ) -> Result<(), RuntimeError> {
        self.verify_named()?;
        self.verify_file_named(name, expected)?;
        let name =
            CString::new(name.as_bytes()).map_err(|_| RuntimeError::Invalid("entry name"))?;
        // SAFETY: validated leaf and retained directory; no recursive removal.
        if unsafe { libc::unlinkat(self.raw_fd(), name.as_ptr(), 0) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        self.sync()
            .map_err(|_| RuntimeError::PublicationUncertain)?;
        self.verify_named()
    }

    pub fn publish(
        &self,
        name: &OsStr,
        expected: Option<&FileFingerprint>,
        bytes: &[u8],
    ) -> Result<FileFingerprint, RuntimeError> {
        validate_leaf(name)?;
        self.verify_named()?;
        match expected {
            Some(expected) => self.verify_file_named(name, expected)?,
            None if self.open_file(name, libc::O_RDONLY)?.is_some() => {
                return Err(RuntimeError::Changed("record already exists"))
            }
            None => (),
        }
        let temporary = format!(".{}.tmp", uuid::Uuid::new_v4());
        let temporary = OsStr::new(&temporary);
        let mut file = self
            .open_file(temporary, libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL)?
            .ok_or(RuntimeError::Unavailable("record staging inode"))?;
        // Staging failures cannot publish a record. Preserve the staging inode
        // for explicit reconciliation rather than blindly unlinking by name.
        file.write_all(bytes)?;
        file.sync_all()?;
        let staged = FileFingerprint::of(&file.metadata()?);
        self.verify_file_named(temporary, &staged)?;
        self.verify_named()?;
        let source = CString::new(temporary.as_bytes()).unwrap();
        let target = CString::new(name.as_bytes()).unwrap();
        match expected {
            Some(expected) => {
                self.verify_file_named(name, expected)?;
                // SAFETY: both terminated single-component names are relative
                // to one retained validated directory descriptor.
                let result = unsafe {
                    libc::renameat(
                        self.raw_fd(),
                        source.as_ptr(),
                        self.raw_fd(),
                        target.as_ptr(),
                    )
                };
                if result != 0 {
                    return Err(RuntimeError::PublicationUncertain);
                }
            }
            None => {
                // linkat provides portable no-replace publication. During the
                // brief two-link interval readers refuse the inode. A crash or
                // cleanup failure cannot be mistaken for a valid live record.
                // SAFETY: validated names and retained directory descriptor.
                let result = unsafe {
                    libc::linkat(
                        self.raw_fd(),
                        source.as_ptr(),
                        self.raw_fd(),
                        target.as_ptr(),
                        0,
                    )
                };
                if result != 0 {
                    let error = std::io::Error::last_os_error();
                    return if error.kind() == std::io::ErrorKind::AlreadyExists {
                        Err(RuntimeError::Changed("record already exists"))
                    } else {
                        Err(RuntimeError::PublicationUncertain)
                    };
                }
                // SAFETY: source still denotes our retained staging inode in
                // this directory; no asynchronous mutator runs behind us.
                if unsafe { libc::unlinkat(self.raw_fd(), source.as_ptr(), 0) } != 0 {
                    return Err(RuntimeError::PublicationUncertain);
                }
            }
        }
        self.sync()
            .map_err(|_| RuntimeError::PublicationUncertain)?;
        self.verify_file_named(name, &staged)
            .map_err(|_| RuntimeError::PublicationUncertain)?;
        Ok(staged)
    }
}

fn validate_ancestor(meta: &Metadata, uid: u32) -> Result<(), RuntimeError> {
    if !meta.is_dir() || (meta.uid() != 0 && meta.uid() != uid) {
        return Err(RuntimeError::Invalid("untrusted directory ancestor"));
    }
    // Sticky system-owned ancestors permit private children without letting
    // another account replace them. Root selection separately rejects reapable
    // authority paths; socket-only roots have a different persistence contract.
    if meta.mode() & 0o022 != 0 && !(meta.uid() == 0 && meta.mode() & 0o1000 != 0) {
        return Err(RuntimeError::Invalid("writable directory ancestor"));
    }
    Ok(())
}
fn validate_private_directory(meta: &Metadata, uid: u32) -> Result<(), RuntimeError> {
    if !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o7777 != 0o700 {
        return Err(RuntimeError::Invalid(
            "directory is not account-owned private authority",
        ));
    }
    Ok(())
}
fn validate_selected_directory(
    meta: &Metadata,
    uid: u32,
    policy: DirectoryPolicy,
) -> Result<(), RuntimeError> {
    match policy {
        DirectoryPolicy::Private => return validate_private_directory(meta, uid),
        DirectoryPolicy::NativeSocketParent => {
            return validate_native_socket_parent(meta.is_dir(), meta.uid(), meta.mode(), uid);
        }
        DirectoryPolicy::SystemTemporary => {
            if !meta.is_dir() || meta.uid() != 0 || meta.mode() & 0o7777 != 0o1777 {
                return Err(RuntimeError::Invalid("native system temporary parent"));
            }
            return Ok(());
        }
        DirectoryPolicy::NativeHome => (),
    }
    if !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o022 != 0 {
        return Err(RuntimeError::Invalid(
            "native home is not account-owned protected ancestry",
        ));
    }
    Ok(())
}
fn validate_native_socket_parent(
    is_directory: bool,
    owner: u32,
    mode: u32,
    uid: u32,
) -> Result<(), RuntimeError> {
    if !is_directory || owner != uid || mode & 0o6022 != 0 {
        return Err(RuntimeError::Invalid(
            "native socket parent is not account-owned protected ancestry",
        ));
    }
    Ok(())
}
fn validate_private_file(meta: &Metadata, uid: u32) -> Result<(), RuntimeError> {
    if !meta.is_file() || meta.uid() != uid || meta.mode() & 0o7777 != 0o600 || meta.nlink() != 1 {
        return Err(RuntimeError::Invalid(
            "file is not private single-link regular authority",
        ));
    }
    Ok(())
}
fn validate_leaf(name: &OsStr) -> Result<(), RuntimeError> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&b'/')
        || bytes.contains(&0)
    {
        return Err(RuntimeError::Invalid(
            "one relative directory entry required",
        ));
    }
    Ok(())
}
fn open_root() -> Result<File, RuntimeError> {
    let name = c"/";
    // SAFETY: constant valid pathname; ownership transferred once on success.
    let fd = unsafe {
        libc::open(
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    owned_file(fd)
}
fn open_at(parent: &File, name: &OsStr, flags: i32, mode: u32) -> Result<File, RuntimeError> {
    validate_leaf(name)?;
    let name = CString::new(name.as_bytes()).map_err(|_| RuntimeError::Invalid("entry name"))?;
    // SAFETY: parent is live, name is one terminated component, mode is supplied
    // for the variadic create case. Non-following/nonblocking opens precede stat.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
            mode as libc::c_uint,
        )
    };
    owned_file(fd)
}
fn owned_file(fd: i32) -> Result<File, RuntimeError> {
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful open returned a newly owned descriptor, adopted once.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, DirBuilderExt, PermissionsExt};

    #[test]
    fn native_socket_parent_policy_refuses_foreign_non_directory_and_setid_facts() {
        let uid = 123;
        for mode in [0o700, 0o750, 0o755, 0o500] {
            validate_native_socket_parent(true, uid, mode, uid).unwrap();
        }
        // Some filesystems clear setid bits during chmod. Validate their
        // rejection directly without relying on a fixture retaining them.
        for mode in [0o775, 0o757, 0o777, 0o1777, 0o2755, 0o4755, 0o6755] {
            assert!(validate_native_socket_parent(true, uid, mode, uid).is_err());
        }
        assert!(validate_native_socket_parent(true, 0, 0o755, uid).is_err());
        assert!(validate_native_socket_parent(true, uid + 1, 0o755, uid).is_err());
        assert!(validate_native_socket_parent(false, uid, 0o755, uid).is_err());
    }

    // Generic I/O fixtures are persistent-base tests of the same directory
    // validator, not an alternate native C selector or native lifecycle proof.
    pub(in crate::runtime) fn fixture() -> tempfile::TempDir {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".runtime-io-fixtures");
        match std::fs::DirBuilder::new().mode(0o700).create(&base) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => panic!("persistent fixture base: {e}"),
        }
        let base = std::fs::canonicalize(base).unwrap();
        // SAFETY: pure effective UID getter.
        PrivateDirectory::open_canonical(&base, unsafe { libc::geteuid() }).unwrap();
        tempfile::Builder::new()
            .prefix("owned-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in(base)
            .unwrap()
    }
    #[test]
    fn relative_io_refuses_redirects_loose_modes_hardlinks_and_nonregulars() {
        let fixture = fixture();
        let root =
            PrivateDirectory::open_canonical(fixture.path(), unsafe { libc::geteuid() }).unwrap();
        let child = root.child(OsStr::new("records"), true).unwrap().unwrap();
        let file = child
            .open_file(
                OsStr::new("record"),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            file.metadata().unwrap().permissions().mode() & 0o7777,
            0o600
        );
        symlink("record", child.path().join("redirect")).unwrap();
        assert!(child
            .open_file(OsStr::new("redirect"), libc::O_RDONLY)
            .is_err());
        std::fs::hard_link(child.path().join("record"), child.path().join("hardlink")).unwrap();
        assert!(child
            .open_file(OsStr::new("record"), libc::O_RDONLY)
            .is_err());
        std::fs::remove_file(child.path().join("hardlink")).unwrap();
        std::fs::set_permissions(
            child.path().join("record"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(child
            .open_file(OsStr::new("record"), libc::O_RDONLY)
            .is_err());
        assert_eq!(
            std::fs::metadata(child.path().join("record"))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o644
        );
        assert!(root.child(OsStr::new("../escape"), true).is_err());
        assert!(root
            .open_file(OsStr::new("/escape"), libc::O_RDONLY)
            .is_err());
    }
    #[test]
    fn named_descriptor_checks_reject_replaced_directory_and_file() {
        let fixture = fixture();
        let root =
            PrivateDirectory::open_canonical(fixture.path(), unsafe { libc::geteuid() }).unwrap();
        // Path equality normalizes redundant separators/dots. N hashes exact
        // OS bytes, so managed directory validation must refuse such aliases.
        assert!(PrivateDirectory::open_canonical(&fixture.path().join("."), root.uid).is_err());
        use std::os::unix::ffi::OsStringExt;
        let mut alias = fixture.path().as_os_str().as_bytes().to_vec();
        alias.extend_from_slice(b"//");
        assert!(PrivateDirectory::open_canonical(
            &PathBuf::from(std::ffi::OsString::from_vec(alias)),
            root.uid
        )
        .is_err());
        let file = root
            .open_file(
                OsStr::new("record"),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            )
            .unwrap()
            .unwrap();
        let captured = FileFingerprint::of(&file.metadata().unwrap());
        root.verify_file_named(OsStr::new("record"), &captured)
            .unwrap();
        std::fs::rename(root.path().join("record"), root.path().join("old")).unwrap();
        root.open_file(
            OsStr::new("record"),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
        )
        .unwrap();
        assert!(root
            .verify_file_named(OsStr::new("record"), &captured)
            .is_err());
        let child = root.child(OsStr::new("child"), true).unwrap().unwrap();
        std::fs::rename(root.path().join("child"), root.path().join("old-child")).unwrap();
        root.child(OsStr::new("child"), true).unwrap();
        assert!(child.verify_named().is_err());
    }
    #[test]
    fn final_directory_foreign_owner_or_broad_permissions_are_not_repaired() {
        let fixture = fixture();
        let uid = unsafe { libc::geteuid() };
        assert!(PrivateDirectory::open_canonical(fixture.path(), uid.wrapping_add(1)).is_err());
        assert!(PrivateDirectory::open_native_home(fixture.path(), uid.wrapping_add(1)).is_err());
        std::fs::set_permissions(fixture.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(PrivateDirectory::open_canonical(fixture.path(), uid).is_err());
        assert_eq!(
            std::fs::metadata(fixture.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o755
        );
        PrivateDirectory::open_native_home(fixture.path(), uid).unwrap();
        std::fs::set_permissions(fixture.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(PrivateDirectory::open_native_home(fixture.path(), uid).is_err());
        assert_eq!(
            std::fs::metadata(fixture.path()).unwrap().mode() & 0o7777,
            0o777
        );
    }

    #[test]
    fn publication_is_no_replace_then_exact_inode_fenced_and_durable() {
        let fixture = fixture();
        // SAFETY: pure effective UID getter.
        let root =
            PrivateDirectory::open_canonical(fixture.path(), unsafe { libc::geteuid() }).unwrap();
        let name = OsStr::new("record.json");
        let first = root.publish(name, None, b"revision0").unwrap();
        assert!(root.publish(name, None, b"replacement").is_err());
        let file = root.open_file(name, libc::O_RDONLY).unwrap().unwrap();
        assert_eq!(std::fs::read(root.path().join(name)).unwrap(), b"revision0");
        assert_eq!(file.metadata().unwrap().nlink(), 1);
        let second = root.publish(name, Some(&first), b"revision1").unwrap();
        assert_ne!(second.identity, first.identity);
        assert!(root.publish(name, Some(&first), b"stale revision").is_err());
        assert_eq!(std::fs::read(root.path().join(name)).unwrap(), b"revision1");
        root.verify_file_named(name, &second).unwrap();
        std::fs::rename(root.path().join(name), root.path().join("old")).unwrap();
        root.publish(name, None, b"new epoch").unwrap();
        assert!(root.publish(name, Some(&second), b"stale epoch").is_err());
        assert_eq!(std::fs::read(root.path().join(name)).unwrap(), b"new epoch");
    }
    #[test]
    fn captured_removal_refuses_replaced_or_broadened_artifacts() {
        let fixture = fixture();
        let dir =
            PrivateDirectory::open_canonical(fixture.path(), unsafe { libc::geteuid() }).unwrap();
        let name = OsStr::new("handoff");
        let captured = dir.publish(name, None, b"original").unwrap();
        let replacement = dir.publish(name, Some(&captured), b"replacement").unwrap();
        assert!(dir.remove_file(name, &captured).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(name)).unwrap(),
            b"replacement"
        );
        std::fs::set_permissions(
            dir.path().join(name),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(dir.remove_file(name, &replacement).is_err());
        assert!(dir.path().join(name).exists());
        std::fs::set_permissions(
            dir.path().join(name),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        dir.remove_file(name, &replacement).unwrap();
        assert!(!dir.path().join(name).exists());
    }
}
