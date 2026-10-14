use super::RuntimeError;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};

/// Lossless OS pathname bytes in the bounded record, encoded as lowercase hex.
/// Construction validates a path claim; it does not grant filesystem authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct PathBytes(String);

impl PathBytes {
    pub fn from_path(path: &Path) -> Result<Self, RuntimeError> {
        let bytes = path.as_os_str().as_bytes();
        if bytes.is_empty() || bytes.len() > 4096 || bytes.contains(&0) || !path.is_absolute() {
            return Err(RuntimeError::Invalid("absolute bounded OS path required"));
        }
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(RuntimeError::Invalid("parent traversal in runtime path"));
        }
        Ok(Self(hex_encode(bytes)))
    }

    pub fn to_path(&self) -> PathBuf {
        // The sole constructors/decoder validate the encoded representation.
        PathBuf::from(OsString::from_vec(
            hex_decode(&self.0).expect("validated path hex"),
        ))
    }

    pub fn hex(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PathBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(deserializer)?;
        let bytes =
            hex_decode(&hex).ok_or_else(|| D::Error::custom("invalid lowercase path hex"))?;
        let value =
            Self::from_path(&PathBuf::from(OsString::from_vec(bytes))).map_err(D::Error::custom)?;
        if value.0 != hex {
            return Err(D::Error::custom("noncanonical path encoding"));
        }
        Ok(value)
    }
}

/// Namespace encoding is length-delimited; no profile text becomes a filename.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct Namespace(String);

impl Namespace {
    /// Pure encoding. Production callers supply the physically canonicalized
    /// config root and loaded logical profile; C is intentionally absent.
    pub fn derive(uid: u32, config_root: &PathBytes, profile: &str) -> Result<Self, RuntimeError> {
        if profile.is_empty() || profile.as_bytes().contains(&0) {
            return Err(RuntimeError::Invalid("loaded profile name required"));
        }
        let config = config_root.to_path();
        let mut hash = Sha256::new();
        hash.update(b"chanvoy-runtime-namespace-v1\0");
        for part in [
            uid.to_be_bytes().as_slice(),
            config.as_os_str().as_bytes(),
            profile.as_bytes(),
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        Ok(Self(hex_encode(&hash.finalize())))
    }

    pub fn hex(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.len() != 64 || !text.bytes().all(is_lower_hex) {
            return Err(D::Error::custom("namespace must be 64 lowercase hex bytes"));
        }
        Ok(Self(text))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAccount {
    uid: u32,
    home: PathBuf,
}

impl NativeAccount {
    /// Reads the native account database for the effective UID. No environment
    /// variable, cache/temp selector or invocation-specific home elects C.
    pub fn resolve() -> Result<Self, RuntimeError> {
        // SAFETY: geteuid is a pure getter with no pointer arguments.
        let uid = unsafe { libc::geteuid() };
        let mut size = 16 * 1024;
        loop {
            let mut bytes = vec![0u8; size];
            let mut entry = std::mem::MaybeUninit::<libc::passwd>::uninit();
            let mut result = std::ptr::null_mut();
            // SAFETY: buffers are writable for the advertised sizes; entry and
            // result live across the call. Nothing is read before success.
            let error = unsafe {
                libc::getpwuid_r(
                    uid,
                    entry.as_mut_ptr(),
                    bytes.as_mut_ptr().cast(),
                    size,
                    &mut result,
                )
            };
            if error == libc::ERANGE && size < 1024 * 1024 {
                size *= 2;
                continue;
            }
            validate_lookup(error, result, entry.as_mut_ptr())?;
            // SAFETY: successful getpwuid_r initialized this supplied entry.
            let entry = unsafe { entry.assume_init() };
            let home = checked_native_home(uid, entry.pw_uid, entry.pw_dir as usize, &bytes)?;
            // Native home is canonicalized by the authority selector before
            // ancestry classification. Do not classify its unresolved spelling.
            return Ok(Self { uid, home });
        }
    }

    pub fn uid(&self) -> u32 {
        self.uid
    }
    pub fn home(&self) -> &Path {
        &self.home
    }
}

fn validate_lookup(
    error: i32,
    result: *mut libc::passwd,
    supplied: *mut libc::passwd,
) -> Result<(), RuntimeError> {
    if error != 0 || result.is_null() || result != supplied {
        return Err(RuntimeError::Unavailable("native account lookup"));
    }
    Ok(())
}
fn checked_native_home(
    uid: u32,
    returned_uid: u32,
    pointer: usize,
    bytes: &[u8],
) -> Result<PathBuf, RuntimeError> {
    if returned_uid != uid {
        return Err(RuntimeError::Invalid("native account UID mismatch"));
    }
    // Never dereference the account-database pointer. It must index this exact
    // scratch buffer and have a terminator inside the same allocation.
    let offset = pointer
        .checked_sub(bytes.as_ptr() as usize)
        .filter(|n| *n < bytes.len())
        .ok_or(RuntimeError::Unavailable("native home pointer"))?;
    let tail = &bytes[offset..];
    let end = tail
        .iter()
        .position(|b| *b == 0)
        .ok_or(RuntimeError::Unavailable("native home terminator"))?;
    let home = PathBuf::from(OsString::from_vec(tail[..end].to_vec()));
    if !home.is_absolute() || home.as_os_str().as_bytes().len() > 4096 {
        return Err(RuntimeError::Invalid(
            "absolute bounded native home required",
        ));
    }
    Ok(home)
}

fn is_lower_hex(b: u8) -> bool {
    b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        text.push(DIGITS[(b >> 4) as usize] as char);
        text.push(DIGITS[(b & 15) as usize] as char);
    }
    text
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if text.len() > 8192 || !text.len().is_multiple_of(2) || !text.bytes().all(is_lower_hex) {
        return None;
    }
    Some(
        text.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let nibble = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
                (nibble(pair[0]) << 4) | nibble(pair[1])
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_bytes_are_lossless_and_refuse_unsafe_or_noncanonical_encoding() {
        let p = PathBuf::from(OsString::from_vec(b"/owned/\xff/socket".to_vec()));
        let encoded = PathBytes::from_path(&p).unwrap();
        assert_eq!(
            serde_json::from_str::<PathBytes>(&serde_json::to_string(&encoded).unwrap())
                .unwrap()
                .to_path(),
            p
        );
        for value in ["2F6f", "2f00", "2f2e2e2f78", "2", "78", ""] {
            assert!(
                serde_json::from_value::<PathBytes>(serde_json::json!(value)).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn namespace_is_lossless_length_delimited_and_account_scoped() {
        let a = PathBytes::from_path(Path::new("/owned/a")).unwrap();
        let ab = PathBytes::from_path(Path::new("/owned/ab")).unwrap();
        let n = Namespace::derive(501, &a, "bc").unwrap();
        assert_ne!(n, Namespace::derive(501, &ab, "c").unwrap());
        assert_ne!(n, Namespace::derive(502, &a, "bc").unwrap());
        assert_eq!(n, Namespace::derive(501, &a, "bc").unwrap());
        assert!(Namespace::derive(501, &a, "").is_err());
        assert!(Namespace::derive(501, &a, "nul\0name").is_err());
        assert!(serde_json::from_value::<Namespace>(serde_json::json!("A".repeat(64))).is_err());
        assert_ne!(
            Namespace::derive(501, &a, "é").unwrap(),
            Namespace::derive(501, &a, "e\u{301}").unwrap()
        );
    }

    #[test]
    fn native_lookup_refuses_failure_wrong_uid_empty_unterminated_or_foreign_home_buffer() {
        let mut entry = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let supplied = entry.as_mut_ptr();
        assert!(validate_lookup(libc::EIO, supplied, supplied).is_err());
        assert!(validate_lookup(0, std::ptr::null_mut(), supplied).is_err());
        let mut different = std::mem::MaybeUninit::<libc::passwd>::uninit();
        assert!(validate_lookup(0, different.as_mut_ptr(), supplied).is_err());
        validate_lookup(0, supplied, supplied).unwrap();
        for (bytes, returned_uid, in_buffer) in [
            (b"/owned\0".as_slice(), 502, true),
            (b"\0".as_slice(), 501, true),
            (b"relative\0".as_slice(), 501, true),
            (b"/unterminated".as_slice(), 501, true),
            (b"/owned\0".as_slice(), 501, false),
        ] {
            let pointer = if in_buffer {
                bytes.as_ptr() as usize
            } else {
                0
            };
            assert!(checked_native_home(501, returned_uid, pointer, bytes).is_err());
        }
        let bytes = b"/native/\xff\0";
        assert_eq!(
            checked_native_home(501, 501, bytes.as_ptr() as usize, bytes)
                .unwrap()
                .as_os_str()
                .as_bytes(),
            b"/native/\xff"
        );
    }
}
