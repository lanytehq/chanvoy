//! Kernel Unix-stream table observation, separate from process/lease evidence.
//! The Darwin decoder is a qualification candidate, not activation authority.
//! ABI/source pin and native inherited/queued-reference fixtures are required
//! before a lifecycle caller may use a negative observation to reap artifacts.
use super::{PathBytes, RuntimeError};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerTableObservation {
    Present,
    NotObserved,
}
const MAX_TABLE_BYTES: usize = 8 * 1024 * 1024;
const GENERATION_BYTES: usize = 24;
// Darwin xunpcb64 uses pack(4); xsocket64 is108 bytes. These layout values
// reject all other record versions instead of guessing an ABI. Native SDK
// layout proof and supported kernel/source qualification remain separate.
const PCB64_BYTES: usize = 724;
const SOCKET_OFFSET: usize = 616;
const SOCKET_BYTES: usize = 108;
const ADDRESS_OFFSET: usize = 104;

#[cfg(target_os = "macos")]
pub fn observe_listener_table(path: &PathBytes) -> Result<ListenerTableObservation, RuntimeError> {
    use std::os::unix::ffi::OsStrExt;
    let name = c"net.local.stream.pcblist64";
    let mut size = 0usize;
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(RuntimeError::Unavailable(
            "kernel Unix socket table size denied/unavailable",
        ));
    }
    if !(GENERATION_BYTES..=MAX_TABLE_BYTES).contains(&size) {
        return Err(RuntimeError::Unavailable(
            "kernel Unix socket table size budget",
        ));
    }
    let mut bytes = vec![0u8; size];
    let capacity = size;
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            bytes.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(RuntimeError::Unavailable(
            "kernel Unix socket table read denied/truncated",
        ));
    }
    if size > capacity {
        return Err(RuntimeError::Unavailable(
            "kernel Unix socket table overflow",
        ));
    }
    bytes.truncate(size);
    decode_darwin64(&bytes, path.to_path().as_os_str().as_bytes())
}
#[cfg(not(target_os = "macos"))]
pub fn observe_listener_table(_: &PathBytes) -> Result<ListenerTableObservation, RuntimeError> {
    Err(RuntimeError::Unavailable(
        "kernel listener table platform unqualified",
    ))
}
fn u32_at(bytes: &[u8], offset: usize) -> Result<u32, RuntimeError> {
    let field = bytes
        .get(offset..offset + 4)
        .ok_or(RuntimeError::Invalid("socket table short integer"))?;
    Ok(u32::from_le_bytes(field.try_into().unwrap()))
}
fn u64_at(bytes: &[u8], offset: usize) -> Result<u64, RuntimeError> {
    let field = bytes
        .get(offset..offset + 8)
        .ok_or(RuntimeError::Invalid("socket table short integer"))?;
    Ok(u64::from_le_bytes(field.try_into().unwrap()))
}
fn u16_at(bytes: &[u8], offset: usize) -> Result<u16, RuntimeError> {
    let field = bytes
        .get(offset..offset + 2)
        .ok_or(RuntimeError::Invalid("socket table short integer"))?;
    Ok(u16::from_le_bytes(field.try_into().unwrap()))
}
#[cfg_attr(not(any(target_os = "macos", test)), allow(dead_code))]
fn decode_darwin64(bytes: &[u8], target: &[u8]) -> Result<ListenerTableObservation, RuntimeError> {
    if bytes.len() > MAX_TABLE_BYTES || bytes.len() < GENERATION_BYTES * 2 {
        // XNU's global-zero branch may omit the trailer. It deliberately does
        // not become a negative observation in this candidate decoder.
        return Err(RuntimeError::Unavailable(
            "socket table incomplete envelope",
        ));
    }
    if u32_at(bytes, 0)? as usize != GENERATION_BYTES {
        return Err(RuntimeError::Unavailable("socket table header ABI"));
    }
    let generation = u64_at(bytes, 8)?;
    let socket_generation = u64_at(bytes, 16)?;
    let count = u32_at(bytes, 4)?;
    let mut at = GENERATION_BYTES;
    let mut rows = 0u32;
    let mut found = false;
    while at < bytes.len() {
        let length = u32_at(bytes, at)? as usize;
        if length == GENERATION_BYTES {
            let trailer = &bytes[at..];
            if trailer.len() != GENERATION_BYTES
                || u64_at(trailer, 8)? != generation
                || u64_at(trailer, 16)? != socket_generation
                || u32_at(trailer, 4)? != count
            {
                return Err(RuntimeError::Changed("socket table generation/envelope"));
            }
            // Count is global (stream+datagram), not the stream row count.
            if rows > count {
                return Err(RuntimeError::Invalid("socket table row count"));
            }
            return Ok(if found {
                ListenerTableObservation::Present
            } else {
                ListenerTableObservation::NotObserved
            });
        }
        if length != PCB64_BYTES {
            return Err(RuntimeError::Unavailable("socket table record ABI"));
        }
        let row = bytes
            .get(at..at + length)
            .ok_or(RuntimeError::Unavailable("socket table truncated row"))?;
        if u64_at(row, 92)? > generation {
            return Err(RuntimeError::Changed("socket table row generation"));
        }
        if u32_at(row, SOCKET_OFFSET)? as usize != SOCKET_BYTES
            || u16_at(row, SOCKET_OFFSET + 12)? != 1
            || u32_at(row, SOCKET_OFFSET + 32)? != 1
        {
            return Err(RuntimeError::Unavailable("socket table socket ABI/type"));
        }
        let address = &row[ADDRESS_OFFSET..ADDRESS_OFFSET + 256];
        let address_length = usize::from(address[0]);
        let listener = u16_at(row, SOCKET_OFFSET + 14)? & 2 != 0; // SO_ACCEPTCONN
        if address_length != 0 {
            if !(2..=106).contains(&address_length) || address[1] != 1 {
                return Err(RuntimeError::Invalid("socket table Unix address"));
            }
            let pathname = &address[2..address_length];
            // Darwin bind copies the length-delimited bytes into a terminated
            // namei buffer but preserves the original sockaddr. The effective
            // filesystem name stops at the first NUL, even with a nonzero tail.
            // No-terminator addresses use the full length-delimited name.
            let end = pathname
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(pathname.len());
            let pathname = &pathname[..end];
            if listener && pathname.is_empty() {
                return Err(RuntimeError::Unavailable(
                    "socket table listener pathname unavailable",
                ));
            }
            found |= listener && pathname == target;
        } else if listener {
            return Err(RuntimeError::Unavailable(
                "socket table listener address unavailable",
            ));
        }
        rows = rows
            .checked_add(1)
            .ok_or(RuntimeError::Invalid("socket table row overflow"))?;
        at += length;
    }
    Err(RuntimeError::Unavailable("socket table trailer missing"))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn envelope(count: u32) -> Vec<u8> {
        let mut b = vec![0; GENERATION_BYTES];
        b[..4].copy_from_slice(&(GENERATION_BYTES as u32).to_le_bytes());
        b[4..8].copy_from_slice(&count.to_le_bytes());
        b[8..16].copy_from_slice(&7u64.to_le_bytes());
        b[16..24].copy_from_slice(&8u64.to_le_bytes());
        b
    }
    fn row(path: &[u8], listener: bool) -> Vec<u8> {
        let mut b = vec![0; PCB64_BYTES];
        b[..4].copy_from_slice(&(PCB64_BYTES as u32).to_le_bytes());
        b[92..100].copy_from_slice(&6u64.to_le_bytes());
        b[SOCKET_OFFSET..SOCKET_OFFSET + 4].copy_from_slice(&(SOCKET_BYTES as u32).to_le_bytes());
        b[SOCKET_OFFSET + 12] = 1;
        b[SOCKET_OFFSET + 14] = if listener { 2 } else { 0 };
        b[SOCKET_OFFSET + 32] = 1;
        b[ADDRESS_OFFSET] = (path.len() + 3) as u8;
        b[ADDRESS_OFFSET + 1] = 1;
        b[ADDRESS_OFFSET + 2..ADDRESS_OFFSET + 2 + path.len()].copy_from_slice(path);
        b
    }
    #[test]
    fn exact_path_listener_is_distinct_from_nonlistener_and_global_count() {
        let mut b = envelope(99);
        b.extend(row(b"/test/\xff", true));
        b.extend(envelope(99));
        assert_eq!(
            decode_darwin64(&b, b"/test/\xff").unwrap(),
            ListenerTableObservation::Present
        );
        assert_eq!(
            decode_darwin64(&b, b"/different").unwrap(),
            ListenerTableObservation::NotObserved
        );
        b[GENERATION_BYTES + SOCKET_OFFSET + 14] = 0;
        assert_eq!(
            decode_darwin64(&b, b"/test/\xff").unwrap(),
            ListenerTableObservation::NotObserved
        );
    }
    #[test]
    fn incomplete_changing_or_unknown_abi_tables_never_become_absence() {
        assert!(decode_darwin64(&envelope(0), b"/target").is_err());
        let mut complete = envelope(1);
        complete.extend(row(b"/target", true));
        complete.extend(envelope(1));
        for end in [0, 23, 24, 25, complete.len() - 1] {
            assert!(decode_darwin64(&complete[..end], b"/target").is_err());
        }
        let mut changed = complete.clone();
        let last = changed.len();
        changed[last - 8] = 9;
        assert!(decode_darwin64(&changed, b"/target").is_err());
        changed = complete;
        changed[GENERATION_BYTES] = 0;
        assert!(decode_darwin64(&changed, b"/target").is_err());
    }
    #[test]
    fn zero_padded_pathnames_keep_exact_listener_matches() {
        let table = |path: &[u8]| {
            let mut b = envelope(1);
            b.extend(row(path, true));
            b.extend(envelope(1));
            b
        };
        assert_eq!(
            decode_darwin64(&table(b"/target\0\0\0"), b"/target").unwrap(),
            ListenerTableObservation::Present
        );
        assert_eq!(
            decode_darwin64(&table(b"/target-suffix\0\0"), b"/target").unwrap(),
            ListenerTableObservation::NotObserved
        );
        assert_eq!(
            decode_darwin64(&table(b"/prefix\0/target"), b"/prefix").unwrap(),
            ListenerTableObservation::Present
        );
        assert_eq!(
            decode_darwin64(&table(b"/prefix\0/target"), b"/target").unwrap(),
            ListenerTableObservation::NotObserved
        );
        assert!(decode_darwin64(&table(b"\0\0"), b"/target").is_err());
        let mut no_terminator = table(b"/target");
        no_terminator[GENERATION_BYTES + ADDRESS_OFFSET] -= 1;
        assert_eq!(
            decode_darwin64(&no_terminator, b"/target").unwrap(),
            ListenerTableObservation::Present
        );
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn native_bind_preserves_nonzero_tail_but_serves_only_the_nul_prefix() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir_in("/tmp").unwrap();
        let path = dir.path().join("listener");
        let name = path.as_os_str().as_bytes();
        let payload = [name, b"\0ignored-tail"].concat();
        // SAFETY: the native struct is plain integer fields and a byte array.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        assert!(payload.len() <= address.sun_path.len());
        address.sun_family = libc::AF_UNIX as _;
        address.sun_len = (2 + payload.len()) as _;
        for (field, byte) in address.sun_path.iter_mut().zip(&payload) {
            *field = *byte as _;
        }
        // SAFETY: socket has no pointer arguments; fd ownership starts once.
        let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(raw >= 0);
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: fully initialized sockaddr and its exact bounded byte length.
        assert_eq!(
            unsafe {
                libc::bind(
                    descriptor.as_raw_fd(),
                    std::ptr::from_ref(&address).cast(),
                    address.sun_len as _,
                )
            },
            0
        );
        assert_eq!(unsafe { libc::listen(descriptor.as_raw_fd(), 1) }, 0);
        let mut stored: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let mut length = std::mem::size_of_val(&stored) as libc::socklen_t;
        // SAFETY: writable full-size native buffer and valid length pointer.
        assert_eq!(
            unsafe {
                libc::getsockname(
                    descriptor.as_raw_fd(),
                    std::ptr::from_mut(&mut stored).cast(),
                    &mut length,
                )
            },
            0
        );
        assert_eq!(length as usize, 2 + payload.len());
        let returned: Vec<u8> = stored.sun_path[..payload.len()]
            .iter()
            .map(|byte| *byte as u8)
            .collect();
        assert_eq!(returned, payload);
        let listener = std::os::unix::net::UnixListener::from(descriptor);
        let _client = std::os::unix::net::UnixStream::connect(&path).unwrap();
        let (_accepted, _) = listener.accept().unwrap();
        let mut actual_row = row(b"/unused", true);
        actual_row[ADDRESS_OFFSET] = stored.sun_len;
        actual_row[ADDRESS_OFFSET + 1] = stored.sun_family;
        actual_row[ADDRESS_OFFSET + 2..ADDRESS_OFFSET + 2 + returned.len()]
            .copy_from_slice(&returned);
        let mut table = envelope(1);
        table.extend(actual_row);
        table.extend(envelope(1));
        assert_eq!(
            decode_darwin64(&table, name).unwrap(),
            ListenerTableObservation::Present
        );
        assert_eq!(
            decode_darwin64(&table, b"ignored-tail").unwrap(),
            ListenerTableObservation::NotObserved
        );
    }
}
