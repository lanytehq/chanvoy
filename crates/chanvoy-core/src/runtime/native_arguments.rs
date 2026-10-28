//! Lossless, bounded argument observations. Never log the returned argument
//! bytes: they can contain caller data. Environment entries are not returned.
use super::{ProcessId, RuntimeError, RuntimeProcess};
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;

impl RuntimeProcess {
    pub fn observe_arguments(&self) -> Result<Vec<OsString>, RuntimeError> {
        self.revalidate()?;
        let first = arguments(self.pid)?;
        let second = arguments(self.pid)?;
        self.revalidate()?;
        if first != second {
            return Err(RuntimeError::Changed("native process arguments"));
        }
        Ok(second)
    }
}

impl super::native_process::RelevanceIdentity {
    pub fn observe_arguments(&self) -> Result<Vec<OsString>, RuntimeError> {
        self.revalidate()?;
        let first = arguments(self.pid)?;
        let second = arguments(self.pid)?;
        self.revalidate()?;
        if first != second {
            return Err(RuntimeError::Changed("native relevance arguments"));
        }
        Ok(second)
    }
}

#[cfg(target_os = "macos")]
fn arguments(pid: ProcessId) -> Result<Vec<OsString>, RuntimeError> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid.get() as i32];
    let mut bytes = vec![0u8; MAX_ARGUMENT_BYTES];
    let mut size = bytes.len();
    // SAFETY: MIB and output buffer have the supplied lengths; no input buffer.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as u32,
            bytes.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(inspection_failure(std::io::Error::last_os_error()));
    }
    if size > bytes.len() {
        return Err(RuntimeError::Invalid("native arguments byte budget"));
    }
    bytes.truncate(size);
    mac_arguments(&bytes)
}

#[cfg(target_os = "linux")]
fn arguments(pid: ProcessId) -> Result<Vec<OsString>, RuntimeError> {
    use std::io::Read;
    let file =
        std::fs::File::open(format!("/proc/{}/cmdline", pid.get())).map_err(inspection_failure)?;
    let mut bytes = Vec::new();
    file.take(MAX_ARGUMENT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(inspection_failure)?;
    if bytes.len() > MAX_ARGUMENT_BYTES {
        return Err(RuntimeError::Invalid("native arguments byte budget"));
    }
    linux_arguments(&bytes)
}

fn inspection_failure(error: std::io::Error) -> RuntimeError {
    match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES) => {
            RuntimeError::Unavailable("native arguments permission denied")
        }
        _ => RuntimeError::Unavailable("native arguments unreadable"),
    }
}

#[cfg(any(target_os = "macos", test))]
fn mac_arguments(bytes: &[u8]) -> Result<Vec<OsString>, RuntimeError> {
    let count = bytes
        .get(..4)
        .ok_or(RuntimeError::Invalid("native argument count"))?;
    let count = i32::from_ne_bytes(count.try_into().unwrap());
    if count <= 0 || count as usize > bytes.len() {
        return Err(RuntimeError::Invalid("native argument count"));
    }
    let rest = &bytes[4..];
    let executable_end = rest
        .iter()
        .position(|b| *b == 0)
        .ok_or(RuntimeError::Invalid("native executable delimiter"))?;
    if executable_end == 0 {
        return Err(RuntimeError::Unavailable(
            "native executable argument prefix",
        ));
    }
    // KERN_PROCARGS2 pads after its executable path. Managed launches use a
    // nonempty argv[0]; no attempt is made to authorize an ambiguous empty one.
    let rest = &rest[executable_end + 1..];
    let first = rest
        .iter()
        .position(|b| *b != 0)
        .ok_or(RuntimeError::Unavailable("native arguments empty"))?;
    let mut rest = &rest[first..];
    let mut result = Vec::new();
    for _ in 0..count {
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(RuntimeError::Invalid("native argument terminator"))?;
        result.push(OsString::from_vec(rest[..end].to_vec()));
        rest = &rest[end + 1..];
    }
    Ok(result)
}

#[cfg(any(target_os = "linux", test))]
fn linux_arguments(bytes: &[u8]) -> Result<Vec<OsString>, RuntimeError> {
    if bytes.last() != Some(&0) || bytes.first() == Some(&0) {
        return Err(RuntimeError::Unavailable(
            "native arguments empty or incomplete",
        ));
    }
    Ok(bytes[..bytes.len() - 1]
        .split(|b| *b == 0)
        .map(|arg| OsString::from_vec(arg.to_vec()))
        .collect())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn arguments(_: ProcessId) -> Result<Vec<OsString>, RuntimeError> {
    Err(RuntimeError::Unavailable("native arguments platform"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_argument_parsers_preserve_empty_and_non_utf8_arguments() {
        let expected = vec![
            OsString::from("chanvoy"),
            OsString::new(),
            OsString::from_vec(vec![0xff]),
        ];
        assert_eq!(linux_arguments(b"chanvoy\0\0\xff\0").unwrap(), expected);
        let mut mac = 3i32.to_ne_bytes().to_vec();
        mac.extend_from_slice(b"/bin/chanvoy\0\0chanvoy\0\0\xff\0SECRET=not-an-argument\0");
        assert_eq!(mac_arguments(&mac).unwrap(), expected);
        assert!(linux_arguments(b"chanvoy").is_err());
        assert!(linux_arguments(b"").is_err());
        mac.truncate(8);
        assert!(mac_arguments(&mac).is_err());
    }
    #[test]
    #[ignore = "native argument inspection must be permitted"]
    fn current_arguments_match_without_lossy_conversion() {
        let process = RuntimeProcess::observe_current().unwrap();
        assert_eq!(
            process.observe_arguments().unwrap(),
            std::env::args_os().collect::<Vec<_>>()
        );
    }
}
