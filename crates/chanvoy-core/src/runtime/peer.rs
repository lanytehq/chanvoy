use super::{ProcessId, RuntimeError};
use std::os::fd::{AsRawFd, BorrowedFd};

/// Native peer PID observation; neither birth nor liveness/death proof.
pub fn native_peer_pid(fd: BorrowedFd<'_>) -> Result<ProcessId, RuntimeError> {
    #[cfg(target_os = "macos")]
    {
        let mut pid: libc::pid_t = 0;
        let mut size = std::mem::size_of_val(&pid) as libc::socklen_t;
        // SAFETY: live borrowed socket and correctly sized writable buffers.
        let result = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&mut pid as *mut libc::pid_t).cast(),
                &mut size,
            )
        };
        if result != 0 || size as usize != std::mem::size_of_val(&pid) || pid <= 0 {
            return Err(RuntimeError::Unavailable("native peer PID"));
        }
        ProcessId::new(pid as u32)
    }
    #[cfg(target_os = "linux")]
    {
        let mut credentials = std::mem::MaybeUninit::<libc::ucred>::uninit();
        let mut size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: live socket and correctly sized writable credential buffer.
        let result = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                credentials.as_mut_ptr().cast(),
                &mut size,
            )
        };
        if result != 0 || size as usize != std::mem::size_of::<libc::ucred>() {
            return Err(RuntimeError::Unavailable("native peer PID"));
        }
        // SAFETY: successful getsockopt initialized the full credential buffer.
        let credentials = unsafe { credentials.assume_init() };
        let pid =
            u32::try_from(credentials.pid).map_err(|_| RuntimeError::Invalid("native peer PID"))?;
        ProcessId::new(pid)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = fd;
        Err(RuntimeError::Unavailable("native peer PID"))
    }
}
