//! Attempt-local native inventory for qualified same-account host operation.
//! This is not death or listener-descriptor proof. Host deployment qualification
//! is a prerequisite: especially on Linux, observations inside a container cannot
//! establish that no process exists outside its visible PID/mount namespace.
use super::native_process::RelevanceIdentity;
use super::{
    Namespace, OperationLock, ProcessId, RuntimeAuthority, RuntimeError, RuntimeLaunchMarker,
    RuntimeProcess,
};
use std::ffi::{OsStr, OsString};
use std::time::Instant;

/// Native observations only; argument contents are intentionally not Debug.
pub struct NativeCandidate {
    pub process: RuntimeProcess,
    pub kind: NativeCandidateKind,
}
pub enum NativeCandidateKind {
    Managed(RuntimeLaunchMarker),
    /// No configuration root can be inferred from legacy argv. Even an explicit
    /// profile is not enough to exclude another root without native binding.
    Legacy {
        explicit_profile: Option<String>,
    },
}
/// An attempt-local complete scan in the qualified host view, never a cached
/// authority or a protection against nonparticipating legacy launchers.
pub struct NativeInventory {
    pub candidates: Vec<NativeCandidate>,
}
impl NativeInventory {
    pub fn scan(
        authority: &RuntimeAuthority,
        operation: &OperationLock,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        operation.verify(authority)?;
        let record = authority.read_record(operation.namespace())?;
        if record
            .as_ref()
            .is_some_and(|r| r.record.phase == super::RuntimePhase::Starting)
        {
            // Bootstrap references cannot be erased by a relevance exclusion.
            // Their recovery needs its own bound launcher/child reconciliation.
            return Err(RuntimeError::Unavailable(
                "starting runtime requires bootstrap reconciliation",
            ));
        }
        let referenced = record
            .as_ref()
            .and_then(|r| r.record.process.as_ref().map(|p| p.pid))
            .into_iter()
            .collect::<Vec<_>>();
        let result = reconcile_scans(deadline, || {
            scan_native(authority.uid(), &referenced, deadline)
        })?;
        operation.verify(authority)?;
        if authority.read_record(operation.namespace())? != record {
            return Err(RuntimeError::Changed("runtime record during inventory"));
        }
        check_deadline(deadline)?;
        Ok(result)
    }
}
fn check_deadline(deadline: Instant) -> Result<(), RuntimeError> {
    if Instant::now() >= deadline {
        Err(RuntimeError::Unavailable("native inventory deadline"))
    } else {
        Ok(())
    }
}
const MAX_PIDS: usize = 65_536;
fn checked_pids(mut pids: Vec<ProcessId>) -> Result<Vec<ProcessId>, RuntimeError> {
    if pids.len() >= MAX_PIDS {
        return Err(RuntimeError::Unavailable("native inventory truncated"));
    }
    pids.sort_by_key(|p| p.get());
    if pids.windows(2).any(|p| p[0] == p[1]) {
        return Err(RuntimeError::Changed("duplicate native inventory PID"));
    }
    Ok(pids)
}
fn reconcile_scans<T>(
    deadline: Instant,
    mut scan: impl FnMut() -> Result<T, RuntimeError>,
) -> Result<T, RuntimeError> {
    // Host churn may invalidate a read-only pass. Retry at most twice under the
    // same operation ownership and original deadline; never keep partial rows.
    // Invalid grammar/artifacts are not transient and are never retried.
    for attempt in 0..3 {
        check_deadline(deadline)?;
        match scan() {
            Ok(value) => {
                check_deadline(deadline)?;
                return Ok(value);
            }
            Err(error @ (RuntimeError::Changed(_) | RuntimeError::Unavailable(_)))
                if attempt < 2 =>
            {
                let _ = error;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded scan returns on final attempt")
}
fn scan_native(
    uid: u32,
    referenced: &[ProcessId],
    deadline: Instant,
) -> Result<NativeInventory, RuntimeError> {
    scan_staged(
        if cfg!(target_os = "macos") {
            OwnerScope::Selected(uid)
        } else {
            OwnerScope::All(uid)
        },
        referenced,
        deadline,
        || enumerate_for_account(uid),
        owner,
        |pid| {
            let identity = RelevanceIdentity::observe(pid)?;
            let args = identity.observe_arguments()?;
            Ok((identity, args))
        },
        |pid| {
            let process = RuntimeProcess::observe(pid)?;
            let args = process.observe_arguments()?;
            Ok((process, args))
        },
    )
}
#[derive(Clone, Copy)]
enum OwnerScope {
    All(u32),
    Selected(u32),
}
impl OwnerScope {
    fn uid(self) -> u32 {
        match self {
            Self::All(uid) | Self::Selected(uid) => uid,
        }
    }
}
fn potential_candidate(args: &[OsString]) -> bool {
    use std::os::unix::ffi::OsStrExt;
    args.iter()
        .skip(1)
        .any(|a| a.as_bytes().starts_with(b"--runtime-"))
        || (args.iter().skip(1).any(|a| a == "daemon") && args.iter().skip(1).any(|a| a == "serve"))
}
fn scan_staged(
    scope: OwnerScope,
    referenced: &[ProcessId],
    deadline: Instant,
    mut enumerate: impl FnMut() -> Result<Vec<ProcessId>, RuntimeError>,
    mut owner: impl FnMut(ProcessId) -> Result<u32, RuntimeError>,
    mut relevance: impl FnMut(ProcessId) -> Result<(RelevanceIdentity, Vec<OsString>), RuntimeError>,
    mut full: impl FnMut(ProcessId) -> Result<(RuntimeProcess, Vec<OsString>), RuntimeError>,
) -> Result<NativeInventory, RuntimeError> {
    let uid = scope.uid();
    check_deadline(deadline)?;
    let before = checked_pids(enumerate()?)?;
    // Forced references never repair a selected list. Directly reconcile an
    // omitted target, but preserve the raw membership for the second list.
    if let Some(&pid) = referenced.iter().find(|pid| !before.contains(pid)) {
        check_deadline(deadline)?;
        if owner(pid)? != uid {
            return Err(RuntimeError::Changed("referenced runtime owner"));
        }
        full(pid)
            .map_err(|_| RuntimeError::Unavailable("candidate daemon identity unavailable"))?;
        return Err(RuntimeError::Unavailable(
            "referenced runtime PID omitted; reconciliation required",
        ));
    }
    let mut candidates = Vec::new();
    let mut observations = Vec::new();
    let mut owners = Vec::new();
    for &pid in &before {
        check_deadline(deadline)?;
        let observed_uid = owner(pid)?;
        owners.push(observed_uid);
        if observed_uid != uid {
            if matches!(scope, OwnerScope::Selected(_)) {
                return Err(RuntimeError::Changed("selected native inventory owner"));
            }
            if referenced.contains(&pid) {
                return Err(RuntimeError::Changed("referenced runtime owner"));
            }
            continue;
        }
        let (identity, args) = relevance(pid)
            .map_err(|_| RuntimeError::Unavailable("same-account process relevance unavailable"))?;
        if identity.pid != pid || identity.uid != uid {
            return Err(RuntimeError::Changed("inventory relevance owner/PID"));
        }
        observations.push((identity.clone(), argument_digest(&args)));
        if potential_candidate(&args) || referenced.contains(&pid) {
            let (process, full_args) = full(pid)
                .map_err(|_| RuntimeError::Unavailable("candidate daemon identity unavailable"))?;
            if !identity.matches_full(&process) || args != full_args {
                return Err(RuntimeError::Changed(
                    "candidate identity across relevance bracket",
                ));
            }
            let kind = classify(&process, &full_args)?
                .ok_or(RuntimeError::Changed("referenced runtime launch binding"))?;
            candidates.push(NativeCandidate { process, kind });
        }
    }
    if before != checked_pids(enumerate()?)? {
        return Err(RuntimeError::Changed("native inventory PID set"));
    }
    for (&pid, previous_uid) in before.iter().zip(owners) {
        check_deadline(deadline)?;
        if owner(pid)? != previous_uid {
            return Err(RuntimeError::Changed("native inventory owner"));
        }
    }
    // Every exclusion is bracketed. Unknowns never become an empty result.
    for (previous, arguments) in observations {
        check_deadline(deadline)?;
        let (identity, args) = relevance(previous.pid)
            .map_err(|_| RuntimeError::Unavailable("same-account process relevance unavailable"))?;
        if identity != previous || argument_digest(&args) != arguments {
            return Err(RuntimeError::Changed(
                "native account process during inventory",
            ));
        }
    }
    for candidate in &candidates {
        let (process, args) = full(candidate.process.pid)
            .map_err(|_| RuntimeError::Unavailable("candidate daemon identity unavailable"))?;
        if process != candidate.process || !potential_candidate(&args) {
            return Err(RuntimeError::Changed(
                "candidate daemon identity during inventory",
            ));
        }
        // Exact managed marker and legacy grammar checks are repeated, not
        // reduced to process-name or executable equality.
        let observed = classify(&process, &args)?;
        let same = match (&candidate.kind, observed) {
            (NativeCandidateKind::Managed(a), Some(NativeCandidateKind::Managed(b))) => *a == b,
            (
                NativeCandidateKind::Legacy {
                    explicit_profile: a,
                },
                Some(NativeCandidateKind::Legacy {
                    explicit_profile: b,
                }),
            ) => *a == b,
            _ => false,
        };
        if !same {
            return Err(RuntimeError::Changed(
                "candidate launch arguments during inventory",
            ));
        }
    }
    check_deadline(deadline)?;
    Ok(NativeInventory { candidates })
}
#[cfg(test)]
fn scan_with(
    uid: u32,
    deadline: Instant,
    enumerate: impl FnMut() -> Result<Vec<ProcessId>, RuntimeError>,
    owner: impl FnMut(ProcessId) -> Result<u32, RuntimeError>,
    observe: impl FnMut(ProcessId) -> Result<(RuntimeProcess, Vec<OsString>), RuntimeError>,
) -> Result<NativeInventory, RuntimeError> {
    let observe = std::cell::RefCell::new(observe);
    scan_staged(
        OwnerScope::All(uid),
        &[],
        deadline,
        enumerate,
        owner,
        |pid| {
            let (p, args) = observe.borrow_mut()(pid)?;
            Ok((
                RelevanceIdentity {
                    pid: p.pid,
                    uid: p.uid,
                    birth: p.birth,
                    pid_namespace: p.pid_namespace,
                    mount_namespace: p.mount_namespace,
                },
                args,
            ))
        },
        |pid| observe.borrow_mut()(pid),
    )
}
fn argument_digest(arguments: &[OsString]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    use std::os::unix::ffi::OsStrExt;
    let mut hash = Sha256::new();
    for argument in arguments {
        let bytes = argument.as_bytes();
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    hash.finalize().into()
}
fn classify(
    process: &RuntimeProcess,
    args: &[OsString],
) -> Result<Option<NativeCandidateKind>, RuntimeError> {
    use std::os::unix::ffi::OsStrExt;
    let managed = args
        .iter()
        .skip(1)
        .any(|a| a.as_bytes().starts_with(b"--runtime-"));
    if managed {
        // Qualified managed launch uses exactly one canonical argument shape.
        // A malformed marker cannot be used to exclude a possible owner.
        if args.len() != 11
            || args[1] != "--profile"
            || args[3] != "daemon"
            || args[4] != "serve"
            || args[5] != "--runtime-namespace"
            || args[7] != "--runtime-instance"
            || args[9] != "--runtime-config-root"
        {
            return Err(RuntimeError::Invalid("native managed marker shape"));
        }
        let parse = |arg: &OsStr| {
            arg.to_str()
                .map(|s| serde_json::Value::String(s.into()))
                .ok_or(RuntimeError::Invalid("native managed marker encoding"))
        };
        let marker = RuntimeLaunchMarker {
            profile: args[2]
                .to_str()
                .ok_or(RuntimeError::Invalid("native profile encoding"))?
                .into(),
            namespace: serde_json::from_value(parse(&args[6])?)
                .map_err(|_| RuntimeError::Invalid("native namespace marker"))?,
            instance_uuid: serde_json::from_value(parse(&args[8])?)
                .map_err(|_| RuntimeError::Invalid("native instance marker"))?,
            config_root: serde_json::from_value(parse(&args[10])?)
                .map_err(|_| RuntimeError::Invalid("native config marker"))?,
        };
        if marker.namespace != Namespace::derive(process.uid, &marker.config_root, &marker.profile)?
        {
            return Err(RuntimeError::Invalid("native namespace inputs"));
        }
        marker.check_arguments(process.executable.to_path().as_os_str(), args)?;
        return Ok(Some(NativeCandidateKind::Managed(marker)));
    }
    if !args.iter().skip(1).any(|a| a == "daemon") || !args.iter().skip(1).any(|a| a == "serve") {
        return Ok(None);
    }
    let mut profiles = Vec::new();
    for (index, arg) in args.iter().enumerate().skip(1) {
        if arg == "--profile" {
            profiles.push(
                args.get(index + 1)
                    .and_then(|a| a.to_str())
                    .filter(|s| !s.is_empty())
                    .ok_or(RuntimeError::Invalid("legacy profile argument"))?
                    .to_owned(),
            );
        } else if let Some(profile) = arg.as_bytes().strip_prefix(b"--profile=") {
            profiles.push(
                std::str::from_utf8(profile)
                    .ok()
                    .filter(|s| !s.is_empty())
                    .ok_or(RuntimeError::Invalid("legacy profile encoding"))?
                    .to_owned(),
            );
        }
    }
    if profiles.len() > 1 {
        return Err(RuntimeError::Invalid("duplicate legacy profile arguments"));
    }
    Ok(Some(NativeCandidateKind::Legacy {
        explicit_profile: profiles.pop(),
    }))
}

#[cfg(any(target_os = "macos", test))]
fn validate_darwin_list_size(bytes: i32, before: i32, after: i32) -> Result<(), RuntimeError> {
    let capacity = (MAX_PIDS * std::mem::size_of::<i32>()) as i32;
    if bytes <= 0
        || before <= 0
        || after <= 0
        || bytes % 4 != 0
        || before % 4 != 0
        || after % 4 != 0
        || bytes >= capacity
        || bytes >= before
        || bytes >= after
        || before > capacity
        || after > capacity
    {
        return Err(RuntimeError::Unavailable(
            "native UID list size/cap uncertainty",
        ));
    }
    Ok(())
}
#[cfg(target_os = "macos")]
fn enumerate_for_account(uid: u32) -> Result<Vec<ProcessId>, RuntimeError> {
    // PROC_UID_ONLY=4 selects effective UID, not PROC_RUID_ONLY=5. This
    // observation still requires deployed-kernel qualification of omitted
    // shadow/NOCRED states and the internal nprocs+20 allocation bound.
    // Size hints bracket each list; neither a short result nor equal lists
    // alone certifies the host view. Raw lists are compared before references.
    let mut pids = vec![0i32; MAX_PIDS];
    let before = unsafe { libc::proc_listpids(4, uid, std::ptr::null_mut(), 0) };
    let bytes =
        unsafe { libc::proc_listpids(4, uid, pids.as_mut_ptr().cast(), (pids.len() * 4) as i32) };
    let after = unsafe { libc::proc_listpids(4, uid, std::ptr::null_mut(), 0) };
    validate_darwin_list_size(bytes, before, after)?;
    pids.truncate(bytes as usize / 4);
    // A selected account list cannot silently discard zero/negative entries.
    pids.into_iter().map(|p| ProcessId::new(p as u32)).collect()
}
#[cfg(target_os = "macos")]
fn owner(pid: ProcessId) -> Result<u32, RuntimeError> {
    #[repr(C)]
    struct ShortInfo {
        pid: u32,
        ppid: u32,
        pgid: u32,
        status: u32,
        comm: [libc::c_char; 16],
        flags: u32,
        uid: u32,
        gid: u32,
        ruid: u32,
        rgid: u32,
        svuid: u32,
        svgid: u32,
        reserved: u32,
    }
    const _: () = assert!(std::mem::size_of::<ShortInfo>() == 64);
    let mut info: ShortInfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let read = unsafe {
        // A short libproc read need not set errno. Do not attribute an earlier
        // denial on this thread to this process observation.
        *libc::__error() = 0;
        libc::proc_pidinfo(
            pid.get() as i32,
            13,
            1,
            (&mut info as *mut ShortInfo).cast(),
            size,
        )
    };
    let errno = std::io::Error::last_os_error().raw_os_error();
    if read != size {
        if matches!(errno, Some(libc::EPERM | libc::EACCES)) {
            return Err(RuntimeError::Unavailable(
                "native inventory owner permission denied",
            ));
        }
        return Err(RuntimeError::Unavailable(
            "native inventory owner unreadable",
        ));
    }
    if info.pid != pid.get() {
        return Err(RuntimeError::Changed("native inventory owner PID"));
    }
    Ok(info.uid)
}
#[cfg(any(target_os = "linux", test))]
mod linux;
#[cfg(target_os = "linux")]
use linux::{enumerate, owner};
#[cfg(not(target_os = "macos"))]
fn enumerate_for_account(_: u32) -> Result<Vec<ProcessId>, RuntimeError> {
    enumerate()
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn enumerate() -> Result<Vec<ProcessId>, RuntimeError> {
    Err(RuntimeError::Unavailable("native inventory platform"))
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn owner(_: ProcessId) -> Result<u32, RuntimeError> {
    Err(RuntimeError::Unavailable("native inventory platform"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn process() -> RuntimeProcess {
        super::super::RuntimeReadiness::decode(include_bytes!("../../../../schemas/runtime/control/v1/fixtures/chanvoy-runtime-control-v1/runtime_readiness-positive.json")).unwrap().process
    }
    fn deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(2)
    }
    #[test]
    fn uid_list_requires_margin_below_both_native_size_hints_and_caller_cap() {
        assert!(validate_darwin_list_size(4, 12, 8).is_ok());
        for (bytes, before, after) in [
            (0, 8, 8),
            (-1, 8, 8),
            (4, 4, 8),
            (4, 8, 4),
            (4, -1, 8),
            (4, 8, -1),
            (5, 12, 12),
            (4, 9, 12),
            (
                (MAX_PIDS * 4) as i32,
                (MAX_PIDS * 4) as i32,
                (MAX_PIDS * 4) as i32,
            ),
        ] {
            assert!(validate_darwin_list_size(bytes, before, after).is_err());
        }
    }
    #[test]
    fn selected_uid_owner_changes_and_omitted_forced_targets_are_not_exclusions() {
        let p = process();
        assert!(matches!(
            scan_staged(
                OwnerScope::Selected(p.uid),
                &[],
                deadline(),
                || Ok(vec![p.pid]),
                |_| Ok(p.uid + 1),
                |_| unreachable!(),
                |_| unreachable!(),
            ),
            Err(RuntimeError::Changed("selected native inventory owner"))
        ));
        let inspected = std::cell::Cell::new(false);
        assert!(matches!(
            scan_staged(
                OwnerScope::Selected(p.uid),
                &[p.pid],
                deadline(),
                || Ok(vec![]),
                |_| Ok(p.uid),
                |_| unreachable!(),
                |_| {
                    inspected.set(true);
                    Ok((p.clone(), ordinary(&p)))
                },
            ),
            Err(RuntimeError::Unavailable(
                "referenced runtime PID omitted; reconciliation required"
            ))
        ));
        assert!(inspected.get());
    }
    #[test]
    fn forced_reference_does_not_hide_a_change_in_raw_uid_membership() {
        let p = process();
        let args: Vec<OsString> = vec!["old".into(), "daemon".into(), "serve".into()];
        let mut pass = 0;
        assert!(matches!(
            scan_staged(
                OwnerScope::Selected(p.uid),
                &[p.pid],
                deadline(),
                || {
                    pass += 1;
                    Ok(if pass == 1 { vec![p.pid] } else { vec![] })
                },
                |_| Ok(p.uid),
                |_| Ok((relevance_of(&p), args.clone())),
                |_| Ok((p.clone(), args.clone())),
            ),
            Err(RuntimeError::Changed("native inventory PID set"))
        ));
    }
    fn ordinary(process: &RuntimeProcess) -> Vec<OsString> {
        vec![
            process.executable.to_path().into_os_string(),
            "version".into(),
        ]
    }
    #[test]
    fn unreadable_owner_or_same_account_arguments_never_become_absence() {
        let p = process();
        assert!(scan_with(
            p.uid,
            deadline(),
            || Ok(vec![p.pid]),
            |_| Err(RuntimeError::Unavailable("denied")),
            |_| unreachable!()
        )
        .is_err());
        assert!(scan_with(
            p.uid,
            deadline(),
            || Ok(vec![p.pid]),
            |_| Ok(p.uid),
            |_| Err(RuntimeError::Unavailable("argv denied"))
        )
        .is_err());
        let foreign = scan_with(
            p.uid,
            deadline(),
            || Ok(vec![p.pid]),
            |_| Ok(p.uid + 1),
            |_| unreachable!(),
        )
        .unwrap();
        assert!(foreign.candidates.is_empty());
    }
    #[test]
    fn truncation_duplicate_and_churning_pid_sets_refuse() {
        let p = process();
        assert!(checked_pids(vec![p.pid; MAX_PIDS]).is_err());
        assert!(checked_pids(vec![p.pid; 2]).is_err());
        let mut round = 0;
        assert!(scan_with(
            p.uid,
            deadline(),
            || {
                round += 1;
                Ok(if round == 1 { vec![p.pid] } else { vec![] })
            },
            |_| Ok(p.uid),
            |_| Ok((p.clone(), ordinary(&p)))
        )
        .is_err());
    }
    #[test]
    fn initially_irrelevant_process_is_rechecked_for_exec_or_pid_reuse() {
        let p = process();
        let mut round = 0;
        let result = scan_with(
            p.uid,
            deadline(),
            || Ok(vec![p.pid]),
            |_| Ok(p.uid),
            |_| {
                round += 1;
                let mut args = ordinary(&p);
                if round == 2 {
                    args.extend(["daemon".into(), "serve".into()]);
                }
                Ok((p.clone(), args))
            },
        );
        assert!(matches!(
            result,
            Err(RuntimeError::Changed(
                "native account process during inventory"
            ))
        ));
    }
    #[test]
    fn managed_marker_binds_old_executable_lossless_root_and_refuses_duplicate_flags() {
        use std::os::unix::ffi::OsStringExt;
        let p = process();
        let root = super::super::PathBytes::from_path(&std::path::PathBuf::from(
            OsString::from_vec(b"/owned/\xff".to_vec()),
        ))
        .unwrap();
        let marker = RuntimeLaunchMarker {
            namespace: Namespace::derive(p.uid, &root, "fixture").unwrap(),
            instance_uuid: super::super::InstanceId::new(),
            config_root: root,
            profile: "fixture".into(),
        };
        let mut args = marker
            .arguments(p.executable.to_path().as_os_str())
            .unwrap();
        assert!(matches!(
            classify(&p, &args).unwrap(),
            Some(NativeCandidateKind::Managed(_))
        ));
        args.extend(["--profile".into(), "other".into()]);
        assert!(classify(&p, &args).is_err());
    }
    #[test]
    fn legacy_candidates_do_not_require_current_executable_and_missing_profile_stays_ambiguous() {
        let p = process();
        let args = vec![
            OsString::from("old-renamed-binary"),
            "daemon".into(),
            "--json".into(),
            "serve".into(),
        ];
        assert!(matches!(
            classify(&p, &args).unwrap(),
            Some(NativeCandidateKind::Legacy {
                explicit_profile: None
            })
        ));
        let mut args = args;
        args.extend(["--profile=a".into(), "--profile=b".into()]);
        assert!(classify(&p, &args).is_err());
    }
    #[test]
    fn owner_or_birth_changes_with_same_pid_set_refuse() {
        let p = process();
        let mut owners = 0;
        assert!(scan_with(
            p.uid,
            deadline(),
            || Ok(vec![p.pid]),
            |_| {
                owners += 1;
                Ok(if owners == 1 { p.uid } else { p.uid + 1 })
            },
            |_| Ok((p.clone(), ordinary(&p)))
        )
        .is_err());
        let mut observations = 0;
        assert!(scan_with(
            p.uid,
            deadline(),
            || Ok(vec![p.pid]),
            |_| Ok(p.uid),
            |_| {
                observations += 1;
                let mut changed = p.clone();
                if observations > 1 {
                    changed.birth = super::super::NativeBirth::MacOs {
                        seconds: 999,
                        microseconds: 1,
                    };
                }
                Ok((changed, ordinary(&p)))
            }
        )
        .is_err());
    }
    fn relevance_of(p: &RuntimeProcess) -> RelevanceIdentity {
        RelevanceIdentity {
            pid: p.pid,
            uid: p.uid,
            birth: p.birth.clone(),
            pid_namespace: p.pid_namespace,
            mount_namespace: p.mount_namespace,
        }
    }
    #[test]
    fn unrelated_relevance_never_needs_executable_but_candidates_and_recorded_targets_do() {
        let p = process();
        let run = |args: Vec<OsString>, referenced: &[ProcessId]| {
            scan_staged(
                OwnerScope::All(p.uid),
                referenced,
                deadline(),
                || Ok(vec![p.pid]),
                |_| Ok(p.uid),
                |_| Ok((relevance_of(&p), args.clone())),
                |_| Err(RuntimeError::Unavailable("executable unavailable")),
            )
        };
        assert!(run(ordinary(&p), &[]).unwrap().candidates.is_empty());
        assert!(matches!(
            run(ordinary(&p), &[p.pid]),
            Err(RuntimeError::Unavailable(
                "candidate daemon identity unavailable"
            ))
        ));
        for args in [
            vec!["old".into(), "daemon".into(), "serve".into()],
            vec!["old".into(), "--runtime-namespace=broken".into()],
        ] {
            assert!(matches!(
                run(args, &[]),
                Err(RuntimeError::Unavailable(
                    "candidate daemon identity unavailable"
                ))
            ));
        }
    }
    #[test]
    fn referenced_pid_with_unrelated_arguments_is_not_erased_even_with_full_identity() {
        let p = process();
        let args = ordinary(&p);
        assert!(matches!(
            scan_staged(
                OwnerScope::All(p.uid),
                &[p.pid],
                deadline(),
                || Ok(vec![p.pid]),
                |_| Ok(p.uid),
                |_| Ok((relevance_of(&p), args.clone())),
                |_| Ok((p.clone(), args.clone()))
            ),
            Err(RuntimeError::Changed("referenced runtime launch binding"))
        ));
    }
    #[test]
    fn reconciliation_discards_failed_passes_and_remains_bounded() {
        let mut attempts = 0;
        assert_eq!(
            reconcile_scans(deadline(), || {
                attempts += 1;
                if attempts < 3 {
                    Err(RuntimeError::Changed("PID set"))
                } else {
                    Ok(7)
                }
            })
            .unwrap(),
            7
        );
        assert_eq!(attempts, 3);
        attempts = 0;
        assert!(reconcile_scans::<()>(deadline(), || {
            attempts += 1;
            Err(RuntimeError::Unavailable("owner unreadable"))
        })
        .is_err());
        assert_eq!(attempts, 3);
        attempts = 0;
        assert!(reconcile_scans::<()>(deadline(), || {
            attempts += 1;
            Err(RuntimeError::Invalid("marker"))
        })
        .is_err());
        assert_eq!(attempts, 1);
    }
    #[test]
    #[ignore = "native inventory requires qualified host process visibility"]
    fn native_account_inventory_includes_current_managed_or_legacy_candidate_only_when_marked() {
        let process = RuntimeProcess::observe_current().unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        let result = reconcile_scans(deadline, || scan_native(process.uid, &[], deadline));
        // This explicit native probe reports denied/churning scans as errors;
        // it never converts those environments into a passing absence claim.
        result
            .map(|_| ())
            .expect("native inventory did not establish completeness");
    }
}
