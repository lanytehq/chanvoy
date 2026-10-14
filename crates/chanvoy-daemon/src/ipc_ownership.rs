//! IPC delegates admission to the same arena used by local requests.

use crate::wait_owner::{AdmissionError, RequestPermit, RetainedOwner, WaitOwnerRegistry};
use chanvoy_core::request_ownership::{
    RegistrationCompletion, RequestAdmission, RequestCompletion,
};
use chanvoy_core::CoreError;
use std::sync::Arc;

pub(crate) struct IpcAdmission(pub Arc<WaitOwnerRegistry>);

fn local_error(error: AdmissionError) -> CoreError {
    match error {
        AdmissionError::Closed => CoreError::RuntimeAdmissionClosed,
        AdmissionError::Exhausted => CoreError::RuntimeOwnershipExhausted,
        AdmissionError::WrongInstance | AdmissionError::OwnerChanged => {
            std::io::Error::other("IPC request owner changed").into()
        }
    }
}

impl RequestAdmission for IpcAdmission {
    fn reserve_request(&self) -> Result<Box<dyn RequestCompletion>, CoreError> {
        Ok(Box::new(IpcCompletion(
            self.0.request_permit().map_err(local_error)?,
        )))
    }
}

struct IpcCompletion(RequestPermit);
impl RequestCompletion for IpcCompletion {
    fn reserve_registration(&self) -> Result<Box<dyn RegistrationCompletion>, CoreError> {
        Ok(Box::new(IpcRegistration(
            self.0.reserve_stream().map_err(local_error)?,
        )))
    }

    fn confirm_done(self: Box<Self>) -> Result<(), CoreError> {
        self.0.confirm_done().map_err(local_error)
    }
}

struct IpcRegistration(RetainedOwner);
impl RegistrationCompletion for IpcRegistration {
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError> {
        self.0.confirm_done().map_err(local_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait_owner::RetirementDecision;

    #[test]
    fn injected_adapter_uses_the_actual_shared_arena() {
        let arena = Arc::new(WaitOwnerRegistry::new());
        let adapter: Arc<dyn RequestAdmission> = Arc::new(IpcAdmission(Arc::clone(&arena)));
        let request = adapter.reserve_request().unwrap();
        let snapshot = arena.admission_snapshot();
        assert_eq!(snapshot.requests, 1);
        assert!(matches!(
            arena.retire_if_idle(snapshot.instance),
            Ok(RetirementDecision::Busy(_))
        ));
        request.confirm_done().unwrap();
        assert_eq!(arena.admission_snapshot().requests, 0);
        assert!(matches!(
            arena.retire_if_idle(snapshot.instance),
            Ok(RetirementDecision::Closed(_))
        ));
        assert!(matches!(
            adapter.reserve_request(),
            Err(CoreError::RuntimeAdmissionClosed)
        ));
    }

    #[test]
    fn discarded_adapter_handle_does_not_release_another_request() {
        let arena = Arc::new(WaitOwnerRegistry::new());
        let adapter = IpcAdmission(Arc::clone(&arena));
        let lost = adapter.reserve_request().unwrap();
        let completed = adapter.reserve_request().unwrap();
        drop(lost);
        completed.confirm_done().unwrap();
        let snapshot = arena.admission_snapshot();
        assert_eq!(snapshot.requests, 1);
        assert!(matches!(
            arena.retire_if_idle(snapshot.instance),
            Ok(RetirementDecision::Busy(_))
        ));
    }
    #[test]
    fn registration_is_independent_and_drop_preserves_it() {
        let arena = Arc::new(WaitOwnerRegistry::new());
        let adapter = IpcAdmission(Arc::clone(&arena));
        let request = adapter.reserve_request().unwrap();
        let registration = request.reserve_registration().unwrap();
        let snapshot = arena.admission_snapshot();
        assert_eq!((snapshot.requests, snapshot.streams), (1, 1));
        request.confirm_done().unwrap();
        drop(registration);
        let snapshot = arena.admission_snapshot();
        assert_eq!((snapshot.requests, snapshot.streams), (0, 1));
        assert!(matches!(
            arena.retire_if_idle(snapshot.instance),
            Ok(RetirementDecision::Busy(_))
        ));
    }

    #[test]
    fn old_registration_completion_cannot_clear_a_new_instance() {
        let old = Arc::new(WaitOwnerRegistry::new());
        let new = Arc::new(WaitOwnerRegistry::new());
        let old_request = IpcAdmission(Arc::clone(&old)).reserve_request().unwrap();
        let new_request = IpcAdmission(Arc::clone(&new)).reserve_request().unwrap();
        let old_stream = old_request.reserve_registration().unwrap();
        let new_stream = new_request.reserve_registration().unwrap();
        old_request.confirm_done().unwrap();
        new_request.confirm_done().unwrap();
        assert_ne!(
            old.admission_snapshot().instance,
            new.admission_snapshot().instance
        );
        old_stream.confirm_done().unwrap();
        assert_eq!(old.admission_snapshot().streams, 0);
        assert_eq!(new.admission_snapshot().streams, 1);
        let snapshot = new.admission_snapshot();
        assert!(matches!(
            new.retire_if_idle(snapshot.instance),
            Ok(RetirementDecision::Busy(_))
        ));
        new_stream.confirm_done().unwrap();
        assert_eq!(new.admission_snapshot().streams, 0);
    }
}
