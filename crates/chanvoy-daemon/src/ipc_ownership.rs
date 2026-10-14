//! IPC delegates admission to the same arena used by local requests.

use crate::wait_owner::{AdmissionError, RequestPermit, WaitOwnerRegistry};
use chanvoy_core::request_ownership::{RequestAdmission, RequestCompletion};
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
}
