//! In-process admission and explicit request completion.
//!
//! The host supplies the authority. This interface owns no counter or phase,
//! and dropping a completion handle must not confirm completion.

use crate::CoreError;

pub trait RequestAdmission: Send + Sync {
    /// Reserve before provider work, state mutation, or selected output.
    fn reserve_request(&self) -> Result<Box<dyn RequestCompletion>, CoreError>;
}

#[must_use = "retain through all required stages and explicitly confirm completion"]
pub trait RequestCompletion: Send {
    /// Consume this exact owner's handle after all required stages succeed.
    /// Implementations must validate the host's instance and owner identity.
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError>;
}
