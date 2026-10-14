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
pub trait RequestCompletion: Send + Sync {
    /// Reserve an independent registration while this request remains admitted.
    fn reserve_registration(&self) -> Result<Box<dyn RegistrationCompletion>, CoreError>;

    /// Consume this exact owner's handle after all required stages succeed.
    /// Implementations must validate the host's instance and owner identity.
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError>;
}

/// Retained registration ownership survives connection loss and handle Drop.
#[must_use = "store with the registration until exact removal is confirmed"]
pub trait RegistrationCompletion: Send + Sync {
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError>;
}
