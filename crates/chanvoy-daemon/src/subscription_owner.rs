//! UDS registrations and push output have independent arena owners.
//! A lost connection is not proof of inverse registration cleanup.

use crate::{request_frontier, wait_owner, DaemonError};
use chanvoy_core::{CoreError, JsonRpcNotification, SubscriptionFilter};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(crate) struct Registration {
    pub filter: SubscriptionFilter,
    owner: wait_owner::RetainedOwner,
}

fn admission_error(error: wait_owner::AdmissionError) -> CoreError {
    match error {
        wait_owner::AdmissionError::Closed => CoreError::RuntimeAdmissionClosed,
        wait_owner::AdmissionError::Exhausted => CoreError::RuntimeOwnershipExhausted,
        wait_owner::AdmissionError::WrongInstance | wait_owner::AdmissionError::OwnerChanged => {
            std::io::Error::other("subscription owner changed").into()
        }
    }
}

pub(crate) fn insert(
    subscriptions: &mut HashMap<String, Registration>,
    registry: &Arc<wait_owner::WaitOwnerRegistry>,
    id: String,
    filter: SubscriptionFilter,
) -> Result<(), CoreError> {
    match subscriptions.entry(id) {
        Entry::Occupied(_) => Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "subscription id already registered",
        )
        .into()),
        Entry::Vacant(entry) => {
            // Reservation and transfer occur in one Inner section, before map
            // visibility. The subscription map is data, not phase authority.
            let owner = registry.registration_owner().map_err(admission_error)?;
            entry.insert(Registration { filter, owner });
            Ok(())
        }
    }
}

/// Caller holds the existing subscription-map exclusion. Any selected output
/// keeps its independent send owner; removal cannot confirm that output.
pub(crate) fn remove(
    subscriptions: &mut HashMap<String, Registration>,
    id: &str,
) -> Result<bool, CoreError> {
    let Some(Registration { filter, owner }) = subscriptions.remove(id) else {
        return Ok(false);
    };
    drop(filter);
    owner.confirm_done().map_err(admission_error)?;
    Ok(true)
}

pub(crate) async fn finish_send<W: AsyncWrite + Unpin>(
    writer: &mut W,
    notification: &JsonRpcNotification,
    owner: wait_owner::RequestPermit,
) -> Result<(), DaemonError> {
    writer
        .write_all(serde_json::to_string(notification)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    owner
        .confirm_done()
        .map_err(request_frontier::admission_error)
}
