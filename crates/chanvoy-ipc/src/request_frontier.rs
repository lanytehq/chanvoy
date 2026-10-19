//! The selected request transport's actual completion boundary.

use ipcprims::peer::AsyncPeerTx;
use std::future::Future;

pub(super) type SendResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub(super) trait RequestTransport: Sync {
    fn send(&self, channel: u16, payload: &[u8]) -> impl Future<Output = SendResult> + Send;
}

impl RequestTransport for AsyncPeerTx {
    async fn send(&self, channel: u16, payload: &[u8]) -> SendResult {
        // The locked transport awaits write_all and flush before returning.
        AsyncPeerTx::send(self, channel, payload).await?;
        Ok(())
    }
}

/// Session teardown delegates to the actual transport, after forwarder collection.
pub(super) trait SessionTransport: RequestTransport {
    fn shutdown(&self) -> impl Future<Output = SendResult> + Send;
}

impl SessionTransport for AsyncPeerTx {
    async fn shutdown(&self) -> SendResult {
        AsyncPeerTx::shutdown(self).await?;
        Ok(())
    }
}
