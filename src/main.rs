//! `chanvoy` binary entry point.
//!
//! Error rendering is deliberate. Returning `Err` from `main` makes Rust print
//! the error's **`Debug`** representation, which for these `thiserror` types is
//! the enum shape rather than the message — operators saw
//! `Error: Daemon(NotRunning("/var/.../profile.sock"))` while every carefully
//! written `#[error(...)]` string went unread. Worse, the variant name reads as
//! if it were the diagnostic: a `daemon start` refused for want of an explicit
//! profile reported `DestructiveRequiresExplicit`, telling the operator that
//! starting a daemon is "destructive" when it is not.
//!
//! So the error is printed via `Display` and the process exits 1 explicitly.
//! The typed messages are the operator contract; the enum shape is an
//! implementation detail that belongs in tracing, not on stderr.

#[tokio::main]
async fn main() {
    if let Err(err) = chanvoy_cli::run().await {
        // `Display` only, no source-chain walk: every `#[error(...)]` string in
        // this workspace already interpolates its cause (`"io error: {0}"`,
        // `"rpc error {code}: {message}"`), so following `source()` would print
        // the same text twice.
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}
