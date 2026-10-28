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

fn main() {
    // SAFETY: this is the unique process entry before the async runtime and
    // before any other Rust descriptor owner. Errors exit; no legacy fallback.
    let mut invocation = match unsafe { chanvoy_cli::prepare_process_entry() } {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::exit(1);
        }
    };
    let managed = invocation.is_managed();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::exit(1);
        }
    };
    let result = runtime.block_on(invocation.run());
    if let Err(error) = &result {
        eprintln!("Error: {error}");
    }
    if managed || result.is_err() {
        // The managed lifetime anchor remains owned by invocation until the
        // kernel closes this process's descriptions. Serve already collected
        // its runtime tasks; an error never continues with a closed lease.
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
}
