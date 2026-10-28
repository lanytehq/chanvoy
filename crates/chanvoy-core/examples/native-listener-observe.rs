//! Read-only adapter for isolated kernel-listener fixtures, not lifecycle proof.
use chanvoy_core::runtime::{
    observe_listener_table, ListenerTableObservation, PathBytes, RuntimeError,
};

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let observation = if args.len() == 3 && args[1] == "--fixture-path" {
        PathBytes::from_path(std::path::Path::new(&args[2]))
            .and_then(|path| observe_listener_table(&path))
    } else {
        Err(RuntimeError::Invalid(
            "requires --fixture-path <absolute-path>",
        ))
    };
    let (outcome, reason, code) = match observation {
        Ok(ListenerTableObservation::Present) => ("present", None, 0),
        Ok(ListenerTableObservation::NotObserved) => ("not_observed", None, 0),
        // RuntimeError contains closed context labels. Do not expose arbitrary
        // I/O provider messages, path arguments, or kernel table contents.
        Err(RuntimeError::Invalid(reason))
        | Err(RuntimeError::Unavailable(reason))
        | Err(RuntimeError::Changed(reason)) => ("unavailable", Some(reason), 2),
        Err(_) => ("unavailable", Some("native observation failed"), 2),
    };
    println!(
        "{}",
        serde_json::json!({"probe":"native-listener-observation","observation":outcome,
            "reason":reason,"qualification":false})
    );
    std::process::exit(code);
}
