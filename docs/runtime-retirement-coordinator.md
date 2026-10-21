# Runtime retirement coordinator

The daemon's internal retirement coordinator composes the shared ownership arena
with the child-owned record publisher. It does not initiate cancellation,
cleanup, signaling, or a successor.

Construction consumes a child publisher with confirmed Running publication and
requires the same instance in the Running admission arena. The coordinator owns
that publisher and serializes control attempts. It does not introduce another
admission phase or ownership inventory.

An attempt accepts only a strictly validated retirement request. Namespace,
instance, full process claim, observed revision, and complete generation must
match the anchored publisher. A required native recheck callback runs before any
closure or write. Native lifecycle integration must implement that callback from
independent current evidence; a decoded request or record is not such evidence.
There is currently no production caller.

For `begin`, the existing shared arena atomically decides Busy or closes empty
Running admission. The Busy result retains the decision's snapshot. Publication
runs after the arena mutex has been released. Control requests do not acquire an
ordinary request permit, which would count themselves as active work.

For `continue_same_epoch`, the arena must already be Retiring. There is no new
idle decision and no reopening. The caller must capture the current observed
revision. Read-only publisher inspection does not flush pending publication as
a side effect of rejecting a stale request. A desired record observed after a
replacement is still unconfirmed until forward reconciliation completes.

Failures before staging, before replacement, or after replacement leave admission
closed. Forward continuation can stage the original retirement if staging never
completed, or retry the existing desired record without increasing its revision.
Unknown or mismatched targets expose no target counts or publication facts.
Known-target refusals retain the actual closed state. No result proves native
death, free lease, absent listener, or permission to create a successor.

The deterministic tests use the actual shared arena and a controlled publication
adapter. They establish application ordering and failure behavior, not native
filesystem/process qualification. Publisher state tests separately cover exact
observation and pending-state preservation.

## RPC routing

The daemon routes `runtime_retire_v1` and `runtime_observation_v1` before the
ordinary request path. A frame that names either method in any top-level
`method` member is decoded only through its strict selected root: the 16 KiB
frame cap, strict UTF-8, one JSON object, duplicate keys refused, and the exact
schema. Invalid input is answered with a closed protocol error and never falls
back to a legacy handler; a framing or size failure closes the connection.
These are provider-free control exceptions and take no operational request
permit, so active work never makes them refuse themselves. Every other method
keeps its existing path.

Coordinator facts are projected onto `retire_rpc_response` and checked against
that root before they are sent; a combination the contract cannot represent is
a protocol error, not an invented fact. Memory and admission facts come from
the arena snapshot. Graceful shutdown starts only for a committed or continued
retirement, after the projection is known to be representable, and
`shutdown_started` reports whether it actually started.

No coordinator is bound in a production daemon until native lifecycle binding
exists. Until then every retirement is refused with
`native_evidence_unavailable` and unknown target facts, and shutdown never
starts. Observation reports `daemon_application` with the arena phase and
counts and `native_identity_unknown`; epoch, generation and native fields are
null.
