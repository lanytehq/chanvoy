# Runtime retirement coordinator

The daemon's internal retirement coordinator composes the shared ownership arena
with the child-owned record publisher. It is not registered as an RPC and does
not initiate shutdown, cancellation, cleanup, signaling, or a successor.

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
observation and pending-state preservation. Response projection, production
routing, native binding and shutdown/flush frontiers require further integration
and review before lifecycle activation.
