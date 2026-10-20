# Chanvoy local runtime-control v1

## Selection, framing and correlation

The consumer-specific family is
`schemas/ipc/local/chanvoy-runtime-control-v1.schema.json`. No numbered channel,
CHAT260 process-control grant, control-channel overload or registry redesign.
The numbered directory loader does not recursively validate this family.

| Boundary                                         | Exact selected root        |
| ------------------------------------------------ | -------------------------- |
| JSON-RPC `runtime_retire_v1` full request        | retire_rpc_request         |
| Same method received successful response         | retire_rpc_response        |
| JSON-RPC `runtime_observation_v1` full request   | observation_rpc_request    |
| Same method received successful response         | observation_rpc_response   |
| Bounded protocol failure response                | runtime_rpc_error_response |
| CLI locally synthesized missing-ACK report       | retirement_no_ack_report   |
| Credential-tier bootstrap file, not RPC          | runtime_bootstrap          |
| Child readiness/handoff report, not RPC          | runtime_readiness          |
| CLI synthesized native/partial local observation | cli_lifecycle_report       |

RPC id is the existing UUID v4 correlation field; consumer compares the exact
response id with its request. Request id is not target instance, native birth,
retirement identity or permission. A fresh id never selects a successor.
Protocol errors have closed code/message/null-data fields and may have null id
when no request correlation could be decoded. They grant no lifecycle fact.

RPC bodies and readiness objects have an explicit 16,384 UTF8-byte cap;
bootstrap has 32,768. These are independent wire budgets, not inferred
from the persisted-record 64KiB cap. Request process executable can use 8KiB hex;
its bounded identity/generation/correlation fields fit 16KiB. Bootstrap includes
both canonical config bytes and launcher executable bytes, profile/identity,
nonce and lease data, hence 32KiB. Socket roots and complete socket paths retain
separate native 87/91 and 103/107 raw-byte limits. Whitespace/escaping counts
against the frame cap. A schema-valid oversized encoding still refuses.

Read at most cap+1 before parse. Require strict UTF8, one JSON object, no trailing
second value, duplicate keys refused at every depth, unknown version/fields
refused. For RPC the single newline delimiter is outside the byte cap; embedded
newline is JSON escaped. No opaque extension map. Invalid/unknown data never
falls back to legacy destructive work. Bootstrap/readiness use their own roots
and transport validators; they are not exposed by naming an RPC method.

## Claims and retirement facts

Target N/instance/revision/process and caller generation are claims. Consumer
rechecks loaded canonical configuration and native UID/birth/argv/executable,
namespace, peer/socket/lease/record identities plus in-memory instance/phase.
No payload, FD number, fingerprint, record or observed zero grants retirement.
Generation dirty is a known boolean for a retirement request; incomplete pins
are refused, never turned into a match.

`begin` requires the current Running epoch and captured durable revision after
fresh full binding. `continue_same_epoch` requires the already committed
Retiring epoch; no new idle check, reopen or successor. Capture the current
durable revision on retry. A pending desired next revision is not permission
to swap UUID/birth or retarget a newer record. Stale revision refuses; caller
reconciles the same original epoch before a fresh correlated request.

Busy/Refused means this request did not commit. memory_retiring,
durable_retiring and shutdown_started always describe the TARGET, never request
effects. A known same-target stale continuation/already-Retiring refusal retains
true target closure/durability facts. An unestablished/mismatched target has null
target facts and counts, not invented false values. Refusal never erases prior
independently known same-epoch Retiring/admission facts. Cross-message sticky
knowledge and revision comparisons remain consumer checks.

The outcome matrix is exhaustive: Busy is responding Running/no new commit with
known active obligations; Refused is no new request commit with either truthful
known target state or typed unknown target; Committed is a new request commit
with confirmed empty arena/durable Retiring/known revision; Continued finishes
the existing same-epoch barrier, request_committed=false, but target memory and
durable Retiring true, empty arena and known revision. Neither outcome proves
death, free lease or listener absence. Shutdown_started is separately known.

Known publication failure while the daemon responds has memory_retiring=true,
admission_closed=true, shutdown_started=false and unconfirmed outcome; no
oneshot, provider cancel, unlink, rollback or successor. durable_retiring is
false only when failure is known, otherwise null. Forward retry retains the
same immutable target. Lost ACK alone produces the separate CLI no-ACK report:
all retirement/durability/liveness/count facts null. It is not a received RPC
response and cannot prove alive/dead or durable failure. Later reconciliation
uses a separately correlated observation.

Stop is canonical forward completion. Restart during known Retiring is the
no-successor misuse guard, not another recovery or cycle. No force, signal or
new-successor field exists; a later flag would be a contract change.

## Observation and readiness

Received observation_rpc_response is fixed to daemon_application by the selected
transport root. CLI synthesis has its own cli_lifecycle_report root selecting
cli_native_reconciled or cli_partial; a body label cannot promote a received
daemon response into native proof. Observation carries source/id/as-of time and
epoch correlation. Application
phase/admission, counts, ordinary health, generation, native identity/death,
lease and listener are independent. Daemon application source cannot supply
native death/lease/listener proof. A count is a shared-arena snapshot or null;
legacy/unreachable/invalid/inventory-unknown output never converts it to zero.
No observed count authorizes retirement. Consumer rejects cross-epoch synthesis
and retains typed unavailable facts; public procedure reference grants no kill
authority. The referenced guide is a same-release consumer delivery, not a fetched policy.

Bootstrap contains only bounded required identity/configuration/socket/lease
selection and ephemeral validated-identity/fingerprint/age/nonce correlation.
The nonce is synthetic in fixtures and absent from the persistent runtime
record. Parent PID/birth data and descriptor/inode claims are not inherited-OFD
proof. Native intended-child, single open description and CLOEXEC proof are
preconditions to unsafe adoption; launcher retains op/lease ownership through
validated handoff. Readiness follows that handoff, actual childPID bind and
confirmed Running publication. Readiness claims alone cannot prove those events.

## Known refusal and received response coherence

Every received daemon retirement response reports responding_at_snapshot=true.
The already_retiring refusal requires memory_retiring/admission_closed true and
zero obligations. Generic generation/revision refusal may report truthful known
Running or Retiring; any known Retiring result requires zero obligations. Target
facts keep stable meanings, and unknown/mismatched facts remain null. These
constraints combine with branch facts; no dictionary replacement removes them.

## Compatibility and activation

Existing local shutdown, status and observation methods retain their contracts
until explicit consumer migration. The managed v1 path must reject empty legacy
shutdown atomically and use explicit target/generation roots. Older daemons
require identity-validated human recovery; method-not-found is a typed legacy
boundary, not permission to unlink a socket or signal a process.

Bootstrap is a distinct versioned format, not an optional-field extension of
legacy bootstrap files. Preserve credential-tier safe reads, nonce/fingerprint/
age checks, intended-child correlation, startup deadlines and readiness rules.
CHAT260 subscriptions retain their chat-domain wire shape; they gain no process
control authority from this family. Runtime records remain a separate format.

Managed rollout requires a compatible CLI before v1 starts. Guarded refresh
preserves the old profile until predecessor death is proved and refuses active
work. Native binding, lease inheritance, exact instance/revision checks and
process/listener retirement require consumer implementation and qualification.
