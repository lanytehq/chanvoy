# ADR-0001: Authenticated binary release provenance

Status: Proposed

## Context

Signed tag objects authenticate source, while downloaded binaries also need an
inspectable binding to that source and its successful native build. Draft-writing
CI and replacement uploads make the publication boundary harder to review.
Existing consumers rely on checksum, per-binary signature and public-key names.

## Proposal

Keep the three native platforms and locked builds. Verify annotated tags against
public data committed in the tagged tree using trusted checkout code and an
isolated keyring; corroborate with GitHub Verified/valid. Tag CI gets read-only
permissions and produces artifacts only. Maintainers separately create drafts,
sign both manifests in both formats, upload only enumerated missing provenance,
verify freshly downloaded bytes and promote under an explicit cue.

An external staging receipt binds tag object, commit and successful release push
run across entrypoints. Tagged notes, licenses and anchors remain inert data;
verification continues after main advances. One exact inventory includes legacy
aliases/signatures, with byte-identical checksum aliases and no self-reference.
Authenticate inventory/signatures before host execution at direct promotion.

Public ASC and TXT/NDJSON trust roots use existing independently approved inputs,
Decernor >=0.1.8, exactly one signing subkey, explicit digest-guarded replacement
and reviewed rotation notices. Historical verification stays available.

## Consequences

The ceremony requires separate prepare/sign/push, draft, upload and publication
steps. Ambiguous operations need inspection, not blind retries or clobber.
Disposable synthetic keys and stub remotes exercise negative paths in CI.
No CI signing secrets, new keys, additional platforms or registry publication
are introduced. This proposal does not itself authorize signing or publication.
