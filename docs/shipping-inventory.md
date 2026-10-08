# Artifact-associated shipping inventory

The release workflow produces one `sbom-VERSION.cdx.json` asset after all three
native binaries pass qualification. Each application component carries the actual
downloaded payload's SHA-256, source commit/tree and native target. Candidate PR
receipts cannot substitute for the verified tag's source, run or attempt.

Normal Cargo compiler and build-script messages supply the selected package set.
Locked metadata supplies source identities and normal/build relationships. Packages
found only in metadata, a lockfile or test compilation do not enter this inventory.
Registry checksums, resolved git commits and workspace source commits remain
distinct even when packages share a name and version.

Component properties distinguish binary observations, conservative target
candidates, and host/build inputs. Proc macros and native builders are build
inputs; overlapping roles and feature-context ambiguity remain visible. Neither
selection nor an archive reference proves every member survived linking. Default
build-tool discovery is not proof of which compiler a build script executed.

Native build legs retain `readelf` or `otool` metadata and available owned archive
hashes before fixture compilation. Dynamic-load requirements describe external
requirements, without inventing a shipped host-library version. Syft scans each
qualified payload alone using the pinned image, updates disabled, a read-only
filesystem and no container network. Empty package results do not imply a
dependency-free binary or complete static coverage.

The separate Ubuntu 22.04 tool-route job exercises the fixed schema closure,
invalid data, namespace setup, a live owned canary and denied validator fetches.
Generation requires its successful same-source/run/attempt receipt. Validation
snapshots both the BOM and all four checksum-pinned schema documents, checks the
closed reference set, and runs Goneat inside a user/network namespace. Namespace
failure has no ordinary-execution or sudo fallback. Local macOS adapter checks
do not establish the Ubuntu boundary.

Raw compiler, native, scanner, namespace and validation evidence stays in separate
CI artifacts. Only the successfully validated BOM enters the canonical release
inventory; failed or incomplete tool-route evidence blocks that inventory.
