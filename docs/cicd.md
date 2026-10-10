# CI and release gates

Use [the release checklist](../RELEASE_CHECKLIST.md) for the ordered ceremony.
Each gate proves a different boundary. A successful source check does not prove
that the tag's shipping artifacts are ready to publish.

## Source qualification

`make pr-final` runs version and workflow checks, synthetic release-tooling
tests, Rust formatting, Clippy with warnings denied, workspace tests, the
restart, wait and post integration suites, and a locked Rust 1.89 workspace
check. The default Rust toolchain supplies formatting, Clippy and tests; the
final MSRV check explicitly selects Rust 1.89. Install actionlint locally so
workflow linting is exercised rather than reported as skipped.

`.github/workflows/check.yml` runs on PRs and pushes to main. Its nine jobs
cover the minimum toolchain, current stable Clippy and full `pr-final`, macOS
on both toolchains, integration tests, synthetic release provenance, optimized
native Linux x86 qualification, and the offline SBOM tool route. Inspect each
job's actual steps and exact commit when assessing its evidence. PR success
does not replace the post-merge checks on the resulting main commit.

The optimized qualification job preserves the normal production binary,
compiler receipts and lifecycle results. The SBOM tool-route job verifies the
pinned scanner's version, effective configuration and access to a synthetic
file through its production mount. It also tests the pinned validator's
immutable schema closure under Linux network denial. These jobs preserve
evidence even when a later step fails.

## Local release preparation

`make release-prep` includes `pr-final`, license checks, security scanning and
a checkout SBOM. Review findings as well as exit status: the configured
severity threshold can permit reported findings. The checkout inventory is
diagnostic; it is not the artifact-associated shipping SBOM.

`make release-preflight` first runs `release-scanner-preflight`, then the
release-prep gates and maintainer preflight. The scanner check uses the pinned
image with no network, a read-only root filesystem, dropped capabilities and
no new privileges. It scans one owned synthetic file and verifies the observed
source path and SHA-256. Its read-only snapshot is traversable across user
identities; the surrounding evidence directory remains private.

The Make target prints its retained evidence path before execution. By default
it creates a private directory beside the checkout. If Docker cannot access
that location, select an existing Docker-visible directory outside the checkout
with `CHANVOY_SCANNER_PREFLIGHT_ROOT`. Do not loosen container controls to make
the probe pass. A failed or unavailable probe stops preflight.

Live smoke is a separate operator step requiring the selected identity and
team. Run it with a binary built from the final cut and retain its PASS receipt.
It is not part of the credential-free PR gate. Repository readiness, signing
and tag push follow the checklist and require their separate approvals.

## Tag artifact qualification

`.github/workflows/release.yml` runs for the signed tag. It verifies the exact
annotated object and committed public pin, qualifies the offline tool route,
and builds and tests the three native shipping binaries on their respective
runners. The shipping SBOM job consumes those exact binaries and compiler
receipts. It scans each artifact, associates production dependencies, validates
the versioned CycloneDX result offline, and preserves its evidence. The final
inventory job requires those results before assembling the exact base assets.

The workflow has read-only repository permissions and receives no signing
inputs. It uploads artifacts; local receipt-bound staging, signing, fresh
download verification and publication remain separate maintainer operations.

## Handling a failed gate

Record the commit, tag object when applicable, run ID, attempt, failing step
and retained artifact before changing source or rerunning. Keep the first
failure alongside later results. An isolated pass does not explain an earlier
failure, and a sandbox-denied check is blocked rather than passed.

Inspect the failing command and its evidence. Scanner admission, artifact
scanning and schema validation are separate stages; identify which was reached.
Do not substitute an unrelated green job for the required native or artifact
proof, silently change policy allowances, or infer ownership or death from an
inconclusive process inspection.

Source corrections belong on a reviewed branch with fresh normal gates. A
rerun of an existing tag uses that tag's unchanged source; it cannot consume a
new main commit. If a corrected release cut is needed, the maintainer decides
the tag's disposition and repeats the checklist for the new exact cut. Never
force-update a signed tag or erase failed evidence as part of a retry.
