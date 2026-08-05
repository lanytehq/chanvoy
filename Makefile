SHELL := /bin/bash
MSRV := $(shell awk -F\" '/^rust-version =/ {print $$2; exit}' Cargo.toml)
GONEAT_FMT := @if command -v goneat >/dev/null 2>&1; then goneat format --types yaml,json,markdown --folders . --finalize-eof --quiet; else echo "goneat not found; skipping non-Rust formatting"; fi
GONEAT_ASSESS := @if command -v goneat >/dev/null 2>&1; then goneat assess . --categories lint --check; else echo "goneat not found; skipping goneat assess"; fi

# Userspace install location. Mirrors the cross-platform convention used by
# sibling 3leaps tools (sfetch, kitfly):
#   Linux / macOS: $HOME/.local/bin/chanvoy
#   Windows:       $USERPROFILE/bin/chanvoy.exe
# Override either path with LOCAL_BIN= on the make invocation.
ifeq ($(OS),Windows_NT)
LOCAL_BIN ?= $(USERPROFILE)/bin
EXT := .exe
else
LOCAL_BIN ?= $(HOME)/.local/bin
EXT :=
endif

# Repo-root VERSION file is the source of truth for chanvoy's version.
# Cargo.toml versions across workspace + crates are synced from it via
# `make version-sync` (which uses cargo-set-version under the hood).
VERSION_FILE := VERSION

.PHONY: all clean check fmt quality test test-integration build build-release install ensure-msrv msrv precommit prepush pr-final
.PHONY: version version-patch version-minor version-major version-set version-sync version-check
.PHONY: sbom security-scan license-check release-prep release-smoke workflow-lint
.PHONY: release-preflight release-clean release-download release-checksums release-sign
.PHONY: release-export-keys release-verify-signatures release-verify-keys release-verify
.PHONY: release-notes release-upload release-undraft release-upload-all help

all: check

clean:
	cargo clean

check:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets

test-integration:
	cargo test --package chanvoy --test restart_harness -- --ignored --nocapture
	cargo test --package chanvoy --test post_show_and_thread -- --ignored --nocapture

fmt:
	cargo fmt
	$(GONEAT_FMT)

quality:
	cargo clippy --workspace --all-targets -- -D warnings
	$(GONEAT_ASSESS)

test:
	cargo test --workspace --all-targets

build:
	cargo build --workspace --all-targets

# PER-031 AC 7a (entarch 2026-05-11 P2 + cxotech absorption pin):
# release builds must use --locked so Cargo.lock is the trust anchor.
# Without --locked, a release runner can refresh Cargo.lock mid-build
# and ship binaries built against different transitive code than what
# was tested at tag time — defeating the signing/verification trust
# posture operators rely on. The contract lives here in the Makefile,
# not as a workflow-only bypass, so local `make build-release` and CI
# `make build-release` produce identical dependency resolution.
build-release:
	cargo build --release --locked --package chanvoy

# Install the release binary into $(LOCAL_BIN).
#
# Uses `rm -f` before `cp` intentionally. If a running chanvoy daemon
# was spawned from the current $(LOCAL_BIN)/chanvoy, overwriting that
# file in place (e.g. plain `cp` or `install`) leaves the running
# process referencing an inode macOS / Linux kernels may flag as
# "modified while in use," causing the next exec of that path to be
# killed with SIGKILL (observed on macOS 2026-04-23). Unlinking the
# directory entry first is the standard Unix idiom for replacing a
# binary that may still be referenced by running processes: existing
# daemons keep their own open inode until they exit on their own
# lifecycle, while new execs resolve to the fresh file.
install: build-release
	@mkdir -p $(LOCAL_BIN)
	@rm -f $(LOCAL_BIN)/chanvoy$(EXT)
	@cp target/release/chanvoy$(EXT) $(LOCAL_BIN)/chanvoy$(EXT)
	@echo "[ok] installed chanvoy to $(LOCAL_BIN)/chanvoy$(EXT)"
	@echo "     note: if a chanvoy daemon was already running, it"
	@echo "     keeps the previous binary until you restart it via"
	@echo "     'chanvoy daemon stop' + 'chanvoy auto-setup'"

ensure-msrv:
	@echo "Checking MSRV $(MSRV)..."
	@if ! rustup toolchain list | grep -q "$(MSRV)"; then \
		echo "Installing toolchain $(MSRV)..."; \
		rustup toolchain install $(MSRV) --profile minimal; \
	fi

msrv: ensure-msrv
	cargo +$(MSRV) check --workspace --all-targets --locked
	@echo "[ok] MSRV $(MSRV) verified"

pr-final: ensure-msrv version-check workflow-lint
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets
	cargo test --package chanvoy --test restart_harness -- --ignored
	cargo test --package chanvoy --test post_show_and_thread -- --ignored
	cargo +$(MSRV) check --workspace --all-targets --locked
	@echo "[ok] pr-final gate passed"

# PER-031 AC #10: actionlint guards .github/workflows/ from drift on the
# release surface. Skips with a hint when actionlint isn't installed so
# day-to-day dev on machines without the tool isn't blocked; release-cycle
# review and CI environments are expected to have it on PATH. Install:
# `brew install actionlint` (macOS) or `go install
# github.com/rhysd/actionlint/cmd/actionlint@latest`.
workflow-lint:
	@if command -v actionlint >/dev/null 2>&1; then \
		actionlint; \
		echo "[ok] actionlint clean"; \
	else \
		echo "[warn] actionlint not on PATH; skipping. Install via 'brew install actionlint'."; \
	fi

# ---- v0.2.1+ release-prep tooling ---------------------------------------
# Goneat (3leaps DX tool) handles SBOM, license compliance, and security
# scanning for chanvoy. Run via individual targets during dev or via
# `make release-prep` umbrella once before tagging a release. Not part of
# `pr-final` to keep day-to-day CI fast — these scans are the
# release-cycle gate, not the commit-cycle gate.
#
# Goneat install: `sfetch --repo fulmenhq/goneat --tag v0.5.10` (or a
# later tag). The targets below check for goneat presence and fail
# with a clear install hint if it isn't on PATH. Defensible failure
# mode for a release gate — letting a misconfigured environment ship
# un-scanned releases would be the worse trade.

sbom: ## Generate CycloneDX SBOM artifact for the current workspace
	@if ! command -v goneat >/dev/null 2>&1; then \
		echo "[!!] goneat not installed; skipping SBOM. Install via 'sfetch --repo fulmenhq/goneat'."; \
		exit 1; \
	fi
	@mkdir -p sbom
	@version=$$(awk -F'"' '/^version =/ {print $$2; exit}' crates/chanvoy-core/Cargo.toml); \
		goneat dependencies --sbom \
			--sbom-output "sbom/chanvoy-v$$version.cdx.json" \
			--quiet
	@echo "[ok] SBOM written under sbom/ (gitignored)"

security-scan: ## Run goneat security (cargo-audit + cargo-deny on Rust)
	@if ! command -v goneat >/dev/null 2>&1; then \
		echo "[!!] goneat not installed; skipping security scan."; \
		exit 1; \
	fi
	goneat security --fail-on high

license-check: ## Run goneat license compliance per .goneat/dependencies.yaml
	@if ! command -v goneat >/dev/null 2>&1; then \
		echo "[!!] goneat not installed; skipping license check."; \
		exit 1; \
	fi
	goneat dependencies --licenses --fail-on high

release-prep: pr-final license-check security-scan sbom ## Full release-cycle gate (slower than pr-final; run before tagging)
	@echo "[ok] release-prep gate passed"
	@echo "     pr-final ✓"
	@echo "     license-check ✓"
	@echo "     security-scan ✓"
	@echo "     SBOM generated under sbom/"
	@echo "     ready to tag"

# PER-032 Item J Tier-B — live-MM URL-shape smoke harness.
#
# Pinned release-cycle ordering (PER-030 RELEASE_CHECKLIST.md is canonical):
#   make release-prep      (commit-cycle gate — does NOT include this target)
#   make release-smoke     (this target — live MM + ephemeral channel)
#   make release-preflight (final pre-tag checks)
#   git tag -a vX.Y.Z      (only if smoke passed)
#   git push origin vX.Y.Z (only if smoke passed)
#
# Smoke FAILS the release cycle BEFORE any tag exists, draft release
# is created, or signed artifact is produced. The failure surface is
# "no release tag yet" — never "signed release that doesn't work."
#
# Deliberately NOT a dependency of release-prep (PR-032 AC #9):
# release-prep is a commit-cycle gate that runs in CI without live
# credentials; release-smoke is a release-cycle action that needs live
# Mattermost access and is invoked only at RC time.
release-smoke: ## PER-032 Tier-B — live-MM URL-shape smoke against a disposable test channel
	@bash scripts/release-smoke.sh

# ---- PER-030 signing rails ----------------------------------------------
# Manual-signing v0.2.2 baseline: CI (PER-031 release.yml) produces a
# draft release with binaries + checksums.txt + release notes; Dave
# runs the targets below locally to sign / verify / upload / undraft.
# Signing keys are NEVER in CI.
#
# Canonical release sequence (RELEASE_CHECKLIST.md is the source of
# truth for ordering):
#   make release-prep        — commit-cycle gate (license + security + SBOM)
#   make release-smoke       — PER-032 live-MM URL-shape gate
#   make release-preflight   — pre-tag readiness (this section, AC #4)
#   git tag -a vX.Y.Z && git push
#   (PER-031 GHA produces draft release)
#   make release-download    — fetch draft artifacts
#   make release-checksums   — regenerate checksums.txt locally
#   make release-sign        — minisign per-binary + GPG over checksums.txt
#   make release-verify      — verify signatures + key fingerprints
#   make release-upload      — attach signed artifacts (atomic)
#   make release-undraft     — flip draft → published (atomic)
#   make release-upload-all  — composite of upload + undraft
#
# RELEASE_DIR is the local working directory for a given release
# cycle. Derived from VERSION so the tag-mismatch foot-gun is
# impossible: every target operates on the same directory.
RELEASE_DIR ?= release/v$(shell cat $(VERSION_FILE))
RELEASE_TAG ?= v$(shell cat $(VERSION_FILE))

release-preflight: release-prep ## Pre-tag readiness gate — clean tree, version sync, no conflicting tag/release, tooling + signing keys present (AC #4)
	@echo "[..] release-preflight: pre-tag readiness checks"
	@if ! git diff --quiet HEAD; then \
		echo "[!!] working tree has uncommitted changes"; \
		git status --short; \
		exit 1; \
	fi
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "[!!] working tree has untracked files"; \
		git status --short; \
		exit 1; \
	fi
	@echo "[ok] working tree clean"
	@file_version=$$(cat $(VERSION_FILE)); \
	cargo_version=$$(awk -F'"' '/^version =/ {print $$2; exit}' Cargo.toml); \
	if [ "$$file_version" != "$$cargo_version" ]; then \
		echo "[!!] VERSION ($$file_version) != Cargo.toml ($$cargo_version)"; \
		echo "     run 'make version-sync' to resolve"; \
		exit 1; \
	fi
	@echo "[ok] VERSION + Cargo.toml in sync"
	@tag="v$$(cat $(VERSION_FILE))"; \
	if git rev-parse --verify "refs/tags/$$tag" >/dev/null 2>&1; then \
		echo "[!!] tag $$tag already exists locally"; \
		echo "     pick a fresh version, or delete the stale tag"; \
		exit 1; \
	fi; \
	if git ls-remote --tags origin "$$tag" 2>/dev/null | grep -q "$$tag"; then \
		echo "[!!] tag $$tag already exists on origin"; \
		exit 1; \
	fi
	@echo "[ok] no conflicting tag for v$$(cat $(VERSION_FILE))"
	@tag="v$$(cat $(VERSION_FILE))"; \
	if command -v gh >/dev/null 2>&1 && \
	   gh release view "$$tag" --repo lanytehq/chanvoy >/dev/null 2>&1; then \
		echo "[!!] GitHub release $$tag already exists"; \
		echo "     gh release view $$tag --repo lanytehq/chanvoy"; \
		exit 1; \
	fi
	@echo "[ok] no conflicting GitHub release for v$$(cat $(VERSION_FILE))"
	@for tool in gh minisign gpg; do \
		if ! command -v $$tool >/dev/null 2>&1; then \
			echo "[!!] $$tool not on PATH"; \
			exit 1; \
		fi; \
	done
	@echo "[ok] release tooling (gh + minisign + gpg) on PATH"
	@if [ -z "$${CHANVOY_MINISIGN_KEY:-}" ]; then \
		echo "[!!] CHANVOY_MINISIGN_KEY not set"; \
		echo "     export CHANVOY_MINISIGN_KEY=/path/to/minisign-secret-key"; \
		exit 1; \
	fi
	@if [ ! -f "$$CHANVOY_MINISIGN_KEY" ]; then \
		echo "[!!] CHANVOY_MINISIGN_KEY path not found: $$CHANVOY_MINISIGN_KEY"; \
		exit 1; \
	fi
	@echo "[ok] minisign signing key present at $$CHANVOY_MINISIGN_KEY"
	@if [ -z "$${CHANVOY_PGP_KEY_ID:-}" ]; then \
		echo "[!!] CHANVOY_PGP_KEY_ID not set"; \
		echo "     GPG signature over checksums.txt is mandatory for"; \
		echo "     v0.2.2 trust posture (devrev PR #33 review)"; \
		echo "     export CHANVOY_PGP_KEY_ID=<your-gpg-key-id>"; \
		exit 1; \
	fi
	@if ! gpg --list-secret-keys "$$CHANVOY_PGP_KEY_ID" >/dev/null 2>&1; then \
		echo "[!!] CHANVOY_PGP_KEY_ID not in gpg keyring: $$CHANVOY_PGP_KEY_ID"; \
		echo "     gpg --list-secret-keys to inspect"; \
		exit 1; \
	fi
	@echo "[ok] GPG signing key present in keyring ($$CHANVOY_PGP_KEY_ID)"
	@notes="docs/releases/v$$(cat $(VERSION_FILE)).md"; \
	if [ ! -f "$$notes" ]; then \
		echo "[!!] release notes missing at $$notes"; \
		echo "     create the file before pushing the tag"; \
		exit 1; \
	fi
	@echo "[ok] release notes present at docs/releases/v$$(cat $(VERSION_FILE)).md"
	@echo "[ok] release-preflight passed — ready to tag v$$(cat $(VERSION_FILE))"

release-clean: ## Remove the local release working directory
	@rm -rf release/
	@echo "[ok] release working directory cleaned"

release-download: ## Download draft-release artifacts from GitHub into $(RELEASE_DIR)
	@bash scripts/download-release-assets.sh $(RELEASE_TAG) $(RELEASE_DIR)

release-checksums: ## Regenerate checksums.txt locally over downloaded binaries
	@bash scripts/generate-checksums.sh $(RELEASE_DIR)

release-sign: ## Produce minisign per-binary + GPG over checksums.txt
	@bash scripts/sign-release-assets.sh $(RELEASE_TAG) $(RELEASE_DIR)

release-export-keys: ## Export public signing keys into $(RELEASE_DIR)
	@bash scripts/export-release-keys.sh $(RELEASE_DIR)

release-verify-signatures: ## Verify minisign + GPG signatures on signed artifacts
	@bash scripts/verify-signatures.sh $(RELEASE_DIR)

release-verify-keys: ## Verify public-key fingerprints match keys/expected-fingerprints.txt
	@bash scripts/verify-public-keys.sh $(RELEASE_DIR)

release-verify: release-verify-signatures release-verify-keys ## Composite — signatures + key fingerprints
	@echo "[ok] release-verify passed (signatures + key fingerprints)"

release-notes: ## Display the canonical release notes for the current VERSION
	@notes="docs/releases/v$$(cat $(VERSION_FILE)).md"; \
	if [ ! -f "$$notes" ]; then \
		echo "[!!] release notes missing at $$notes"; \
		exit 1; \
	fi; \
	cat "$$notes"

release-upload: release-verify ## Attach signed artifacts + public keys to the draft release (gates on release-verify; does NOT flip draft state)
	@bash scripts/upload-release-assets.sh $(RELEASE_TAG) $(RELEASE_DIR)

release-undraft: ## Flip the GitHub release from draft → published (atomic — does NOT touch assets)
	@if ! command -v gh >/dev/null 2>&1; then \
		echo "[!!] gh CLI is required"; \
		exit 1; \
	fi
	@# Explicit existence check first — without this, the previous
	@# implementation swallowed gh's "release not found" error and
	@# reported "already published" for a missing release. Per devrev
	@# PR #33 review, idempotency must distinguish "exists + already
	@# published" from "does not exist."
	@if ! gh release view $(RELEASE_TAG) --repo lanytehq/chanvoy >/dev/null 2>&1; then \
		echo "[!!] release $(RELEASE_TAG) not found on lanytehq/chanvoy"; \
		echo "     check the tag was pushed and the GHA workflow created the draft"; \
		exit 1; \
	fi
	@is_draft=$$(gh release view $(RELEASE_TAG) --repo lanytehq/chanvoy --json isDraft --jq .isDraft); \
	if [ "$$is_draft" = "true" ]; then \
		gh release edit $(RELEASE_TAG) --repo lanytehq/chanvoy --draft=false; \
		echo "[ok] $(RELEASE_TAG) flipped draft → published"; \
	elif [ "$$is_draft" = "false" ]; then \
		echo "[ok] $(RELEASE_TAG) already published (no-op; idempotent)"; \
	else \
		echo "[!!] unexpected isDraft value for $(RELEASE_TAG): '$$is_draft'"; \
		exit 1; \
	fi

release-upload-all: release-upload release-undraft ## Composite — verify + upload signed artifacts then flip to published (release-verify chains transitively via release-upload)

# ---- help -----------------------------------------------------------------
# Auto-grouped from `##` annotations on target lines. Targets prefixed
# with "release-" land under "Release operations" (per PER-030 AC #2).
help: ## Print available targets grouped by category
	@printf "\nchanvoy Makefile targets\n\n"
	@printf "Release operations:\n"
	@awk -F':.*## ' '/^release-[a-z][a-zA-Z0-9_-]*:.*## / {printf "  %-28s %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST) | sort
	@printf "\nVersion management:\n"
	@awk -F':.*## ' '/^version[a-zA-Z0-9_-]*:.*## / {printf "  %-28s %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST) | sort
	@printf "\nQuality + build:\n"
	@awk -F':.*## ' '/^[a-z][a-zA-Z0-9_-]*:.*## / && !/^release-/ && !/^version/ {printf "  %-28s %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST) | sort
	@printf "\nFull procedure: RELEASE_CHECKLIST.md\n\n"

precommit: check fmt quality

prepush: precommit build msrv version-check

# -----------------------------------------------------------------------------
# Version management
# -----------------------------------------------------------------------------
#
# Pattern follows ~/dev/3leaps/sysprims/Makefile (cxotech 2026-04-26 versioning
# convention rollout). VERSION at repo root is the SSOT; bump targets edit it
# AND propagate the new value into Cargo.toml across the workspace and per-crate
# manifests in one step. `version-sync` is also exposed standalone for cases
# where VERSION was edited directly. `version-check` verifies the SSOT/Cargo
# files agree and is hooked into `pr-final` and `prepush` so drift cannot land.
#
# Typical flow for a code-revision PR:
#
#   make version-patch          # 0.1.0 -> 0.1.1 in VERSION + Cargo.toml
#   git add VERSION Cargo.toml crates/*/Cargo.toml Cargo.lock
#   git commit -m "..."
#
# `version-sync` requires `cargo-set-version` (part of `cargo-edit`):
#   cargo install cargo-edit
#
# Note: cargo-set-version refuses to downgrade. To set a lower version,
# edit VERSION and the Cargo.toml files manually, then verify via
# `make version-check`.

version: ## Print current version
	@cat $(VERSION_FILE)

version-patch: ## Bump patch version (0.1.0 -> 0.1.1): Cargo.toml first, VERSION on success
	@current=$$(cat $(VERSION_FILE)); \
	major=$$(echo $$current | cut -d. -f1); \
	minor=$$(echo $$current | cut -d. -f2); \
	patch=$$(echo $$current | cut -d. -f3); \
	new_version="$$major.$$minor.$$((patch + 1))"; \
	if ! command -v cargo-set-version >/dev/null 2>&1; then \
		echo "[!!] cargo-set-version not installed (cargo install cargo-edit)"; \
		exit 1; \
	fi; \
	if ! cargo set-version --workspace "$$new_version"; then \
		echo "[!!] cargo set-version failed; VERSION not changed"; \
		echo "     (note: cargo-set-version refuses downgrades; edit manifests manually for those)"; \
		exit 1; \
	fi; \
	echo "$$new_version" > $(VERSION_FILE); \
	echo "Version bumped: $$current -> $$new_version (VERSION + Cargo.toml)"

version-minor: ## Bump minor version (0.1.0 -> 0.2.0): Cargo.toml first, VERSION on success
	@current=$$(cat $(VERSION_FILE)); \
	major=$$(echo $$current | cut -d. -f1); \
	minor=$$(echo $$current | cut -d. -f2); \
	new_version="$$major.$$((minor + 1)).0"; \
	if ! command -v cargo-set-version >/dev/null 2>&1; then \
		echo "[!!] cargo-set-version not installed (cargo install cargo-edit)"; \
		exit 1; \
	fi; \
	if ! cargo set-version --workspace "$$new_version"; then \
		echo "[!!] cargo set-version failed; VERSION not changed"; \
		echo "     (note: cargo-set-version refuses downgrades; edit manifests manually for those)"; \
		exit 1; \
	fi; \
	echo "$$new_version" > $(VERSION_FILE); \
	echo "Version bumped: $$current -> $$new_version (VERSION + Cargo.toml)"

version-major: ## Bump major version (0.1.0 -> 1.0.0): Cargo.toml first, VERSION on success
	@current=$$(cat $(VERSION_FILE)); \
	major=$$(echo $$current | cut -d. -f1); \
	new_version="$$((major + 1)).0.0"; \
	if ! command -v cargo-set-version >/dev/null 2>&1; then \
		echo "[!!] cargo-set-version not installed (cargo install cargo-edit)"; \
		exit 1; \
	fi; \
	if ! cargo set-version --workspace "$$new_version"; then \
		echo "[!!] cargo set-version failed; VERSION not changed"; \
		echo "     (note: cargo-set-version refuses downgrades; edit manifests manually for those)"; \
		exit 1; \
	fi; \
	echo "$$new_version" > $(VERSION_FILE); \
	echo "Version bumped: $$current -> $$new_version (VERSION + Cargo.toml)"

version-set: ## Set explicit version (V=X.Y.Z): Cargo.toml first, VERSION on success
	@if [ -z "$(V)" ]; then \
		echo "Usage: make version-set V=1.2.3"; \
		exit 1; \
	fi
	@current=$$(cat $(VERSION_FILE)); \
	new_version="$(V)"; \
	if ! command -v cargo-set-version >/dev/null 2>&1; then \
		echo "[!!] cargo-set-version not installed (cargo install cargo-edit)"; \
		exit 1; \
	fi; \
	if ! cargo set-version --workspace "$$new_version"; then \
		echo "[!!] cargo set-version failed; VERSION not changed"; \
		echo "     (note: cargo-set-version refuses downgrades; edit manifests manually for those)"; \
		exit 1; \
	fi; \
	echo "$$new_version" > $(VERSION_FILE); \
	echo "Version set: $$current -> $$new_version (VERSION + Cargo.toml)"

version-sync: ## Sync VERSION file to Cargo.toml across workspace and crates
	@ver=$$(cat $(VERSION_FILE)); \
	if ! command -v cargo-set-version >/dev/null 2>&1; then \
		echo "[!!] cargo-set-version not installed (cargo install cargo-edit)"; \
		echo "Manual update required: set version = \"$$ver\" in Cargo.toml + crates/*/Cargo.toml"; \
		exit 1; \
	fi; \
	if ! cargo set-version --workspace "$$ver"; then \
		echo "[!!] cargo set-version failed; Cargo.toml not synced"; \
		echo "     (note: cargo-set-version refuses downgrades; for those, edit manifests manually)"; \
		exit 1; \
	fi; \
	echo "[ok] Synced Cargo.toml to $$ver"

version-check: ## Verify VERSION file matches Cargo.toml versions (CI gate)
	@version_file=$$(cat $(VERSION_FILE)); \
	mismatched=0; \
	for f in Cargo.toml crates/*/Cargo.toml; do \
		ver=$$(grep -m1 "^version" "$$f" | cut -d'"' -f2); \
		if [ "$$ver" != "$$version_file" ]; then \
			echo "[!!] $$f: version=$$ver (expected $$version_file from VERSION)"; \
			mismatched=1; \
		fi; \
	done; \
	if [ $$mismatched -eq 0 ]; then \
		echo "[ok] VERSION ($$version_file) matches all Cargo.toml versions"; \
	else \
		echo "     Run 'make version-sync' to align Cargo.toml to VERSION"; \
		exit 1; \
	fi
