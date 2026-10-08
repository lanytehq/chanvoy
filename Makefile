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

.PHONY: all clean check fmt quality test test-integration build build-release install install-restart-daemons installer-reporting-test ensure-msrv msrv precommit prepush pr-final
.PHONY: version version-patch version-minor version-major version-set version-sync version-check
.PHONY: sbom security-scan license-check release-prep release-smoke workflow-lint
.PHONY: release-preflight release-guard-tag-version release-guard-release-target release-tag release-tag-push release-clean release-download release-checksums release-sign
.PHONY: release-export-keys release-verify-signatures release-verify-keys release-verify release-verify-identity insert-expected-fingerprints
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
	cargo test --package chanvoy --test per_038_wait -- --ignored --nocapture
	cargo test --package chanvoy --test per_040_wait -- --ignored --nocapture
	cargo test --package chanvoy --test per_043_wait_follow -- --ignored --nocapture
	cargo test --package chanvoy --test wait_direct_message -- --ignored --nocapture
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

# Guidance only: Make manufactures MAKEFLAGS, which the producer must refuse.
.PHONY: build-release-receipt
build-release-receipt: ## Show the direct production-receipt command (does not build)
	@echo 'Run directly from a clean, fresh-target frozen checkout:' >&2
	@echo 'python3 scripts/build-production-binary.py --root /absolute/checkout --platform <linux-x86_64|linux-aarch64|macos-aarch64> --expected-commit <full-commit> --mode local --output /absolute/external-evidence' >&2
	@echo 'This guidance target does not invoke Cargo or the producer.' >&2
	@exit 1

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
#
# Installing updates the CLI only. Running daemon candidates are reported,
# never automatically stopped or started. Manual migration requires independent
# ownership, same-candidate death and guarded runtime-cleanup confirmation.
# The legacy target name is retained as a reporting-only compatibility alias.
# Standalone reporting requires an explicitly designated independent artifact:
# CHANVOY_INSTALL_QUALIFIED_ARTIFACT=/path/to/qualified/chanvoy make install-restart-daemons
# Opt out: CHANVOY_INSTALL_SKIP_DAEMON_RESTART=1 make install
install: build-release
	@mkdir -p "$(LOCAL_BIN)"
	@rm -f "$(LOCAL_BIN)/chanvoy$(EXT)"
	@cp "target/release/chanvoy$(EXT)" "$(LOCAL_BIN)/chanvoy$(EXT)"
	@echo "[ok] installed chanvoy to $(LOCAL_BIN)/chanvoy$(EXT)"
	@CHANVOY_INSTALL_QUALIFIED_ARTIFACT="$(CURDIR)/target/release/chanvoy$(EXT)" $(MAKE) --no-print-directory install-restart-daemons

install-restart-daemons:
ifeq ($(OS),Windows_NT)
	@echo "[..] install-restart-daemons reports installed-daemon candidates; no automatic restart in this release."
	@echo "[!!] process discovery is Unix-only; use the reviewed manual ownership/death/cleanup procedure."
	@echo "[!!] daemon report: candidates=0 withheld=0 unresolved=1"
else
	@bash scripts/report-installed-daemons.sh "$(LOCAL_BIN)/chanvoy$(EXT)"
endif

installer-reporting-test:
	@bash scripts/report-installed-daemons.test.sh

ensure-msrv:
	@echo "Checking MSRV $(MSRV)..."
	@if ! rustup toolchain list | grep -q "$(MSRV)"; then \
		echo "Installing toolchain $(MSRV)..."; \
		rustup toolchain install $(MSRV) --profile minimal; \
	fi

msrv: ensure-msrv
	cargo +$(MSRV) check --workspace --all-targets --locked
	@echo "[ok] MSRV $(MSRV) verified"

pr-final: ensure-msrv version-check workflow-lint release-tooling-test
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets
	cargo test --package chanvoy --test restart_harness -- --ignored
	cargo test --package chanvoy --test per_038_wait -- --ignored
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

release-prep: pr-final release-tooling-test license-check security-scan sbom ## Full release-cycle gate (slower than pr-final; run before tagging)
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
#   make release-tag      (only if smoke passed)
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

# ---- Authenticated release provenance -------------------------------------
# Post-tag operations require an explicit cut, even when main has advanced.
RELEASE_TAG ?=
CHANVOY_RELEASE_TAG ?= $(RELEASE_TAG)
RELEASE_DIR ?= release/$(CHANVOY_RELEASE_TAG)
RELEASE_ENV = CHANVOY_RELEASE_TAG="$(CHANVOY_RELEASE_TAG)"

.PHONY: release-tooling-test release-prepare-tag-message release-fetch-ci-artifacts
.PHONY: release-create-draft release-stage-anchors release-verify-draft release-verify-published-tag
.PHONY: release-export-pin release-insert-anchors release-validate-pin

release-tooling-test: installer-reporting-test ## Synthetic-key and stub-remote provenance regression corpus
	@bash scripts/release-tooling-test.sh

release-preflight: release-prep ## Fresh quality gates and maintainer tag preflight
	@$(RELEASE_ENV) bash scripts/release-preflight.sh

release-guard-tag-version: ## Check canonical version for tag creation
	@$(RELEASE_ENV) bash scripts/release-guard-tag-version.sh

release-guard-release-target: ## Check receipt and published tag without requiring current main
	@$(RELEASE_ENV) bash scripts/release-verify-staged.sh "$(RELEASE_DIR)"

release-prepare-tag-message: ## Prepare external public tag message for review
	@$(RELEASE_ENV) bash scripts/release-tag-operator.sh prepare-message

release-tag: ## Create and verify the signed local tag only
	@$(RELEASE_ENV) bash scripts/release-tag-operator.sh local-tag

release-tag-push: ## Separately verify and push the exact signed tag
	@$(RELEASE_ENV) bash scripts/release-tag-operator.sh remote-push

release-clean: ## Refuse automatic cleanup of potentially partial release evidence
	@echo 'Inspect staging and its .anchor receipt before explicitly removing either.' >&2
	@exit 1

release-fetch-ci-artifacts: ## Stage exact verified CI artifacts with external receipt
	@$(RELEASE_ENV) bash scripts/release-fetch-ci-artifacts.sh "$(CHANVOY_RELEASE_TAG)" "$(RELEASE_DIR)"

release-download: release-fetch-ci-artifacts ## Compatibility alias: download CI artifacts, not a draft

release-stage-anchors: ## Stage exact tagged notes and anchors as inert data
	@$(RELEASE_ENV) bash scripts/stage-release-anchors.sh "$(RELEASE_DIR)"

release-checksums: ## Generate two manifests and byte-identical legacy alias
	@$(RELEASE_ENV) bash scripts/generate-checksums.sh "$(RELEASE_DIR)"

release-create-draft: ## Maintainer creates a draft from receipt-bound checksummed assets
	@$(RELEASE_ENV) bash scripts/release-create-draft.sh "$(CHANVOY_RELEASE_TAG)" "$(RELEASE_DIR)"

release-sign: ## Sign both manifests and the legacy per-binary surfaces
	@$(RELEASE_ENV) bash scripts/sign-release-assets.sh "$(RELEASE_DIR)"

release-export-keys: ## Export and validate exact public verification keys
	@$(RELEASE_ENV) bash scripts/export-release-keys.sh "$(RELEASE_DIR)"

release-verify-signatures: ## Verify all required signatures and metadata
	@$(RELEASE_ENV) bash scripts/verify-signatures.sh "$(RELEASE_DIR)"

release-verify-keys: ## Verify public keys against tagged paired anchors
	@$(RELEASE_ENV) bash scripts/verify-public-keys.sh "$(RELEASE_DIR)"

release-verify: release-verify-signatures ## Complete local signed-cut verification

release-verify-identity: release-guard-release-target ## Authenticate then execute the host binary
	@$(RELEASE_ENV) bash scripts/verify-release-binary-identity.sh "$(CHANVOY_RELEASE_TAG)" "$(RELEASE_DIR)"

release-verify-published-tag: ## Verify tag/object/commit using tagged public data after main advances
	@$(RELEASE_ENV) bash scripts/release-verify-published-tag.sh

release-upload: ## Upload only missing provenance names, never clobber
	@$(RELEASE_ENV) bash scripts/upload-release-assets.sh "$(RELEASE_DIR)"

release-verify-draft: ## Verify fresh remote bytes and authenticated host identity
	@$(RELEASE_ENV) bash scripts/release-verify-draft.sh "$(RELEASE_DIR)"

release-undraft: ## Separate guarded promotion; CHANVOY_CONFIRM_PUBLISH must equal tag
	@$(RELEASE_ENV) bash scripts/release-publish.sh "$(RELEASE_DIR)"

release-upload-all: ## Compatibility alias with ordered upload then separately cued promotion
	@$(MAKE) release-upload
	@$(MAKE) release-undraft

release-export-pin: ## Export an approved existing public pin (replacement requires explicit script flag)
	@bash scripts/release-export-pin.sh

release-validate-pin: ## Independently validate approved primary and exact signing subkey
	@bash scripts/release-validate-pin.sh

release-insert-anchors: ## Derive paired anchors (rotation requires explicit script flag)
	@bash scripts/release-insert-anchors.sh

insert-expected-fingerprints: release-insert-anchors ## Compatibility alias for paired, guarded derivation

release-notes: ## Display notes from the explicitly verified cut
	@$(RELEASE_ENV) bash scripts/release-verify-published-tag.sh
	@git cat-file blob "refs/tags/$(CHANVOY_RELEASE_TAG):docs/releases/$(CHANVOY_RELEASE_TAG).md"

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
