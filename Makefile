SHELL := /bin/bash
MSRV := $(shell awk -F\" '/^rust-version =/ {print $$2; exit}' Cargo.toml)
GONEAT_FMT := @if command -v goneat >/dev/null 2>&1; then goneat format --types yaml,json,markdown --folders . --finalize-eof --quiet; else echo "goneat not found; skipping non-Rust formatting"; fi
GONEAT_ASSESS := @if command -v goneat >/dev/null 2>&1; then goneat assess . --categories lint --check; else echo "goneat not found; skipping goneat assess"; fi

# Userspace install location. Override per-OS if needed:
#   Linux / macOS: $HOME/.local/bin (default)
#   Windows / other: set LOCAL_BIN on the make invocation
LOCAL_BIN ?= $(HOME)/.local/bin

# Repo-root VERSION file is the source of truth for chanvoy's version.
# Cargo.toml versions across workspace + crates are synced from it via
# `make version-sync` (which uses cargo-set-version under the hood).
VERSION_FILE := VERSION

.PHONY: all clean check fmt quality test test-integration build build-release install ensure-msrv msrv precommit prepush pr-final
.PHONY: version version-patch version-minor version-major version-set version-sync version-check

all: check

clean:
	cargo clean

check:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets

test-integration:
	cargo test --package chanvoy --test restart_harness -- --ignored --nocapture

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

build-release:
	cargo build --release --package chanvoy

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
	@rm -f $(LOCAL_BIN)/chanvoy
	@cp target/release/chanvoy $(LOCAL_BIN)/chanvoy
	@echo "[ok] installed chanvoy to $(LOCAL_BIN)/chanvoy"
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

pr-final: ensure-msrv version-check
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets
	cargo test --package chanvoy --test restart_harness -- --ignored
	cargo +$(MSRV) check --workspace --all-targets --locked
	@echo "[ok] pr-final gate passed"

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
