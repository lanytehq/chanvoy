SHELL := /bin/bash
MSRV := $(shell awk -F\" '/^rust-version =/ {print $$2; exit}' Cargo.toml)
GONEAT_FMT := @if command -v goneat >/dev/null 2>&1; then goneat format --types yaml,json,markdown --folders . --finalize-eof --quiet; else echo "goneat not found; skipping non-Rust formatting"; fi
GONEAT_ASSESS := @if command -v goneat >/dev/null 2>&1; then goneat assess . --categories lint --check; else echo "goneat not found; skipping goneat assess"; fi

# Userspace install location. Override per-OS if needed:
#   Linux / macOS: $HOME/.local/bin (default)
#   Windows / other: set LOCAL_BIN on the make invocation
LOCAL_BIN ?= $(HOME)/.local/bin

.PHONY: all clean check fmt quality test test-integration build build-release install ensure-msrv msrv precommit prepush pr-final

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

pr-final: ensure-msrv
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets
	cargo test --package chanvoy --test restart_harness -- --ignored
	cargo +$(MSRV) check --workspace --all-targets --locked
	@echo "[ok] pr-final gate passed"

precommit: check fmt quality

prepush: precommit build msrv
