SHELL := /bin/bash
MSRV := $(shell awk -F\" '/^rust-version =/ {print $$2; exit}' Cargo.toml)
GONEAT_FMT := @if command -v goneat >/dev/null 2>&1; then goneat format --types yaml,json,markdown --folders . --finalize-eof --quiet; else echo "goneat not found; skipping non-Rust formatting"; fi
GONEAT_ASSESS := @if command -v goneat >/dev/null 2>&1; then goneat assess . --categories lint --check; else echo "goneat not found; skipping goneat assess"; fi

.PHONY: all clean check fmt quality test build ensure-msrv msrv precommit prepush pr-final

all: check

clean:
	cargo clean

check:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets

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
	cargo +$(MSRV) check --workspace --all-targets --locked
	@echo "[ok] pr-final gate passed"

precommit: check fmt quality

prepush: precommit build msrv
