# Production build evidence

Shipped receipt binaries use bundled AWS-LC; ordinary source builds without the
producer may select system AWS-LC.

`make build-release` and `make install` remain ordinary cached source builds.
They do not create production receipts. The install daemon report's artifact
comparison is also non-qualifying. `make build-release-receipt` displays guidance
and exits nonzero without building: Make supplies `MAKEFLAGS`, which the producer
must refuse rather than strip or treat as a trusted selector.

For a separately approved local final-candidate proof, run the producer directly
from a clean frozen checkout with no `target` directory, using Rust 1.89.0:

```sh
python3 scripts/build-production-binary.py \
  --root /absolute/frozen-checkout --platform macos-aarch64 \
  --expected-commit <full-commit> --mode local \
  --output /absolute/external-evidence
```

Platforms are `linux-x86_64`, `linux-aarch64` and `macos-aarch64`. Evidence must be
outside the source checkout. Every repeat needs a new clean checkout/output;
the producer never removes or refreshes old target files. Local receipts cannot
substitute for hosted candidate or actual tag-run shipping evidence. The release
ceremony stages the qualified CI artifacts.

The shared producer rejects incoming compiler, Cargo configuration, target and
native selector overrides, including empty values. Only its owned Cargo child
receives `AWS_LC_SYS_USE_SYSTEM=0`. It uses the ordinary standalone release,
locked-package compilation flags and records every selected AWS-LC build-script
invocation, its fresh script compilation, exact CC or CMake source path, script
output and observed prefixed static archive. A cached script event, system
library path, missing export or unknown builder is insufficient.

Before fixture compilation, `native-build-policy-v1` binds source, target,
mode, workflow/run/attempt, producer/policy source hashes, normal compiler
messages and the normal executable's SHA256 and byte count. Portable native
snapshots bind each observed script output and archive. Qualification verifies
that association against the normal and preserved executable and writes
`normal-build-inputs-v2`. Shipping admission requires those same associations,
the actual shipping payload and the existing same-tag/run integrity controls.
Old input receipts cannot establish this policy.

The fixed fail-closed bounds are 16 native invocations, 1 MiB per script output,
64 MiB per archive, 256 MiB total snapshots and 64 MiB per executable. Chunked
capture consumes the existing operation budget; synchronous kernel file reads
are not independently interruptible. Unknown, changed, missing, duplicated,
symlinked, escaping or oversized evidence refuses admission. These bounds need
actual three-platform qualification; they are not measured native sizes.

The separate shipping inventory job rehashes only artifact-relative snapshots
and payloads. It does not access original build-runner paths or Cargo caches.
Public inventory omits host paths and describes archives as observed build
inputs. This evidence does not establish which static archive members were
linked into the executable.
