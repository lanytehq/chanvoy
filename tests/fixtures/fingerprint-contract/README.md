# Synthetic public fixtures for the fingerprint-contract tests

Public halves only. Synthetic test keys, not estate signing keys.
Do not treat these hex values as production pins.

| File | Kind |
| --- | --- |
| `chanvoy.pub` | minisign public |
| `chanvoy.gpg.asc` | OpenPGP public (primary + signing subkey) |
| `expected-valid.txt` | well-formed filled contract |
| `expected-duplicate-minisign.txt` | two minisign lines |
| `expected-extra-field.txt` | extra token on a contract line |
| `expected-unknown-algo.txt` | unknown algorithm |
| `expected-malformed-hex.txt` | uppercase minisign blob (must be lowercase) |

Secret key material is not in this tree. Re-generate with
`minisign -G -W` and `gpg --quick-gen-key` if the files need replacing.
