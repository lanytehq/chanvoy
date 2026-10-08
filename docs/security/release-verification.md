# Release verification and public identity rotation

Obtain signing fingerprints from the independently reviewed release's tagged
`keys/expected-fingerprints.txt` and matching NDJSON, not solely from keys supplied
alongside the download. Check the public ASC primary and exact signing subkey;
reject expired/revoked keys or a signature by another key. The local maintainer
ceremony additionally verifies the pinned tag and exact CI run before signing.

## A single-platform consumer download

Download the chosen binary, its `.minisig`, `chanvoy.pub`, `chanvoy.gpg.asc`,
`checksums.txt` and `checksums.txt.asc`. Authenticate the public keys against your
independently pinned fingerprints, import the approved public GPG key into an
`verification-home` isolated keyring, then run:

```bash
minisign -Vm chanvoy-vX.Y.Z-linux-x86_64 -p chanvoy.pub
gpg --homedir verification-home --verify checksums.txt.asc checksums.txt
sha256sum -c checksums.txt --ignore-missing
```

Require success for the binary you downloaded. `--ignore-missing` permits the
other platforms and metadata to be absent; it does not authenticate the manifest.
Check the GPG signature before using its hashes. On macOS GNU coreutils supplies
`gsha256sum` if `sha256sum` is unavailable. Verify before executing the binary.

The additive manifests are `SHA256SUMS` and `SHA512SUMS`, each with GPG `.asc`
and minisign `.minisig` detached signatures. `checksums.txt` remains byte-identical
to `SHA256SUMS`, with the same GPG signature bytes. All legacy names remain in
v0.3.2; removal requires a later release and notice.

## Reviewed rotation

A new public identity is a separate independently reviewed change. The maintainer
approves existing public inputs; derivation exports a public ASC and TXT/NDJSON
pair with Decernor >=0.1.8. Exactly one primary and one live signing subkey are
selected. Replacement requires the reviewed digest of the old ASC or TXT, and
pair installation rolls back on failure. See [the checklist](../../RELEASE_CHECKLIST.md).

The rotation notice must name the effective version, old and re-derived new
fingerprints, exact signing subkey and verification commands. Tooling adoption
alone does not rotate keys or authorize donor fingerprints. Keep historical
verification guidance and anchors available for prior releases.

## v0.3.2 identity

Starting with v0.3.2, authenticate release keys using the tagged TXT/NDJSON
anchor pair and the [selected public GPG export](release-signing-keys.asc).
The identity changes from the v0.3.1 fingerprints below to:

| Key | v0.3.2 fingerprint |
|---|---|
| OpenPGP primary | `0CACA49B3119B6BC12B2CA11B9B485F294B9FE07` |
| Minisign decoded public blob SHA-256 | `c6c2a0d6e08842889ba07cf1ad28fcd9275336d86a0e868ed6070fdfbe818928` |

The exact OpenPGP signing subkey is
`DAB70DD758911B26BB45A08C3B75AC591449DEBE`. Manifest signatures must use this
subkey. The selected public export contains the primary and this signing
subkey. The maintainer selects it with the full fingerprint followed by `!`.

Using independently obtained anchors and Decernor >=0.1.8, verify the downloaded
public files before the signature and checksum commands above:

```bash
mkdir -m 700 verification-home
decernor fingerprint verify --anchors expected-fingerprints.txt \
  --anchors-ndjson expected-fingerprints.ndjson \
  --gpg chanvoy.gpg.asc --minisign chanvoy.pub
gpg --homedir verification-home --show-keys --with-subkey-fingerprint chanvoy.gpg.asc
```

Confirm the primary and signing-subkey fingerprints shown match this identity,
then import the verified public key into that isolated keyring:

```bash
gpg --homedir verification-home --import chanvoy.gpg.asc
gpg --homedir verification-home --status-fd 1 --verify checksums.txt.asc checksums.txt
```

Require a successful `VALIDSIG` for the exact signing subkey and primary above
before using the checksum commands. The key import and verification use only
this isolated keyring.
Use the historical identity for v0.3.1 downloads.

## Historical v0.3.1

The historical minisign decoded-public-blob SHA-256 is
`36a80acfa44f5cf9ac402d3ce8e51fcc083e5a1dca22180d6a0ea85b7e5340ad`.
The historical OpenPGP primary is
`83FCC69CB060EDB8374EDE0547AAC7D6EB946A84`.
Use those historical pins and the original v0.3.1 release's keys/signatures for
that release. Do not reinterpret it under a later public pin. The three consumer
commands above and [v0.3.1 notes](../releases/v0.3.1.md) remain applicable.
