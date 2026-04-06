# Chanvoy

Chanvoy is the Mattermost/chat bridge peer for the Lanyte platform.

This repository currently contains the M1 control-plane scaffold:

- `chanvoy-core`: shared domain types, profile model, JSON-RPC envelopes, and Mattermost client
- `chanvoy-daemon`: local unix-socket daemon and profile-bound control plane
- `chanvoy-cli`: CLI client surface
- `chanvoy-mcp`: MCP bridge scaffold

## Security Note

Chanvoy M1 assumes the local Unix account is the trust boundary.

- Runtime and config files are permission-hardened (`0700` directories, `0600` files) to reduce accidental cross-process exposure.
- Admin-only operations are gated by explicit profile capability class.
- Chanvoy M1 does not attempt to protect one process from another process running as the same Unix user.

That same-user limitation is an acknowledged M1 boundary and should be revisited before adding richer daemon lifecycle ergonomics, auto-start behavior, or deployment models that span different Unix users or service accounts.
