# Chanvoy

**AI-native team chat bridge with delegation and multi-platform support**

## Elevator Pitch

Chanvoy does for team chat what mlvoy does for email: it gives AI agents a
seat at the table in the tools where teams already work. An agent running
through Chanvoy joins Mattermost channels and Slack workspaces as a
first-class participant — reading context, responding to mentions, posting
status updates — all under the same delegation and autonomy-gating model
that governs every other Lanyte communication channel.

Where mlvoy bridges the inbox, Chanvoy bridges the war room.

## The Problem

AI agents today interact with humans through dedicated chat interfaces —
Claude in a browser, Copilot in an IDE, a bot behind a slash command. These
are airlocks: the human goes to the agent's space. But real team
coordination happens in Mattermost, Slack, and Teams — and agents aren't
there.

The gap creates friction:

1. **Context loss**: an agent finishes work but can't announce it where the
   team will see it. Someone has to relay the message.
2. **Instruction indirection**: a human writes a task in Slack, then
   copy-pastes it into the agent's interface. The original thread loses
   traceability.
3. **Coordination blindness**: agents working on related tasks can't observe
   each other's status updates in the team channel. The human becomes a
   message router.

Chanvoy eliminates this by making agents native participants in the
platforms where coordination already happens.

## What It Is

A **headless team chat peer** that connects to the Lanyte core via IPC and
bridges to workspace chat providers. It is both a **reader** (observing
channel activity, receiving mentions) and a **bridge/gateway** (posting
messages, reacting, replying in threads) — hence the `-voy` suffix, in the
same family as mlvoy.

```
Team Chat Platforms              Chanvoy                    Lanyte Core
┌──────────────┐           ┌──────────────────┐       ┌──────────────────┐
│  Mattermost  │◄─WebSocket─►│                │       │                  │
│  (channels,  │  REST API  │  Chat Reader    │       │  Orchestrator    │
│   threads)   │           │  + Bridge        │◄─IPC──►│  (routing,       │
└──────────────┘           │                  │  ch260 │   autonomy gate) │
┌──────────────┐           │  Delegation &    │       │                  │
│    Slack     │◄─Socket────►│  Permission     │       │  Gateway         │
│  (channels,  │  Mode     │  Enforcement     │       │  (schema         │
│   threads)   │  Web API  │                  │       │   validation)    │
└──────────────┘           └──────────────────┘       └──────────────────┘
```

## What It Is Not

- **Not a chatbot framework.** Chanvoy doesn't decide what to say. The
  orchestrator and LLM decide. Chanvoy is pure transport + access control.
- **Not a Slack/Mattermost app builder.** No slash commands, no interactive
  components, no app directory listing. It's a bridge, not a platform feature.
- **Not a person-to-person messenger.** WhatsApp, SMS, Signal, and Telegram
  are phone-number-bound, person-to-person platforms with different identity,
  cost, and compliance models. Those belong to a future `textvoy` peer.

## Core Concepts

### The `-voy` Pattern

Chanvoy follows the `-voy` (from envoy) naming convention established by
mlvoy. A `-voy` tool has two halves:

1. **Reader**: understands the domain. Chanvoy parses channel messages,
   threads, reactions, mentions — the semantics of team chat.
2. **Bridge/Gateway**: connects to external providers. Chanvoy speaks
   Mattermost REST/WebSocket and Slack Web API/Socket Mode.

This distinguishes it from a `-bolt` (like idpbolt), which is a specialized
proxy with no domain interpretation.

### Delegation Model

Borrowed from mlvoy and generalized through the peer contract (STD-006):

- **Principal**: the workspace admin or channel owner granting access
- **Delegate**: the AI agent receiving channel access
- **Permissions**: which channels to monitor, whether posting is allowed,
  whether thread replies are allowed
- **Autonomy gate**: posting a message requires a `gate_token` from the
  orchestrator, just like sending an email requires one in mlvoy

### Push-First, Request-Second

Unlike mlvoy (primarily request/response — "search my inbox"), Chanvoy is
**push-first**: messages arrive asynchronously as they're posted to
monitored channels. The agent receives `inbound_message` and
`inbound_mention` events without requesting them.

Request/response verbs exist too (`chat_list_channels`, `chat_read_history`)
but the primary interaction model is event-driven. This has architectural
implications for the orchestrator — it must handle unsolicited events, not
just request/response cycles.

### Addressing

In Mattermost (and similarly in Slack):

- **Roles → channels**: `#lanyte-cxotech`, `#lanyte-devlead`, `#lanyte-ops`
- **Agents → bot users**: `@agent-rust`, `@agent-go`
- **Humans → their normal accounts**: no separate identity needed

An agent monitoring `#lanyte-devlead` sees the same messages any team member
would. When it posts, the message appears from its bot user identity with
full attribution.

## Why Now

Three converging factors:

1. **mlvoy proved the model.** Delegation, autonomy gating, and the peer
   contract work. Chanvoy applies the same proven patterns to a different
   communication domain.

2. **Agents are becoming teammates, not tools.** Long-running autonomous
   agents (devlead, devrev, prodmktg) are doing real work across multiple
   repos for hours at a time. They need to coordinate — with each other and
   with humans — in the place where the team already coordinates.

3. **Self-hosted chat is available.** Mattermost can be self-hosted (aligning
   with Lanyte's self-hosted philosophy), and Slack Socket Mode eliminates
   the need for public webhook endpoints. The infrastructure prerequisites
   are met.

## Scope

### In Scope (MVP)

- Mattermost backend (first target — Dave building server)
- Slack backend (second target — Socket Mode preferred)
- Channel monitoring with configurable channel list
- Inbound event delivery (`inbound_message`, `inbound_mention`)
- Outbound posting with autonomy gate (`chat_send`, `chat_thread_reply`)
- Channel discovery (`chat_list_channels`)
- History retrieval (`chat_read_history`)
- Reactions (`chat_react`)
- Delegation and permission enforcement
- IPC peer contract compliance (STD-006)

### Out of Scope

- **SMS/WhatsApp/Signal/Telegram** — future `textvoy` peer
- **Interactive Slack components** (modals, buttons, dropdowns) — not a bot
  framework
- **Multi-provider bridging** (Mattermost ↔ Slack relay) — one provider per
  instance
- **Voice/video** — different modality entirely
- **Presence/typing indicators** — theater, not function

## Name

**`chanvoy`** — from **chan**nel + en**voy**.

Selected via namelens study (2026-03-11). 11/11 available across .com, .dev,
.io, cargo, npm, pypi, GitHub. "Chan" names the core abstraction (workspace
channels) rather than the overloaded category ("chat"). Follows the
`[domain-abbreviation] + voy` pattern: `ml` + `voy` = mail envoy,
`chan` + `voy` = channel envoy.

## Relationship to mlvoy

| Aspect | mlvoy | chanvoy |
|--------|-------|---------|
| Domain | Email (IMAP/SMTP/JMAP) | Team chat (Mattermost/Slack) |
| IPC Channel | 256 (MAIL) | 260 (CHAT) |
| Primary model | Request/response | Push-first (events) |
| Autonomy gate | mail_send | chat_send, chat_thread_reply, chat_react |
| Delegation | Per-account, folder-level | Per-workspace, channel-level |
| Provider auth | App passwords, OAuth | Bot tokens, Socket Mode |
| Self-hosted option | IMAP (any provider) | Mattermost |

Both share: peer contract compliance (STD-006), `request_id` correlation,
`delegation_id` scoping, `gate_token` for write ops, typed error envelope.

## References

- mlvoy staging: `~/dev/lanytehq/mlvoy/`
- mlvoy ARCHITECTURE.md: `~/dev/lanytehq/mlvoy/ARCHITECTURE.md`
- PER-002 architecture spec: `lanyte-productbook-internal/content/projmgmt/peers/PER-002-chanvoy-architecture.md`
- Peer contract: STD-006 (draft, pending ratification)
- Channel 256 schema (mlvoy reference): `lanyte-crucible/schemas/ipc/channel_256.schema.json`
