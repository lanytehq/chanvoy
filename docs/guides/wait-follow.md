# Held wait follow

`chanvoy wait --follow` keeps **one** wait armed and writes a JSONL stream
until deadman, replacement, cancellation, or a hard failure. It is a
**stream**, not a harness doorbell. A held process does not, by itself,
start a new agent turn.

## One sink, one owner

Follow requires exactly one sink:

```bash
chanvoy wait <channel> --follow --timeout 1h --after <id> --out PATH
chanvoy wait <channel> --follow --timeout 1h --after <id> --follow-stdout
```

Bare `--follow` is refused. `--out` and `--follow-stdout` cannot be combined.
The file sink is opened before daemon admission (append, mode `0600`, no
symlink). On a shared host, pass `--profile` so two seats do not replace
each other on the same channel.

Do not run a second wait on the same profile and channel. Do not detach and
forget a follower.

## JSONL stdout

`--follow-stdout` is **JSONL only**. Human text such as
`following new messages…` stays on stderr as a static breadcrumb and never
includes a post body.

Each stdout or `--out` line is one self-identifying JSONL record.

Without `--coalesce` (the default), records are `wait_follow_v1.event`:

1. `armed` — a receipt that admission succeeded. Consumers must not treat it
   as work.
2. zero or more `backlog` or `live` records — exactly one message each;
   `tip` equals that message id and is the exclusive `--after` for a later
   new follow.
3. at most one terminal: `deadman`, `canceled`, `replaced`, or `failed`.

`--coalesce <duration>` is **off unless you pass it**. Bare `5` and `5s` are
five seconds. Five seconds is recommended; ten seconds is the hard maximum.
A burst holds at most 32 messages; hitting 32 flushes without dropping a
match. Coalesced records use `wait_follow_v2.event` with `messages` of length
1 through 32. `tip` equals the **last** message id in that record and is the
resume baseline. `armed` is written immediately and is never delayed for the
window. A pending burst is flushed before a terminal record.

Message bodies are JSON-escaped, so a newline in a post cannot start a
second record.

## After the follower ends

A **new** wait is admitted only after a stream **terminal record** *or*
**confirmed old-process exit**. Those are not the same: a writable sink
gets a terminal JSONL line before lease release; a sink failure exits 2
and releases the owner **without** a terminal record.

Resume from the last validated **live** or `backlog` `tip` (the last
message id in that record). If the stream emitted no message records, the
previous explicit `--after` is still valid. If neither exists, drain and
re-establish a baseline before re-arming. Self-posts never match.

| Stream terminal | Exit |
| --------------- | ---: |
| `deadman` | 1 |
| `canceled` (`Ctrl-C`) | 130 |
| `replaced` / `failed` | 2 |

| Process outcome (may have no terminal line) | Exit |
| ------------------------------------------- | ---: |
| Sink write/flush failure | 2 |

After a sink failure: repair the sink, then resume from the last
validated tip (or the original `--after` / a fresh drain) only after the
old follower has exited.

## Which host posture

| Host | Sit with |
| ---- | -------- |
| Output-line monitor (including Grok `monitor`) | Supervised `--follow --follow-stdout`. Stay silent on `armed`. Do not exit on `live`. Redirect stderr so the breadcrumb is not a wake. |
| Codex / OpenCode (turn starts on process exit) | Foreground **one-shot** `wait` (no `--follow`). A held stream stays alive through the first post, so the tool call never returns on fire. |
| No background wake at all | Keep one-shot or follow in the **foreground** of the sitting turn and collect that process. |

Follow removes re-arm gaps only while that process lives. It is not a
substitute for a host that cannot wake on a stdout line.

## Mention filter

`--mention` completes the wait only when **this bot** is mentioned. It is
not any `@`, and not a substring of the username (`@bot-suffix` does not
match). It is logical AND with `--contains` and `--pattern` when those
are set. Non-mention peer posts are not tips and do not emit `live`
JSONL on `--follow --mention`. Self-posts never match.

`--mention` applies to a channel wait, fan-in, `--dm`, and `--inbox`.

## Direct-message follow

`--dm <username> --follow` waits on one peer conversation. `--after` is a
Mattermost post id on that DM, not an inbox cursor.

`--inbox --follow` waits on any DM to this bot. `--after` is the opaque
`inv1.` inbox cursor from a prior inbox result. Do not pass a channel id.

`--inbox --follow --coalesce` inherits the same window (default off,
recommended 5s, hard max 10s / 32). Coalesced records are
`wait_inbox_follow_v2.event` with per-item `peer_username` and `dm_name`.
There is no top-level `tip`; resume from `next_inbox_cursor` (an `inv1.`
cursor, not a post id).
