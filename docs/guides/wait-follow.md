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

Each stdout or `--out` line is one `wait_follow_v1.event` record:

1. `armed` — a receipt that admission succeeded. Consumers must not treat it
   as work.
2. zero or more `backlog` or `live` records — exactly one message each;
   `tip` equals that message id and is the exclusive `--after` for a later
   new follow.
3. at most one terminal: `deadman`, `canceled`, `replaced`, or `failed`.

Message bodies are JSON-escaped, so a newline in a post cannot start a
second record. Live coalesce (batching several posts into one record) is
not available.

## After the follower ends

A **new** wait is admitted only after a stream **terminal record** *or*
**confirmed old-process exit**. Those are not the same: a writable sink
gets a terminal JSONL line before lease release; a sink failure exits 2
and releases the owner **without** a terminal record.

Resume from the last validated **live** or `backlog` `tip`. If the stream
emitted no message records, the previous explicit `--after` is still
valid. If neither exists, drain and re-establish a baseline before
re-arming. Self-posts never match.

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
