# Wait on a DM

`chanvoy wait` can sit on a direct message without a team-channel id.

`--dm` and `--inbox` are mutually exclusive with positional `CHANNEL`,
repeated `--channel` fan-in, `--after-channel`, and `--team`. Self-posts
never wake.

## `--dm <username>` — one peer

Wait for a DM from **this** user. Do not pass a channel id.

```bash
chanvoy wait --dm dave-3leaps --after <post-id> --timeout 10m --json
chanvoy wait --dm dave-3leaps --timeout 4h --follow --follow-stdout
```

`--after` is a **Mattermost post id** on that conversation (drain first,
then exclusive `--after`). It is not an inbox cursor.

The JSON result names `peer_username` and canonical `dm_name`
(`{uid}__{uid}`) so callers never reverse a channel UUID.

Unknown, self, UUID, user-id, and `{uid}__{uid}` values refuse as **not a
waitable peer**.

## `--inbox` — any DM to this bot

Wait for **any** direct message to this bot, including a new DM opened
while armed. Do not pass a channel id.

```bash
chanvoy wait --inbox --after <inbox-cursor> --timeout 10m --json
chanvoy wait --inbox --timeout 4h --follow --follow-stdout
```

`--after` is an **inbox cursor** returned by a prior inbox result or
follow record (`inv1.…`). It is **not** a Mattermost post id. Passing a
post id refuses with that distinction.

JSON (and each inbox follow message record) labels four fields separately:

- `peer_username`
- `dm_name`
- `matched_post_id`
- `next_inbox_cursor`

Do not treat the cursor as a post id. Terminal follow records carry only
the last proven cursor (`inbox_cursor`). Cursor progress is
sink-acknowledged: a failed `--follow` write does not publish a later
cursor.

`--replace-wait` on inbox replaces only another inbox wait. It does not
silently replace `--dm` or a positional DM wait.

## Which `--after`?

| Selector | `--after` means |
| -------- | --------------- |
| positional channel / `--dm` | Mattermost post id on that conversation |
| `--inbox` | opaque inbox cursor (`inv1.…`) from a prior inbox result |

A post-id `--after` on `--inbox` is the usual operator miss. The CLI
refuses it instead of treating it as a cursor.
