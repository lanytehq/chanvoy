use std::path::PathBuf;
use std::process;
use std::process::Stdio;
use std::{env, ffi::OsStr};

use chanvoy_core::{
    check_search_operator_conflicts, list_profiles, load_active_profile, load_profile, load_token,
    parse_time_window, pid_path_for_profile, socket_path_for_profile, store_active_profile,
    store_profile, AckResult, AttentionListResult, AttentionShowResult, AttentionSource,
    CapabilityClass, Channel, ChanvoyScopes, CheckResult, CredentialMode, DaemonHealthState,
    DaemonStatus, DmConversation, Identity, LegacyChannel, MattermostClient, Message, Notification,
    PinResult, PostReceipt, Profile, ProfileStatus, Provider, ReactionResult, SearchResult,
    SeedCursorsResult, SeededChannelOutcome, TimeWindowDefaultUnit, UnpinResult,
    UnreadNotifications, WaitResult, WsConnectionState,
};
use chanvoy_daemon::{daemon_client, ping, ping_full, start, status, stop, DaemonError};
use chrono::{TimeZone, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use thiserror::Error;
use tokio::process::Command;

#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Daemon(#[from] DaemonError),
    #[error(transparent)]
    Core(#[from] chanvoy_core::CoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("bootstrap error: {0}")]
    Bootstrap(String),
    #[error(transparent)]
    Resolver(chanvoy_core::ResolverError),
}

impl From<chanvoy_core::ResolverError> for CliError {
    fn from(value: chanvoy_core::ResolverError) -> Self {
        CliError::Resolver(value)
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "chanvoy",
    version,
    about = "Mattermost control-plane client for Lanyte"
)]
struct Cli {
    #[arg(long, global = true)]
    profile: Option<String>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: CommandSet,
}

#[derive(Debug, Subcommand)]
enum CommandSet {
    #[command(subcommand)]
    Daemon(DaemonCommand),
    #[command(subcommand)]
    Profile(ProfileCommand),
    Whoami,
    Channels(ChannelsArgs),
    Dms,
    Read(ReadArgs),
    Check(CheckArgs),
    Post(PostArgs),
    #[command(subcommand)]
    Dm(DmCommand),
    Notify(NotifyArgs),
    Notifications(ReadWindowArgs),
    Wait(WaitArgs),
    /// Fetch a channel's pinned posts. Pure read, no cursor side
    /// effects. Uses the cross-team channel resolver; accepts
    /// <team>/<channel> syntax and the --team flag.
    Pinned(PinnedArgs),
    /// Advance the attention cursor to the channel's current latest
    /// post without fetching content. Uses the cross-team channel
    /// resolver; accepts <team>/<channel> syntax.
    Ack(AckArgs),
    /// Add an emoji reaction to a post under the bot's identity.
    /// Channel positional for multi-provider portability (Slack's
    /// reactions API requires channel + message_ts even though
    /// Mattermost keys by post-id alone). Idempotent on duplicate-react.
    React(ReactArgs),
    /// Remove the bot's emoji reaction. Idempotent on missing-reaction
    /// (success exit).
    Unreact(ReactArgs),
    /// Pin a post under the bot's identity. Channel positional matches
    /// the cross-team γ hybrid resolver convention; accepts
    /// <team>/<channel> syntax and the --team flag. Idempotent on
    /// already-pinned: JSON output surfaces `was_already_pinned`.
    Pin(PinArgs),
    /// Unpin a post. Symmetric to `pin`. Idempotent on already-unpinned;
    /// JSON output surfaces `was_already_unpinned`.
    Unpin(PinArgs),
    /// Channel-scoped search via Mattermost's
    /// `/teams/{id}/posts/search` endpoint. Channel positional and
    /// required (cross-channel search not yet supported). Refuses
    /// with a diagnostic on inline operator conflicts (`in:`,
    /// `from:`, `before:` / `after:`).
    Search(SearchArgs),
    #[command(subcommand)]
    Channel(ChannelCommand),
    /// Bootstrap: create/refresh profile from identity env and ensure daemon is healthy.
    AutoSetup(AutoSetupArgs),
    /// Inspect daemon-held attention state (cursors, staleness verdicts).
    /// Strictly read-only: never mutates daemon state, never issues
    /// Mattermost API calls.
    #[command(subcommand)]
    Attention(AttentionCommand),
}

#[derive(Debug, Subcommand)]
enum AttentionCommand {
    /// List tracked channels and cursor state for a profile.
    List,
    /// Show single-channel attention-state detail.
    Show(AttentionShowArgs),
}

#[derive(Debug, Args)]
struct ChannelsArgs {
    /// Filter to a single team. Without this, `chanvoy channels`
    /// lists every team the bot is a member of, grouped.
    #[arg(long, conflicts_with = "primary_team")]
    team: Option<String>,
    /// Print only the profile's primary team in the pre-cross-team
    /// single-team format. Back-compat escape hatch for tooling that
    /// depends on the old shape; the legacy JSON shape is preserved
    /// exactly (no `last_post_at` field added).
    #[arg(long, conflicts_with = "team")]
    primary_team: bool,
    /// Sort channels by recency. `active` = most-recent-first within
    /// each team group; channels with missing or zero activity sort
    /// last within their group. Preserves cross-team grouping; does
    /// NOT flatten globally.
    #[arg(long, conflicts_with = "primary_team", value_parser = ["active"])]
    sort: Option<String>,
}

#[derive(Debug, Args)]
struct AttentionShowArgs {
    /// Channel name.
    channel: String,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct AutoSetupArgs {
    /// Do not set the resulting profile as active. Use when bootstrapping or repairing a
    /// secondary profile without stealing active-profile resolution (multi-profile operators).
    #[arg(long)]
    no_activate: bool,
    /// PER-035: register an identity-reduction policy on this profile.
    /// The value is the bare family profile name to reduce to. After
    /// bootstrap, channel-targeted writes whose resolved channel lives
    /// outside this profile's team post under the family identity
    /// instead — no per-call `--profile` discipline needed. The scope
    /// marker is the profile's own `team_name` (set from the sourced
    /// identity); there is no separate `--reduce-outside-team` flag.
    /// Omitting the flag on a refresh preserves any existing policy;
    /// passing a new value updates it (surfaced as a refresh).
    #[arg(long, value_name = "FAMILY_PROFILE")]
    reduce_profile: Option<String>,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Start,
    Serve,
    Stop,
    Status,
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    List,
    Active,
    Create(ProfileCreateArgs),
    CreateFromEnv(ProfileCreateFromEnvArgs),
    /// PER-035 diagnostic: report a profile's identity + reduction
    /// policy. Read-only; operates on profile storage directly (no
    /// daemon, no resolver) so it works for a freshly-provisioned
    /// stream profile before its daemon is up.
    Show(ProfileShowArgs),
}

#[derive(Debug, Args)]
struct ProfileShowArgs {
    /// Profile name to inspect (e.g. `dataeng-galaxy-s2`).
    name: String,
}

#[derive(Debug, Subcommand)]
enum DmCommand {
    Send(DmSendArgs),
    Read(DmReadArgs),
    #[command(external_subcommand)]
    Raw(Vec<String>),
}

#[derive(Debug, Subcommand)]
enum ChannelCommand {
    Create(ChannelCreateArgs),
    Archive(ChannelNameArgs),
    Restore(ChannelNameArgs),
    AddMember(ChannelAddMemberArgs),
}

#[derive(Debug, Args)]
struct ReadArgs {
    channel: String,
    /// Time window for the read. Accepts s/m/h/d suffixes; bare
    /// integer = minutes (today's semantics). Mutually exclusive
    /// with --after / --since-last-mine / --since-bootstrap.
    #[arg(
        long,
        conflicts_with_all = ["after", "since_last_mine", "since_bootstrap"],
        long_help = "Time window for the read. Bare integer = minutes (today's default). Accepted suffixes: s/m/h/d (e.g., 30s, 5m, 4h, 2d). Rejected: uppercase 'M', 'mo' (months/minutes ambiguity). Mutually exclusive with --after / --since-last-mine / --since-bootstrap.",
    )]
    since: Option<String>,
    #[arg(long, conflicts_with_all = ["since", "since_last_mine", "since_bootstrap"])]
    after: Option<String>,
    #[arg(long, conflicts_with_all = ["since", "after", "since_bootstrap"])]
    since_last_mine: bool,
    /// Bounded most-recent-N posts (default N=50; override with
    /// --limit). Use this to bootstrap into a long channel without
    /// scanning history.
    #[arg(
        long,
        conflicts_with_all = ["since", "after", "since_last_mine"],
        long_help = "Bounded most-recent-N posts (default 50; override with --limit). Use this to bootstrap into a long channel without scanning history. Replaces the --since 999999 hack."
    )]
    since_bootstrap: bool,
    /// Hard cap on the result set. Composes with any read-mode flag
    /// — `--limit` truncates the existing read-mode result; it does
    /// NOT add full-window pagination semantics. Bare `--limit N`
    /// (no read-mode flag) is rejected — use
    /// `--since-bootstrap --limit N` for "give me the latest N".
    #[arg(long)]
    limit: Option<u32>,
    /// Advance the attention cursor to the latest post **returned**
    /// by this read (mode-independent rule). No-op when zero posts
    /// are returned.
    #[arg(long)]
    advance: bool,
    /// Explicit team override for cross-team channel resolution
    /// (per-invocation only). Equivalent to the `<team>/<channel>`
    /// positional syntax. When unset, the cross-team resolver tries
    /// the profile's primary team first, then falls back across
    /// other teams the bot is a member of.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct ReadWindowArgs {
    /// Time window for notifications. Accepts s/m/h/d suffixes;
    /// bare integer = minutes (today's semantics).
    ///
    /// Resolution note: minute-rounded — sub-minute suffixes (e.g.
    /// `30s`) round up to the next whole minute because the underlying
    /// MM notifications surface is minute-keyed. For second-precise
    /// time windows, use `chanvoy read --since` (millisecond precision
    /// against MM `posts?since=`).
    ///
    /// `--unread` interaction: this value is still parsed and
    /// validated (a malformed suffix on either path still rejects
    /// loudly), but the parsed window is not used for `--unread`
    /// counts — those count since the stored anchor cursor instead.
    #[arg(
        long,
        default_value = "1440",
        long_help = "Time window for notifications. Bare integer = minutes (today's default; default 1440 = 24h). Accepted suffixes: s/m/h/d. Rejected: uppercase 'M', 'mo'. Resolution: minute-rounded (sub-minute suffixes round up to the next whole minute; the underlying MM notifications surface is minute-keyed). --unread interaction: the value is still parsed/validated (malformed suffix rejects loudly on either path), but the parsed window is not used for --unread counts (those count since the stored anchor cursor)."
    )]
    since: String,
    /// Filter to unread mentions only. The parsed `--since` value is
    /// unused on this branch — unread counts run from the stored
    /// anchor cursor, not from a time window. (`--since` is still
    /// parsed/validated for shape, so malformed suffixes reject.)
    #[arg(long)]
    unread: bool,
}

#[derive(Debug, Args)]
struct PinnedArgs {
    channel: String,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct AckArgs {
    channel: String,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct CheckArgs {
    channel: String,
    #[arg(long)]
    after: Option<String>,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct WaitArgs {
    channel: String,
    /// Timeout window for the wait. Bare integer = minutes (today's
    /// default; default 10m). Accepts s/m/h/d suffixes.
    #[arg(
        long,
        default_value = "10",
        long_help = "Wait timeout. Bare integer = minutes (today's default; default 10 = 10m). Accepted suffixes: s/m/h/d (e.g., 30s, 5m, 4h, 2d). Rejected: uppercase 'M', 'mo'."
    )]
    timeout: String,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct PostArgs {
    channel: String,
    message: String,
    /// When set, the post is created as a thread reply under the
    /// named parent post (Mattermost `root_id` / Slack `thread_ts`
    /// semantic). Channel resolution unchanged from the top-level
    /// post case. Validation: refuse with a clear diagnostic if the
    /// parent doesn't exist on the resolved channel.
    #[arg(long)]
    reply_to: Option<String>,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct SearchArgs {
    /// Channel positional + required (cross-channel search not yet
    /// supported). Accepts `<team>/<channel>` syntax for cross-team
    /// resolution.
    channel: String,
    /// Search query — passed through to Mattermost's search endpoint
    /// as the `terms` field after chanvoy composes its owned scopes
    /// (`in:<resolved-channel>` always; `from:<author>` and
    /// `after:<date>` if `--from` / `--since` are set). Inline
    /// operators that conflict with chanvoy-owned scopes refuse
    /// with a clear diagnostic; non-conflicting operators pass
    /// through verbatim.
    query: String,
    /// Cap result count (default 20).
    #[arg(long, default_value_t = 20)]
    limit: u32,
    /// Narrow to a specific author (folded into the Mattermost
    /// `terms` field as `from:<author>`). Conflicts with an inline
    /// `from:` in the query — refuses with a diagnostic.
    #[arg(long)]
    from: Option<String>,
    /// Time-window suffix (s/m/h/d). Folded into the Mattermost
    /// `terms` field as `after:<computed-date>` (date granularity is
    /// the Mattermost-native surface). Conflicts with an inline
    /// `before:` / `after:` in the query.
    #[arg(long)]
    since: Option<String>,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct ReactArgs {
    /// Channel context (positional, required for multi-provider
    /// portability). Accepts `<team>/<channel>` syntax for cross-team
    /// resolution.
    channel: String,
    /// Post ID to react/unreact on. Format matches `chanvoy post`'s
    /// returned ID and `chanvoy read --json`'s `id` field.
    post_id: String,
    /// Emoji name. Bare names (`+1`, `eyes`) preferred; colon-wrapped
    /// MM-UI form (`:+1:`) is accepted with the colons stripped before
    /// the API call.
    emoji: String,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct PinArgs {
    /// Channel context (positional, required). Accepts
    /// `<team>/<channel>` syntax for cross-team resolution; uses the
    /// γ hybrid resolver (same shape as `pinned` / `react`).
    channel: String,
    /// Post ID to pin / unpin. Format matches `chanvoy post`'s
    /// returned ID and `chanvoy read --json`'s `id` field.
    post_id: String,
    /// Explicit team override for cross-team channel resolution.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct DmSendArgs {
    username: String,
    message: String,
}

#[derive(Debug, Args)]
struct DmReadArgs {
    username: String,
    #[arg(long, default_value_t = 60)]
    since: u64,
}

#[derive(Debug, Args)]
struct NotifyArgs {
    bot_username: String,
    message: String,
}

#[derive(Debug, Args)]
struct ChannelCreateArgs {
    name: String,
    display_name: String,
    purpose: Option<String>,
    /// Explicit team override. When unset, the channel lands on the
    /// profile's primary team (legacy default). When set, the
    /// channel is created on the named alternate team (which must
    /// be a team the bot is a member of). Cross-team channel
    /// creation parallels the cross-team channel resolver used for
    /// reads and writes.
    #[arg(long)]
    team: Option<String>,
}

#[derive(Debug, Args)]
struct ChannelNameArgs {
    name: String,
}

#[derive(Debug, Args)]
struct ChannelAddMemberArgs {
    channel: String,
    username: String,
}

#[derive(Debug, Args)]
struct ProfileCreateArgs {
    name: String,
    role: String,
    scope: String,
    bot_username: String,
    server_url: String,
    #[arg(long = "env-name")]
    env_name: String,
    /// Team name (e.g., `org-lanytehq`). Defaults to `org-${scope}`
    /// derived from the positional `<scope>` argument; pass
    /// explicitly only when the team name does not follow the
    /// convention.
    #[arg(long)]
    team_name: Option<String>,
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = CliCredentialMode::EnvName)]
    credential_mode: CliCredentialMode,
    #[arg(long, value_enum, default_value_t = CliCapabilityClass::Standard)]
    capability_class: CliCapabilityClass,
    #[arg(long)]
    activate: bool,
}

#[derive(Debug, Args)]
struct ProfileCreateFromEnvArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    team_name: Option<String>,
    #[arg(long = "env-name", default_value = "LANYTE_MM_TOKEN")]
    env_name: String,
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = CliCredentialMode::EnvName)]
    credential_mode: CliCredentialMode,
    #[arg(long, value_enum, default_value_t = CliCapabilityClass::Standard)]
    capability_class: CliCapabilityClass,
    #[arg(long)]
    activate: bool,
}

#[derive(Debug, Clone, ValueEnum)]
enum CliCredentialMode {
    EnvName,
    EnvFile,
    SeclusorRun,
}

#[derive(Debug, Clone, ValueEnum)]
enum CliCapabilityClass {
    Standard,
    Elevated,
}

pub async fn run() -> Result<(), CliError> {
    init_tracing();
    let cli = Cli::parse();
    execute(cli).await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .try_init();
}

async fn execute(cli: Cli) -> Result<(), CliError> {
    // auto-setup is the bootstrap / repair surface and must not depend on resolving
    // an existing persisted profile — a malformed unrelated profile would otherwise
    // block the very path meant to fix it. Dispatch before resolve_profile_name.
    // The --profile flag (cli.profile) acts as an explicit name override for
    // auto-setup; it is NOT resolved against the persisted profile set.
    if let CommandSet::AutoSetup(args) = cli.command {
        return handle_auto_setup(cli.json, cli.profile.as_deref(), args).await;
    }
    // PER-012 secrev follow-up: profile-management verbs operate on
    // profile storage directly and do not need a resolved target — and
    // running them through the resolver breaks bootstrap. A fresh
    // operator (including a new-org adopter) running
    // `chanvoy profile list` or `chanvoy profile create` with no env,
    // no daemons, and an empty config dir would otherwise hit
    // `CannotResolve` before the command surface even ran. Dispatch
    // them before the resolver, like `auto-setup`.
    if let CommandSet::Profile(command) = cli.command {
        return handle_profile(cli.json, command).await;
    }
    // Side-effecting verbs that could disrupt another operator's daemon
    // on a shared dev machine resolve via explicit sources only.
    // Read/inspect/post verbs may consult the broader fallback chain.
    // Only `daemon stop` qualifies in the current verb surface; widening
    // requires an explicit policy choice in the brief.
    let policy = match &cli.command {
        CommandSet::Daemon(DaemonCommand::Stop) => chanvoy_core::FallbackPolicy::ExplicitOnly,
        _ => chanvoy_core::FallbackPolicy::AllowReadFallbacks,
    };
    let profile = resolve_profile_name(cli.profile.as_deref(), policy)?;
    match cli.command {
        CommandSet::AutoSetup(_) | CommandSet::Profile(_) => {
            unreachable!("dispatched above")
        }
        CommandSet::Daemon(command) => handle_daemon(&profile, cli.json, command).await,
        CommandSet::Whoami => print_identity(cli.json, &daemon_client(&profile).whoami().await?),
        CommandSet::Channels(args) => handle_channels_command(&profile, cli.json, args).await,
        CommandSet::Dms => print_value(cli.json, &daemon_client(&profile).list_dms().await?),
        CommandSet::Read(args) => {
            // PER-023 §Resolved Decisions (PR #47): bare `read --limit N`
            // (no read-mode flag) is rejected with diagnostic suggesting
            // `--since-bootstrap --limit N`. Loud failure on
            // ambiguous-intent input; reject-then-relax preserves
            // optionality for a future shorthand.
            if args.limit.is_some()
                && args.since.is_none()
                && args.after.is_none()
                && !args.since_last_mine
                && !args.since_bootstrap
            {
                return Err(CliError::Bootstrap(
                    "`--limit` requires an explicit read-mode flag — use \
                     `--since-bootstrap --limit N` for 'give me the latest N posts', \
                     or `--since <window> --limit N` to cap a time-window read. \
                     Bare `read --limit N` is rejected."
                        .to_string(),
                ));
            }
            let since_secs = match args.since.as_deref() {
                Some(raw) => Some(
                    parse_time_window(raw, TimeWindowDefaultUnit::Minutes)
                        .map_err(CliError::Bootstrap)?,
                ),
                None => None,
            };
            print_value(
                cli.json,
                &daemon_client(&profile)
                    .read_channel(
                        &args.channel,
                        since_secs,
                        args.after.clone(),
                        args.since_last_mine,
                        args.since_bootstrap,
                        args.limit,
                        args.advance,
                        args.team.clone(),
                    )
                    .await?,
            )
        }
        CommandSet::Check(args) => match daemon_client(&profile)
            .check_channel(&args.channel, args.after.clone(), args.team.clone())
            .await?
        {
            result if result.has_new_messages => print_value(cli.json, &result),
            result => {
                print_value(cli.json, &result)?;
                process::exit(1);
            }
        },
        CommandSet::Post(args) => print_receipt(
            cli.json,
            "posted",
            &daemon_client(&profile)
                .post_message(
                    &args.channel,
                    &args.message,
                    args.team.clone(),
                    args.reply_to.clone(),
                )
                .await?,
        ),
        CommandSet::Dm(DmCommand::Send(args)) => print_dm_receipt(
            cli.json,
            &args.username,
            &daemon_client(&profile)
                .direct_message(&args.username, &args.message)
                .await?,
        ),
        CommandSet::Dm(DmCommand::Read(args)) => print_value(
            cli.json,
            &daemon_client(&profile)
                .read_direct_messages(&args.username, args.since)
                .await?,
        ),
        CommandSet::Dm(DmCommand::Raw(args)) => handle_dm_raw(&profile, cli.json, args).await,
        CommandSet::Notify(args) => print_notify_receipt(
            cli.json,
            &args.bot_username,
            &daemon_client(&profile)
                .notify(&args.bot_username, &args.message)
                .await?,
        ),
        CommandSet::Notifications(args) => {
            let since_secs = parse_time_window(&args.since, TimeWindowDefaultUnit::Minutes)
                .map_err(CliError::Bootstrap)?;
            print_value(
                cli.json,
                &if args.unread {
                    serde_json::from_value::<UnreadNotifications>(
                        daemon_client(&profile)
                            .notifications(since_secs, true)
                            .await?,
                    )?
                } else {
                    return print_value(
                        cli.json,
                        &serde_json::from_value::<Vec<Notification>>(
                            daemon_client(&profile)
                                .notifications(since_secs, false)
                                .await?,
                        )?,
                    );
                },
            )
        }
        CommandSet::Wait(args) => {
            let timeout_secs = parse_time_window(&args.timeout, TimeWindowDefaultUnit::Minutes)
                .map_err(CliError::Bootstrap)?;
            if !cli.json {
                eprintln!(
                    "waiting for new message in #{} (timeout: {}s)...",
                    args.channel, timeout_secs
                );
            }
            match daemon_client(&profile)
                .wait_channel(&args.channel, timeout_secs, args.team.clone())
                .await
            {
                Ok(result) => {
                    if !cli.json && !result.messages.is_empty() {
                        eprintln!("--- new message ---");
                    }
                    print_value(cli.json, &result)
                }
                Err(DaemonError::Rpc {
                    code: -32005,
                    message,
                }) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "timeout": true,
                                "channel": args.channel,
                                "message": message
                            }))?
                        );
                    } else {
                        eprintln!(
                            "timeout: no new messages in #{} after {} seconds",
                            args.channel, timeout_secs
                        );
                    }
                    process::exit(1);
                }
                Err(error) => Err(error.into()),
            }
        }
        CommandSet::Pinned(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .pinned_channel(&args.channel, args.team.clone())
                .await?,
        ),
        CommandSet::Ack(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .ack_channel(&args.channel, args.team.clone())
                .await?,
        ),
        CommandSet::React(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .react_post(&args.channel, &args.post_id, &args.emoji, args.team.clone())
                .await?,
        ),
        CommandSet::Unreact(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .unreact_post(&args.channel, &args.post_id, &args.emoji, args.team.clone())
                .await?,
        ),
        CommandSet::Pin(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .pin_post(&args.channel, &args.post_id, args.team.clone())
                .await?,
        ),
        CommandSet::Unpin(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .unpin_post(&args.channel, &args.post_id, args.team.clone())
                .await?,
        ),
        CommandSet::Search(args) => {
            // PER-025 AC #4a: scan the query for inline operators that
            // conflict with chanvoy-owned scopes BEFORE issuing the
            // daemon RPC. Refuses with a diagnostic naming the
            // conflicting flag/arg explicitly per the broadened pin
            // from entarch's #brief-per-025 review.
            let scopes = ChanvoyScopes {
                channel_arg: true, // channel positional is always set
                from_flag: args.from.is_some(),
                since_flag: args.since.is_some(),
            };
            check_search_operator_conflicts(&args.query, &scopes).map_err(CliError::Bootstrap)?;
            // PER-023 time-window suffix parsing on `--since` (the
            // PER-025 brief soft-deps on PER-023 for this exact path).
            let since_secs = match args.since.as_deref() {
                Some(raw) => Some(
                    parse_time_window(raw, TimeWindowDefaultUnit::Minutes)
                        .map_err(CliError::Bootstrap)?,
                ),
                None => None,
            };
            print_value(
                cli.json,
                &daemon_client(&profile)
                    .search_channel(
                        &args.channel,
                        &args.query,
                        Some(args.limit),
                        args.from.clone(),
                        since_secs,
                        args.team.clone(),
                    )
                    .await?,
            )
        }
        CommandSet::Channel(ChannelCommand::Create(args)) => print_value(
            cli.json,
            &daemon_client(&profile)
                .create_channel(
                    &args.name,
                    &args.display_name,
                    args.purpose,
                    args.team.clone(),
                )
                .await?,
        ),
        CommandSet::Channel(ChannelCommand::Archive(args)) => {
            daemon_client(&profile).archive_channel(&args.name).await?;
            print_bool_action(
                cli.json,
                serde_json::json!({ "archived": true, "channel": args.name }),
                &format!("archived: #{}", args.name),
            )
        }
        CommandSet::Channel(ChannelCommand::Restore(args)) => {
            daemon_client(&profile).restore_channel(&args.name).await?;
            print_bool_action(
                cli.json,
                serde_json::json!({ "restored": true, "channel": args.name }),
                &format!("restored: #{}", args.name),
            )
        }
        CommandSet::Channel(ChannelCommand::AddMember(args)) => {
            daemon_client(&profile)
                .add_member(&args.channel, &args.username)
                .await?;
            print_bool_action(
                cli.json,
                serde_json::json!({
                    "added": true,
                    "channel": args.channel,
                    "username": args.username,
                }),
                &format!("added: @{} → #{}", args.username, args.channel),
            )
        }
        CommandSet::Attention(AttentionCommand::List) => {
            let result = daemon_client(&profile).attention_list().await?;
            print_json_or_text(cli.json, &result, &render_attention_list_text(&result))
        }
        CommandSet::Attention(AttentionCommand::Show(args)) => {
            let result = daemon_client(&profile)
                .attention_show(&args.channel, args.team.clone())
                .await?;
            print_json_or_text(cli.json, &result, &render_attention_show_text(&result))
        }
    }
}

async fn handle_dm_raw(profile: &str, json: bool, args: Vec<String>) -> Result<(), CliError> {
    if args.first().map(String::as_str) == Some("read") {
        let username = args.get(1).ok_or_else(dm_usage_error)?;
        let since = parse_since_arg(&args[2..])?;
        return print_value(
            json,
            &daemon_client(profile)
                .read_direct_messages(username, since)
                .await?,
        );
    }

    let username = args.first().ok_or_else(dm_usage_error)?;
    if args.len() < 2 {
        return Err(dm_usage_error());
    }
    let message = args[1..].join(" ");
    print_dm_receipt(
        json,
        username,
        &daemon_client(profile)
            .direct_message(username, &message)
            .await?,
    )
}

fn parse_since_arg(args: &[String]) -> Result<u64, CliError> {
    match args {
        [] => Ok(60),
        [flag, value] if flag == "--since" => value
            .parse::<u64>()
            .map_err(|_| CliError::Bootstrap("invalid --since value for dm read".to_string())),
        _ => Err(dm_usage_error()),
    }
}

fn dm_usage_error() -> CliError {
    CliError::Bootstrap(
        "usage: chanvoy dm <username> <message>\n       chanvoy dm read <username> [--since <minutes>]"
            .to_string(),
    )
}

async fn handle_daemon(profile: &str, json: bool, command: DaemonCommand) -> Result<(), CliError> {
    match command {
        DaemonCommand::Start => {
            if let Ok(health) = ping(profile).await {
                return print_json_or_text(
                    json,
                    &health,
                    &format!(
                        "daemon already running for profile {} at {}",
                        health.profile_name,
                        health.socket_path.display()
                    ),
                );
            }
            let exe = std::env::current_exe()?;
            Command::new(exe)
                .arg("--profile")
                .arg(profile)
                .arg("daemon")
                .arg("serve")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            for _ in 0..20 {
                if let Ok(health) = ping(profile).await {
                    return print_json_or_text(
                        json,
                        &health,
                        &format!(
                            "daemon listening for profile {} at {}",
                            health.profile_name,
                            health.socket_path.display()
                        ),
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Err(DaemonError::NotRunning(profile.to_string()).into())
        }
        DaemonCommand::Serve => {
            let health = start(profile).await?;
            print_json_or_text(
                json,
                &health,
                &format!(
                    "daemon listening for profile {} at {}",
                    health.profile,
                    health.socket_path.display()
                ),
            )
        }
        DaemonCommand::Stop => {
            stop(profile).await?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"stopped": true, "profile": profile})
                    )?
                );
            } else {
                println!("stopped daemon for profile {profile}");
            }
            Ok(())
        }
        DaemonCommand::Status => {
            let daemon_status = status(profile).await?;
            print_value(json, &daemon_status)
        }
    }
}

async fn handle_profile(json: bool, command: ProfileCommand) -> Result<(), CliError> {
    match command {
        ProfileCommand::List => print_value(json, &list_profiles()?),
        ProfileCommand::Active => {
            // PER-012: display the marker file's contents directly. The
            // pre-PER-012 shortcut of falling back to the resolver-derived
            // name when the marker was empty conflated "what's persisted"
            // with "what would resolve right now" — operator could see a
            // profile name and reasonably believe it was activated when it
            // was just env-derived. Truthful answer: print null/empty when
            // no marker is set.
            let active = load_active_profile()?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "active_profile": active }))?
                );
            } else {
                match active {
                    Some(name) => println!("{name}"),
                    None => println!("(none)"),
                }
            }
            Ok(())
        }
        ProfileCommand::Create(args) => {
            let profile = profile_from_create_args(&args);
            store_profile_and_maybe_activate(json, &profile, args.activate)
        }
        ProfileCommand::CreateFromEnv(args) => {
            let profile = profile_from_env_args(&args).await?;
            store_profile_and_maybe_activate(json, &profile, args.activate)
        }
        ProfileCommand::Show(args) => handle_profile_show(json, &args.name),
    }
}

/// PER-035: report a profile's identity + reduction policy. The
/// `reduce` block, when present, is reported with the family target and
/// a one-line semantics reminder so an operator can confirm a stream
/// profile is configured before relying on auto-reduction. Surfaces
/// whether the named `use_profile` exists on disk (a dangling target is
/// the negative case the daemon refuses to start on).
fn handle_profile_show(json: bool, name: &str) -> Result<(), CliError> {
    let profile = load_profile(name)?;
    let reduce_target_exists = profile
        .reduce
        .as_ref()
        .map(|r| load_profile(&r.use_profile).is_ok());
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "name": profile.name,
                "role": profile.role,
                "scope": profile.scope,
                "bot_username": profile.bot_username,
                "team_name": profile.team_name,
                "server_url": profile.server_url,
                "reduce": profile.reduce.as_ref().map(|r| serde_json::json!({
                    "use_profile": r.use_profile,
                    "use_profile_exists": reduce_target_exists,
                })),
            }))?
        );
        return Ok(());
    }
    println!("profile:      {}", profile.name);
    println!(
        "identity:     {} ({}/{})",
        profile.bot_username, profile.role, profile.scope
    );
    println!("team_name:    {}", profile.team_name);
    println!("server_url:   {}", profile.server_url);
    match &profile.reduce {
        Some(policy) => {
            println!("reduce:       → {} (identity-reduce)", policy.use_profile);
            println!(
                "              posts inside {} keep this identity; posts elsewhere reduce to {}",
                profile.team_name, policy.use_profile
            );
            if reduce_target_exists == Some(false) {
                println!(
                    "  WARNING:    reduce target '{}' does not exist on disk — the daemon will \
                     refuse to start until it is created or the policy is corrected",
                    policy.use_profile
                );
            }
        }
        None => println!("reduce:       (none — this profile posts as itself everywhere)"),
    }
    Ok(())
}

async fn handle_auto_setup(
    json: bool,
    profile_override: Option<&str>,
    args: AutoSetupArgs,
) -> Result<(), CliError> {
    let desired =
        match build_desired_profile_from_env(profile_override, args.reduce_profile.as_deref()) {
            Ok(profile) => profile,
            Err(err) => {
                print_auto_setup_error(json, "env_input", &err.to_string())?;
                process::exit(EXIT_ENV_INPUT);
            }
        };

    let existing = match chanvoy_core::load_profile(&desired.name) {
        Ok(existing) => Some(existing),
        Err(chanvoy_core::CoreError::ProfileNotFound(_)) => None,
        Err(err) => return Err(err.into()),
    };

    let action = decide_profile_action(&desired, existing.as_ref());
    let (profile_state, persisted_profile, persisted_identity, refresh_diff) = match action {
        ProfileAction::Create => {
            let (validated, identity) = match validate_and_finalize_profile(desired).await {
                Ok(pair) => pair,
                Err(err) => return exit_on_preflight(json, err),
            };
            store_profile(&validated)?;
            (ProfileState::Created, validated, identity, Vec::new())
        }
        ProfileAction::Refresh(diff) => {
            let existing = existing.clone().expect("Refresh implies existing profile");
            let merged = merge_forward_for_refresh(desired, &existing);
            let (validated, identity) = match validate_and_finalize_profile(merged).await {
                Ok(pair) => pair,
                Err(err) => return exit_on_preflight(json, err),
            };
            store_profile(&validated)?;
            // A running daemon holds Profile + token in-memory from the last start
            // (`chanvoy-daemon::start`). Refresh writes to disk but the live daemon
            // keeps its stale copy, so env_name / credential_mode / capability_class
            // changes — and crucially a rotated token — would not take effect until
            // an unrelated restart. Stop the daemon if present (even when healthy)
            // so the subsequent `ensure_daemon_running` spawns a fresh one against
            // the updated profile. The Reuse path gets analogous zombie protection
            // inside `ensure_daemon_running` itself.
            if let Err(err) = stop_daemon_if_present(&validated.name).await {
                print_auto_setup_error(json, "daemon_refresh_stop", &err.to_string())?;
                process::exit(EXIT_DAEMON_FAILED);
            }
            (ProfileState::Refreshed, validated, identity, diff)
        }
        ProfileAction::Reuse => {
            let existing = existing.expect("Reuse implies existing profile");
            // AC #3 (amended brief): team/token validation must happen against
            // the *current* env credential before reporting success, on every
            // path including Reuse. Without this, a token rotated in place
            // under the same env var name would be unobserved and the report
            // would claim success based purely on token-source presence from
            // `check_token_available`, never proving the token actually works
            // for the configured team.
            let (validated, identity) = match validate_and_finalize_profile(existing.clone()).await
            {
                Ok(pair) => pair,
                Err(err) => return exit_on_preflight(json, err),
            };
            // If the env credential now authenticates as a different bot than
            // the persisted profile, the running daemon (if any) is holding a
            // token for a different identity. Promote to a refresh-style
            // reload so the daemon uses the env-current credential and the
            // stored profile reflects reality. bot_username drift on a Reuse
            // is not classified as EXIT_IDENTITY_DRIFT because the brief lists
            // bot_username as "derived / ignored for drift" at the
            // decide_profile_action phase; treating a post-whoami change as a
            // surfaced refresh is consistent with that rule.
            if validated.bot_username != existing.bot_username {
                store_profile(&validated)?;
                if let Err(err) = stop_daemon_if_present(&validated.name).await {
                    print_auto_setup_error(json, "daemon_refresh_stop", &err.to_string())?;
                    process::exit(EXIT_DAEMON_FAILED);
                }
                let diff = vec![ProfileFieldDiff {
                    field: "bot_username".to_string(),
                    from: existing.bot_username.clone(),
                    to: validated.bot_username.clone(),
                }];
                (ProfileState::Refreshed, validated, identity, diff)
            } else {
                (ProfileState::Reused, validated, identity, Vec::new())
            }
        }
        ProfileAction::IdentityDrift(diff) => {
            print_identity_drift_error(json, &diff)?;
            process::exit(EXIT_IDENTITY_DRIFT);
        }
    };

    // PER-012 AC #3: persist the active marker unconditionally when
    // activate_requested. Previous logic skipped the store when the
    // file already matched, which was order-dependent in subtle ways
    // (a stale on-disk write between load and store, or any external
    // mutation, could leave the printed "active:" line out of sync
    // with the file). Always-persist removes the gap entirely; the
    // store is one small idempotent write.
    let activate_requested = !args.no_activate;
    let is_active_now = if activate_requested {
        store_active_profile(&persisted_profile.name)?;
        true
    } else {
        load_active_profile()?
            .as_deref()
            .map(|name| name == persisted_profile.name)
            .unwrap_or(false)
    };

    let daemon_state = match ensure_daemon_running(&persisted_profile, &persisted_identity).await {
        Ok(state) => state,
        Err(err) => {
            print_auto_setup_error(json, "daemon_start", &err.to_string())?;
            process::exit(EXIT_DAEMON_FAILED);
        }
    };

    let seed_outcomes: Vec<SeedOutcome> = match daemon_client(&persisted_profile.name)
        .seed_cursors()
        .await
    {
        Ok(SeedCursorsResult { outcomes }) => outcomes.into_iter().map(SeedOutcome::from).collect(),
        Err(DaemonError::NotRunning(_)) => {
            // Daemon died between the health check and the seed RPC. This is a
            // daemon health failure (exit 4), not a per-channel seed problem
            // (exit 1). auto-setup's contract requires a healthy daemon at the
            // point of success — soft-degraded would mask the collapse.
            print_auto_setup_error(
                json,
                "daemon_unreachable",
                "daemon socket unavailable during seed_cursors RPC — daemon exited after the health check",
            )?;
            process::exit(EXIT_DAEMON_FAILED);
        }
        Err(err) => {
            // Other failures (upstream Mattermost errors during enumeration,
            // serialization issues) surface as a single synthetic seed failure.
            // Profile is still coherent; readiness flips to degraded (exit 1).
            vec![SeedOutcome::Failed {
                channel: "<membership-enumeration>".to_string(),
                reason: err.to_string(),
            }]
        }
    };
    let degraded = seed_outcomes
        .iter()
        .any(|outcome| matches!(outcome, SeedOutcome::Failed { .. }));

    let report = AutoSetupReport {
        profile_name: persisted_profile.name.clone(),
        bot_username: persisted_profile.bot_username.clone(),
        profile_state,
        daemon_state,
        is_active: is_active_now,
        refresh_diff,
        seed_outcomes,
        degraded,
    };

    print_auto_setup_report(json, &report)?;
    if degraded {
        process::exit(EXIT_SOFT_DEGRADED);
    }
    Ok(())
}

const EXIT_SOFT_DEGRADED: i32 = 1;
const EXIT_ENV_INPUT: i32 = 2;
const EXIT_PREFLIGHT_FAILED: i32 = 3;
const EXIT_DAEMON_FAILED: i32 = 4;
const EXIT_IDENTITY_DRIFT: i32 = 5;

fn exit_on_preflight(json: bool, err: CliError) -> Result<(), CliError> {
    let (code, message) = classify_preflight_error(&err);
    print_auto_setup_error(json, code, &message)?;
    process::exit(EXIT_PREFLIGHT_FAILED);
}

fn classify_preflight_error(err: &CliError) -> (&'static str, String) {
    if let CliError::Core(chanvoy_core::CoreError::Api { status, message }) = err {
        let code = match status.as_u16() {
            401 => "token_invalid",
            403 => "bot_not_in_team",
            404 => "team_missing",
            _ => "preflight_failed",
        };
        return (code, format!("{status}: {message}"));
    }
    ("preflight_failed", err.to_string())
}

fn merge_forward_for_refresh(mut desired: Profile, existing: &Profile) -> Profile {
    // Non-env-owned fields survive refresh to avoid wiping operator-configured state
    // (monitored_channels, IPC config, env_file). Env-owned fields come from `desired`.
    desired.monitored_channels = existing.monitored_channels.clone();
    desired.ipc = existing.ipc.clone();
    if desired.env_file.is_none() && existing.env_file.is_some() {
        desired.env_file = existing.env_file.clone();
    }
    // PER-035: reduction policy is flag-owned but sticky. Omitting
    // `--reduce-profile` on a refresh (desired.reduce == None) preserves
    // any previously-configured policy; passing the flag carries the new
    // value forward and is surfaced as a refresh diff. (Removing a
    // policy is YAGNI — no `--no-reduce` today; rewrite the profile or
    // edit the TOML.)
    if desired.reduce.is_none() {
        desired.reduce = existing.reduce.clone();
    }
    desired
}

fn print_auto_setup_error(json: bool, code: &str, message: &str) -> Result<(), CliError> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "error_code": code,
                "message": message,
            }))?
        );
    } else {
        eprintln!("{code}: {message}");
    }
    Ok(())
}

fn print_identity_drift_error(json: bool, diff: &[ProfileFieldDiff]) -> Result<(), CliError> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "error_code": "identity_drift",
                "message": "persisted profile identity does not match env-derived identity; refusing silent refresh",
                "drift": diff,
                "recovery": "resolve the mismatch (correct the env, or rename/remove the persisted profile), then re-run auto-setup",
            }))?
        );
    } else {
        eprintln!("identity_drift: persisted profile identity does not match env-derived identity");
        for field in diff {
            eprintln!(
                "  * {}: persisted={} env={}",
                field.field, field.from, field.to
            );
        }
        eprintln!(
            "recovery: resolve the mismatch (correct the env, or rename/remove the persisted profile), then re-run auto-setup"
        );
    }
    Ok(())
}

fn build_desired_profile_from_env(
    profile_override: Option<&str>,
    reduce_profile: Option<&str>,
) -> Result<Profile, CliError> {
    let role = required_env("LANYTE_AGENT_ROLE")?;
    let scope = required_env("LANYTE_AGENT_SCOPE")?;
    let server_url = required_env("LANYTE_MM_URL")?;
    // Precedence: explicit --profile flag > CHANVOY_PROFILE env > canonical
    // `<role>-<scope>` derived from sourced identity. PER-012 / entarch:
    // synthesizing a bare `<role>` here would undercut the resolver fix
    // every time auto-setup runs, since the materialized profile name
    // would not match the canonical resolution target.
    let name = profile_override
        .map(ToString::to_string)
        .or_else(|| env_var_nonempty("CHANVOY_PROFILE"))
        .unwrap_or_else(|| format!("{role}-{scope}"));
    let team_name = derive_team_name(&scope);

    let profile = Profile {
        name,
        role,
        scope,
        provider: Provider::Mattermost,
        bot_username: String::new(),
        team_name,
        server_url,
        env_name: env_var_nonempty("CHANVOY_TOKEN_ENV_NAME")
            .unwrap_or_else(|| "LANYTE_MM_TOKEN".to_string()),
        env_file: None,
        credential_mode: CredentialMode::EnvName,
        capability_class: CapabilityClass::Standard,
        monitored_channels: Vec::new(),
        ipc: None,
        // PER-035: reduction policy from the `--reduce-profile` flag.
        // The scope marker is `team_name` (derived above from scope), so
        // no second field is needed. None ⇒ no reduction (today's
        // behavior); on a refresh, a None here is merge-forward-preserved
        // from the existing profile so omitting the flag never wipes a
        // configured policy.
        reduce: reduce_profile.map(|name| chanvoy_core::ReducePolicy {
            use_profile: name.to_string(),
        }),
    };
    validate_profile_create_args(&profile)?;
    // Missing credential is an env-input problem (exit 2), not a remote preflight
    // failure (exit 3). Probe token availability here so the exit table matches
    // the brief contract.
    check_token_available(&profile)?;
    Ok(profile)
}

/// Probe the configured credential source for the token without returning its value.
/// Missing / empty credential maps to `CliError::Bootstrap` so the caller exits as
/// env-input (EXIT_ENV_INPUT). Other load errors (e.g., unreadable env_file) also
/// map here since they are local contract failures.
fn check_token_available(profile: &Profile) -> Result<(), CliError> {
    load_token(profile).map(|_| ()).map_err(|err| {
        CliError::Bootstrap(format!("token unavailable via {}: {err}", profile.env_name))
    })
}

async fn validate_and_finalize_profile(
    mut profile: Profile,
) -> Result<(Profile, Identity), CliError> {
    let token = load_token(&profile)?;
    let client = MattermostClient::new(&profile, token)?;
    let identity = client.whoami().await?;
    client.validate_team_access().await?;
    profile.bot_username = identity.username.clone();
    Ok((profile, identity))
}

async fn ensure_daemon_running(
    profile: &Profile,
    identity: &Identity,
) -> Result<DaemonState, CliError> {
    // Bound the pre-spawn health-check. Two distinct things can be wrong
    // with an existing daemon:
    //   (1) Wedged daemon (SIGSTOPed, deadlocked, I/O-stuck): socket open,
    //       RPCs never respond. PING_TIMEOUT bounds us so auto-setup
    //       routes through the zombie-stop path instead of hanging.
    //   (2) Running daemon with a stale / revoked / drifted token: socket
    //       open and local RPCs answer fine, but the cached Mattermost
    //       credential won't survive seed/read calls. PER-014 entarch
    //       residual finding (2026-04-28): use the network-aware
    //       `ping_full` (= `daemon_status`, runs `probe_whoami`) at
    //       the pre-spawn check to surface this case so the existing
    //       daemon gets stopped and respawned with the freshly
    //       validated parent credential. The local-only `ping()`
    //       elsewhere does NOT make that distinction by design.
    let profile_name = profile.name.as_str();
    let ping_outcome = tokio::time::timeout(PING_TIMEOUT, ping_full(profile_name)).await;
    if let Ok(Ok(status)) = &ping_outcome {
        // Daemon is bound AND its network probe completed. Reuse it
        // only if it's actually healthy (token reachable, no identity
        // drift). Anything else falls through to the stop+respawn path
        // so the new daemon picks up the parent's freshly-validated
        // identity and a current token from the env-name lookup.
        let drifted = status.mattermost_identity_drift.unwrap_or(false);
        if status.mattermost_ok && !drifted {
            return Ok(DaemonState::AlreadyRunning);
        }
    }
    // ping_full failing/timing out OR returning unhealthy/drifted does
    // not mean the daemon is absent — a wedged daemon hangs ping; a
    // stale-token daemon answers but flunks `mattermost_ok`. Blindly
    // spawning in either case would leave a zombie alongside the fresh
    // daemon (two-daemons-one-profile condition secrev F5 / devrev F6
    // flagged). Call stop_daemon_if_present before spawning; it
    // short-circuits when no socket exists (normal cold-start), uses
    // the local `shutdown` RPC when a daemon is responsive, and falls
    // back to pid-file-driven SIGKILL when shutdown can't be served.
    stop_daemon_if_present(profile_name).await?;

    // PER-014: write the bootstrap-state file co-located with the spawn.
    // Site discipline by structural placement — only `ensure_daemon_running`
    // emits a bootstrap file, so non-daemon-spawn paths (`profile create`)
    // cannot produce one by construction. Daemon child reads, validates
    // (freshness + profile_fingerprint + nonce-env match + username match),
    // consumes-and-deletes, then binds without calling whoami.
    let nonce = chanvoy_core::generate_nonce();
    let bootstrap = chanvoy_core::build_bootstrap_state(
        profile,
        identity.id.as_str(),
        nonce.as_str(),
        std::process::id(),
    )
    .map_err(|err| CliError::Bootstrap(format!("build bootstrap state: {err}")))?;
    chanvoy_core::write_bootstrap_state(&bootstrap)
        .map_err(|err| CliError::Bootstrap(format!("write bootstrap state: {err}")))?;

    let exe = std::env::current_exe()?;
    let mut cmd = Command::new(exe);
    cmd.arg("--profile")
        .arg(profile_name)
        .arg("daemon")
        .arg("serve")
        .env(chanvoy_core::BOOTSTRAP_NONCE_ENV, &nonce)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    detach_into_new_session(&mut cmd);
    cmd.spawn()?;
    // Each per-iteration `ping()` is bounded the same way as the
    // pre-spawn health check, for the same reason: a freshly spawned
    // daemon could wedge during startup (deadlocked WebSocket init,
    // stuck dependency probe) and leave us polling a ping that never
    // returns. The outer deadline bounds total wait to a fixed budget
    // independent of per-iteration timeout.
    let spawn_ready_deadline = std::time::Instant::now() + SPAWN_READY_DEADLINE;
    while std::time::Instant::now() < spawn_ready_deadline {
        let ping_outcome = tokio::time::timeout(POST_SPAWN_PING_TIMEOUT, ping(profile_name)).await;
        if matches!(ping_outcome, Ok(Ok(_))) {
            return Ok(DaemonState::Started);
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(DaemonError::NotRunning(profile_name.to_string()).into())
}

/// Detach the spawned daemon into a new session so it survives the
/// termination of the spawning shell. Without this, the daemon stays in
/// the spawning shell's process group and session: when the shell exits
/// (or its controlling terminal closes), the daemon receives `SIGHUP`
/// and dies. Operators returning to a machine then find no running
/// daemon despite an earlier successful `auto-setup` — the
/// motivating failure mode for the detachment design.
///
/// `setsid(2)` makes the new process the leader of a new session and
/// process group with no controlling terminal. `SIGHUP` from the
/// parent's terminal close cannot reach it.
///
/// Mirrors the `pre_exec(|| { libc::setsid()... })` pattern used by
/// `sysprims-cli`'s own guard daemon. If sysprims later exposes a
/// public `detached_command()` primitive (tracked in the lanytehq /
/// sysprims memo at `.plans/memos/lanytehq/`), migrate to that.
#[cfg(unix)]
fn detach_into_new_session(cmd: &mut Command) {
    // tokio::process::Command exposes pre_exec directly on Unix — no
    // separate trait import needed.
    //
    // SAFETY: pre_exec runs in the post-fork child between fork() and
    // execve(). Only async-signal-safe operations are permitted here
    // (POSIX async-signal-safe list). `libc::setsid` is on that list.
    // The closure performs no allocation, no logging, no locking, no
    // Rust runtime operations — only the syscall and an io::Error
    // construction on failure (Error::last_os_error reads errno, also
    // async-signal-safe).
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn detach_into_new_session(_cmd: &mut Command) {
    // chanvoy is Unix-only for v1; this stub exists so the call site
    // compiles cleanly under hypothetical non-Unix builds. PER-008D's
    // detachment story is Unix-shaped (setsid / process groups).
}

/// Stop the daemon if it is present, and wait until its socket is actually gone
/// so the caller's subsequent spawn lands on a clean slate.
///
/// **Daemon presence is detected via socket file existence, not via a probe RPC.**
/// The pre-spawn health check in `ensure_daemon_running` uses `ping_full()`
/// (network-aware `daemon_status`, runs `probe_whoami` against Mattermost); a
/// daemon running with a revoked credential or drifted identity fails that
/// probe while being very much alive. Falling back to socket existence ensures
/// the stop path catches those zombies on both the Refresh path (explicit stop
/// to force reload) and the Reuse path (invoked from `ensure_daemon_running`
/// when the network-aware probe fails or returns degraded). The local-only
/// `ping()` (= `profile_status`) is reserved for post-spawn readiness, not
/// stale-daemon detection.
///
/// The daemon's `shutdown` RPC is handled locally (no Mattermost calls) so it
/// works even when `whoami()` is failing. A stale socket (process already gone)
/// surfaces as `DaemonError::NotRunning` from `stop()`; that is treated as
/// no-op since the next `daemon::start()` cleans up the stale socket.
async fn stop_daemon_if_present(profile: &str) -> Result<(), CliError> {
    let socket = socket_path_for_profile(profile);
    if !socket.exists() {
        return Ok(());
    }
    // Try graceful shutdown with a bounded timeout. A wedged daemon
    // (SIGSTOPed, deadlocked, or stuck on a blocking dependency) holds
    // the socket but never accepts the shutdown RPC. If the RPC doesn't
    // complete in SHUTDOWN_RPC_TIMEOUT, fall through to the pid-file
    // force-kill fallback instead of blocking auto-setup indefinitely.
    let stop_outcome = tokio::time::timeout(SHUTDOWN_RPC_TIMEOUT, stop(profile)).await;
    match stop_outcome {
        Ok(Ok(_)) => {}
        Ok(Err(DaemonError::NotRunning(_))) => return Ok(()),
        Ok(Err(err)) => return Err(err.into()),
        Err(_) => {
            // RPC timed out — daemon is wedged. Fall through to force-kill.
        }
    }
    for _ in 0..20 {
        if !socket_path_for_profile(profile).exists() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    // Socket still present after the shutdown grace window. Force-kill
    // the pid recorded in the runtime-dir pid file, then sweep the
    // SIGKILL-orphaned runtime files (SIGKILL skips the daemon's own
    // `cleanup_runtime_files`). Uses `kill` via std::process::Command
    // instead of pulling sysprims into the prod graph — a single-purpose
    // shell-out is cheaper than a new prod dependency for one fallback.
    if let Some(pid) = read_daemon_pid_for_force_kill(profile) {
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
        // Wait for the process to be reaped so the next start() doesn't
        // see an inconsistent pid/socket state.
        for _ in 0..20 {
            if !is_pid_alive(pid) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        // Sweep SIGKILL-orphaned runtime files. Ignoring errors because
        // either file may already be absent.
        let _ = std::fs::remove_file(socket_path_for_profile(profile));
        let _ = std::fs::remove_file(pid_path_for_profile(profile));
        return Ok(());
    }
    Err(CliError::Bootstrap(format!(
        "daemon for profile {profile} did not exit within the shutdown grace \
         window and no pid file was readable for the force-kill fallback"
    )))
}

/// Read the daemon's pid from the runtime-dir pid file. Returns None on any
/// read/parse error — the caller falls through to a bootstrap error in that
/// case, which surfaces EXIT_DAEMON_FAILED with a clear message.
fn read_daemon_pid_for_force_kill(profile: &str) -> Option<u32> {
    let pid_path = pid_path_for_profile(profile);
    std::fs::read_to_string(pid_path).ok()?.trim().parse().ok()
}

/// Check whether a pid is live without sending a signal. Uses `kill -0`
/// (POSIX: "check for existence without signalling") via shell-out to avoid
/// a libc/nix/sysprims dep for a single predicate. Exit 0 = alive,
/// non-zero (e.g., ESRCH) = not alive.
fn is_pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

const PING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const SHUTDOWN_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// Per-iteration ping budget on the post-spawn readiness poll. Shorter than
/// `PING_TIMEOUT` because a healthy fresh daemon should answer quickly; a
/// slow ping during spawn polling signals "not ready yet, keep waiting"
/// rather than a wedged-daemon classification (that's what the outer
/// deadline handles).
const POST_SPAWN_PING_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(750);
/// Total time budget for the post-spawn readiness loop. Bounds the worst-
/// case wait across all iterations independent of per-ping timing.
const SPAWN_READY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
enum ProfileAction {
    Create,
    Refresh(Vec<ProfileFieldDiff>),
    Reuse,
    IdentityDrift(Vec<ProfileFieldDiff>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct ProfileFieldDiff {
    field: String,
    from: String,
    to: String,
}

fn decide_profile_action(desired: &Profile, existing: Option<&Profile>) -> ProfileAction {
    let Some(existing) = existing else {
        return ProfileAction::Create;
    };
    let identity_diff = identity_surface_diff(existing, desired);
    if !identity_diff.is_empty() {
        return ProfileAction::IdentityDrift(identity_diff);
    }
    let refreshable_diff = refreshable_profile_diff(existing, desired);
    if refreshable_diff.is_empty() {
        ProfileAction::Reuse
    } else {
        ProfileAction::Refresh(refreshable_diff)
    }
}

/// Fields that define WHO this profile is talking as and WHERE. Any change here must
/// hard-error (exit 5) — silent refresh would let one persisted attention state slide
/// across identities.
fn identity_surface_diff(from: &Profile, to: &Profile) -> Vec<ProfileFieldDiff> {
    let mut diff = Vec::new();
    push_if_diff(&mut diff, "role", &from.role, &to.role);
    push_if_diff(&mut diff, "scope", &from.scope, &to.scope);
    push_if_diff(&mut diff, "server_url", &from.server_url, &to.server_url);
    push_if_diff(&mut diff, "team_name", &from.team_name, &to.team_name);
    diff
}

/// Fields that are safely refreshable on env change.
fn refreshable_profile_diff(from: &Profile, to: &Profile) -> Vec<ProfileFieldDiff> {
    let mut diff = Vec::new();
    push_if_diff(&mut diff, "env_name", &from.env_name, &to.env_name);
    push_if_diff(
        &mut diff,
        "credential_mode",
        &format!("{:?}", from.credential_mode),
        &format!("{:?}", to.credential_mode),
    );
    push_if_diff(
        &mut diff,
        "capability_class",
        &format!("{:?}", from.capability_class),
        &format!("{:?}", to.capability_class),
    );
    // PER-035: a *changed* reduction policy is a visible refresh, never
    // an identity-drift hard-error (it does not change WHO the profile
    // authenticates as or WHERE its primary team is — only which family
    // identity outside-team writes defer to). Only diff when `to` (the
    // env/flag-derived desired) actually carries a policy: omitting the
    // flag leaves `to.reduce == None`, which merge-forward then restores
    // from `from`, so a no-flag refresh must not register a spurious
    // diff here.
    if to.reduce.is_some() && from.reduce != to.reduce {
        push_if_diff(
            &mut diff,
            "reduce.use_profile",
            from.reduce
                .as_ref()
                .map(|r| r.use_profile.as_str())
                .unwrap_or("(none)"),
            to.reduce
                .as_ref()
                .map(|r| r.use_profile.as_str())
                .unwrap_or("(none)"),
        );
    }
    // Excluded from all diffs: `name` (key), `bot_username` (derived from token),
    // `monitored_channels`, `ipc`, `env_file`, `provider` (pass through via merge-forward).
    diff
}

fn push_if_diff(diff: &mut Vec<ProfileFieldDiff>, field: &str, from: &str, to: &str) {
    if from != to {
        diff.push(ProfileFieldDiff {
            field: field.to_string(),
            from: from.to_string(),
            to: to.to_string(),
        });
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ProfileState {
    Created,
    Refreshed,
    Reused,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum DaemonState {
    AlreadyRunning,
    Started,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SeedOutcome {
    /// Cursor seeded to HEAD for a channel that had no stored anchor.
    Seeded { channel: String, post_id: String },
    /// Joined channel has no posts yet — explicitly left unseeded. Not a failure.
    UnseededEmptyChannel { channel: String },
    /// Seed attempt failed (membership enumeration error or per-channel HEAD fetch error).
    /// Flips overall report to degraded.
    Failed { channel: String, reason: String },
}

impl From<SeededChannelOutcome> for SeedOutcome {
    fn from(outcome: SeededChannelOutcome) -> Self {
        match outcome {
            SeededChannelOutcome::Seeded { channel, post_id } => {
                SeedOutcome::Seeded { channel, post_id }
            }
            SeededChannelOutcome::UnseededEmptyChannel { channel } => {
                SeedOutcome::UnseededEmptyChannel { channel }
            }
            SeededChannelOutcome::Failed { channel, reason } => {
                SeedOutcome::Failed { channel, reason }
            }
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct AutoSetupReport {
    profile_name: String,
    bot_username: String,
    profile_state: ProfileState,
    daemon_state: DaemonState,
    is_active: bool,
    degraded: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    refresh_diff: Vec<ProfileFieldDiff>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    seed_outcomes: Vec<SeedOutcome>,
}

fn print_auto_setup_report(json: bool, report: &AutoSetupReport) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    // Short status lines first (narrow-terminal friendly per A5 / entarch #7).
    let profile_line = match report.profile_state {
        ProfileState::Created => format!("profile {} created", report.profile_name),
        ProfileState::Refreshed => format!("profile {} refreshed", report.profile_name),
        ProfileState::Reused => format!("profile {} reused", report.profile_name),
    };
    let daemon_line = match report.daemon_state {
        DaemonState::AlreadyRunning => "daemon already running".to_string(),
        DaemonState::Started => "daemon started".to_string(),
    };
    println!("{profile_line}");
    println!("{daemon_line}");
    println!("bot_username: {}", report.bot_username);
    if report.is_active {
        println!("active: {}", report.profile_name);
    }
    if report.degraded {
        println!("status: degraded (see seed outcomes below)");
    }
    // Detail sections follow.
    for field in &report.refresh_diff {
        println!("  refresh {}: {} -> {}", field.field, field.from, field.to);
    }
    for outcome in &report.seed_outcomes {
        match outcome {
            SeedOutcome::Seeded { channel, post_id } => {
                println!("  seeded: {channel} -> {post_id}")
            }
            SeedOutcome::UnseededEmptyChannel { channel } => {
                println!("  unseeded: {channel} (empty channel)")
            }
            SeedOutcome::Failed { channel, reason } => {
                println!("  seed_failed: {channel} ({reason})")
            }
        }
    }
    Ok(())
}

/// Text-format renderers for the attention-state inspection commands.
/// JSON shape is owned by the core result structs; these helpers only
/// build a narrow-terminal-friendly human view. Both commands are
/// strictly read-only — no daemon-state mutation, no Mattermost API
/// calls.
fn render_attention_list_text(result: &AttentionListResult) -> String {
    let mut out = String::new();
    out.push_str(&format!("profile: {}\n", result.profile));
    if result.channels.is_empty() {
        out.push_str("(no tracked channels)\n");
    } else {
        // CHECKED column (freshness of the staleness verdict) is load-
        // bearing for D1 — operators need to distinguish "freshly
        // verified" from "never checked since establishment" (cxotech's
        // refinement, entarch's 2026-04-22 review finding). Columns
        // tightened slightly from the brief's example to keep the line
        // within ~100 chars on narrow terminals.
        out.push_str(&format!(
            "{:<24} {:<20} {:<20} {:<18} CHECKED\n",
            "CHANNEL", "SOURCE", "NEWEST_SEEN", "UPDATED"
        ));
        for entry in &result.channels {
            out.push_str(&format!(
                "{:<24} {:<20} {:<20} {:<18} {}\n",
                truncate(&entry.channel, 24),
                attention_source_label(&entry.source),
                entry
                    .newest_seen
                    .as_deref()
                    .map(|s| truncate(s, 20).to_string())
                    .unwrap_or_else(|| "—".to_string()),
                format_ts(entry.updated_at),
                format_ts(entry.last_checked_at),
            ));
        }
    }
    out.push_str(&format!(
        "\nmentions: source={} newest_seen={} updated={}\n",
        attention_source_label(&result.mentions.source),
        result.mentions.newest_seen.as_deref().unwrap_or("—"),
        format_ts(result.mentions.updated_at),
    ));
    // PER-019 (devrev PR #17 follow-up, 2026-04-30): surface
    // quarantined legacy cursor records in the default human output.
    // The JSON-side fix at 3156a0a added the field to
    // `AttentionListResult` but the renderer ignored it; quarantined
    // records stayed invisible to operators using the default text
    // output. Render a small section listing the original bare
    // channel name + the ambiguous teams the migration found, so
    // operators know which cursors need manual disambiguation via
    // `--team` or `<team>/<channel>` on next access.
    if !result.quarantined.is_empty() {
        out.push_str(&format!(
            "\nquarantined ({} record{}):\n",
            result.quarantined.len(),
            if result.quarantined.len() == 1 {
                ""
            } else {
                "s"
            },
        ));
        out.push_str(&format!(
            "  {:<24} {:<40} {}\n",
            "LEGACY_NAME", "AMBIGUOUS_TEAMS", "QUARANTINED_AT"
        ));
        for q in &result.quarantined {
            out.push_str(&format!(
                "  {:<24} {:<40} {}\n",
                truncate(&q.legacy_channel_name, 24),
                truncate(&q.ambiguous_teams.join(", "), 40),
                format_ts(Some(q.quarantined_at)),
            ));
        }
        out.push_str(
            "  (re-establish per-team cursors via `--team <slug>` or `<team>/<channel>` syntax)\n",
        );
    }
    out
}

fn render_attention_show_text(result: &AttentionShowResult) -> String {
    let entry = &result.channel;
    let mut out = String::new();
    out.push_str(&format!("profile: {}\n", result.profile));
    out.push_str(&format!("channel: {}\n", entry.channel));
    out.push_str(&format!(
        "source:  {}\n",
        attention_source_label(&entry.source)
    ));
    out.push_str(&format!(
        "newest_seen:     {}\n",
        entry.newest_seen.as_deref().unwrap_or("—")
    ));
    out.push_str(&format!(
        "updated_at:      {}\n",
        format_ts(entry.updated_at)
    ));
    out.push_str(&format!(
        "last_checked_at: {}\n",
        format_ts(entry.last_checked_at)
    ));
    out.push_str(&format!(
        "\nmentions: source={} newest_seen={} updated={}\n",
        attention_source_label(&result.mentions.source),
        result.mentions.newest_seen.as_deref().unwrap_or("—"),
        format_ts(result.mentions.updated_at),
    ));
    out
}

fn attention_source_label(source: &AttentionSource) -> &'static str {
    match source {
        AttentionSource::NoAnchor => "no_anchor",
        AttentionSource::PostCursor => "post_cursor",
        AttentionSource::NotificationsCursor => "notifications_cursor",
        AttentionSource::StaleCursor => "stale_cursor",
    }
}

fn format_ts(millis: Option<i64>) -> String {
    let Some(ms) = millis else {
        return "—".to_string();
    };
    match Utc.timestamp_millis_opt(ms).single() {
        Some(dt) => dt.format("%Y-%m-%d %H:%MZ").to_string(),
        None => ms.to_string(),
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        &s[..max]
    }
}

/// Thin wrapper that gathers I/O snapshots (profiles list, running
/// daemons, active_profile, env) and delegates to the pure resolver in
/// chanvoy-core. The pure function carries the policy logic; this
/// wrapper only handles the side-effecting bits.
fn resolve_profile_name(
    profile_flag: Option<&str>,
    policy: chanvoy_core::FallbackPolicy,
) -> Result<String, CliError> {
    let profiles = list_profiles()?;
    let profile_names: Vec<String> = profiles.iter().map(|p| p.name.clone()).collect();
    let running = list_running_daemon_profiles(&profiles);
    let active = load_active_profile()?;
    let env_role = env_var_nonempty("LANYTE_AGENT_ROLE");
    let env_scope = env_var_nonempty("LANYTE_AGENT_SCOPE");
    let env_chanvoy_profile = env_var_nonempty("CHANVOY_PROFILE");

    let inputs = chanvoy_core::ResolverInputs {
        profiles: &profile_names,
        running_daemon_profiles: &running,
        active_profile: active.as_deref(),
        env_role: env_role.as_deref(),
        env_scope: env_scope.as_deref(),
        env_chanvoy_profile: env_chanvoy_profile.as_deref(),
    };

    Ok(chanvoy_core::resolve_profile_name(
        profile_flag,
        policy,
        &inputs,
    )?)
}

/// Enumerate profiles whose daemons appear to be running on this
/// machine. A daemon is considered running if its socket exists, its
/// pid file exists, and the recorded pid is alive. This is a best-
/// effort observation for the resolver's single-tenant fallback —
/// stale state surfaces as a downstream RPC error, not silent
/// mis-attribution.
fn list_running_daemon_profiles(profiles: &[Profile]) -> Vec<String> {
    profiles
        .iter()
        .filter(|p| {
            socket_path_for_profile(&p.name).exists()
                && pid_path_for_profile(&p.name).exists()
                && read_daemon_pid_for_force_kill(&p.name)
                    .map(is_pid_alive)
                    .unwrap_or(false)
        })
        .map(|p| p.name.clone())
        .collect()
}

fn validate_profile_create_args(profile: &Profile) -> Result<(), CliError> {
    if matches!(profile.credential_mode, CredentialMode::EnvFile) && profile.env_file.is_none() {
        return Err(chanvoy_core::CoreError::MissingEnvFile.into());
    }
    Ok(())
}

fn profile_from_create_args(args: &ProfileCreateArgs) -> Profile {
    // PER-012 AC #6: when --team-name is absent, derive `org-${scope}`
    // from the positional scope arg rather than falling back to the
    // historical hardcoded `org-lanytehq`. Explicit flag still wins
    // for non-conventional team names.
    let team_name = args
        .team_name
        .clone()
        .unwrap_or_else(|| format!("org-{}", args.scope));
    Profile {
        name: args.name.clone(),
        role: args.role.clone(),
        scope: args.scope.clone(),
        provider: Provider::Mattermost,
        bot_username: args.bot_username.clone(),
        team_name,
        server_url: args.server_url.clone(),
        env_name: args.env_name.clone(),
        env_file: args.env_file.clone(),
        credential_mode: match args.credential_mode {
            CliCredentialMode::EnvName => CredentialMode::EnvName,
            CliCredentialMode::EnvFile => CredentialMode::EnvFile,
            CliCredentialMode::SeclusorRun => CredentialMode::SeclusorRun,
        },
        capability_class: match args.capability_class {
            CliCapabilityClass::Standard => CapabilityClass::Standard,
            CliCapabilityClass::Elevated => CapabilityClass::Elevated,
        },
        monitored_channels: Vec::new(),
        ipc: None,
        // PER-035: `profile create` does not expose a reduction flag;
        // reduction policy is set via `auto-setup --reduce-profile`.
        reduce: None,
    }
}

async fn profile_from_env_args(args: &ProfileCreateFromEnvArgs) -> Result<Profile, CliError> {
    let role = required_env("LANYTE_AGENT_ROLE")?;
    let scope = required_env("LANYTE_AGENT_SCOPE")?;
    let server_url = required_env("LANYTE_MM_URL")?;

    // PER-012: default to canonical `<role>-<scope>` matching the
    // identity-script stem, not bare `<role>`. Explicit --name still
    // overrides for non-canonical cases.
    let default_name = format!("{role}-{scope}");
    let mut profile = Profile {
        name: args.name.clone().unwrap_or(default_name),
        role,
        scope: scope.clone(),
        provider: Provider::Mattermost,
        bot_username: String::new(),
        team_name: args
            .team_name
            .clone()
            .unwrap_or_else(|| derive_team_name(&scope)),
        server_url,
        env_name: args.env_name.clone(),
        env_file: args.env_file.clone(),
        credential_mode: match args.credential_mode {
            CliCredentialMode::EnvName => CredentialMode::EnvName,
            CliCredentialMode::EnvFile => CredentialMode::EnvFile,
            CliCredentialMode::SeclusorRun => CredentialMode::SeclusorRun,
        },
        capability_class: match args.capability_class {
            CliCapabilityClass::Standard => CapabilityClass::Standard,
            CliCapabilityClass::Elevated => CapabilityClass::Elevated,
        },
        monitored_channels: Vec::new(),
        ipc: None,
        // PER-035: reduction policy is auto-setup-owned, not a
        // `profile create-from-env` surface.
        reduce: None,
    };

    validate_profile_create_args(&profile)?;

    let token = load_token(&profile)?;
    let client = MattermostClient::new(&profile, token)?;
    let identity = client.whoami().await?;
    client.validate_team_access().await?;
    profile.bot_username = identity.username;
    Ok(profile)
}

fn derive_team_name(scope: &str) -> String {
    // PER-012: removed the silent fallback to `org-lanytehq` for empty
    // scope — that hardcoded default produced wrong-org profiles when
    // called under a non-lanytehq identity. The two callers
    // (`build_desired_profile_from_env`, `profile_from_env_args`) both
    // require `LANYTE_AGENT_SCOPE` upstream via `required_env`, so the
    // empty-scope path is unreachable; debug_assert pins the invariant.
    if let Some(team) = env_var_nonempty("LANYTE_MM_TEAM") {
        return team;
    }
    debug_assert!(
        !scope.is_empty(),
        "derive_team_name called with empty scope; callers must enforce LANYTE_AGENT_SCOPE"
    );
    format!("org-{scope}")
}

fn required_env(name: &str) -> Result<String, CliError> {
    env_var_nonempty(name)
        .ok_or_else(|| CliError::Bootstrap(format!("missing required environment variable {name}")))
}

fn env_var_nonempty(name: impl AsRef<OsStr>) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn store_profile_and_maybe_activate(
    json: bool,
    profile: &Profile,
    activate_requested: bool,
) -> Result<(), CliError> {
    validate_profile_create_args(profile)?;
    let path = store_profile(profile)?;
    let should_activate = activate_requested || load_active_profile()?.is_none();
    if should_activate {
        store_active_profile(&profile.name)?;
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "path": path,
                "active": should_activate,
                "profile": profile.name,
            }))?
        );
    } else {
        println!("created profile {}", profile.name);
        println!("path: {}", path.display());
        println!("bot_username: {}", profile.bot_username);
        if should_activate {
            println!("active profile set to {}", profile.name);
        }
    }
    Ok(())
}

fn print_value<T>(json: bool, value: &T) -> Result<(), CliError>
where
    T: serde::Serialize + HumanReadable,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{}", value.to_human_string());
    }
    Ok(())
}

fn print_identity(json: bool, identity: &Identity) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(identity)?);
    } else {
        let printable = printable_identity(identity);
        println!("{}", serde_json::to_string_pretty(&printable)?);
    }
    Ok(())
}

fn printable_identity(identity: &Identity) -> serde_json::Value {
    serde_json::json!({
        "username": identity.username,
        "id": identity.id,
        "is_bot": identity.is_bot,
        "email": identity.email.clone().unwrap_or_default(),
    })
}

fn print_receipt(json: bool, label: &str, receipt: &PostReceipt) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(receipt)?);
    } else {
        println!("{label}: {}", receipt.id);
    }
    Ok(())
}

fn print_dm_receipt(json: bool, username: &str, receipt: &PostReceipt) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(receipt)?);
    } else {
        println!("dm sent: {} (to @{})", receipt.id, username);
    }
    Ok(())
}

fn print_notify_receipt(json: bool, username: &str, receipt: &PostReceipt) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(receipt)?);
    } else {
        println!("notified @{}: {}", username, receipt.id);
    }
    Ok(())
}

fn print_bool_action(json: bool, value: serde_json::Value, human: &str) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!("{human}");
    }
    Ok(())
}

fn print_json_or_text<T: serde::Serialize>(
    json: bool,
    value: &T,
    human: &str,
) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{human}");
    }
    Ok(())
}

trait HumanReadable {
    fn to_human_string(&self) -> String;
}

impl HumanReadable for Vec<Channel> {
    fn to_human_string(&self) -> String {
        let name_width = self
            .iter()
            .map(|channel| channel.name.len())
            .max()
            .unwrap_or(0);
        let display_width = self
            .iter()
            .map(|channel| channel.display_name.len())
            .max()
            .unwrap_or(0);

        self.iter()
            .map(|channel| {
                format!(
                    "{:<name_width$}  {:<display_width$}  {}",
                    channel.name,
                    channel.display_name,
                    channel.channel_type,
                    name_width = name_width,
                    display_width = display_width,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Format a `last_post_at` (Unix epoch ms) as a relative-time
/// string for the human-mode `last_active` column. Missing or zero
/// activity renders as `—`. Buckets: seconds / minutes / hours /
/// days / weeks / years. Caches "now" at the call site so all rows
/// in one render pass have a consistent reference (caller passes
/// `now_millis`).
fn format_last_active(last_post_at: Option<i64>, now_millis: i64) -> String {
    let Some(ts) = last_post_at else {
        return "—".to_string();
    };
    if ts <= 0 {
        return "—".to_string();
    }
    let delta_ms = now_millis.saturating_sub(ts);
    if delta_ms < 0 {
        // Future timestamp (clock skew or test harness). Render as
        // "just now" rather than a negative duration.
        return "just now".to_string();
    }
    let delta_s = delta_ms / 1000;
    if delta_s < 60 {
        format!("{delta_s}s ago")
    } else if delta_s < 3600 {
        format!("{}m ago", delta_s / 60)
    } else if delta_s < 86400 {
        format!("{}h ago", delta_s / 3600)
    } else if delta_s < 7 * 86400 {
        format!("{}d ago", delta_s / 86400)
    } else if delta_s < 365 * 86400 {
        format!("{}w ago", delta_s / (7 * 86400))
    } else {
        format!("{}y ago", delta_s / (365 * 86400))
    }
}

/// Render the cross-team channel listing as a grouped human view.
/// Each team gets a `=== <team-slug> ===` header followed by
/// `<team-slug>/<channel-name>` lines so operators can copy any
/// line directly into `chanvoy read` / `post` / `check` if they
/// need the explicit-team form.
///
/// Each row also includes a trailing `last_active` column (relative
/// time, `—` for missing activity). When `--sort active` is in
/// effect, the channel order within each group is most-recent-first
/// (the caller pre-sorts before calling this function); the column
/// renders the same way regardless.
fn render_team_channels_human(groups: &[chanvoy_core::TeamChannels]) -> String {
    let now_millis = chanvoy_core::now_unix_millis();
    let mut out = String::new();
    let mut first = true;
    for group in groups {
        if !first {
            out.push('\n');
        }
        first = false;
        out.push_str(&format!("=== {} ===\n", group.team_name));
        if group.channels.is_empty() {
            out.push_str("  (no channels)\n");
            continue;
        }
        let name_width = group
            .channels
            .iter()
            .map(|c| c.name.len() + group.team_name.len() + 1)
            .max()
            .unwrap_or(0);
        let display_width = group
            .channels
            .iter()
            .map(|c| c.display_name.len())
            .max()
            .unwrap_or(0);
        for channel in &group.channels {
            let qualified = format!("{}/{}", group.team_name, channel.name);
            let last_active = format_last_active(channel.last_post_at, now_millis);
            out.push_str(&format!(
                "  {:<name_width$}  {:<display_width$}  {}  {}\n",
                qualified,
                channel.display_name,
                channel.channel_type,
                last_active,
                name_width = name_width,
                display_width = display_width,
            ));
        }
    }
    out
}

/// Sort channels within each team group by `last_post_at`
/// descending. Missing / zero-activity channels sort last within
/// their group. The group order itself is NOT modified — the
/// primary-first / fallback-alphabetical team ordering is preserved.
/// A flattened global view is intentionally not provided.
fn sort_groups_by_active(groups: &mut [chanvoy_core::TeamChannels]) {
    for group in groups {
        group.channels.sort_by(|a, b| {
            // None sorts last within group: map None → i64::MIN so
            // it loses the descending compare against any real
            // timestamp.
            let a_key = a.last_post_at.unwrap_or(i64::MIN);
            let b_key = b.last_post_at.unwrap_or(i64::MIN);
            b_key.cmp(&a_key)
        });
    }
}

async fn handle_channels_command(
    profile: &str,
    json: bool,
    args: ChannelsArgs,
) -> Result<(), CliError> {
    if args.primary_team {
        let channels = daemon_client(profile).list_channels().await?;
        if json {
            // PER-025 AC #6a: legacy `--primary-team --json` path
            // preserves the pre-PER-025 JSON field set exactly — no
            // `last_post_at` field. Project full `Channel` to
            // `LegacyChannel` so the activity-bearing default shape
            // doesn't leak into the legacy contract. Serialize
            // directly (bypass `print_value`'s HumanReadable bound —
            // the legacy human path stays on the existing
            // `Vec<Channel>` rendering, which already omits the
            // `last_active` column).
            let legacy: Vec<LegacyChannel> = channels.iter().map(Channel::to_legacy).collect();
            println!("{}", serde_json::to_string_pretty(&legacy)?);
            return Ok(());
        }
        return print_value(false, &channels);
    }
    let mut groups = daemon_client(profile).list_channels_across_teams().await?;
    if let Some(team_filter) = args.team {
        groups.retain(|g| g.team_name == team_filter);
    }
    if matches!(args.sort.as_deref(), Some("active")) {
        sort_groups_by_active(&mut groups);
    }
    if json {
        // PER-025 AC #6: default `channels --json` adds `last_post_at`
        // to the grouped multi-team shape via Channel's serde
        // (`Option<i64>` → `null` on None, deterministic shape). This
        // path serializes the grouped TeamChannels structure as-is.
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "teams": groups }))?
        );
    } else {
        print!("{}", render_team_channels_human(&groups));
    }
    Ok(())
}

impl HumanReadable for Vec<DmConversation> {
    fn to_human_string(&self) -> String {
        let mut lines = self
            .iter()
            .map(|conversation| {
                format!(
                    "{} {} {}",
                    format_dm_timestamp(conversation.last_post_at),
                    conversation.id,
                    conversation.name,
                )
            })
            .collect::<Vec<_>>();

        lines.push(String::new());
        lines.push("Use 'chanvoy dm read <username>' to read a conversation.".to_string());
        lines.join("\n")
    }
}

impl HumanReadable for Vec<Message> {
    fn to_human_string(&self) -> String {
        self.iter()
            .map(format_message)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl HumanReadable for Vec<Notification> {
    fn to_human_string(&self) -> String {
        if self.is_empty() {
            return "(no notifications)".to_string();
        }
        self.iter()
            .map(|notification| format_message(&notification.message))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl HumanReadable for CheckResult {
    fn to_human_string(&self) -> String {
        if self.has_new_messages {
            format!(
                "new: {} newest={} anchor={} source={}",
                self.count,
                self.newest_post_id.clone().unwrap_or_default(),
                self.anchor.clone().unwrap_or_else(|| "none".to_string()),
                self.anchor_source,
            )
        } else {
            format!(
                "new: 0 anchor={} source={}",
                self.anchor.clone().unwrap_or_else(|| "none".to_string()),
                self.anchor_source,
            )
        }
    }
}

impl HumanReadable for UnreadNotifications {
    fn to_human_string(&self) -> String {
        format!("unread: {}", self.count)
    }
}

impl HumanReadable for WaitResult {
    fn to_human_string(&self) -> String {
        self.messages
            .iter()
            .map(format_message)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl HumanReadable for AckResult {
    fn to_human_string(&self) -> String {
        match &self.cursor_post_id {
            Some(post_id) => format!(
                "ack: {}/{} cursor advanced to {}",
                self.team, self.channel, post_id
            ),
            None => format!(
                "ack: {}/{} channel has no posts; cursor unchanged",
                self.team, self.channel
            ),
        }
    }
}

impl HumanReadable for ReactionResult {
    fn to_human_string(&self) -> String {
        // PER-024 verb-shape note: the same human format works for
        // both `react` and `unreact` because the result struct
        // doesn't carry a verb discriminator — `ok: true` is the
        // operator-facing contract on either path.
        format!(
            "ok: {}/{} {} on post {}",
            self.team, self.channel, self.emoji, self.post_id
        )
    }
}

impl HumanReadable for PinResult {
    fn to_human_string(&self) -> String {
        // PER-034 §Output formats: human-readable form is
        // `pinned: <post-id-short> in <team>/<channel>`.
        let short = short_post_id(&self.post_id);
        format!("pinned: {} in {}/{}", short, self.team, self.channel)
    }
}

impl HumanReadable for UnpinResult {
    fn to_human_string(&self) -> String {
        let short = short_post_id(&self.post_id);
        format!("unpinned: {} in {}/{}", short, self.team, self.channel)
    }
}

/// PER-034: shorten an MM post-id for the human-readable output.
/// First 8 hex chars are sufficient for operator-visible
/// disambiguation; full IDs land in `--json` output.
fn short_post_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

impl HumanReadable for SearchResult {
    fn to_human_string(&self) -> String {
        if self.posts.is_empty() {
            return format!("no matches in {}/{}", self.team, self.channel);
        }
        let header = format!(
            "{} match(es) in {}/{}:",
            self.posts.len(),
            self.team,
            self.channel
        );
        let lines: Vec<String> = std::iter::once(header)
            .chain(self.posts.iter().map(format_message))
            .collect();
        lines.join("\n")
    }
}

impl HumanReadable for Vec<Profile> {
    fn to_human_string(&self) -> String {
        self.iter()
            .map(|profile| format!("{} ({}/{})", profile.name, profile.scope, profile.role))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl HumanReadable for Channel {
    fn to_human_string(&self) -> String {
        format!("created: #{} ({})", self.name, self.id)
    }
}

impl HumanReadable for ProfileStatus {
    fn to_human_string(&self) -> String {
        format!(
            "profile: {}\nrole: {}\nscope: {}\nbot: {}\nserver: {}\nsocket: {}",
            self.profile_name,
            self.role,
            self.scope,
            self.bot_username,
            self.server_url,
            self.socket_path.display()
        )
    }
}

impl HumanReadable for DaemonStatus {
    fn to_human_string(&self) -> String {
        let mut out = format!(
            "profile: {}\nsocket: {}\nmattermost_username: {}\nmattermost_ok: {}",
            self.profile_name,
            self.socket_path.display(),
            self.mattermost_username,
            self.mattermost_ok
        );
        if let Some(h) = self.health {
            out.push_str(&format!("\nhealth: {}", health_label(h)));
        }
        if let Some(state) = self.ws_connection_state {
            out.push_str(&format!("\nws_state: {}", ws_state_label(state)));
        }
        if let Some(rc) = self.ws_reconnect_count {
            out.push_str(&format!("\nreconnect_count: {}", rc));
        }
        if let Some(last_event) = self.ws_last_event_at {
            out.push_str(&format!(
                "\nws_last_event_at: {}",
                format_timestamp(last_event)
            ));
        }
        if let Some(last_disc) = self.ws_last_disconnect_at {
            out.push_str(&format!(
                "\nws_last_disconnect_at: {}",
                format_timestamp(last_disc)
            ));
        }
        if let Some(last_rec) = self.ws_last_recovered_at {
            out.push_str(&format!(
                "\nws_last_recovered_at: {}",
                format_timestamp(last_rec)
            ));
        }
        if self.ws_suspected_gap == Some(true) {
            out.push_str("\nsuspected_gap: yes");
        }
        if let Some(err) = &self.mattermost_last_error {
            out.push_str(&format!("\nmattermost_last_error: {}", err));
        }
        out
    }
}

fn health_label(h: DaemonHealthState) -> &'static str {
    match h {
        DaemonHealthState::Healthy => "healthy",
        DaemonHealthState::Connecting => "connecting",
        DaemonHealthState::Degraded => "degraded",
        DaemonHealthState::Disconnected => "disconnected",
        DaemonHealthState::Recovering => "recovering",
    }
}

fn ws_state_label(s: WsConnectionState) -> &'static str {
    match s {
        WsConnectionState::Disconnected => "disconnected",
        WsConnectionState::Connecting => "connecting",
        WsConnectionState::Healthy => "healthy",
        WsConnectionState::Degraded => "degraded",
    }
}

fn format_message(message: &Message) -> String {
    format!(
        "{} [{}]\n{}\n---",
        format_timestamp(message.create_at),
        message.username,
        message.message
    )
}

fn format_timestamp(millis: i64) -> String {
    match Utc.timestamp_millis_opt(millis).single() {
        Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => millis.to_string(),
    }
}

fn format_dm_timestamp(millis: i64) -> String {
    match Utc.timestamp_millis_opt(millis).single() {
        Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M").to_string(),
        None => millis.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::sync::Mutex;

    /// Serialize env-mutating tests in this module. Cargo runs tests on
    /// multiple threads by default, and bare `env::set_var` /
    /// `env::remove_var` calls would otherwise race with each other and
    /// with any test that reads the same vars.
    static CONFIG_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn format_last_active_buckets() {
        let now = 1_700_000_000_000i64;
        // Missing / zero — `—` per AC #5.
        assert_eq!(format_last_active(None, now), "—");
        assert_eq!(format_last_active(Some(0), now), "—");
        // Sub-minute, minute, hour, day, week, year buckets.
        assert_eq!(format_last_active(Some(now - 30_000), now), "30s ago");
        assert_eq!(format_last_active(Some(now - 5 * 60_000), now), "5m ago");
        assert_eq!(format_last_active(Some(now - 4 * 3_600_000), now), "4h ago");
        assert_eq!(
            format_last_active(Some(now - 2 * 86_400_000), now),
            "2d ago"
        );
        assert_eq!(
            format_last_active(Some(now - 14 * 86_400_000), now),
            "2w ago"
        );
        assert_eq!(
            format_last_active(Some(now - 730 * 86_400_000), now),
            "2y ago"
        );
    }

    #[test]
    fn format_last_active_clock_skew_renders_just_now() {
        // Future timestamp (clock skew) should not render as a
        // negative duration.
        let now = 1_700_000_000_000i64;
        assert_eq!(format_last_active(Some(now + 5_000), now), "just now");
    }

    #[test]
    fn sort_groups_by_active_within_group_only() {
        use chanvoy_core::TeamChannels;
        let mut groups = vec![
            TeamChannels {
                team_id: "t1".to_string(),
                team_name: "team-a".to_string(),
                team_display_name: "Team A".to_string(),
                channels: vec![
                    Channel {
                        id: "c1".to_string(),
                        name: "old".to_string(),
                        display_name: "old".to_string(),
                        channel_type: "O".to_string(),
                        last_post_at: Some(100),
                    },
                    Channel {
                        id: "c2".to_string(),
                        name: "newer".to_string(),
                        display_name: "newer".to_string(),
                        channel_type: "O".to_string(),
                        last_post_at: Some(500),
                    },
                    Channel {
                        id: "c3".to_string(),
                        name: "never".to_string(),
                        display_name: "never".to_string(),
                        channel_type: "O".to_string(),
                        last_post_at: None,
                    },
                ],
            },
            TeamChannels {
                team_id: "t2".to_string(),
                team_name: "team-b".to_string(),
                team_display_name: "Team B".to_string(),
                channels: vec![Channel {
                    id: "c4".to_string(),
                    name: "only".to_string(),
                    display_name: "only".to_string(),
                    channel_type: "O".to_string(),
                    last_post_at: Some(300),
                }],
            },
        ];

        sort_groups_by_active(&mut groups);

        // Group order itself preserved (NOT flattened) per AC #5.
        assert_eq!(groups[0].team_name, "team-a");
        assert_eq!(groups[1].team_name, "team-b");
        // Within team-a: most-recent first, never-active last.
        let names: Vec<&str> = groups[0].channels.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["newer", "old", "never"]);
    }

    // Regression test: the human `chanvoy attention list` output
    // must surface `AttentionListResult.quarantined` so operators
    // using the default text mode see legacy cursors that the
    // cross-team migration couldn't bind cleanly. Pre-fix, the JSON
    // output exposed the field but `render_attention_list_text`
    // ignored it.
    #[test]
    fn devrev_pr17_attention_list_renderer_surfaces_quarantined() {
        use chanvoy_core::{
            AttentionListResult, AttentionMentionEntry, AttentionSource, QuarantinedCursor,
        };

        let result = AttentionListResult {
            profile: "bravo-devlead-lanytehq".to_string(),
            channels: Vec::new(),
            mentions: AttentionMentionEntry {
                source: AttentionSource::NoAnchor,
                newest_seen: None,
                updated_at: None,
            },
            quarantined: vec![QuarantinedCursor {
                legacy_channel_name: "general".to_string(),
                ambiguous_teams: vec!["org-lanytehq".to_string(), "3-leaps-operations".to_string()],
                state: chanvoy_core::ChannelCursorState::default(),
                quarantined_at: 1_777_500_000_000,
            }],
        };

        let rendered = render_attention_list_text(&result);

        assert!(
            rendered.contains("quarantined (1 record):"),
            "renderer must surface quarantined section header. got:\n{rendered}"
        );
        assert!(
            rendered.contains("LEGACY_NAME"),
            "renderer must include the column header. got:\n{rendered}"
        );
        assert!(
            rendered.contains("general"),
            "renderer must include the legacy channel name. got:\n{rendered}"
        );
        assert!(
            rendered.contains("org-lanytehq") && rendered.contains("3-leaps-operations"),
            "renderer must list both ambiguous teams. got:\n{rendered}"
        );
        assert!(
            rendered.contains("--team") || rendered.contains("<team>/<channel>"),
            "renderer must point operators at the disambiguation syntax. got:\n{rendered}"
        );
    }

    #[test]
    fn devrev_pr17_attention_list_renderer_omits_quarantined_when_empty() {
        // Symmetric: if there are no quarantined entries, the section
        // is omitted entirely (no spurious empty header).
        use chanvoy_core::{AttentionListResult, AttentionMentionEntry, AttentionSource};

        let result = AttentionListResult {
            profile: "bravo-devlead-lanytehq".to_string(),
            channels: Vec::new(),
            mentions: AttentionMentionEntry {
                source: AttentionSource::NoAnchor,
                newest_seen: None,
                updated_at: None,
            },
            quarantined: Vec::new(),
        };
        let rendered = render_attention_list_text(&result);
        assert!(
            !rendered.contains("quarantined"),
            "no quarantined section when the vec is empty. got:\n{rendered}"
        );
    }

    #[test]
    fn identity_uses_lanyte_chat_shape() {
        let identity = Identity {
            id: "user-id".to_string(),
            username: "agent-bravo-devlead".to_string(),
            is_bot: true,
            nickname: None,
            email: Some("agent-bravo-devlead@localhost".to_string()),
        };

        let printable = printable_identity(&identity);
        assert_eq!(printable["username"], "agent-bravo-devlead");
        assert_eq!(printable["id"], "user-id");
        assert_eq!(printable["is_bot"], true);
        assert_eq!(printable["email"], "agent-bravo-devlead@localhost");
    }

    #[test]
    fn identity_json_preserves_full_structured_fields() {
        let identity = Identity {
            id: "user-id".to_string(),
            username: "agent-bravo-devlead".to_string(),
            is_bot: true,
            nickname: Some("Bravo devlead".to_string()),
            email: None,
        };

        let rendered = serde_json::to_value(&identity).unwrap();
        assert_eq!(rendered["nickname"], "Bravo devlead");
        assert!(rendered["email"].is_null());
        assert_eq!(rendered["is_bot"], true);
    }

    #[test]
    fn channels_render_as_legacy_table() {
        let channels = vec![
            Channel {
                id: "dm-id".to_string(),
                name: "dm-channel".to_string(),
                display_name: String::new(),
                channel_type: "D".to_string(),
                last_post_at: None,
            },
            Channel {
                id: "chan-id".to_string(),
                name: "per-007".to_string(),
                display_name: "PER-007".to_string(),
                channel_type: "O".to_string(),
                last_post_at: None,
            },
        ];

        let rendered = channels.to_human_string();
        assert!(rendered.contains("dm-channel"));
        assert!(rendered.contains("per-007"));
        assert!(rendered.contains("PER-007"));
        assert!(rendered.contains("D"));
        assert!(rendered.contains("O"));
    }

    #[test]
    fn dms_render_with_helper_line() {
        let conversations = vec![DmConversation {
            id: "dm-id".to_string(),
            name: "user_a__user_b".to_string(),
            last_post_at: 1_744_327_700_000,
        }];

        let rendered = conversations.to_human_string();
        assert!(rendered.contains("dm-id"));
        assert!(rendered.contains("user_a__user_b"));
        assert!(rendered.contains("Use 'chanvoy dm read <username>' to read a conversation."));
    }

    #[test]
    fn dms_render_shows_twenty_rows() {
        let conversations = (0..20)
            .map(|idx| DmConversation {
                id: format!("dm-{idx}"),
                name: format!("user-{idx}"),
                last_post_at: 1_744_327_700_000 + idx,
            })
            .collect::<Vec<_>>();

        let rendered = conversations.to_human_string();
        assert!(rendered.contains("dm-19"));
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with("2025-"))
                .count(),
            20
        );
    }

    // The previous `env_profile_resolution_prefers_scope_match_over_name_match`
    // test exercised the legacy `derive_env_profile_name` function whose
    // role+scope-filter-then-fall-through logic was replaced in PER-012. The
    // equivalent contract — env-derived `${role}-${scope}` exact-name match
    // wins, including over sibling profiles sharing role+scope — is now
    // pinned by `chanvoy_core::tests::resolver::*` (see
    // `env_exact_name_wins_over_sibling_profiles_sharing_role_scope`).

    #[test]
    fn team_name_uses_mattermost_env_when_present() {
        let _lock = CONFIG_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { env::set_var(OsStr::new("LANYTE_MM_TEAM"), OsStr::new("custom-team")) };
        assert_eq!(derive_team_name("lanytehq"), "custom-team");
        unsafe { env::remove_var(OsStr::new("LANYTE_MM_TEAM")) };
    }

    #[test]
    fn team_name_derives_org_scope_for_non_lanytehq_scope() {
        // PER-012: removed the silent fallback to `org-lanytehq`.
        // For scopes other than lanytehq the derived team must follow
        // the scope, not bias to the historical default.
        let _lock = CONFIG_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { env::remove_var(OsStr::new("LANYTE_MM_TEAM")) };
        assert_eq!(derive_team_name("enacthq"), "org-enacthq");
        assert_eq!(derive_team_name("fulmenhq"), "org-fulmenhq");
        assert_eq!(derive_team_name("lanytehq"), "org-lanytehq");
    }

    #[test]
    fn profile_create_team_name_derives_from_scope_when_flag_absent() {
        // PER-012 AC #6 / devrev follow-up blocker: `profile create`
        // no longer hardcodes `org-lanytehq` as the team-name default
        // at the clap level. When --team-name is absent, derive from
        // the (required positional) scope arg.
        let args = ProfileCreateArgs {
            name: "delta-devlead-enacthq".into(),
            role: "delta-devlead".into(),
            scope: "enacthq".into(),
            bot_username: "agent-delta-devlead".into(),
            server_url: "https://mm.example.com".into(),
            env_name: "LANYTE_MM_TOKEN".into(),
            team_name: None,
            env_file: None,
            credential_mode: CliCredentialMode::EnvName,
            capability_class: CliCapabilityClass::Standard,
            activate: false,
        };
        let profile = profile_from_create_args(&args);
        assert_eq!(profile.team_name, "org-enacthq");
    }

    /// RAII guard for clean-bootstrap tests. `save_and_clear` snapshots
    /// the four env vars that participate in the resolver, clears them,
    /// and returns a guard whose `Drop` impl restores the prior values
    /// — including on panic-unwind paths. Combined with
    /// `CONFIG_ENV_LOCK` this prevents test-state leak across runs and
    /// keeps later tests from seeing the wrong failure if a current
    /// test panics mid-execution.
    struct EnvSnapshot {
        config_dir: Option<std::ffi::OsString>,
        role: Option<std::ffi::OsString>,
        scope: Option<std::ffi::OsString>,
        chanvoy_profile: Option<std::ffi::OsString>,
    }

    impl EnvSnapshot {
        fn save_and_clear(temp_config_dir: &std::path::Path) -> Self {
            let snap = Self {
                config_dir: env::var_os("CHANVOY_CONFIG_DIR"),
                role: env::var_os("LANYTE_AGENT_ROLE"),
                scope: env::var_os("LANYTE_AGENT_SCOPE"),
                chanvoy_profile: env::var_os("CHANVOY_PROFILE"),
            };
            unsafe {
                env::set_var(OsStr::new("CHANVOY_CONFIG_DIR"), temp_config_dir);
                env::remove_var(OsStr::new("LANYTE_AGENT_ROLE"));
                env::remove_var(OsStr::new("LANYTE_AGENT_SCOPE"));
                env::remove_var(OsStr::new("CHANVOY_PROFILE"));
            }
            snap
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            // `take()` is the canonical way to move `Option<T>` out of
            // a `&mut self` borrow that `Drop::drop` provides.
            unsafe {
                restore_env("CHANVOY_CONFIG_DIR", self.config_dir.take());
                restore_env("LANYTE_AGENT_ROLE", self.role.take());
                restore_env("LANYTE_AGENT_SCOPE", self.scope.take());
                restore_env("CHANVOY_PROFILE", self.chanvoy_profile.take());
            }
        }
    }

    unsafe fn restore_env(name: &str, prior: Option<std::ffi::OsString>) {
        match prior {
            Some(v) => unsafe { env::set_var(OsStr::new(name), v) },
            None => unsafe { env::remove_var(OsStr::new(name)) },
        }
    }

    // Hold the env lock across `execute(cli).await` deliberately. The
    // lock serializes test functions that mutate process-global env;
    // dropping it before the await would let a parallel test mutate
    // env while our `execute` runs, breaking the bootstrap precondition.
    // Every grabber of this lock is a test function — no tokio task
    // competes for it — so the cross-await hold cannot deadlock here.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn profile_list_succeeds_on_clean_bootstrap_with_no_env_no_profiles() {
        // secrev follow-up blocker: the centralized resolver was running
        // before every command except `auto-setup`, which broke
        // profile-management verbs that operate on storage directly. A
        // fresh operator (or new-org adopter) installing chanvoy and
        // running `chanvoy profile list` with empty config dir, no env,
        // and no daemons would hit `CannotResolve { available: [] }`
        // before the management surface ran. Bootstrap was unreachable.
        //
        // Post-fix: profile management dispatches before the resolver,
        // mirroring auto-setup's path.
        let dir = tempfile::tempdir().unwrap();
        let _lock = CONFIG_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env_snap = EnvSnapshot::save_and_clear(dir.path());
        let cli = Cli {
            profile: None,
            json: true,
            command: CommandSet::Profile(ProfileCommand::List),
        };
        // `_env_snap`'s Drop restores env at scope exit, including on
        // panic-unwind paths from `execute()` or `expect()`.
        execute(cli)
            .await
            .expect("profile list must succeed on clean bootstrap");
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn profile_create_succeeds_on_clean_bootstrap_with_valid_args() {
        // PER-012A: closes the create-half of PER-012 AC #7. The list-
        // half sibling test pins that the management-verb resolver
        // bypass fixes empty-config enumeration; this test pins that
        // the same bypass fixes empty-config creation. Together they
        // establish the full fresh-bootstrap regression envelope
        // (secrev's original PER-012 finding).
        //
        // Also re-pins AC #6 (no `org-lanytehq` hardcoded default for
        // `--team-name`): we omit `--team-name` and assert the derived
        // value is `org-<scope>` from the positional scope arg.
        //
        // Pure storage-only test by design: no daemon, no Mattermost,
        // no token material. Dummy `--env-name` value avoids implying
        // `profile create` validates token material — that path is
        // `create-from-env`'s job, not `create`'s.
        let dir = tempfile::tempdir().unwrap();
        let _lock = CONFIG_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _env_snap = EnvSnapshot::save_and_clear(dir.path());

        let create_args = ProfileCreateArgs {
            name: "delta-devlead-enacthq".into(),
            role: "delta-devlead".into(),
            scope: "enacthq".into(),
            bot_username: "agent-delta-devlead".into(),
            server_url: "https://mm.example.com".into(),
            env_name: "DUMMY_TOKEN_VAR".into(),
            team_name: None, // omitted -> re-pins AC #6 derivation
            env_file: None,
            credential_mode: CliCredentialMode::EnvName,
            capability_class: CliCapabilityClass::Standard,
            activate: false,
        };
        let cli = Cli {
            profile: None,
            json: true,
            command: CommandSet::Profile(ProfileCommand::Create(create_args)),
        };

        // `_env_snap`'s Drop restores env at scope exit, including
        // panic-unwind paths from any of the assertions below. Capture
        // `list_profiles()` while CHANVOY_CONFIG_DIR still points at
        // the tempdir (the env snapshot is restored on scope exit, not
        // here).
        execute(cli)
            .await
            .expect("profile create must succeed on clean bootstrap (resolver-bypass contract)");
        let profiles =
            list_profiles().expect("list_profiles must succeed against the fresh config dir");
        let created = profiles
            .iter()
            .find(|p| p.name == "delta-devlead-enacthq")
            .expect("created profile must be present in list_profiles output");
        assert_eq!(created.role, "delta-devlead");
        assert_eq!(created.scope, "enacthq");
        assert_eq!(created.bot_username, "agent-delta-devlead");
        assert_eq!(created.env_name, "DUMMY_TOKEN_VAR");
        // AC #6 re-pin: with no `--team-name` flag, team must be
        // derived from the positional scope arg, not the historical
        // hardcoded `org-lanytehq` default.
        assert_eq!(created.team_name, "org-enacthq");
    }

    #[test]
    fn profile_create_team_name_uses_explicit_flag_when_provided() {
        let args = ProfileCreateArgs {
            name: "n".into(),
            role: "r".into(),
            scope: "enacthq".into(),
            bot_username: "b".into(),
            server_url: "https://mm.example.com".into(),
            env_name: "LANYTE_MM_TOKEN".into(),
            team_name: Some("custom-team".into()),
            env_file: None,
            credential_mode: CliCredentialMode::EnvName,
            capability_class: CliCapabilityClass::Standard,
            activate: false,
        };
        let profile = profile_from_create_args(&args);
        assert_eq!(profile.team_name, "custom-team");
    }

    #[test]
    fn check_result_renders_compact_probe_output() {
        let result = CheckResult {
            channel: "per-008".to_string(),
            anchor: Some("anchor-1".to_string()),
            anchor_source: "daemon_cursor".to_string(),
            has_new_messages: true,
            count: 3,
            newest_post_id: Some("post-3".to_string()),
        };

        assert_eq!(
            result.to_human_string(),
            "new: 3 newest=post-3 anchor=anchor-1 source=daemon_cursor"
        );
    }

    #[test]
    fn unread_notifications_render_as_count_only() {
        let unread = UnreadNotifications { count: 4 };
        assert_eq!(unread.to_human_string(), "unread: 4");
    }

    fn sample_profile() -> Profile {
        Profile {
            name: "bravo-devlead".to_string(),
            role: "bravo-devlead".to_string(),
            scope: "lanytehq".to_string(),
            provider: Provider::Mattermost,
            bot_username: "agent-bravo-devlead".to_string(),
            team_name: "org-lanytehq".to_string(),
            server_url: "https://mm.example.com".to_string(),
            env_name: "LANYTE_MM_TOKEN".to_string(),
            env_file: None,
            credential_mode: CredentialMode::EnvName,
            capability_class: CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
            reduce: None,
        }
    }

    #[test]
    fn decide_profile_action_creates_when_absent() {
        let desired = sample_profile();
        assert_eq!(decide_profile_action(&desired, None), ProfileAction::Create);
    }

    #[test]
    fn decide_profile_action_reuses_when_all_compared_fields_match() {
        let desired = sample_profile();
        let mut existing = sample_profile();
        // Non-compared fields diverging must still Reuse.
        existing.bot_username = "stale-username".to_string();
        existing.monitored_channels = vec!["extra-channel".to_string()];
        assert_eq!(
            decide_profile_action(&desired, Some(&existing)),
            ProfileAction::Reuse
        );
    }

    #[test]
    fn decide_profile_action_refreshes_on_refreshable_diff_only() {
        let mut desired = sample_profile();
        desired.env_name = "NEW_TOKEN_ENV".to_string();
        let existing = sample_profile();
        match decide_profile_action(&desired, Some(&existing)) {
            ProfileAction::Refresh(diff) => {
                assert_eq!(diff.len(), 1);
                assert_eq!(diff[0].field, "env_name");
            }
            other => panic!("expected Refresh, got {other:?}"),
        }
    }

    #[test]
    fn decide_profile_action_hard_errors_on_identity_drift() {
        let desired = sample_profile();
        let mut existing = sample_profile();
        existing.server_url = "https://old.example.com".to_string();
        existing.team_name = "old-team".to_string();
        match decide_profile_action(&desired, Some(&existing)) {
            ProfileAction::IdentityDrift(diff) => {
                let fields: Vec<&str> = diff.iter().map(|d| d.field.as_str()).collect();
                assert!(fields.contains(&"server_url"));
                assert!(fields.contains(&"team_name"));
                assert_eq!(fields.len(), 2);
            }
            other => panic!("expected IdentityDrift, got {other:?}"),
        }
    }

    #[test]
    fn identity_drift_takes_precedence_over_refreshable_diff() {
        // When both identity and refreshable fields differ, IdentityDrift wins so the
        // operator sees the hard-error rather than a silent refresh.
        let mut desired = sample_profile();
        desired.env_name = "NEW_TOKEN_ENV".to_string();
        let mut existing = sample_profile();
        existing.scope = "other-scope".to_string();
        match decide_profile_action(&desired, Some(&existing)) {
            ProfileAction::IdentityDrift(diff) => {
                let fields: Vec<&str> = diff.iter().map(|d| d.field.as_str()).collect();
                assert_eq!(fields, vec!["scope"]);
            }
            other => panic!("expected IdentityDrift, got {other:?}"),
        }
    }

    #[test]
    fn refreshable_diff_excludes_identity_and_non_env_fields() {
        let mut a = sample_profile();
        let mut b = sample_profile();
        a.name = "a-name".to_string();
        b.name = "b-name".to_string();
        a.bot_username = "bot-a".to_string();
        b.bot_username = "bot-b".to_string();
        a.monitored_channels = vec!["chan-a".to_string()];
        b.monitored_channels = vec!["chan-b".to_string()];
        assert!(refreshable_profile_diff(&a, &b).is_empty());
        assert!(identity_surface_diff(&a, &b).is_empty());
    }

    // ---- PER-035: reduction policy in auto-setup decide/merge logic ----

    fn reduce_to(name: &str) -> Option<chanvoy_core::ReducePolicy> {
        Some(chanvoy_core::ReducePolicy {
            use_profile: name.to_string(),
        })
    }

    #[test]
    fn changing_reduce_policy_is_a_refresh_not_identity_drift() {
        // A new/changed reduction target is a visible refresh — it does
        // not change WHO the profile authenticates as or its primary
        // team, so it must never hard-error as identity drift.
        let mut desired = sample_profile();
        desired.reduce = reduce_to("dataeng-galaxy");
        let existing = sample_profile(); // reduce: None
        match decide_profile_action(&desired, Some(&existing)) {
            ProfileAction::Refresh(diff) => {
                assert_eq!(diff.len(), 1);
                assert_eq!(diff[0].field, "reduce.use_profile");
                assert_eq!(diff[0].to, "dataeng-galaxy");
            }
            other => panic!("expected Refresh, got {other:?}"),
        }
    }

    #[test]
    fn omitting_reduce_flag_on_refresh_preserves_existing_policy() {
        // desired.reduce == None models "flag not passed". decide must
        // NOT register a spurious diff (Reuse), and merge-forward must
        // restore the existing policy so it is not wiped.
        let desired = sample_profile(); // reduce: None (flag omitted)
        let mut existing = sample_profile();
        existing.reduce = reduce_to("dataeng-galaxy");
        assert_eq!(
            decide_profile_action(&desired, Some(&existing)),
            ProfileAction::Reuse
        );
        let merged = merge_forward_for_refresh(desired, &existing);
        assert_eq!(
            merged.reduce.as_ref().map(|r| r.use_profile.as_str()),
            Some("dataeng-galaxy"),
            "omitting --reduce-profile must preserve the existing policy"
        );
    }

    #[test]
    fn unchanged_reduce_policy_reuses() {
        let mut desired = sample_profile();
        desired.reduce = reduce_to("dataeng-galaxy");
        let mut existing = sample_profile();
        existing.reduce = reduce_to("dataeng-galaxy");
        assert_eq!(
            decide_profile_action(&desired, Some(&existing)),
            ProfileAction::Reuse
        );
    }

    #[test]
    fn repointing_reduce_target_is_a_refresh() {
        let mut desired = sample_profile();
        desired.reduce = reduce_to("dataeng-galaxy-new");
        let mut existing = sample_profile();
        existing.reduce = reduce_to("dataeng-galaxy-old");
        match decide_profile_action(&desired, Some(&existing)) {
            ProfileAction::Refresh(diff) => {
                assert_eq!(diff.len(), 1);
                assert_eq!(diff[0].field, "reduce.use_profile");
                assert_eq!(diff[0].from, "dataeng-galaxy-old");
                assert_eq!(diff[0].to, "dataeng-galaxy-new");
            }
            other => panic!("expected Refresh, got {other:?}"),
        }
    }

    #[test]
    fn check_token_available_maps_missing_env_to_bootstrap() {
        // devrev finding #2 (chanvoy#7): missing token env is an env-input failure
        // (exit 2), not a remote preflight failure (exit 3). Pin the mapping.
        let unique = "CHANVOY_UNSET_TOKEN_FOR_UNIT_TEST_98765_XYZ";
        unsafe { env::remove_var(unique) };
        let profile = Profile {
            name: "test".to_string(),
            role: "test".to_string(),
            scope: "test".to_string(),
            provider: Provider::Mattermost,
            bot_username: String::new(),
            team_name: "t".to_string(),
            server_url: "https://mm.example.com".to_string(),
            env_name: unique.to_string(),
            env_file: None,
            credential_mode: CredentialMode::EnvName,
            capability_class: CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
            reduce: None,
        };
        let err = check_token_available(&profile).expect_err("must fail when env unset");
        assert!(
            matches!(err, CliError::Bootstrap(_)),
            "expected Bootstrap (maps to EXIT_ENV_INPUT), got {err:?}"
        );
    }

    #[test]
    fn merge_forward_preserves_non_env_fields() {
        // devrev item #3: refresh path must not wipe monitored_channels / ipc.
        let mut existing = sample_profile();
        existing.monitored_channels = vec!["per-008".to_string(), "per-009".to_string()];
        existing.env_file = Some(PathBuf::from("/secrets/bravo.env"));
        let mut desired = sample_profile();
        desired.env_name = "REFRESHED_TOKEN_ENV".to_string();
        // Desired comes from env — env does not populate monitored_channels or env_file.
        assert!(desired.monitored_channels.is_empty());
        assert_eq!(desired.env_file, None);

        let merged = merge_forward_for_refresh(desired, &existing);
        assert_eq!(merged.monitored_channels, existing.monitored_channels);
        assert_eq!(merged.env_file, existing.env_file);
        // Env-owned field still took the desired value.
        assert_eq!(merged.env_name, "REFRESHED_TOKEN_ENV");
    }
}
