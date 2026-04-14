use std::path::PathBuf;
use std::process;
use std::process::Stdio;
use std::{env, ffi::OsStr};

use chanvoy_core::{
    list_profiles, load_active_profile, load_token, store_active_profile, store_profile,
    CapabilityClass, Channel, CredentialMode, DaemonStatus, DmConversation, Identity,
    MattermostClient, Message, Notification, PostReceipt, Profile, ProfileStatus, Provider,
    WaitResult, DEFAULT_TEAM,
};
use chanvoy_daemon::{daemon_client, ping, start, status, stop, DaemonError};
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
    Channels,
    Dms,
    Read(ReadArgs),
    Post(PostArgs),
    #[command(subcommand)]
    Dm(DmCommand),
    Notify(NotifyArgs),
    Notifications(ReadWindowArgs),
    Wait(WaitArgs),
    #[command(subcommand)]
    Channel(ChannelCommand),
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
    #[arg(long, default_value_t = 60)]
    since: u64,
}

#[derive(Debug, Args)]
struct ReadWindowArgs {
    #[arg(long, default_value_t = 1440)]
    since: u64,
}

#[derive(Debug, Args)]
struct WaitArgs {
    channel: String,
    #[arg(long, default_value_t = 10)]
    timeout: u64,
}

#[derive(Debug, Args)]
struct PostArgs {
    channel: String,
    message: String,
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
    #[arg(long, default_value = "org-lanytehq")]
    team_name: String,
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
    let profile = resolve_profile_name(cli.profile.as_deref())?;
    match cli.command {
        CommandSet::Daemon(command) => handle_daemon(&profile, cli.json, command).await,
        CommandSet::Profile(command) => handle_profile(&profile, cli.json, command).await,
        CommandSet::Whoami => print_identity(cli.json, &daemon_client(&profile).whoami().await?),
        CommandSet::Channels => {
            print_value(cli.json, &daemon_client(&profile).list_channels().await?)
        }
        CommandSet::Dms => print_value(cli.json, &daemon_client(&profile).list_dms().await?),
        CommandSet::Read(args) => print_value(
            cli.json,
            &daemon_client(&profile)
                .read_channel(&args.channel, args.since)
                .await?,
        ),
        CommandSet::Post(args) => print_receipt(
            cli.json,
            "posted",
            &daemon_client(&profile)
                .post_message(&args.channel, &args.message)
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
        CommandSet::Notifications(args) => print_value(
            cli.json,
            &daemon_client(&profile).notifications(args.since).await?,
        ),
        CommandSet::Wait(args) => {
            if !cli.json {
                eprintln!(
                    "waiting for new message in #{} (timeout: {}m)...",
                    args.channel, args.timeout
                );
            }
            match daemon_client(&profile)
                .wait_channel(&args.channel, args.timeout)
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
                            "timeout: no new messages in #{} after {} minutes",
                            args.channel, args.timeout
                        );
                    }
                    process::exit(1);
                }
                Err(error) => Err(error.into()),
            }
        }
        CommandSet::Channel(ChannelCommand::Create(args)) => print_value(
            cli.json,
            &daemon_client(&profile)
                .create_channel(&args.name, &args.display_name, args.purpose)
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

async fn handle_profile(
    profile: &str,
    json: bool,
    command: ProfileCommand,
) -> Result<(), CliError> {
    match command {
        ProfileCommand::List => print_value(json, &list_profiles()?),
        ProfileCommand::Active => {
            let active = load_active_profile()?.unwrap_or_else(|| profile.to_string());
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "active_profile": active }))?
                );
            } else {
                println!("{active}");
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
    }
}

fn resolve_profile_name(profile: Option<&str>) -> Result<String, CliError> {
    if let Some(profile) = profile {
        return Ok(profile.to_string());
    }
    if let Some(profile) = resolve_env_profile_name()? {
        return Ok(profile);
    }
    Ok(load_active_profile()?.unwrap_or_else(|| "default".to_string()))
}

fn resolve_env_profile_name() -> Result<Option<String>, CliError> {
    let explicit = env_var_nonempty("CHANVOY_PROFILE");
    let role = env_var_nonempty("LANYTE_AGENT_ROLE");
    let scope = env_var_nonempty("LANYTE_AGENT_SCOPE");
    let profiles = list_profiles()?;

    Ok(derive_env_profile_name(
        explicit.as_deref(),
        role.as_deref(),
        scope.as_deref(),
        &profiles,
    ))
}

fn derive_env_profile_name(
    explicit: Option<&str>,
    role: Option<&str>,
    scope: Option<&str>,
    profiles: &[Profile],
) -> Option<String> {
    if let Some(explicit) = explicit {
        return Some(explicit.to_string());
    }

    let role = role?;

    if let Some(scope) = scope {
        let mut scoped_matches = profiles
            .iter()
            .filter(|profile| profile.role == role && profile.scope == scope);
        let first = scoped_matches.next();
        if let Some(first) = first {
            if scoped_matches.next().is_none() {
                return Some(first.name.clone());
            }
            return None;
        }
    }

    if profiles.iter().any(|profile| profile.name == role) {
        return Some(role.to_string());
    }

    let mut matches = profiles.iter().filter(|profile| profile.role == role);

    let first = matches.next()?;
    if matches.next().is_none() {
        Some(first.name.clone())
    } else {
        None
    }
}

fn validate_profile_create_args(profile: &Profile) -> Result<(), CliError> {
    if matches!(profile.credential_mode, CredentialMode::EnvFile) && profile.env_file.is_none() {
        return Err(chanvoy_core::CoreError::MissingEnvFile.into());
    }
    Ok(())
}

fn profile_from_create_args(args: &ProfileCreateArgs) -> Profile {
    Profile {
        name: args.name.clone(),
        role: args.role.clone(),
        scope: args.scope.clone(),
        provider: Provider::Mattermost,
        bot_username: args.bot_username.clone(),
        team_name: args.team_name.clone(),
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
    }
}

async fn profile_from_env_args(args: &ProfileCreateFromEnvArgs) -> Result<Profile, CliError> {
    let role = required_env("LANYTE_AGENT_ROLE")?;
    let scope = required_env("LANYTE_AGENT_SCOPE")?;
    let server_url = required_env("LANYTE_MM_URL")?;

    let mut profile = Profile {
        name: args.name.clone().unwrap_or_else(|| role.clone()),
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
    if let Some(team) = env_var_nonempty("LANYTE_MM_TEAM") {
        return team;
    }
    if scope.is_empty() {
        DEFAULT_TEAM.to_string()
    } else {
        format!("org-{scope}")
    }
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
        lines.push("Use 'lanyte-chat dm read <username>' to read a conversation.".to_string());
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

impl HumanReadable for WaitResult {
    fn to_human_string(&self) -> String {
        self.messages
            .iter()
            .map(format_message)
            .collect::<Vec<_>>()
            .join("\n")
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
        format!(
            "profile: {}\nsocket: {}\nmattermost_username: {}\nmattermost_ok: {}",
            self.profile_name,
            self.socket_path.display(),
            self.mattermost_username,
            self.mattermost_ok
        )
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
            },
            Channel {
                id: "chan-id".to_string(),
                name: "per-007".to_string(),
                display_name: "PER-007".to_string(),
                channel_type: "O".to_string(),
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
        assert!(rendered.contains("Use 'lanyte-chat dm read <username>' to read a conversation."));
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

    #[test]
    fn env_profile_resolution_prefers_scope_match_over_name_match() {
        let profiles = vec![
            Profile {
                name: "bravo-devlead".to_string(),
                role: "bravo-devlead".to_string(),
                scope: "other-scope".to_string(),
                provider: Provider::Mattermost,
                bot_username: "other-bot".to_string(),
                team_name: "org-other-scope".to_string(),
                server_url: "https://mm.example.com".to_string(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: Vec::new(),
                ipc: None,
            },
            Profile {
                name: "bravo-devlead-lanytehq".to_string(),
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
            },
        ];

        let resolved =
            derive_env_profile_name(None, Some("bravo-devlead"), Some("lanytehq"), &profiles);
        assert_eq!(resolved.as_deref(), Some("bravo-devlead-lanytehq"));
    }

    #[test]
    fn team_name_uses_mattermost_env_when_present() {
        unsafe { env::set_var(OsStr::new("LANYTE_MM_TEAM"), OsStr::new("custom-team")) };
        assert_eq!(derive_team_name("lanytehq"), "custom-team");
        unsafe { env::remove_var(OsStr::new("LANYTE_MM_TEAM")) };
    }
}
