use std::path::PathBuf;
use std::process;
use std::process::Stdio;

use chanvoy_core::{
    list_profiles, load_active_profile, store_active_profile, store_profile, CapabilityClass,
    Channel, CredentialMode, DaemonStatus, Identity, Message, Notification, PostReceipt, Profile,
    ProfileStatus, Provider, WaitResult,
};
use chanvoy_daemon::{daemon_client, ping, start, status, stop, DaemonError};
use chrono::{Local, TimeZone};
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
}

#[derive(Debug, Subcommand)]
enum DmCommand {
    Send(DmSendArgs),
    Read(DmReadArgs),
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
    #[arg(long, default_value_t = 60)]
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
        CommandSet::Dm(DmCommand::Send(args)) => print_receipt(
            cli.json,
            "dm sent",
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
        CommandSet::Notify(args) => print_receipt(
            cli.json,
            "notification sent",
            &daemon_client(&profile)
                .notify(&args.bot_username, &args.message)
                .await?,
        ),
        CommandSet::Notifications(args) => print_value(
            cli.json,
            &daemon_client(&profile).notifications(args.since).await?,
        ),
        CommandSet::Wait(args) => {
            match daemon_client(&profile)
                .wait_channel(&args.channel, args.timeout)
                .await
            {
                Ok(result) => print_value(cli.json, &result),
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
                        eprintln!("{message}");
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
        CommandSet::Channel(ChannelCommand::Archive(args)) => print_value(
            cli.json,
            &daemon_client(&profile).archive_channel(&args.name).await?,
        ),
        CommandSet::Channel(ChannelCommand::Restore(args)) => print_value(
            cli.json,
            &daemon_client(&profile).restore_channel(&args.name).await?,
        ),
        CommandSet::Channel(ChannelCommand::AddMember(args)) => print_value(
            cli.json,
            &daemon_client(&profile)
                .add_member(&args.channel, &args.username)
                .await?,
        ),
    }
}

async fn handle_daemon(profile: &str, json: bool, command: DaemonCommand) -> Result<(), CliError> {
    match command {
        DaemonCommand::Start => {
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
            let profile = Profile {
                name: args.name,
                role: args.role,
                scope: args.scope,
                provider: Provider::Mattermost,
                bot_username: args.bot_username,
                team_name: args.team_name,
                server_url: args.server_url,
                env_name: args.env_name,
                env_file: args.env_file,
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
            let path = store_profile(&profile)?;
            let should_activate = args.activate || load_active_profile()?.is_none();
            if should_activate {
                store_active_profile(&profile.name)?;
            }
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({ "path": path, "active": should_activate })
                    )?
                );
            } else {
                println!("created profile {}", profile.name);
                println!("path: {}", path.display());
                if should_activate {
                    println!("active profile set to {}", profile.name);
                }
            }
            Ok(())
        }
    }
}

fn resolve_profile_name(profile: Option<&str>) -> Result<String, CliError> {
    if let Some(profile) = profile {
        return Ok(profile.to_string());
    }
    Ok(load_active_profile()?.unwrap_or_else(|| "default".to_string()))
}

fn validate_profile_create_args(profile: &Profile) -> Result<(), CliError> {
    if matches!(profile.credential_mode, CredentialMode::EnvFile) && profile.env_file.is_none() {
        return Err(chanvoy_core::CoreError::MissingEnvFile.into());
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
        println!("username: {}", identity.username);
        if let Some(nickname) = &identity.nickname {
            if !nickname.is_empty() {
                println!("nickname: {nickname}");
            }
        }
        if let Some(email) = &identity.email {
            if !email.is_empty() {
                println!("email: {email}");
            }
        }
        println!("id: {}", identity.id);
    }
    Ok(())
}

fn print_receipt(json: bool, label: &str, receipt: &PostReceipt) -> Result<(), CliError> {
    if json {
        println!("{}", serde_json::to_string_pretty(receipt)?);
    } else {
        println!("{label}: {}", receipt.id);
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
        self.iter()
            .map(|channel| format!("{} [{}]", channel.name, channel.display_name))
            .collect::<Vec<_>>()
            .join("\n")
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
        self.iter()
            .map(|notification| {
                format!(
                    "#{}\n{}",
                    notification.from_channel,
                    format_message(&notification.message)
                )
            })
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
        format!(
            "created channel {} [{}] ({})",
            self.name, self.display_name, self.id
        )
    }
}

impl HumanReadable for bool {
    fn to_human_string(&self) -> String {
        if *self {
            "ok".to_string()
        } else {
            "false".to_string()
        }
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
    match Local.timestamp_millis_opt(millis).single() {
        Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => millis.to_string(),
    }
}
