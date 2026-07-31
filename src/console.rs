use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};
use console::Term;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use fs2::FileExt;
use tokio::sync::{mpsc, watch};

use crate::account::{AccountRuntime, authorize_account, validate_source};
use crate::config::{
    AccountConfig, Config, DEFAULT_MAX_ACTIVE_FILES, DEFAULT_MAX_LANES, MediaFilter, SourceAccess,
    SourceConfig, normalize_channel, normalize_extension, proxy_url_from_env,
    validate_account_name,
};
use crate::download::DownloadPhase;
use crate::history::{HistoryEvent, HistoryProgress, HistoryRange, HistorySummary, import_history};
use crate::monitor::run_monitor;
use crate::paths::AppPaths;
use crate::proxy::{ProxyKind, parse_proxy_input, proxy_label};
use crate::scheduler::{SchedulerEvent, run_scheduler};
use crate::security;
use crate::session_import::{import_tdata_session, import_telethon_session};
use crate::store::{Job, Store};
use crate::ui::{
    CursorGuard, accent_text, enter_terminal_session, error_text, fixed_width_text, info_text,
    muted_text, normal_text, panel_bottom, panel_row, panel_top, pause, print_wrapped,
    property_rows, render_page, select_menu, select_menu_with_body, success_text, terminal_width,
    view_data_page, warning_text, wrap_hanging_text_lines, wrap_text_lines,
};
use crate::util::{human_bytes, normalize_user_path, prompt, prompt_password};

#[derive(Debug)]
enum AccountCommand {
    Add {
        name: String,
        tdata: Option<PathBuf>,
        tdata_index: Option<usize>,
        tdata_passcode: bool,
        telethon_session: Option<PathBuf>,
        proxy: Option<String>,
    },
    Enable {
        name: String,
    },
    Disable {
        name: String,
    },
    SetPrimary {
        name: String,
    },
    SetProxy {
        name: String,
        proxy: Option<String>,
    },
    Remove {
        name: String,
        yes: bool,
    },
}

pub async fn run_console() -> Result<()> {
    let _terminal_session = enter_terminal_session()?;
    let config_override = std::env::var_os("TGDL_CONFIG").map(PathBuf::from);
    let paths = AppPaths::discover(config_override.as_deref())?;
    console_loop(&paths).await
}

async fn console_loop(paths: &AppPaths) -> Result<()> {
    paths.ensure()?;
    loop {
        if !paths.config.exists() {
            let body = vec![
                warning_text("● SETUP REQUIRED"),
                muted_text("No configuration was found. Complete setup before starting."),
                muted_text(format!("Configuration: {}", paths.config.display())),
            ];
            match select_menu_with_body(
                "First-time setup",
                &body,
                &["Set up tgdl", "Show application paths"],
                "Exit",
            )? {
                Some(0) => {
                    render_page("First-time setup", &[])?;
                    report_action(setup(paths).await);
                    pause()?;
                }
                Some(1) => {
                    render_page("Application paths", &[])?;
                    show_application_paths(paths);
                    pause()?;
                }
                None => return Ok(()),
                Some(_) => unreachable!("menu returns only an offered selection"),
            }
            continue;
        }

        let body = dashboard_lines(paths);
        match select_menu_with_body(
            "Main menu",
            &body,
            &[
                "Start monitoring and downloads",
                "Download channel or group history",
                "Show status",
                "Manage accounts",
                "Retry failed downloads",
                "Settings",
                "Show application paths",
            ],
            "Exit",
        )? {
            Some(0) => run_download_session(paths).await?,
            Some(1) => match download_history(paths).await {
                Ok(start_downloads) => {
                    if start_downloads {
                        run_download_session(paths).await?;
                    }
                }
                Err(error) => {
                    render_page("History download", &[])?;
                    report_action(Err(error));
                    pause()?;
                }
            },
            Some(2) => {
                if let Err(error) = status(paths) {
                    render_page("Status", &[])?;
                    report_action(Err(error));
                    pause()?;
                }
            }
            Some(3) => account_menu(paths).await?,
            Some(4) => {
                render_page("Retry failed downloads", &[])?;
                report_action(retry(paths));
                pause()?;
            }
            Some(5) => settings_menu(paths).await?,
            Some(6) => {
                render_page("Application paths", &[])?;
                show_application_paths(paths);
                pause()?;
            }
            None => return Ok(()),
            Some(_) => unreachable!("menu returns only an offered selection"),
        }
    }
}

async fn run_download_session(paths: &AppPaths) -> Result<()> {
    render_page("Downloads", &[])?;
    let result = run(paths).await;
    let stopped_message = if result.is_ok() {
        success_text("✓ All active jobs were checkpointed safely.")
    } else {
        error_text("✕ The worker stopped because of an error.")
    };
    render_page("Downloads stopped", &[stopped_message])?;
    report_action(result);
    pause()
}

fn report_action(result: Result<()>) {
    if let Err(error) = result {
        let message = format!(
            "{} {}",
            error_text("✕ ERROR"),
            error_text(format!("{error:#}"))
        );
        eprintln!();
        for line in wrap_text_lines(&message, terminal_width(&Term::stderr())) {
            eprintln!("{line}");
        }
    }
}

fn acknowledge_page_error(title: &str, error: anyhow::Error) -> Result<()> {
    render_page(title, &[])?;
    report_action(Err(error));
    pause()
}

fn show_application_paths(paths: &AppPaths) {
    let width = terminal_width(&Term::stdout());
    println!("{}", panel_top(&accent_text("APPLICATION PATHS"), width));
    for (label, value) in [
        ("Data directory", paths.root.display()),
        ("Configuration", paths.config.display()),
        ("Sessions", paths.sessions.display()),
        ("State database", paths.state_db.display()),
    ] {
        for row in property_rows(label, value, width) {
            println!("{row}");
        }
    }
    println!("{}", panel_bottom(width));
}

fn dashboard_lines(paths: &AppPaths) -> Vec<String> {
    let result = (|| -> Result<Vec<String>> {
        let config = Config::load(&paths.config)?;
        let store = Store::open(&paths.state_db)?;
        let jobs = store.jobs()?;
        let active = jobs
            .iter()
            .filter(|job| matches!(job.state.as_str(), "queued" | "downloading" | "retrying"))
            .count();
        let failed = jobs
            .iter()
            .filter(|job| job.state.as_str() == "failed")
            .count();
        let completed = jobs
            .iter()
            .filter(|job| job.state.as_str() == "completed")
            .count();
        let enabled = config
            .accounts
            .iter()
            .filter(|account| account.enabled)
            .count();
        let premium = config
            .accounts
            .iter()
            .filter(|account| account.enabled && account.premium)
            .count();
        Ok(vec![
            format!(
                "{}  {}",
                success_text("● READY"),
                normal_text(format!(
                    "{} monitored source{}",
                    config.sources.len(),
                    if config.sources.len() == 1 { "" } else { "s" }
                ))
            ),
            format!(
                "{}  {}    {}  {}",
                accent_text("ACCOUNTS"),
                normal_text(format!("{enabled} enabled / {premium} premium")),
                accent_text("MONITOR"),
                normal_text(&config.monitor_primary)
            ),
            format!(
                "{}  {}    {}  {}    {}  {}",
                accent_text("ACTIVE"),
                count_text(active, CountTone::Normal),
                accent_text("DONE"),
                count_text(completed, CountTone::Success),
                accent_text("FAILED"),
                count_text(failed, CountTone::Error)
            ),
            muted_text(format!("Downloads: {}", config.output_dir.display())),
        ])
    })();
    result.unwrap_or_else(|error| {
        vec![
            error_text("● CONFIGURATION ERROR"),
            error_text(format!("{error:#}")),
        ]
    })
}

async fn setup(paths: &AppPaths) -> Result<()> {
    paths.ensure()?;
    if paths.config.exists() {
        bail!(
            "configuration already exists at {}; edit it or remove it before setup",
            paths.config.display()
        );
    }
    print_wrapped(&info_text(
        "Create a Telegram API application at https://my.telegram.org before continuing.",
    ));
    let api_id: i32 = prompt("API ID: ")?
        .parse()
        .context("API ID must be an integer")?;
    let api_hash = prompt_password("API hash: ")?;
    let output_dir = normalize_user_path(&prompt("Download folder: ")?)?;
    let name = prompt("First account alias (for example premium-main): ")?;
    validate_account_name(&name)?;
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let placeholder = AccountConfig {
        name: name.clone(),
        enabled: true,
        monitor_priority: 1,
        user_id: None,
        display_name: None,
        premium: false,
        premium_checked_at: None,
        proxy_url: None,
    };
    let mut config = Config {
        api_id,
        api_hash,
        channel: String::new(),
        sources: Vec::new(),
        media_filter: MediaFilter::default(),
        output_dir,
        proxy_url: None,
        monitor_primary: name.clone(),
        monitor_account_downloads: false,
        max_active_files: DEFAULT_MAX_ACTIVE_FILES,
        max_download_lanes_per_account: DEFAULT_MAX_LANES,
        verify_file_hashes: false,
        accounts: vec![placeholder],
    };
    let mut runtime_config = config.clone();
    if std::env::var_os("TGDL_PROXY_URL").is_some() {
        runtime_config.proxy_url = proxy_url_from_env()?;
    }
    let mut account =
        authorize_account(&name, &runtime_config, &paths.session_path(&name), None).await?;
    account.monitor_priority = 1;
    config.accounts = vec![account.clone()];
    let runtime =
        AccountRuntime::connect(&account, &runtime_config, &paths.session_path(&name)).await?;
    let source = prompt_initial_source(&runtime)
        .await?
        .context("setup requires at least one monitored channel or group")?;
    if source.access == SourceAccess::Public {
        config.channel = source.username.clone().unwrap_or_default();
    }
    config.sources.push(source);
    drop(runtime);
    config.save(&paths.config)?;
    let _ = Store::open(&paths.state_db)?;
    print_wrapped(&success_text(
        "✓ Setup complete. The downloader is ready to start.",
    ));
    Ok(())
}

async fn account_command(paths: &AppPaths, command: AccountCommand) -> Result<()> {
    paths.ensure()?;
    let mut config = Config::load_stored(&paths.config)?;
    let effective_config = config.effective()?;
    let store = Store::open(&paths.state_db)?;
    ensure_worker_stopped(&paths.lock_file)?;
    match command {
        AccountCommand::Add {
            name,
            tdata,
            tdata_index,
            tdata_passcode,
            telethon_session,
            proxy,
        } => {
            validate_account_name(&name)?;
            if let Some(proxy_url) = proxy.as_deref() {
                crate::proxy::validate_proxy_url(proxy_url)?;
            }
            if config
                .accounts
                .iter()
                .any(|account| account.name.eq_ignore_ascii_case(&name))
            {
                bail!("account '{name}' already exists");
            }
            let session_path = paths.session_path(&name);
            let account = match (tdata, telethon_session) {
                (Some(source), None) => {
                    print_wrapped(&warning_text(
                        "Close Telegram Desktop before importing. Do not reuse the copied session concurrently from another network.",
                    ));
                    let passcode = if tdata_passcode {
                        Some(prompt_password("Telegram Desktop local passcode: ")?)
                    } else {
                        None
                    };
                    import_tdata_session(
                        &name,
                        &effective_config,
                        &source,
                        tdata_index,
                        passcode.as_deref(),
                        &session_path,
                        proxy.as_deref(),
                    )
                    .await?
                }
                (None, Some(source)) => {
                    print_wrapped(&warning_text(
                        "Stop every Telethon process using this file. Do not reuse the source session concurrently from another network.",
                    ));
                    import_telethon_session(
                        &name,
                        &effective_config,
                        &source,
                        &session_path,
                        proxy.as_deref(),
                    )
                    .await?
                }
                (None, None) => {
                    authorize_account(&name, &effective_config, &session_path, proxy.as_deref())
                        .await?
                }
                (Some(_), Some(_)) => {
                    unreachable!("the console offers exactly one import source")
                }
            };
            if let Some(existing) = config
                .accounts
                .iter()
                .find(|existing| existing.user_id.is_some() && existing.user_id == account.user_id)
            {
                remove_session_files(&session_path)?;
                bail!(
                    "Telegram user {} is already configured as '{}'; each download account must be distinct",
                    account.user_id.unwrap_or_default(),
                    existing.name
                );
            }
            let display_name = account.display_name.clone().unwrap_or_else(|| name.clone());
            let user_id = account.user_id.unwrap_or_default();
            let tier = if account.premium { "premium" } else { "normal" };
            config.accounts.push(account);
            config.save(&paths.config)?;
            print_wrapped(&success_text(format!(
                "✓ Added {display_name} ({user_id}, {tier})."
            )));
            Ok(())
        }
        AccountCommand::Enable { name } => {
            config
                .account_mut(&name)
                .with_context(|| format!("unknown account '{name}'"))?
                .enabled = true;
            config.save(&paths.config)?;
            print_wrapped(&success_text(format!("✓ Enabled account '{name}'.")));
            Ok(())
        }
        AccountCommand::Disable { name } => {
            let enabled = config
                .accounts
                .iter()
                .filter(|account| account.enabled)
                .count();
            let account = config
                .account_mut(&name)
                .with_context(|| format!("unknown account '{name}'"))?;
            if account.enabled && enabled == 1 {
                bail!("cannot disable the last enabled monitoring account");
            }
            account.enabled = false;
            if config.monitor_primary == name {
                config.monitor_primary = config
                    .accounts
                    .iter()
                    .filter(|account| account.enabled)
                    .min_by_key(|account| account.monitor_priority)
                    .context("no enabled replacement monitor")?
                    .name
                    .clone();
            }
            config.save(&paths.config)?;
            let released =
                store.release_jobs_for_account(&name, "assigned download account was disabled")?;
            let suffix = if released == 0 {
                String::new()
            } else {
                format!(" Released {released} queued job(s).")
            };
            print_wrapped(&success_text(format!(
                "✓ Disabled account '{name}'.{suffix}"
            )));
            Ok(())
        }
        AccountCommand::SetPrimary { name } => {
            let account = config
                .account(&name)
                .with_context(|| format!("unknown account '{name}'"))?;
            if !account.enabled {
                bail!("enable account '{name}' before making it primary");
            }
            let account = account.clone();
            let runtime =
                AccountRuntime::connect(&account, &effective_config, &paths.session_path(&name))
                    .await?;
            for source in config
                .sources
                .iter()
                .filter(|source| source.access == SourceAccess::Private)
            {
                runtime.resolve_source(source).await.with_context(|| {
                    format!(
                        "'{name}' cannot monitor private source '{}'; join the account first",
                        source.display_name()
                    )
                })?;
            }
            drop(runtime);
            config.monitor_primary = name.clone();
            config.save(&paths.config)?;
            if !config.monitor_account_downloads {
                store.release_jobs_for_account(
                    &name,
                    "configured monitoring account is monitor-only",
                )?;
            }
            print_wrapped(&success_text(format!(
                "✓ '{name}' is now the primary monitor."
            )));
            Ok(())
        }
        AccountCommand::SetProxy { name, proxy } => {
            if let Some(proxy_url) = proxy.as_deref() {
                crate::proxy::validate_proxy_url(proxy_url)?;
            }
            config
                .account_mut(&name)
                .with_context(|| format!("unknown account '{name}'"))?
                .proxy_url = proxy;
            config.save(&paths.config)?;
            print_wrapped(&success_text(format!(
                "✓ Updated the network route for '{name}'."
            )));
            Ok(())
        }
        AccountCommand::Remove { name, yes } => {
            let account = config
                .account(&name)
                .with_context(|| format!("unknown account '{name}'"))?;
            if config.accounts.len() == 1 {
                bail!("cannot remove the only configured account");
            }
            if !yes {
                let confirmation = prompt(&format!(
                    "Type the account alias '{name}' to delete its local session: "
                ))?;
                if confirmation != name {
                    bail!("confirmation did not match; nothing was removed");
                }
            }
            let was_primary = config.monitor_primary == name;
            let was_enabled = account.enabled;
            config.accounts.retain(|account| account.name != name);
            if was_primary {
                config.monitor_primary = config
                    .accounts
                    .iter()
                    .filter(|account| account.enabled)
                    .min_by_key(|account| account.monitor_priority)
                    .or_else(|| config.accounts.first())
                    .context("no replacement account")?
                    .name
                    .clone();
            }
            if was_enabled && !config.accounts.iter().any(|account| account.enabled) {
                config.accounts[0].enabled = true;
            }
            config.save(&paths.config)?;
            store.remove_account_state(&name)?;
            remove_session_files(&paths.session_path(&name))?;
            print_wrapped(&success_text(format!(
                "✓ Removed '{name}' and its local session."
            )));
            Ok(())
        }
    }
}

async fn account_menu(paths: &AppPaths) -> Result<()> {
    loop {
        let action = async {
            let body = account_summary_lines(paths);
            match select_menu_with_body(
                "Account manager",
                &body,
                &[
                    "List accounts",
                    "Add account",
                    "Enable account",
                    "Disable account",
                    "Set primary monitor",
                    "Change account proxy",
                    "Remove account",
                ],
                "Back",
            )? {
                Some(0) => {
                    if let Err(error) = account_list_page(paths) {
                        render_page("Accounts", &[])?;
                        report_action(Err(error));
                        pause()?;
                    }
                }
                Some(1) => {
                    render_page("Add account", &[])?;
                    report_action(add_account_interactive(paths).await);
                    pause()?;
                }
                Some(2) => {
                    if let Some(name) =
                        choose_account(paths, "Enable account", |account, _| !account.enabled)?
                    {
                        render_page(
                            "Enable account",
                            &[info_text(format!("Selected account: {name}"))],
                        )?;
                        report_action(
                            account_command(paths, AccountCommand::Enable { name }).await,
                        );
                        pause()?;
                    }
                }
                Some(3) => {
                    if let Some(name) =
                        choose_account(paths, "Disable account", |account, _| account.enabled)?
                    {
                        render_page(
                            "Disable account",
                            &[warning_text(format!("Selected account: {name}"))],
                        )?;
                        report_action(
                            account_command(paths, AccountCommand::Disable { name }).await,
                        );
                        pause()?;
                    }
                }
                Some(4) => {
                    if let Some(name) =
                        choose_account(paths, "Set primary monitor", |account, config| {
                            account.enabled && account.name != config.monitor_primary
                        })?
                    {
                        render_page(
                            "Primary monitor",
                            &[info_text(format!("Selected account: {name}"))],
                        )?;
                        report_action(
                            account_command(paths, AccountCommand::SetPrimary { name }).await,
                        );
                        pause()?;
                    }
                }
                Some(5) => {
                    if let Some(name) = choose_account(paths, "Change account proxy", |_, _| true)?
                    {
                        let config = Config::load(&paths.config)?;
                        render_page(
                            "Account proxy",
                            &[info_text(format!("Selected account: {name}"))],
                        )?;
                        match prompt_account_proxy(&config) {
                            Ok(Some(proxy)) => {
                                report_action(
                                    account_command(
                                        paths,
                                        AccountCommand::SetProxy { name, proxy },
                                    )
                                    .await,
                                );
                                pause()?;
                            }
                            Ok(None) => {}
                            Err(error) => acknowledge_page_error("Account proxy", error)?,
                        }
                    }
                }
                Some(6) => {
                    if let Some(name) = choose_account(paths, "Remove account", |_, _| true)? {
                        render_page(
                            "Remove account",
                            &[warning_text(format!(
                                "This will delete the local session for: {name}"
                            ))],
                        )?;
                        report_action(
                            account_command(paths, AccountCommand::Remove { name, yes: false })
                                .await,
                        );
                        pause()?;
                    }
                }
                None => return Ok(true),
                Some(_) => unreachable!("menu returns only an offered selection"),
            }
            Result::<bool>::Ok(false)
        }
        .await;
        match action {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => acknowledge_page_error("Account manager", error)?,
        }
    }
}

fn account_summary_lines(paths: &AppPaths) -> Vec<String> {
    Config::load(&paths.config)
        .map(|config| {
            config
                .accounts
                .iter()
                .map(|account| {
                    let tier = if account.premium {
                        success_text("PREMIUM")
                    } else {
                        normal_text("NORMAL")
                    };
                    let route = if config.proxy_for_account(account).is_some() {
                        warning_text(proxy_label(config.proxy_for_account(account)))
                    } else {
                        muted_text("DIRECT")
                    };
                    let role = if account.name == config.monitor_primary {
                        accent_text(if config.monitor_account_downloads {
                            "PRIMARY + DOWNLOAD"
                        } else {
                            "PRIMARY MONITOR"
                        })
                    } else if account.enabled {
                        success_text("ENABLED")
                    } else {
                        muted_text("DISABLED")
                    };
                    format!(
                        "{}  {}  {}  {}",
                        normal_text(&account.name),
                        tier,
                        route,
                        role
                    )
                })
                .collect()
        })
        .unwrap_or_else(|error| vec![error_text(format!("{error:#}"))])
}

async fn add_account_interactive(paths: &AppPaths) -> Result<()> {
    let config = Config::load(&paths.config)?;
    let name = prompt_required("Account alias: ")?;
    validate_account_name(&name)?;
    let Some(method) = select_menu(
        "Authorization source",
        &[
            "Phone number and login code",
            "Telegram Desktop tdata folder",
            "Telethon SQLite session file",
        ],
        "Cancel",
    )?
    else {
        return Ok(());
    };
    let Some(proxy) = prompt_account_proxy(&config)? else {
        return Ok(());
    };

    let command = match method {
        0 => AccountCommand::Add {
            name,
            tdata: None,
            tdata_index: None,
            tdata_passcode: false,
            telethon_session: None,
            proxy,
        },
        1 => {
            let source = normalize_user_path(&prompt_required("Telegram Desktop tdata folder: ")?)?;
            let tdata_index = parse_optional_usize(&prompt(
                "Account index (leave empty for the first/only account): ",
            )?)?;
            let tdata_passcode =
                prompt_yes_no("Is this tdata protected by a local passcode?", false)?;
            AccountCommand::Add {
                name,
                tdata: Some(source),
                tdata_index,
                tdata_passcode,
                telethon_session: None,
                proxy,
            }
        }
        2 => AccountCommand::Add {
            name,
            tdata: None,
            tdata_index: None,
            tdata_passcode: false,
            telethon_session: Some(normalize_user_path(&prompt_required(
                "Telethon .session file: ",
            )?)?),
            proxy,
        },
        _ => unreachable!("menu returns only an offered selection"),
    };
    account_command(paths, command).await
}

fn choose_account(
    paths: &AppPaths,
    title: &str,
    predicate: impl Fn(&AccountConfig, &Config) -> bool,
) -> Result<Option<String>> {
    let config = Config::load(&paths.config)?;
    let accounts = config
        .accounts
        .iter()
        .filter(|account| predicate(account, &config))
        .collect::<Vec<_>>();
    if accounts.is_empty() {
        render_page(
            title,
            &[warning_text("No matching accounts are available.")],
        )?;
        pause()?;
        return Ok(None);
    }
    let labels = accounts
        .iter()
        .map(|account| {
            format!(
                "{} — {}{}{}",
                account.name,
                if account.premium { "premium" } else { "normal" },
                if account.enabled { "" } else { ", disabled" },
                if account.name == config.monitor_primary {
                    ", primary monitor"
                } else {
                    ""
                }
            )
        })
        .collect::<Vec<_>>();
    Ok(select_menu(title, &labels, "Cancel")?.map(|index| accounts[index].name.clone()))
}

fn prompt_account_proxy(config: &Config) -> Result<Option<Option<String>>> {
    let inherited = if config.proxy_url.is_some() {
        "Use the configured global proxy"
    } else {
        "Use a direct connection (no account override)"
    };
    let Some(selection) = select_menu(
        "Account network route",
        &[
            inherited,
            "Use an account-specific SOCKS5 proxy",
            "Use an account-specific HTTP proxy",
        ],
        "Cancel",
    )?
    else {
        return Ok(None);
    };
    if selection == 0 {
        Ok(Some(None))
    } else {
        let kind = if selection == 1 {
            ProxyKind::Socks5
        } else {
            ProxyKind::Http
        };
        Ok(Some(Some(prompt_proxy(kind)?)))
    }
}

fn prompt_proxy(kind: ProxyKind) -> Result<String> {
    let input = prompt_required(&format!(
        "{} proxy (IP:port or IP:port:username:password): ",
        kind.label()
    ))?;
    parse_proxy_input(kind, &input)
}

fn parse_optional_usize(value: &str) -> Result<Option<usize>> {
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            value
                .parse()
                .context("account index must be a non-negative integer")?,
        ))
    }
}

fn prompt_required(label: &str) -> Result<String> {
    let value = prompt(label)?;
    if value.trim().is_empty() {
        bail!("a value is required");
    }
    Ok(value)
}

fn prompt_yes_no(label: &str, default: bool) -> Result<bool> {
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    loop {
        let value = prompt(&format!("{label} {suffix}: "))?.to_ascii_lowercase();
        match value.as_str() {
            "" => return Ok(default),
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => print_wrapped(&warning_text("Enter y or n.")),
        }
    }
}

async fn prompt_initial_source(runtime: &AccountRuntime) -> Result<Option<SourceConfig>> {
    let Some(selection) = select_menu(
        "First monitored source",
        &[
            "Public channel or group",
            "Private channel or group (account must already be joined)",
        ],
        "Cancel setup",
    )?
    else {
        return Ok(None);
    };
    if selection == 0 {
        print_wrapped(&muted_text(
            "Public sources do not require the monitor account to join.",
        ));
        let username = normalize_channel(&prompt_required("Public @username: ")?);
        Ok(Some(
            validate_source(runtime, &SourceConfig::public(username)).await?,
        ))
    } else {
        print_wrapped(&warning_text(
            "⚠ The monitor account must already be joined to the private channel or group.",
        ));
        choose_private_source(runtime, &[]).await
    }
}

async fn choose_private_source(
    runtime: &AccountRuntime,
    existing_sources: &[SourceConfig],
) -> Result<Option<SourceConfig>> {
    let configured = existing_sources
        .iter()
        .filter_map(|source| source.peer_id)
        .collect::<std::collections::HashSet<_>>();
    let sources = runtime
        .available_private_sources()
        .await?
        .into_iter()
        .filter(|source| {
            source
                .peer_id
                .is_none_or(|peer_id| !configured.contains(&peer_id))
        })
        .collect::<Vec<_>>();
    if sources.is_empty() {
        bail!(
            "no unconfigured joined channels or groups were found for monitor account '{}'",
            runtime.name
        );
    }
    let labels = sources
        .iter()
        .map(|source| {
            format!(
                "{} — {}",
                source.title,
                source.kind_label().to_ascii_lowercase()
            )
        })
        .collect::<Vec<_>>();
    Ok(
        select_menu("Choose joined channel or group", &labels, "Cancel")?
            .map(|index| sources[index].clone()),
    )
}

async fn connect_primary(paths: &AppPaths, config: &Config) -> Result<Arc<AccountRuntime>> {
    let account = config
        .account(&config.monitor_primary)
        .context("primary monitor account is missing")?;
    AccountRuntime::connect(account, config, &paths.session_path(&account.name)).await
}

async fn settings_menu(paths: &AppPaths) -> Result<()> {
    loop {
        let action = async {
            let config = Config::load_stored(&paths.config)?;
            let body = settings_lines(&config);
            let selection = select_menu_with_body(
                "Settings",
                &body,
                &[
                    "Manage monitored channels and groups",
                    "Change monitor account download role",
                    "Change media filters",
                    "Change download folder",
                    "Change global proxy",
                    "Change simultaneous file limit",
                    "Change transfer lanes per account",
                    "Toggle full-file hash verification",
                    "Change Telegram API credentials",
                ],
                "Back",
            )?;
            let Some(selection) = selection else {
                return Ok(true);
            };
            let result = match selection {
                0 => {
                    source_menu(paths).await?;
                    return Ok(false);
                }
                1 => change_monitor_download_role(paths),
                2 => {
                    change_media_filter(paths)?;
                    return Ok(false);
                }
                3 => change_output_folder(paths),
                4 => change_global_proxy(paths),
                5 => change_active_file_limit(paths),
                6 => change_lane_limit(paths),
                7 => toggle_hash_verification(paths),
                8 => change_api_credentials(paths),
                _ => unreachable!("menu returns only an offered selection"),
            };
            report_action(result);
            pause()?;
            Result::<bool>::Ok(false)
        }
        .await;
        match action {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => acknowledge_page_error("Settings", error)?,
        }
    }
}

fn settings_lines(config: &Config) -> Vec<String> {
    let public = config
        .sources
        .iter()
        .filter(|source| source.access == SourceAccess::Public)
        .count();
    let private = config.sources.len() - public;
    vec![
        format!(
            "{}  {} ({} public / {} private)    {}  {}",
            accent_text("SOURCES"),
            normal_text(config.sources.len()),
            public,
            private,
            accent_text("MONITOR"),
            normal_text(&config.monitor_primary)
        ),
        format!(
            "{}  {}",
            accent_text("MONITOR DOWNLOADS"),
            if config.monitor_account_downloads {
                success_text("ALLOWED")
            } else {
                muted_text("BLOCKED")
            }
        ),
        format!(
            "{}  {}",
            accent_text("DOWNLOADS"),
            normal_text(config.output_dir.display())
        ),
        format!(
            "{}  {}    {}  {}",
            accent_text("EXT"),
            normal_text(extension_filter_label(&config.media_filter)),
            accent_text("SIZE"),
            normal_text(size_filter_label(&config.media_filter))
        ),
        format!(
            "{}  {}    {}  {}    {}  {}",
            accent_text("ROUTE"),
            match config.proxy_url.as_deref() {
                Some(proxy) => warning_text(proxy_label(Some(proxy))),
                None => success_text("DIRECT"),
            },
            accent_text("FILES"),
            normal_text(config.max_active_files),
            accent_text("LANES"),
            normal_text(config.max_download_lanes_per_account)
        ),
        format!(
            "{}  {}    {}  {}",
            accent_text("FULL HASH"),
            if config.verify_file_hashes {
                warning_text("ENABLED")
            } else {
                muted_text("DISABLED")
            },
            accent_text("API ID"),
            normal_text(config.api_id)
        ),
        muted_text("API hash and proxy credentials are hidden"),
    ]
}

async fn source_menu(paths: &AppPaths) -> Result<()> {
    loop {
        let action = async {
            let config = Config::load_stored(&paths.config)?;
            let body = source_lines(&config);
            let Some(selection) = select_menu_with_body(
                "Monitored sources",
                &body,
                &[
                    "Add public channel or group",
                    "Add private channel or group",
                    "Remove channel or group",
                ],
                "Back",
            )?
            else {
                return Ok(true);
            };
            let result = match selection {
                0 => add_public_source(paths).await,
                1 => add_private_source(paths).await,
                2 => remove_source(paths),
                _ => unreachable!("menu returns only an offered selection"),
            };
            report_action(result);
            pause()?;
            Result::<bool>::Ok(false)
        }
        .await;
        match action {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => acknowledge_page_error("Monitored sources", error)?,
        }
    }
}

fn source_lines(config: &Config) -> Vec<String> {
    let mut lines = vec![muted_text(format!(
        "{} channel(s) and group(s) are monitored by one active monitor account.",
        config.sources.len()
    ))];
    for source in &config.sources {
        lines.push(format!(
            "{}  {}  {}  {}",
            accent_text("●"),
            normal_text(source.display_name()),
            if source.access == SourceAccess::Private {
                warning_text(source.access_label())
            } else {
                success_text(source.access_label())
            },
            muted_text(source.kind_label())
        ));
    }
    lines
}

async fn add_public_source(paths: &AppPaths) -> Result<()> {
    render_page(
        "Add public source",
        &[muted_text(
            "Public channels and groups can be monitored without joining.",
        )],
    )?;
    ensure_worker_stopped(&paths.lock_file)?;
    let mut config = Config::load_stored(&paths.config)?;
    let effective_config = config.effective()?;
    let runtime = connect_primary(paths, &effective_config).await?;
    let username = normalize_channel(&prompt_required("Public @username: ")?);
    let source = validate_source(&runtime, &SourceConfig::public(username)).await?;
    drop(runtime);
    if config.channel.is_empty() {
        config.channel = source.username.clone().unwrap_or_default();
    }
    config.sources.push(source);
    config.save(&paths.config)?;
    print_wrapped(&success_text("✓ Public source added."));
    Ok(())
}

async fn add_private_source(paths: &AppPaths) -> Result<()> {
    render_page(
        "Add private source",
        &[
            warning_text(
                "⚠ The monitor account must already be joined to the private channel or group.",
            ),
            muted_text("tgdl will only list existing dialogs; it will never join automatically."),
        ],
    )?;
    ensure_worker_stopped(&paths.lock_file)?;
    let mut config = Config::load_stored(&paths.config)?;
    let effective_config = config.effective()?;
    let runtime = connect_primary(paths, &effective_config).await?;
    let source = choose_private_source(&runtime, &config.sources).await?;
    drop(runtime);
    let Some(source) = source else {
        return Ok(());
    };
    config.sources.push(source);
    config.save(&paths.config)?;
    print_wrapped(&success_text("✓ Private source added."));
    Ok(())
}

fn remove_source(paths: &AppPaths) -> Result<()> {
    ensure_worker_stopped(&paths.lock_file)?;
    let mut config = Config::load_stored(&paths.config)?;
    if config.sources.len() == 1 {
        bail!("at least one monitored channel or group must remain");
    }
    let labels = config
        .sources
        .iter()
        .map(|source| {
            format!(
                "{} — {} {}",
                source.display_name(),
                source.access_label().to_ascii_lowercase(),
                source.kind_label().to_ascii_lowercase()
            )
        })
        .collect::<Vec<_>>();
    let Some(index) = select_menu("Remove monitored source", &labels, "Cancel")? else {
        return Ok(());
    };
    let removed = config.sources.remove(index);
    if removed.username.as_deref() == Some(config.channel.as_str()) {
        config.channel = config
            .sources
            .iter()
            .find(|source| source.access == SourceAccess::Public)
            .and_then(|source| source.username.clone())
            .unwrap_or_default();
    }
    config.save(&paths.config)?;
    print_wrapped(&success_text(format!(
        "✓ Removed {}.",
        removed.display_name()
    )));
    Ok(())
}

fn change_monitor_download_role(paths: &AppPaths) -> Result<()> {
    ensure_worker_stopped(&paths.lock_file)?;
    let mut config = Config::load_stored(&paths.config)?;
    let Some(selection) = select_menu_with_body(
        "Monitor account download role",
        &[muted_text(
            "The primary monitor continues watching every source in either mode.",
        )],
        &[
            "Keep the primary monitor out of downloads",
            "Allow the primary monitor to download",
        ],
        "Cancel",
    )?
    else {
        return Ok(());
    };
    config.monitor_account_downloads = selection == 1;
    config.save(&paths.config)?;
    if !config.monitor_account_downloads {
        Store::open(&paths.state_db)?.release_jobs_for_account(
            &config.monitor_primary,
            "configured monitoring account is monitor-only",
        )?;
    }
    print_wrapped(&success_text(format!(
        "✓ The monitor account is now {} for downloads.",
        if config.monitor_account_downloads {
            "eligible"
        } else {
            "excluded"
        }
    )));
    Ok(())
}

fn change_media_filter(paths: &AppPaths) -> Result<()> {
    loop {
        let action = (|| -> Result<bool> {
            ensure_worker_stopped(&paths.lock_file)?;
            let config = Config::load_stored(&paths.config)?;
            let Some(selection) = select_menu_with_body(
                "Media filters",
                &media_filter_lines(&config.media_filter),
                &[
                    "Change accepted file extensions",
                    "Change accepted file sizes",
                ],
                "Back",
            )?
            else {
                return Ok(true);
            };

            let changed = match selection {
                0 => change_extension_filter(paths)?,
                1 => change_size_filter(paths)?,
                _ => unreachable!("menu returns only an offered selection"),
            };
            if changed {
                pause()?;
            }
            Ok(false)
        })();

        match action {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => acknowledge_page_error("Media filters", error)?,
        }
    }
}

fn media_filter_lines(filter: &MediaFilter) -> Vec<String> {
    vec![
        format!(
            "{}  {}",
            accent_text("EXTENSIONS"),
            normal_text(extension_filter_label(filter))
        ),
        format!(
            "{}  {}",
            accent_text("FILE SIZE"),
            normal_text(size_filter_label(filter))
        ),
        muted_text("Filters apply to live monitoring and history imports."),
    ]
}

fn change_extension_filter(paths: &AppPaths) -> Result<bool> {
    let config = Config::load_stored(&paths.config)?;
    let Some(mode) = select_menu_with_body(
        "File extensions",
        &[format!(
            "{}  {}",
            accent_text("CURRENT"),
            normal_text(extension_filter_label(&config.media_filter))
        )],
        &["Accept any file extension", "Only specified extensions"],
        "Back",
    )?
    else {
        return Ok(false);
    };

    let extensions = if mode == 0 {
        Vec::new()
    } else {
        render_page(
            "File extensions",
            &[
                muted_text("Separate extensions with commas or spaces."),
                muted_text("Examples: rar, zip, tar.gz, mp4"),
            ],
        )?;
        parse_extensions(&prompt_required("Extensions: ")?)?
    };
    update_config(paths, |config| {
        config.media_filter.extensions = extensions;
        config.media_filter.validate()
    })?;
    print_wrapped(&success_text("✓ File extension filter updated."));
    Ok(true)
}

fn change_size_filter(paths: &AppPaths) -> Result<bool> {
    let config = Config::load_stored(&paths.config)?;
    let Some(mode) = select_menu_with_body(
        "File size limits",
        &[format!(
            "{}  {}",
            accent_text("CURRENT"),
            normal_text(size_filter_label(&config.media_filter))
        )],
        &[
            "Accept any file size",
            "Set a minimum size",
            "Set a maximum size",
            "Set a minimum and maximum range",
        ],
        "Back",
    )?
    else {
        return Ok(false);
    };

    let (min_size, max_size) = if mode == 0 {
        (None, None)
    } else {
        render_page(
            "File size limits",
            &[
                muted_text("Enter a number with an optional unit."),
                muted_text("Examples: 500 MB, 1.5 GB, 2 GiB"),
            ],
        )?;
        match mode {
            1 => (Some(prompt_file_size("Minimum size: ")?), None),
            2 => (None, Some(prompt_file_size("Maximum size: ")?)),
            3 => (
                Some(prompt_file_size("Minimum size: ")?),
                Some(prompt_file_size("Maximum size: ")?),
            ),
            _ => unreachable!("menu returns only an offered selection"),
        }
    };
    update_config(paths, |config| {
        config.media_filter.min_size = min_size;
        config.media_filter.max_size = max_size;
        config.media_filter.validate()
    })?;
    print_wrapped(&success_text("✓ File size filter updated."));
    Ok(true)
}

fn parse_extensions(value: &str) -> Result<Vec<String>> {
    let mut extensions = Vec::new();
    let mut unique = HashSet::new();
    for value in value.split(|character: char| character == ',' || character.is_whitespace()) {
        if value.trim().is_empty() {
            continue;
        }
        let extension = normalize_extension(value)?;
        if unique.insert(extension.clone()) {
            extensions.push(extension);
        }
    }
    if extensions.is_empty() {
        bail!("enter at least one file extension");
    }
    Ok(extensions)
}

fn prompt_file_size(label: &str) -> Result<u64> {
    parse_file_size(&prompt_required(label)?)
}

fn parse_file_size(value: &str) -> Result<u64> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(value.len());
    let amount = value[..split]
        .parse::<f64>()
        .context("file size must start with a number")?;
    if !amount.is_finite() || amount < 0.0 {
        bail!("file size must be a non-negative finite number");
    }
    let unit = value[split..].trim().to_ascii_lowercase().replace(' ', "");
    let multiplier = match unit.as_str() {
        "" | "b" | "byte" | "bytes" => 1_f64,
        "k" | "kb" | "kib" => 1024_f64,
        "m" | "mb" | "mib" => 1024_f64.powi(2),
        "g" | "gb" | "gib" => 1024_f64.powi(3),
        "t" | "tb" | "tib" => 1024_f64.powi(4),
        _ => bail!("unsupported file-size unit"),
    };
    let bytes = amount * multiplier;
    if bytes > u64::MAX as f64 {
        bail!("file size is too large");
    }
    Ok(bytes.round() as u64)
}

fn extension_filter_label(filter: &MediaFilter) -> String {
    if filter.extensions.is_empty() {
        "ANY".to_owned()
    } else {
        filter.extensions.join(", ")
    }
}

fn size_filter_label(filter: &MediaFilter) -> String {
    match (filter.min_size, filter.max_size) {
        (None, None) => "ANY".to_owned(),
        (Some(minimum), None) => format!("{} OR LARGER", human_bytes(minimum)),
        (None, Some(maximum)) => format!("UP TO {}", human_bytes(maximum)),
        (Some(minimum), Some(maximum)) => {
            format!("{} – {}", human_bytes(minimum), human_bytes(maximum))
        }
    }
}

fn change_output_folder(paths: &AppPaths) -> Result<()> {
    render_page("Change download folder", &[])?;
    ensure_worker_stopped(&paths.lock_file)?;
    let output_dir = normalize_user_path(&prompt_required("New download folder: ")?)?;
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    update_config(paths, |config| {
        config.output_dir = output_dir;
        Ok(())
    })?;
    print_wrapped(&success_text("✓ Download folder updated."));
    Ok(())
}

fn change_global_proxy(paths: &AppPaths) -> Result<()> {
    ensure_worker_stopped(&paths.lock_file)?;
    let Some(selection) = select_menu(
        "Global network route",
        &["Direct connection", "SOCKS5 proxy", "HTTP proxy"],
        "Cancel",
    )?
    else {
        return Ok(());
    };
    let proxy = match selection {
        0 => None,
        1 => Some(prompt_proxy(ProxyKind::Socks5)?),
        2 => Some(prompt_proxy(ProxyKind::Http)?),
        _ => unreachable!("menu returns only an offered selection"),
    };
    update_config(paths, |config| {
        config.proxy_url = proxy;
        Ok(())
    })?;
    print_wrapped(&success_text(
        "✓ Global route updated. Account-specific proxy overrides remain unchanged.",
    ));
    Ok(())
}

fn change_active_file_limit(paths: &AppPaths) -> Result<()> {
    render_page("Simultaneous downloads", &[])?;
    ensure_worker_stopped(&paths.lock_file)?;
    let current = Config::load_stored(&paths.config)?.max_active_files;
    let value = prompt_usize_in_range(
        &format!("Simultaneous files [{current}] (1-2): "),
        current,
        1,
        2,
    )?;
    update_config(paths, |config| {
        config.max_active_files = value;
        Ok(())
    })?;
    print_wrapped(&success_text("✓ Simultaneous file limit updated."));
    Ok(())
}

fn change_lane_limit(paths: &AppPaths) -> Result<()> {
    render_page("Transfer lanes", &[])?;
    ensure_worker_stopped(&paths.lock_file)?;
    let current = Config::load_stored(&paths.config)?.max_download_lanes_per_account;
    let value = prompt_usize_in_range(
        &format!("Maximum lanes per account [{current}] (1-8): "),
        current,
        1,
        8,
    )?;
    update_config(paths, |config| {
        config.max_download_lanes_per_account = value;
        Ok(())
    })?;
    print_wrapped(&success_text("✓ Transfer lane limit updated."));
    Ok(())
}

fn toggle_hash_verification(paths: &AppPaths) -> Result<()> {
    render_page("Full-file verification", &[])?;
    ensure_worker_stopped(&paths.lock_file)?;
    let enabled = Config::load_stored(&paths.config)?.verify_file_hashes;
    update_config(paths, |config| {
        config.verify_file_hashes = !enabled;
        Ok(())
    })?;
    print_wrapped(&success_text(format!(
        "✓ Full-file hash verification is now {}.",
        if enabled { "disabled" } else { "enabled" }
    )));
    Ok(())
}

fn change_api_credentials(paths: &AppPaths) -> Result<()> {
    render_page(
        "Telegram API credentials",
        &[warning_text(
            "Credentials are hidden and the API hash is never logged.",
        )],
    )?;
    ensure_worker_stopped(&paths.lock_file)?;
    let mut config = Config::load_stored(&paths.config)?;
    let api_id = prompt(&format!("Telegram API ID [{}]: ", config.api_id))?;
    if !api_id.is_empty() {
        config.api_id = api_id
            .parse()
            .context("Telegram API ID must be a positive integer")?;
    }
    let api_hash = prompt_password("Telegram API hash (empty keeps current): ")?;
    if !api_hash.is_empty() {
        config.api_hash = api_hash;
    }
    config.save(&paths.config)?;
    print_wrapped(&success_text("✓ Telegram API credentials updated."));
    Ok(())
}

fn prompt_usize_in_range(
    label: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize> {
    loop {
        let value = prompt(label)?;
        if value.is_empty() {
            return Ok(default);
        }
        match value.parse::<usize>() {
            Ok(value) if (minimum..=maximum).contains(&value) => return Ok(value),
            _ => print_wrapped(&warning_text(format!(
                "Enter a number from {minimum} to {maximum}."
            ))),
        }
    }
}

fn update_config(paths: &AppPaths, update: impl FnOnce(&mut Config) -> Result<()>) -> Result<()> {
    let mut config = Config::load_stored(&paths.config)?;
    update(&mut config)?;
    config.save(&paths.config)
}

fn persist_runtime_metadata(paths: &AppPaths, runtime: &Config) -> Result<()> {
    let mut stored = Config::load_stored(&paths.config)?;
    stored.sources = runtime.sources.clone();
    for account in &mut stored.accounts {
        let Some(current) = runtime.account(&account.name) else {
            continue;
        };
        account.user_id = current.user_id;
        account.display_name = current.display_name.clone();
        account.premium = current.premium;
        account.premium_checked_at = current.premium_checked_at;
    }
    stored.save(&paths.config)
}

async fn download_history(paths: &AppPaths) -> Result<bool> {
    let lock = acquire_lock(&paths.lock_file)?;
    let config = Config::load(&paths.config)?;
    loop {
        let Some(sources) = choose_history_sources(&config)? else {
            return Ok(false);
        };
        let Some((range, range_label)) = choose_history_range()? else {
            continue;
        };
        match execute_history_import(paths, &config, sources, range, &range_label).await {
            Ok((summary, source_count)) => {
                drop(lock);
                return history_summary_page(&summary, &range_label, source_count);
            }
            Err(error) => acknowledge_page_error("History download", error)?,
        }
    }
}

async fn execute_history_import(
    paths: &AppPaths,
    config: &Config,
    sources: Vec<SourceConfig>,
    range: HistoryRange,
    range_label: &str,
) -> Result<(HistorySummary, usize)> {
    let account = connect_history_account(paths, config, &sources).await?;
    let store = Store::open(&paths.state_db)?;
    let source_count = sources.len();
    let account_name = account.name.clone();
    let config = Arc::new(config.clone());
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let history_account = Arc::clone(&account);
    let history_config = Arc::clone(&config);
    let history_store = store.clone();
    let mut task = tokio::spawn(async move {
        import_history(
            history_account,
            history_config,
            sources,
            range,
            history_store,
            event_tx,
            shutdown_rx,
        )
        .await
    });

    let summary = {
        let input = MonitorInputGuard::new()?;
        let _cursor = if input.enabled {
            Some(CursorGuard::new(Term::stdout())?)
        } else {
            None
        };
        if !input.enabled {
            let signal_tx = shutdown_tx.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    let _ = signal_tx.send(true);
                }
            });
        }
        let mut view = HistoryView::new(
            &account_name,
            source_count,
            range_label.to_owned(),
            input.enabled,
        )?;
        let mut input_poll = tokio::time::interval(Duration::from_millis(100));
        input_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut task => {
                    break result.context("history import task panicked")??;
                }
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        view.render_event(event)?;
                    }
                }
                _ = input_poll.tick(), if input.enabled => {
                    while let Some(action) = poll_monitor_action()? {
                        match action {
                            MonitorAction::Stop => {
                                let _ = shutdown_tx.send(true);
                            }
                            MonitorAction::Refresh => view.refresh()?,
                            MonitorAction::Scroll(_) => {}
                        }
                    }
                }
            }
        }
    };
    account.client.disconnect();
    drop(account);
    drop(config);
    Ok((summary, source_count))
}

fn choose_history_sources(config: &Config) -> Result<Option<Vec<SourceConfig>>> {
    loop {
        let Some(mode) = select_menu_with_body(
            "History sources",
            &[
                muted_text("Choose where old media should be imported from."),
                warning_text(
                    "Private sources require the selected monitoring account to already be joined.",
                ),
            ],
            &[
                "Use all configured sources",
                "Choose one or multiple sources",
            ],
            "Back",
        )?
        else {
            return Ok(None);
        };
        if mode == 0 {
            return Ok(Some(config.sources.clone()));
        }

        let mut selected = vec![false; config.sources.len()];
        let mut warning = None;
        loop {
            let mut body = vec![muted_text(
                "Select a source to toggle it. Choose Continue when ready.",
            )];
            if let Some(message) = warning.take() {
                body.push(warning_text(message));
            }
            let mut entries = config
                .sources
                .iter()
                .enumerate()
                .map(|(index, source)| {
                    format!(
                        "{} {} — {} {}",
                        if selected[index] { "✓" } else { "○" },
                        source.display_name(),
                        source.access_label().to_ascii_lowercase(),
                        source.kind_label().to_ascii_lowercase()
                    )
                })
                .collect::<Vec<_>>();
            let selected_count = selected.iter().filter(|selected| **selected).count();
            entries.push(format!(
                "Continue with {selected_count} selected source{}",
                if selected_count == 1 { "" } else { "s" }
            ));
            let Some(choice) =
                select_menu_with_body("Choose history sources", &body, &entries, "Back")?
            else {
                break;
            };
            if choice < config.sources.len() {
                selected[choice] = !selected[choice];
                continue;
            }
            let sources = config
                .sources
                .iter()
                .zip(&selected)
                .filter(|(_, selected)| **selected)
                .map(|(source, _)| source.clone())
                .collect::<Vec<_>>();
            if sources.is_empty() {
                warning = Some("Select at least one source.".to_owned());
            } else {
                return Ok(Some(sources));
            }
        }
    }
}

fn choose_history_range() -> Result<Option<(HistoryRange, String)>> {
    loop {
        let body = vec![muted_text(
            "Times are entered in your local timezone unless an explicit UTC offset is included.",
        )];
        let Some(mode) = select_menu_with_body(
            "History time range",
            &body,
            &["Download full history", "Download a date and time range"],
            "Back",
        )?
        else {
            return Ok(None);
        };
        if mode == 0 {
            return Ok(Some((HistoryRange::default(), "FULL HISTORY".to_owned())));
        }

        render_page(
            "History time range",
            &[muted_text(
                "Accepted formats: YYYY-MM-DD, YYYY-MM-DD HH:MM, or RFC 3339.",
            )],
        )?;
        let range = (|| -> Result<_> {
            let start_text = prompt_required("Start: ")?;
            let end_text = prompt_required("End: ")?;
            parse_history_range_text(&start_text, &end_text)
        })();
        match range {
            Ok(range) => return Ok(Some(range)),
            Err(error) => acknowledge_page_error("History time range", error)?,
        }
    }
}

fn parse_history_range_text(start_text: &str, end_text: &str) -> Result<(HistoryRange, String)> {
    if start_text.trim().is_empty() || end_text.trim().is_empty() {
        bail!("a value is required");
    }
    let start = parse_history_time(start_text, false)?;
    let end = parse_history_time(end_text, true)?;
    let range = HistoryRange::new(start, end)?;
    let label = format!(
        "{} – {}",
        start.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S"),
        end.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S")
    );
    Ok((range, label))
}

fn parse_history_time(value: &str, end_boundary: bool) -> Result<DateTime<Utc>> {
    let value = value.trim();
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc));
    }
    let naive = if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        value
    } else if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M") {
        if end_boundary {
            value + chrono::Duration::seconds(59)
        } else {
            value
        }
    } else {
        let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .context("use YYYY-MM-DD, YYYY-MM-DD HH:MM, or RFC 3339")?;
        date.and_hms_opt(
            if end_boundary { 23 } else { 0 },
            if end_boundary { 59 } else { 0 },
            if end_boundary { 59 } else { 0 },
        )
        .context("invalid history date")?
    };
    let local = Local.from_local_datetime(&naive);
    let resolved = if end_boundary {
        local.latest()
    } else {
        local.earliest()
    }
    .context("the local history time does not exist in this timezone")?;
    Ok(resolved.with_timezone(&Utc))
}

async fn connect_history_account(
    paths: &AppPaths,
    config: &Config,
    sources: &[SourceConfig],
) -> Result<Arc<AccountRuntime>> {
    let mut accounts = config
        .accounts
        .iter()
        .filter(|account| account.enabled)
        .collect::<Vec<_>>();
    accounts.sort_by_key(|account| {
        (
            account.name != config.monitor_primary,
            account.monitor_priority,
            account.name.clone(),
        )
    });
    let mut failures = Vec::new();
    for account in accounts {
        let runtime = match AccountRuntime::connect(
            account,
            config,
            &paths.session_path(&account.name),
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                failures.push(format!("{}: {error:#}", account.name));
                continue;
            }
        };
        let mut inaccessible = None;
        for source in sources {
            if let Err(error) = runtime.resolve_source(source).await {
                inaccessible = Some(format!("{}: {error:#}", source.display_name()));
                break;
            }
        }
        if let Some(error) = inaccessible {
            failures.push(format!("{} cannot access {error}", account.name));
        } else {
            return Ok(runtime);
        }
    }
    bail!(
        "no enabled monitoring account can read every selected source: {}",
        failures.join("; ")
    )
}

fn history_summary_page(
    summary: &HistorySummary,
    range: &str,
    source_count: usize,
) -> Result<bool> {
    let body = vec![
        format!(
            "{}  {}    {}  {}",
            accent_text("SOURCES"),
            normal_text(source_count),
            accent_text("RANGE"),
            normal_text(range)
        ),
        format!(
            "{}  {}    {}  {}    {}  {}",
            accent_text("SCANNED"),
            normal_text(summary.scanned),
            accent_text("MEDIA"),
            normal_text(summary.media),
            accent_text("QUEUED"),
            success_text(summary.queued)
        ),
        format!(
            "{}  {}    {}  {}    {}  {}",
            accent_text("DUP"),
            normal_text(summary.duplicates),
            accent_text("FILTERED"),
            normal_text(summary.filtered),
            accent_text("EXISTING"),
            normal_text(summary.existing)
        ),
        if summary.cancelled {
            warning_text("The history scan was cancelled. Already queued jobs were preserved.")
        } else if summary.failed_sources > 0 {
            warning_text(format!(
                "{} source(s) could not be scanned completely.",
                summary.failed_sources
            ))
        } else {
            success_text("✓ History scan completed.")
        },
    ];
    if summary.queued == 0 {
        render_page("History import complete", &body)?;
        pause()?;
        return Ok(false);
    }
    Ok(select_menu_with_body(
        "History import complete",
        &body,
        &["Start downloading queued media now"],
        "Return to main menu",
    )?
    .is_some())
}

async fn run(paths: &AppPaths) -> Result<()> {
    paths.ensure()?;
    let lock = acquire_lock(&paths.lock_file)?;
    let mut config = Config::load(&paths.config)?;
    let store = Store::open(&paths.state_db)?;
    let mut startup_events = Vec::new();
    let mut runtimes = Vec::new();
    for account in config.accounts.clone().into_iter().filter(|a| a.enabled) {
        match AccountRuntime::connect(&account, &config, &paths.session_path(&account.name)).await {
            Ok(runtime) => {
                if let Some(saved) = config.account_mut(&account.name) {
                    saved.premium = runtime.is_premium();
                    saved.user_id = Some(runtime.user_id);
                    saved.display_name = Some(runtime.display_name.clone());
                    saved.premium_checked_at = Some(Utc::now());
                }
                runtimes.push(runtime);
            }
            Err(error) => startup_events.push((
                LiveEventLevel::Error,
                format!("Account {} unavailable: {error:#}", account.name),
            )),
        }
    }
    if runtimes.is_empty() {
        let details = startup_events
            .iter()
            .map(|(_, message)| message.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        bail!("none of the enabled accounts could be authorized: {details}");
    }
    let mut monitor_candidates = runtimes.clone();
    monitor_candidates.sort_by_key(|runtime| {
        (
            runtime.name != config.monitor_primary,
            config
                .account(&runtime.name)
                .map(|account| account.monitor_priority)
                .unwrap_or(u32::MAX),
            runtime.name.clone(),
        )
    });
    let mut validated_sources = None;
    for runtime in &monitor_candidates {
        let mut resolved = Vec::with_capacity(config.sources.len());
        let mut failure = None;
        for source in &config.sources {
            match validate_source(runtime, source).await {
                Ok(source) => resolved.push(source),
                Err(error) => {
                    failure = Some((source.display_name(), error));
                    break;
                }
            }
        }
        if let Some((source, error)) = failure {
            startup_events.push((
                LiveEventLevel::Warning,
                format!(
                    "Monitor candidate {} cannot access {source}: {error:#}",
                    runtime.name
                ),
            ));
        } else {
            validated_sources = Some(resolved);
            break;
        }
    }
    config.sources = validated_sources
        .context("no enabled monitoring account can access every configured channel and group")?;
    persist_runtime_metadata(paths, &config)?;
    let config = Arc::new(config);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let monitor_input = MonitorInputGuard::new()?;
    let _monitor_cursor = if monitor_input.enabled {
        Some(CursorGuard::new(Term::stdout())?)
    } else {
        None
    };
    if !monitor_input.enabled {
        let signal_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = signal_tx.send(true);
            }
        });
    }
    let (monitor_tx, monitor_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let monitor_shutdown = shutdown_tx.clone();
    let monitor_accounts = runtimes.clone();
    let monitor_config = config.clone();
    let monitor_store = store.clone();
    let monitor_shutdown_rx = shutdown_rx.clone();
    let monitor = tokio::spawn(async move {
        let result = run_monitor(
            monitor_accounts,
            monitor_config,
            monitor_store,
            monitor_tx,
            monitor_shutdown_rx,
        )
        .await;
        if result.is_err() {
            let _ = monitor_shutdown.send(true);
        }
        result
    });
    let scheduler = tokio::spawn(run_scheduler(
        runtimes,
        config,
        store.clone(),
        monitor_rx,
        event_tx,
        shutdown_rx,
    ));

    render_page("Downloads", &[])?;
    let mut view = DownloadView::new(&store)?;
    for (level, message) in startup_events {
        view.append_event(level, message);
    }
    view.render()?;
    let mut input_poll = tokio::time::interval(Duration::from_millis(100));
    input_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event = event_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                view.render_event(&store, event)?;
            }
            _ = input_poll.tick(), if monitor_input.enabled => {
                while let Some(action) = poll_monitor_action()? {
                    match action {
                        MonitorAction::Stop => {
                            let _ = shutdown_tx.send(true);
                        }
                        MonitorAction::Scroll(action) => view.scroll_logs(action)?,
                        MonitorAction::Refresh => view.refresh_log_view()?,
                    }
                }
                view.render()?;
            }
        }
    }
    let scheduler_result = scheduler.await.context("scheduler task panicked")?;
    let _ = shutdown_tx.send(true);
    let monitor_result = monitor.await.context("monitor task panicked")?;
    drop(lock);
    scheduler_result?;
    monitor_result.context("monitor stopped")
}

struct MonitorInputGuard {
    enabled: bool,
}

impl MonitorInputGuard {
    fn new() -> Result<Self> {
        let enabled = Term::stdout().is_term();
        if enabled {
            enable_raw_mode().context("failed to enable monitoring keyboard controls")?;
        }
        Ok(Self { enabled })
    }
}

impl Drop for MonitorInputGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = disable_raw_mode();
        }
    }
}

struct HistoryView {
    term: Option<Term>,
    rendered_size: Option<(u16, u16)>,
    rendered_lines: Vec<String>,
    account: String,
    source_count: usize,
    range: String,
    progress: Option<HistoryProgress>,
    warnings: Vec<String>,
}

impl HistoryView {
    fn new(account: &str, source_count: usize, range: String, interactive: bool) -> Result<Self> {
        let mut view = Self {
            term: interactive.then(Term::stdout),
            rendered_size: None,
            rendered_lines: Vec::new(),
            account: account.to_owned(),
            source_count,
            range,
            progress: None,
            warnings: Vec::new(),
        };
        if interactive {
            view.render()?;
        } else {
            render_page(
                "History import",
                &[muted_text(
                    "Scanning selected sources. Press Ctrl+C to cancel.",
                )],
            )?;
        }
        Ok(view)
    }

    fn render_event(&mut self, event: HistoryEvent) -> Result<()> {
        match event {
            HistoryEvent::Progress(progress) => self.progress = Some(progress),
            HistoryEvent::Warning(message) => self.warnings.push(compact_event_text(&message)),
        }
        self.render()
    }

    fn refresh(&mut self) -> Result<()> {
        self.rendered_size = None;
        self.render()
    }

    fn render(&mut self) -> Result<()> {
        let Some(term) = self.term.clone() else {
            return Ok(());
        };
        let size = term.size();
        let height = usize::from(size.0);
        let width = terminal_width(&term);
        if self.rendered_size != Some(size) {
            render_page("History import", &[])?;
            self.rendered_lines.clear();
            self.rendered_size = Some(size);
        }
        let lines = self.dashboard_lines(height, width);
        for index in 0..self.rendered_lines.len().max(lines.len()) {
            let next = lines.get(index).map(String::as_str).unwrap_or("");
            if self.rendered_lines.get(index).map(String::as_str) == Some(next) {
                continue;
            }
            term.move_cursor_to(0, 5 + index)
                .context("failed to position the history dashboard")?;
            term.write_str(&fixed_width_text(next, width))
                .context("failed to draw the history dashboard")?;
        }
        term.move_cursor_to(0, height.saturating_sub(1))
            .context("failed to park the history cursor")?;
        term.flush()
            .context("failed to draw the history dashboard")?;
        self.rendered_lines = lines;
        Ok(())
    }

    fn dashboard_lines(&self, height: usize, width: usize) -> Vec<String> {
        let inner_width = width.saturating_sub(4);
        let session = format!(
            "{} {}   {} {}   {} {}",
            accent_text("ACCOUNT"),
            normal_text(&self.account),
            accent_text("SOURCES"),
            normal_text(self.source_count),
            accent_text("RANGE"),
            normal_text(&self.range)
        );
        let mut lines = vec![panel_top(&accent_text("HISTORY SESSION"), width)];
        lines.extend(
            wrap_text_lines(&session, inner_width)
                .iter()
                .map(|line| panel_row(line, width)),
        );
        lines.push(panel_bottom(width));
        lines.push(panel_top(&accent_text("SCAN PROGRESS"), width));
        if let Some(progress) = &self.progress {
            lines.push(panel_row(
                &format!(
                    "{} {}   {} {}/{}",
                    accent_text("SOURCE"),
                    normal_text(&progress.source),
                    muted_text("POSITION"),
                    normal_text(progress.source_index),
                    normal_text(progress.source_count)
                ),
                width,
            ));
            lines.push(panel_row(
                &format!(
                    "{} {}   {} {}   {} {}",
                    accent_text("SCANNED"),
                    normal_text(progress.scanned),
                    accent_text("MEDIA"),
                    normal_text(progress.media),
                    accent_text("QUEUED"),
                    success_text(progress.queued)
                ),
                width,
            ));
            lines.push(panel_row(
                &format!(
                    "{} {}   {} {}   {} {}",
                    accent_text("DUP"),
                    normal_text(progress.duplicates),
                    accent_text("FILTERED"),
                    normal_text(progress.filtered),
                    accent_text("EXISTING"),
                    normal_text(progress.existing)
                ),
                width,
            ));
        } else {
            lines.push(panel_row(
                &muted_text("Preparing the selected sources…"),
                width,
            ));
        }
        lines.push(panel_bottom(width));

        let available = height.saturating_sub(6);
        let warning_capacity = available.saturating_sub(lines.len() + 3).max(1);
        lines.push(panel_top(
            &panel_heading("WARNINGS", Some(self.warnings.len())),
            width,
        ));
        let warning_rows = self
            .warnings
            .iter()
            .flat_map(|warning| {
                wrap_text_lines(
                    &format!("{} {}", warning_text("⚠"), normal_text(warning)),
                    inner_width,
                )
            })
            .collect::<Vec<_>>();
        if warning_rows.is_empty() {
            lines.push(panel_row(&muted_text("No warnings."), width));
            lines.extend(std::iter::repeat_n(
                panel_row("", width),
                warning_capacity.saturating_sub(1),
            ));
        } else {
            let first = warning_rows.len().saturating_sub(warning_capacity);
            for row in &warning_rows[first..] {
                lines.push(panel_row(row, width));
            }
            lines.extend(std::iter::repeat_n(
                panel_row("", width),
                warning_capacity.saturating_sub(warning_rows.len() - first),
            ));
        }
        lines.push(panel_bottom(width));
        lines.push(fixed_width_text(&muted_text("Ctrl+C cancel"), width));
        lines
    }
}

#[derive(Clone, Copy)]
enum MonitorAction {
    Stop,
    Scroll(LogScroll),
    Refresh,
}

#[derive(Clone, Copy)]
enum LogScroll {
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
}

fn poll_monitor_action() -> Result<Option<MonitorAction>> {
    if !event::poll(Duration::ZERO).context("failed to poll monitoring keyboard input")? {
        return Ok(None);
    }
    let event = event::read().context("failed to read monitoring keyboard input")?;
    let action = match event {
        Event::Resize(_, _) => Some(MonitorAction::Refresh),
        Event::Key(key) if key.kind != KeyEventKind::Release => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Some(MonitorAction::Scroll(LogScroll::Up)),
            KeyCode::Down | KeyCode::Char('j') => Some(MonitorAction::Scroll(LogScroll::Down)),
            KeyCode::PageUp => Some(MonitorAction::Scroll(LogScroll::PageUp)),
            KeyCode::PageDown => Some(MonitorAction::Scroll(LogScroll::PageDown)),
            KeyCode::Home => Some(MonitorAction::Scroll(LogScroll::Home)),
            KeyCode::End => Some(MonitorAction::Scroll(LogScroll::End)),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(MonitorAction::Stop)
            }
            _ => None,
        },
        _ => None,
    };
    Ok(action)
}

#[derive(Default)]
struct DownloadStats {
    queued: usize,
    active: usize,
    completed: usize,
    duplicates: usize,
    warnings: usize,
    errors: usize,
}

impl DownloadStats {
    fn refresh_jobs(&mut self, store: &Store, active: usize) -> Result<()> {
        self.queued = store.pending_job_count()?;
        self.active = active;
        Ok(())
    }

    fn line(&self) -> String {
        format!(
            "{} {}   {} {}   {} {}   {} {}   {} {}   {} {}",
            accent_text("QUEUE"),
            count_text(self.queued, CountTone::Warning),
            accent_text("ACTIVE"),
            count_text(self.active, CountTone::Normal),
            accent_text("DONE"),
            count_text(self.completed, CountTone::Success),
            accent_text("DUP"),
            count_text(self.duplicates, CountTone::Normal),
            accent_text("WARN"),
            count_text(self.warnings, CountTone::Warning),
            accent_text("ERR"),
            count_text(self.errors, CountTone::Error),
        )
    }

    fn compact_line(&self) -> String {
        format!(
            "{}{}  {}{}  {}{}  {}{}  {}{}  {}{}",
            accent_text("Q"),
            count_text(self.queued, CountTone::Warning),
            accent_text("A"),
            count_text(self.active, CountTone::Normal),
            accent_text("D"),
            count_text(self.completed, CountTone::Success),
            accent_text("X"),
            count_text(self.duplicates, CountTone::Normal),
            accent_text("W"),
            count_text(self.warnings, CountTone::Warning),
            accent_text("E"),
            count_text(self.errors, CountTone::Error),
        )
    }
}

#[derive(Clone, Copy)]
enum CountTone {
    Normal,
    Success,
    Warning,
    Error,
}

fn count_text(count: usize, tone: CountTone) -> String {
    if count == 0 {
        return muted_text(count);
    }
    match tone {
        CountTone::Normal => normal_text(count),
        CountTone::Success => success_text(count),
        CountTone::Warning => warning_text(count),
        CountTone::Error => error_text(count),
    }
}

#[derive(Clone, Copy)]
enum LiveEventLevel {
    Info,
    Warning,
    Error,
}

struct LogEntry {
    timestamp: String,
    level: LiveEventLevel,
    message: String,
}

struct MonitorStatus {
    account: String,
    sources: Option<usize>,
}

struct ActiveDownload {
    account: String,
    route: String,
    filename: String,
    total: u64,
    downloaded: u64,
    initial_bytes: u64,
    lanes: usize,
    phase: DownloadPhase,
    started_at: StdInstant,
    sampled_at: StdInstant,
    sampled_bytes: u64,
    speed: f64,
}

impl ActiveDownload {
    fn new(account: String, route: String, filename: String, total: u64, completed: u64) -> Self {
        let now = StdInstant::now();
        Self {
            account,
            route,
            filename,
            total,
            downloaded: completed,
            initial_bytes: completed,
            lanes: 0,
            phase: DownloadPhase::Downloading,
            started_at: now,
            sampled_at: now,
            sampled_bytes: completed,
            speed: 0.0,
        }
    }

    fn update(&mut self, downloaded: u64, lanes: usize, phase: DownloadPhase) {
        let now = StdInstant::now();
        let sample_time = now.duration_since(self.sampled_at).as_secs_f64();
        if sample_time >= 0.2 {
            let sample_bytes = downloaded.saturating_sub(self.sampled_bytes) as f64;
            let sample_speed = sample_bytes / sample_time;
            self.speed = if self.speed == 0.0 {
                sample_speed
            } else {
                self.speed * 0.7 + sample_speed * 0.3
            };
            self.sampled_at = now;
            self.sampled_bytes = downloaded;
        }
        self.downloaded = downloaded;
        self.lanes = lanes;
        self.phase = phase;
    }

    fn phase_label(&self) -> String {
        match self.phase {
            DownloadPhase::Downloading if self.lanes == 0 => "starting".to_owned(),
            DownloadPhase::Downloading => format!("{} lanes", self.lanes),
            DownloadPhase::Verifying => "checking".to_owned(),
            DownloadPhase::Finalizing => "saving".to_owned(),
        }
    }
}

struct DownloadView {
    term: Option<Term>,
    rendered_size: Option<(u16, u16)>,
    rendered_lines: Vec<String>,
    logs: Vec<LogEntry>,
    wrapped_logs: Vec<String>,
    wrapped_log_width: Option<usize>,
    monitor_status: Option<MonitorStatus>,
    log_offset: usize,
    follow_log_tail: bool,
    downloads: HashMap<i64, ActiveDownload>,
    stats: DownloadStats,
}

impl DownloadView {
    fn new(store: &Store) -> Result<Self> {
        Self::with_terminal(store, Some(Term::stdout()))
    }

    #[cfg(test)]
    fn for_test(store: &Store) -> Result<Self> {
        Self::with_terminal(store, None)
    }

    fn with_terminal(store: &Store, term: Option<Term>) -> Result<Self> {
        let mut view = Self {
            term,
            rendered_size: None,
            rendered_lines: Vec::new(),
            logs: Vec::new(),
            wrapped_logs: Vec::new(),
            wrapped_log_width: None,
            monitor_status: None,
            log_offset: 0,
            follow_log_tail: true,
            downloads: HashMap::new(),
            stats: DownloadStats::default(),
        };
        view.refresh_stats(store)?;
        view.render()?;
        Ok(view)
    }

    fn render_event(&mut self, store: &Store, event: SchedulerEvent) -> Result<()> {
        match event {
            SchedulerEvent::Monitoring { account, sources } => {
                self.monitor_status = Some(MonitorStatus {
                    account,
                    sources: Some(sources),
                });
            }
            SchedulerEvent::Assigned {
                job_id,
                account,
                filename,
                route,
            } => {
                self.downloads.remove(&job_id);
                let job = store.job(job_id)?;
                let total = job.as_ref().and_then(|job| job.total_size).unwrap_or(0);
                let completed = job.as_ref().map(|job| job.completed_bytes()).unwrap_or(0);
                self.downloads.insert(
                    job_id,
                    ActiveDownload::new(account.clone(), route, filename.clone(), total, completed),
                );
                self.refresh_stats(store)?;
                self.append_event(
                    LiveEventLevel::Info,
                    format!("Started: {filename} · {account}"),
                );
            }
            SchedulerEvent::Progress(update) => {
                if let Some(download) = self.downloads.get_mut(&update.job_id) {
                    download.update(update.downloaded_bytes, update.lanes, update.phase);
                }
            }
            SchedulerEvent::Inactive { job_id } => {
                self.downloads.remove(&job_id);
                self.refresh_stats(store)?;
            }
            SchedulerEvent::Completed { job_id, filename } => {
                self.downloads.remove(&job_id);
                self.stats.completed += 1;
                self.refresh_stats(store)?;
                self.append_event(LiveEventLevel::Info, format!("Done: {filename}"));
            }
            SchedulerEvent::Duplicate { filename, path } => {
                self.stats.duplicates += 1;
                self.refresh_stats(store)?;
                self.append_event(
                    LiveEventLevel::Info,
                    format!("Already downloaded: {filename} · {path}"),
                );
            }
            SchedulerEvent::Cooldown {
                job_id,
                account,
                seconds,
                reason,
            } => {
                self.downloads.remove(&job_id);
                self.refresh_stats(store)?;
                let wait = if reason == "FLOOD_PREMIUM_WAIT" {
                    "speed wait"
                } else {
                    "Telegram wait"
                };
                self.append_event(
                    LiveEventLevel::Warning,
                    format!(
                        "{} paused {seconds}s · {account} · {wait}",
                        job_label(store, job_id)
                    ),
                );
            }
            SchedulerEvent::Failed { job_id, error } => {
                self.downloads.remove(&job_id);
                self.refresh_stats(store)?;
                self.append_event(
                    LiveEventLevel::Error,
                    format!("Failed: {} · {error}", job_label(store, job_id)),
                );
            }
            SchedulerEvent::Warning(message) => {
                self.refresh_stats(store)?;
                self.append_event(LiveEventLevel::Warning, message);
            }
            SchedulerEvent::Notice(message) => {
                self.refresh_stats(store)?;
                self.append_event(LiveEventLevel::Info, message);
            }
        }
        self.render()
    }

    fn refresh_stats(&mut self, store: &Store) -> Result<()> {
        self.stats.refresh_jobs(store, self.downloads.len())
    }

    fn append_event(&mut self, level: LiveEventLevel, message: impl AsRef<str>) {
        match level {
            LiveEventLevel::Info => {}
            LiveEventLevel::Warning => self.stats.warnings += 1,
            LiveEventLevel::Error => self.stats.errors += 1,
        }
        self.logs.push(LogEntry {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            level,
            message: compact_event_text(message.as_ref()),
        });
        self.wrapped_log_width = None;
    }

    fn refresh_log_view(&mut self) -> Result<()> {
        let (capacity, width) = self.current_log_layout();
        self.ensure_wrapped_logs(width);
        self.normalize_log_offset(capacity);
        self.render()
    }

    fn scroll_logs(&mut self, action: LogScroll) -> Result<()> {
        let (capacity, width) = self.current_log_layout();
        self.ensure_wrapped_logs(width);
        let max_offset = log_tail_offset(self.wrapped_logs.len(), capacity);
        self.log_offset = match action {
            LogScroll::Up => self.log_offset.saturating_sub(1),
            LogScroll::Down => self.log_offset.saturating_add(1).min(max_offset),
            LogScroll::PageUp => self.log_offset.saturating_sub(capacity),
            LogScroll::PageDown => self.log_offset.saturating_add(capacity).min(max_offset),
            LogScroll::Home => 0,
            LogScroll::End => max_offset,
        };
        self.follow_log_tail = self.log_offset == max_offset;
        self.render()
    }

    fn current_log_layout(&self) -> (usize, usize) {
        let height = self
            .term
            .as_ref()
            .map(|term| usize::from(term.size().0))
            .unwrap_or(24);
        let width = self
            .term
            .as_ref()
            .map(terminal_width)
            .unwrap_or(100)
            .saturating_sub(4);
        let active_rows = active_download_content_rows(&self.downloads, width);
        (monitor_log_capacity(height, active_rows), width)
    }

    fn ensure_wrapped_logs(&mut self, width: usize) {
        if self.wrapped_log_width == Some(width) {
            return;
        }
        self.wrapped_logs = wrap_log_entries(&self.logs, width);
        self.wrapped_log_width = Some(width);
    }

    fn normalize_log_offset(&mut self, capacity: usize) {
        let max_offset = log_tail_offset(self.wrapped_logs.len(), capacity);
        if self.follow_log_tail {
            self.log_offset = max_offset;
        } else {
            self.log_offset = self.log_offset.min(max_offset);
        }
    }

    fn render(&mut self) -> Result<()> {
        let Some(term) = self.term.clone() else {
            let (capacity, width) = self.current_log_layout();
            self.ensure_wrapped_logs(width);
            self.normalize_log_offset(capacity);
            return Ok(());
        };
        let size = term.size();
        let height = usize::from(size.0);
        let width = terminal_width(&term);
        if self.rendered_size != Some(size) {
            render_page("Downloads", &[])?;
            self.rendered_lines.clear();
            self.rendered_size = Some(size);
        }

        let lines = self.dashboard_lines(height, width);
        let count = self.rendered_lines.len().max(lines.len());
        for index in 0..count {
            let next = lines.get(index).map(String::as_str).unwrap_or("");
            if self.rendered_lines.get(index).map(String::as_str) == Some(next) {
                continue;
            }
            term.move_cursor_to(0, 5 + index)
                .context("failed to position the monitor dashboard")?;
            let row = if next.is_empty() {
                " ".repeat(width)
            } else {
                fixed_width_text(next, width)
            };
            term.write_str(&row)
                .context("failed to draw a monitor dashboard row")?;
        }
        term.move_cursor_to(0, height.saturating_sub(1))
            .context("failed to park the monitor cursor")?;
        term.flush()
            .context("failed to draw the monitor dashboard")?;
        self.rendered_lines = lines;
        Ok(())
    }

    fn dashboard_lines(&mut self, height: usize, width: usize) -> Vec<String> {
        let inner_width = width.saturating_sub(4);
        let active_rows = active_download_content_rows(&self.downloads, inner_width);
        let capacity = monitor_log_capacity(height, active_rows);
        self.ensure_wrapped_logs(inner_width);
        self.normalize_log_offset(capacity);
        let compact = width < 90;
        let mut stats_line = if compact {
            self.stats.compact_line()
        } else {
            self.stats.line()
        };
        if let Some(status) = &self.monitor_status {
            stats_line.push_str("   ");
            stats_line.push_str(&accent_text("MON"));
            stats_line.push(' ');
            stats_line.push_str(&normal_text(ellipsize(
                &status.account,
                inner_width.saturating_sub(::console::measure_text_width(&stats_line) + 1),
            )));
            if let Some(sources) = status.sources {
                stats_line.push_str("   ");
                stats_line.push_str(&accent_text("SRC"));
                stats_line.push(' ');
                stats_line.push_str(&count_text(sources, CountTone::Normal));
            }
        }
        let mut lines = vec![
            panel_top(&panel_heading("LIVE SESSION", None), width),
            panel_row(&stats_line, width),
            panel_bottom(width),
            panel_top(
                &panel_heading("ACTIVE DOWNLOADS", Some(self.downloads.len())),
                width,
            ),
        ];

        if self.downloads.is_empty() {
            lines.push(panel_row(
                &muted_text("No active downloads · Waiting for new media…"),
                width,
            ));
        } else {
            let mut job_ids = self.downloads.keys().copied().collect::<Vec<_>>();
            job_ids.sort_unstable();
            for (index, job_id) in job_ids.into_iter().enumerate() {
                if index > 0 {
                    lines.push(panel_row(&muted_text("┄".repeat(inner_width)), width));
                }
                let download = &self.downloads[&job_id];
                lines.extend(
                    active_download_lines(download, inner_width)
                        .iter()
                        .map(|line| panel_row(line, width)),
                );
            }
        }

        lines.push(panel_bottom(width));
        lines.push(panel_top(
            &panel_heading("ACTIVITY LOG", Some(self.logs.len())),
            width,
        ));
        let total_log_rows = self.wrapped_logs.len();
        let end = (self.log_offset + capacity).min(total_log_rows);
        let log_lines = self.wrapped_logs[self.log_offset..end].to_vec();
        for line in &log_lines {
            lines.push(panel_row(line, width));
        }
        lines.extend(std::iter::repeat_n(
            panel_row("", width),
            capacity.saturating_sub(log_lines.len()),
        ));
        lines.push(panel_bottom(width));

        let scrollable = total_log_rows > capacity;
        let footer = if scrollable && width < 80 {
            format!(
                "↑/↓ PgUp/PgDn Home/End Ctrl+C    {}–{}/{}",
                self.log_offset + 1,
                end,
                total_log_rows
            )
        } else if scrollable {
            format!(
                "↑/↓ scroll  PgUp/PgDn page  Home/End  Ctrl+C stop    {}–{} / {} rows",
                self.log_offset + 1,
                end,
                total_log_rows
            )
        } else {
            format!("Ctrl+C stop    {} event(s)", self.logs.len())
        };
        lines.push(fixed_width_text(
            &muted_text(ellipsize(&footer, width)),
            width,
        ));
        lines
    }
}

fn panel_heading(label: &str, count: Option<usize>) -> String {
    match count {
        Some(count) => format!(
            "{}  {}",
            accent_text(label),
            count_text(count, CountTone::Normal)
        ),
        None => accent_text(label),
    }
}

fn active_download_content_rows(downloads: &HashMap<i64, ActiveDownload>, width: usize) -> usize {
    if downloads.is_empty() {
        return 1;
    }
    downloads
        .values()
        .map(|download| active_download_lines(download, width).len())
        .sum::<usize>()
        + downloads.len().saturating_sub(1)
}

fn active_download_lines(download: &ActiveDownload, width: usize) -> Vec<String> {
    let marker = success_text("●");
    let filename_lines = wrap_text_lines(download.filename.trim(), width.saturating_sub(2));
    let mut lines = filename_lines
        .into_iter()
        .enumerate()
        .map(|(index, filename)| {
            if index == 0 {
                format!("{marker} {}", normal_text(filename))
            } else {
                format!("  {}", normal_text(filename))
            }
        })
        .collect::<Vec<_>>();
    let elapsed = format_elapsed(download.started_at.elapsed());
    let transferred = download.downloaded.saturating_sub(download.initial_bytes);
    let speed = if download.speed > 0.0 {
        format!("{}/s", human_bytes(download.speed as u64))
    } else {
        "—/s".to_owned()
    };
    let details = format!(
        "  {} · {} · {} · {}    {} / {}    {}",
        download.account,
        download.route,
        download.phase_label(),
        elapsed,
        human_bytes(download.downloaded),
        if download.total > 0 {
            human_bytes(download.total)
        } else {
            "unknown".to_owned()
        },
        speed
    );
    lines.extend(
        wrap_text_lines(&details, width.saturating_sub(2))
            .into_iter()
            .map(|details| muted_text(format!("  {details}"))),
    );
    let ratio = if download.total > 0 {
        (download.downloaded as f64 / download.total as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let percent = ratio * 100.0;
    let suffix = if download.total > 0 {
        format!("{percent:>6.2}%")
    } else {
        format!("{} transferred", human_bytes(transferred))
    };
    let bar_width = width.saturating_sub(suffix.chars().count() + 4).max(10);
    let filled = ((bar_width as f64) * ratio).round() as usize;
    let filled_bar = "━".repeat(filled.min(bar_width));
    let remaining_bar = "─".repeat(bar_width.saturating_sub(filled));
    let progress = format!(
        "  {}{}  {}",
        accent_text(filled_bar),
        muted_text(remaining_bar),
        normal_text(suffix)
    );
    lines.push(progress);
    lines
}

fn log_entry_lines(entry: &LogEntry, width: usize) -> Vec<String> {
    let (label, styled_label) = match entry.level {
        LiveEventLevel::Info => ("INFO", info_text("INFO ")),
        LiveEventLevel::Warning => ("WARN", warning_text("WARN ")),
        LiveEventLevel::Error => ("ERROR", error_text("ERROR")),
    };
    let prefix_width = 8 + 2 + label.len().max(5) + 2;
    let mut messages = wrap_text_lines(&entry.message, width.saturating_sub(prefix_width));
    remove_separators_at_wrap_boundaries(&mut messages);
    messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| {
            if index == 0 {
                format!(
                    "{}  {}  {}",
                    muted_text(&entry.timestamp),
                    styled_label,
                    normal_text(message)
                )
            } else {
                format!("{}{}", " ".repeat(prefix_width), normal_text(message))
            }
        })
        .collect()
}

fn remove_separators_at_wrap_boundaries(lines: &mut [String]) {
    for index in 0..lines.len().saturating_sub(1) {
        let current = lines[index].trim_end();
        if let Some(without_separator) = current.strip_suffix('·') {
            lines[index] = without_separator.trim_end().to_owned();
        }

        let continuation = lines[index + 1].trim_start();
        if let Some(without_separator) = continuation.strip_prefix('·') {
            lines[index + 1] = without_separator.trim_start().to_owned();
        }
    }
}

fn wrap_log_entries(logs: &[LogEntry], width: usize) -> Vec<String> {
    logs.iter()
        .flat_map(|entry| log_entry_lines(entry, width))
        .collect()
}

fn log_tail_offset(total_rows: usize, capacity: usize) -> usize {
    total_rows.saturating_sub(capacity)
}

fn ellipsize(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut shortened = value
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    shortened.push('…');
    shortened
}

fn format_elapsed(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

fn monitor_log_capacity(height: usize, active_content_rows: usize) -> usize {
    let active_panel_rows = active_content_rows + 2;
    // Five rows belong to the application header, and the final terminal row
    // stays unused to prevent the alternate screen from scrolling.
    let dashboard_rows = height.saturating_sub(6);
    let fixed_rows = 3 + active_panel_rows + 2 + 1;
    dashboard_rows.saturating_sub(fixed_rows).max(1)
}

fn job_label(store: &Store, job_id: i64) -> String {
    store
        .job(job_id)
        .ok()
        .flatten()
        .map(|job| job.filename)
        .unwrap_or_else(|| format!("Job #{job_id}"))
}

fn compact_event_text(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn status(paths: &AppPaths) -> Result<()> {
    let config = Config::load(&paths.config)?;
    let store = Store::open(&paths.state_db)?;
    store.clear_expired_cooldowns()?;
    let jobs = store.jobs()?;
    let (mut fixed_lines, account_rows) = account_page_sections(&config, &store, &jobs)?;
    fixed_lines.extend(account_rows);
    fixed_lines.push(String::new());
    let width = terminal_width(&Term::stdout());
    fixed_lines.push(panel_top(&accent_text("JOBS"), width));
    fixed_lines.push(panel_row(
        &muted_text(format!(
            "{:<7} {:<11} {:<18} {:>10}  FILE",
            "JOB", "STATE", "ACCOUNT", "SIZE"
        )),
        width,
    ));
    let mut data_lines = Vec::new();
    let inner_width = width.saturating_sub(4);
    if jobs.is_empty() {
        data_lines.push(panel_row(
            &muted_text("No jobs have been accepted yet."),
            width,
        ));
    }
    for job in jobs {
        let prefix = format!(
            "#{:<5} {:<11} {:<18} {:>10}  ",
            job.id,
            job.state.as_str(),
            job.assigned_account.as_deref().unwrap_or("-"),
            human_bytes(job.completed_bytes()),
        );
        let content = format!(
            "{}{}",
            job.filename,
            job.last_error
                .as_deref()
                .map(|error| format!(" — {error}"))
                .unwrap_or_default()
        );
        for row in wrap_hanging_text_lines(&prefix, &content, inner_width) {
            let styled = match job.state.as_str() {
                "completed" => success_text(row),
                "failed" => error_text(row),
                "retrying" => warning_text(row),
                "downloading" => accent_text(row),
                _ => normal_text(row),
            };
            data_lines.push(panel_row(&styled, width));
        }
    }
    data_lines.push(panel_bottom(width));
    view_data_page("Status", &fixed_lines, &data_lines)
}

fn retry(paths: &AppPaths) -> Result<()> {
    let store = Store::open(&paths.state_db)?;
    let count = store.retry_failed()?;
    if count == 0 {
        print_wrapped(&success_text("✓ No failed jobs need retrying."));
    } else {
        print_wrapped(&success_text(format!("✓ Requeued {count} failed job(s).")));
    }
    Ok(())
}

fn account_list_page(paths: &AppPaths) -> Result<()> {
    let config = Config::load(&paths.config)?;
    let store = Store::open(&paths.state_db)?;
    store.clear_expired_cooldowns()?;
    let jobs = store.jobs()?;
    let (fixed_lines, data_lines) = account_page_sections(&config, &store, &jobs)?;
    view_data_page("Accounts", &fixed_lines, &data_lines)
}

fn account_page_sections(
    config: &Config,
    store: &Store,
    jobs: &[Job],
) -> Result<(Vec<String>, Vec<String>)> {
    let width = terminal_width(&Term::stdout());
    let inner_width = width.saturating_sub(4);
    let fixed_lines = vec![
        panel_top(&accent_text("ACCOUNTS"), width),
        panel_row(
            &muted_text(format!(
                "{:<16} {:<7} {:<7} {:<14} {:<7} {:<20} DISPLAY NAME",
                "ALIAS", "TIER", "ROUTE", "STATE", "MONITOR", "CURRENT FILE"
            )),
            width,
        ),
    ];
    let mut data_lines = Vec::new();
    let current_by_account = jobs
        .iter()
        .filter_map(|job| {
            job.assigned_account
                .as_deref()
                .map(|account| (account, job.filename.as_str()))
        })
        .collect::<HashMap<_, _>>();
    for account in &config.accounts {
        let state = store.account_state(&account.name)?;
        let current = current_by_account
            .get(account.name.as_str())
            .copied()
            .unwrap_or("-");
        let health = state
            .cooldown_until
            .filter(|until| *until > Utc::now())
            .map(|until| format!("cooldown {}s", (until - Utc::now()).num_seconds().max(0)))
            .unwrap_or_else(|| {
                if !account.enabled {
                    "disabled".into()
                } else if config.monitor_primary == account.name {
                    if config.monitor_account_downloads {
                        "monitor+download".into()
                    } else {
                        "monitor-only".into()
                    }
                } else if current != "-" {
                    "downloading".into()
                } else {
                    "idle".into()
                }
            });
        let prefix = format!(
            "{:<16} {:<7} {:<7} {:<14} {:<7} {:<20} ",
            account.name,
            if account.premium { "premium" } else { "normal" },
            proxy_label(config.proxy_for_account(account)),
            health,
            if config.monitor_primary == account.name {
                "primary"
            } else {
                "standby"
            },
            current,
        );
        let content = format!(
            "{}{}",
            account.display_name.as_deref().unwrap_or("-"),
            state
                .last_error
                .as_deref()
                .map(|error| format!(" — {error}"))
                .unwrap_or_default()
        );
        for row in wrap_hanging_text_lines(&prefix, &content, inner_width) {
            let styled = if !account.enabled {
                muted_text(row)
            } else if state.cooldown_until.is_some_and(|until| until > Utc::now())
                || state.last_error.is_some()
            {
                warning_text(row)
            } else if account.premium {
                success_text(row)
            } else if config.monitor_primary == account.name {
                accent_text(row)
            } else {
                normal_text(row)
            };
            data_lines.push(panel_row(&styled, width));
        }
    }
    data_lines.push(panel_bottom(width));
    Ok((fixed_lines, data_lines))
}

fn acquire_lock(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        security::create_private_dir(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    security::restrict_file(path)?;
    file.try_lock_exclusive()
        .with_context(|| "another tgdl worker is already running")?;
    Ok(file)
}

fn ensure_worker_stopped(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    if file.try_lock_exclusive().is_err() {
        bail!("stop the running tgdl worker before changing accounts");
    }
    FileExt::unlock(&file)?;
    Ok(())
}

fn remove_session_files(path: &Path) -> Result<()> {
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let mut shm = path.as_os_str().to_os_string();
    shm.push("-shm");
    let candidates = [path.to_path_buf(), PathBuf::from(wal), PathBuf::from(shm)];
    for candidate in candidates {
        if candidate.exists() {
            fs::remove_file(&candidate)
                .with_context(|| format!("failed to remove {}", candidate.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::NewJob;

    #[test]
    fn runtime_metadata_does_not_persist_environment_overrides() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("data");
        let paths = AppPaths {
            config: root.join("config.toml"),
            sessions: root.join("sessions"),
            state_db: root.join("state.sqlite"),
            lock_file: root.join("tgdl.lock"),
            root,
        };
        let stored = Config {
            api_id: 1,
            api_hash: "stored-hash".to_owned(),
            channel: "drop_box".to_owned(),
            sources: vec![SourceConfig {
                title: "Drop box".to_owned(),
                access: SourceAccess::Public,
                peer_kind: crate::config::SourcePeerKind::Channel,
                username: Some("drop_box".to_owned()),
                peer_id: Some(-1_000_000_000_123),
            }],
            media_filter: MediaFilter::default(),
            output_dir: temp.path().join("downloads"),
            proxy_url: Some("socks5://stored.example:1080".to_owned()),
            monitor_primary: "main".to_owned(),
            monitor_account_downloads: false,
            max_active_files: 2,
            max_download_lanes_per_account: 8,
            verify_file_hashes: false,
            accounts: vec![AccountConfig {
                name: "main".to_owned(),
                enabled: true,
                monitor_priority: 1,
                user_id: None,
                display_name: None,
                premium: false,
                premium_checked_at: None,
                proxy_url: None,
            }],
        };
        stored.save(&paths.config)?;

        let mut runtime = stored.clone();
        runtime.api_id = 999;
        runtime.api_hash = "temporary-hash".to_owned();
        runtime.proxy_url = None;
        runtime.sources[0].title = "Resolved source".to_owned();
        runtime.accounts[0].user_id = Some(42);
        runtime.accounts[0].display_name = Some("Telegram User".to_owned());
        runtime.accounts[0].premium = true;
        runtime.accounts[0].premium_checked_at = Some(Utc::now());
        persist_runtime_metadata(&paths, &runtime)?;

        let saved = Config::load_stored(&paths.config)?;
        assert_eq!(saved.api_id, 1);
        assert_eq!(saved.api_hash, "stored-hash");
        assert_eq!(
            saved.proxy_url.as_deref(),
            Some("socks5://stored.example:1080")
        );
        assert_eq!(saved.sources[0].title, "Resolved source");
        assert_eq!(saved.accounts[0].user_id, Some(42));
        assert!(saved.accounts[0].premium);
        Ok(())
    }

    #[test]
    fn parses_optional_tdata_account_indexes() -> Result<()> {
        assert_eq!(parse_optional_usize("")?, None);
        assert_eq!(parse_optional_usize(" 2 ")?, Some(2));
        assert!(parse_optional_usize("-1").is_err());
        assert!(parse_optional_usize("second").is_err());
        Ok(())
    }

    #[test]
    fn parses_human_file_sizes_and_extension_lists() -> Result<()> {
        assert_eq!(parse_file_size("512")?, 512);
        assert_eq!(parse_file_size("1 MB")?, 1024 * 1024);
        assert_eq!(parse_file_size("1.5 GiB")?, 3 * 1024 * 1024 * 1024 / 2);
        assert!(parse_file_size("large").is_err());
        assert_eq!(
            parse_extensions("*.RAR, zip tar.gz rar")?,
            vec!["rar", "zip", "tar.gz"]
        );
        assert!(parse_extensions(" , ").is_err());
        Ok(())
    }

    #[test]
    fn date_only_history_ranges_cover_the_entire_local_day() -> Result<()> {
        assert!(parse_history_range_text("", "2026-07-30").is_err());
        assert!(parse_history_range_text("2026-07-30", "").is_err());
        assert!(parse_history_range_text("invalid", "2026-07-30").is_err());
        let start = parse_history_time("2026-07-30", false)?;
        let end = parse_history_time("2026-07-30", true)?;
        assert_eq!((end - start).num_seconds(), 86_399);
        assert_eq!(
            start.with_timezone(&Local).format("%H:%M:%S").to_string(),
            "00:00:00"
        );
        assert_eq!(
            end.with_timezone(&Local).format("%H:%M:%S").to_string(),
            "23:59:59"
        );
        Ok(())
    }

    #[test]
    fn active_download_card_wraps_every_long_field_into_bounded_rows() {
        for total in [0, 1024 * 1024] {
            let download = ActiveDownload::new(
                "premium".to_owned(),
                "SOCKS5".to_owned(),
                "a very long archive filename that needs to be shortened.rar".to_owned(),
                total,
                0,
            );
            let lines = active_download_lines(&download, 60);
            assert!(lines.len() >= 3);
            assert!(lines[0].contains('●'));
            assert!(
                lines
                    .iter()
                    .all(|line| ::console::measure_text_width(line) <= 60)
            );
        }
    }

    #[test]
    fn monitor_dashboard_has_fixed_professional_sections() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let mut view = DownloadView::for_test(&store)?;
        view.downloads.insert(
            1,
            ActiveDownload::new(
                "premium".to_owned(),
                "SOCKS5".to_owned(),
                "archive.rar".to_owned(),
                1024 * 1024,
                256 * 1024,
            ),
        );
        view.append_event(LiveEventLevel::Warning, "Temporary Telegram wait");

        let lines = view.dashboard_lines(24, 100);
        assert_eq!(lines.len(), 18);
        assert!(lines[0].contains("LIVE SESSION"));
        assert!(lines[1].contains("QUEUE"));
        assert!(!lines[1].contains("LOGS"));
        assert!(lines.iter().any(|line| line.contains("ACTIVE DOWNLOADS")));
        assert!(lines.iter().any(|line| line.contains("ACTIVITY LOG")));
        assert!(lines.iter().any(|line| line.contains("WARN")));
        assert!(
            lines
                .iter()
                .all(|line| ::console::measure_text_width(line) == 100)
        );

        for index in 0..20 {
            view.append_event(LiveEventLevel::Info, format!("Extra event {index}"));
        }
        let narrow_lines = view.dashboard_lines(24, 60);
        assert_eq!(narrow_lines.len(), 18);
        for line in &narrow_lines {
            assert!(
                ::console::measure_text_width(line) == 60,
                "line is not fixed-width: {} columns: {line}",
                ::console::measure_text_width(line)
            );
        }
        Ok(())
    }

    #[test]
    fn inactive_downloads_are_removed_from_the_live_view() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let mut view = DownloadView::for_test(&store)?;
        view.downloads.insert(
            7,
            ActiveDownload::new(
                "account".to_owned(),
                "DIRECT".to_owned(),
                "archive.zip".to_owned(),
                10,
                10,
            ),
        );
        view.render_event(&store, SchedulerEvent::Inactive { job_id: 7 })?;

        assert!(view.downloads.is_empty());
        assert_eq!(view.stats.active, 0);
        Ok(())
    }

    #[test]
    fn history_dashboard_stays_fixed_width_with_long_ranges_and_warnings() {
        let view = HistoryView {
            term: None,
            rendered_size: None,
            rendered_lines: Vec::new(),
            account: "monitor-account".to_owned(),
            source_count: 3,
            range: "2026-01-01 00:00:00 – 2026-07-30 23:59:59".to_owned(),
            progress: Some(HistoryProgress {
                account: "monitor-account".to_owned(),
                source: "@a_very_long_source_name_for_history".to_owned(),
                source_index: 2,
                source_count: 3,
                scanned: 12_345,
                media: 1_000,
                queued: 800,
                duplicates: 100,
                filtered: 50,
                existing: 50,
            }),
            warnings: vec![
                "A long warning that must wrap without scrolling or damaging the page frame"
                    .to_owned(),
            ],
        };
        for width in [60, 100] {
            let lines = view.dashboard_lines(24, width);
            assert_eq!(lines.len(), 18);
            assert!(lines.iter().any(|line| line.contains("HISTORY SESSION")));
            assert!(lines.iter().any(|line| line.contains("SCAN PROGRESS")));
            assert!(
                lines
                    .iter()
                    .all(|line| ::console::measure_text_width(line) == width)
            );
        }
    }

    #[test]
    fn live_view_counts_and_appends_every_event() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let mut view = DownloadView::for_test(&store)?;

        view.render_event(
            &store,
            SchedulerEvent::Duplicate {
                filename: "archive.zip".to_owned(),
                path: "D:/Downloads/archive.zip".to_owned(),
            },
        )?;
        assert_eq!(view.stats.duplicates, 1);

        view.render_event(
            &store,
            SchedulerEvent::Warning("Connection is unstable".to_owned()),
        )?;
        assert_eq!(view.stats.warnings, 1);

        view.render_event(
            &store,
            SchedulerEvent::Monitoring {
                account: "backup".to_owned(),
                sources: 3,
            },
        )?;
        assert_eq!(view.logs.len(), 2);
        assert!(view.logs[0].message.contains("Already downloaded"));
        assert!(view.logs[1].message.contains("Connection is unstable"));
        let monitor = view.monitor_status.as_ref().expect("monitor status");
        assert_eq!(monitor.account, "backup");
        assert_eq!(monitor.sources, Some(3));
        let dashboard = view.dashboard_lines(30, 100);
        assert!(dashboard[1].contains("MON"));
        assert!(dashboard[1].contains("backup"));
        assert!(dashboard[1].contains("SRC"));
        assert!(!dashboard[1].contains("backup ·"));
        assert!(dashboard.iter().all(|line| !line.contains("Watching with")));

        view.render_event(
            &store,
            SchedulerEvent::Monitoring {
                account: "backup".to_owned(),
                sources: 1,
            },
        )?;
        let singular = view.dashboard_lines(30, 100);
        assert!(singular[1].contains("SRC 1"));
        assert!(!singular[1].contains("SOURCE"));
        Ok(())
    }

    #[test]
    fn event_text_is_single_line_without_losing_content() {
        let message =
            compact_event_text("  a long\n technical   message that should remain complete  ");
        assert_eq!(
            message,
            "a long technical message that should remain complete"
        );
        assert!(!message.contains('\n'));
    }

    #[test]
    fn long_log_messages_wrap_to_a_second_aligned_row() {
        let entry = LogEntry {
            timestamp: "19:25:33".to_owned(),
            level: LiveEventLevel::Error,
            message:
                "Failed: test3.rar · request error: rpc error -503: Timeout caused by Telegram"
                    .to_owned(),
        };
        let lines = log_entry_lines(&entry, 60);

        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("Failed: test3.rar"));
        assert!(lines[1].contains("Timeout caused by Telegram"));
        assert!(lines.iter().all(|line| !line.contains('…')));
        assert!(
            lines
                .iter()
                .all(|line| ::console::measure_text_width(line) <= 60)
        );
    }

    #[test]
    fn log_separator_is_removed_only_when_it_lands_on_a_wrap_boundary() {
        let entry = LogEntry {
            timestamp: "21:10:33".to_owned(),
            level: LiveEventLevel::Info,
            message: "Already downloaded: example-archive.rar · C:\\Users\\Example\\Downloads\\example-archive.rar".to_owned(),
        };

        let wrapped = log_entry_lines(&entry, 96);
        assert!(wrapped.len() > 1);
        assert!(wrapped.iter().all(|line| !line.contains('·')));
        assert!(wrapped[1].contains("C:\\Users\\Example"));

        let single_line = log_entry_lines(&entry, 200);
        assert_eq!(single_line.len(), 1);
        assert!(single_line[0].contains('·'));
    }

    #[test]
    fn wrapped_log_rows_are_included_in_tail_capacity() {
        let short = |message: &str| LogEntry {
            timestamp: "19:25:33".to_owned(),
            level: LiveEventLevel::Info,
            message: message.to_owned(),
        };
        let logs = vec![
            short("First"),
            short("A long activity message that requires a continuation row in a narrow terminal"),
            short("Last"),
        ];

        let rows = wrap_log_entries(&logs, 50);
        let total = rows.len();
        let tail = total.saturating_sub(3);
        assert_eq!(log_tail_offset(total, 3), tail);
        let end = (tail + 3).min(total);
        let lines = &rows[tail..end];
        assert_eq!(lines.len(), 3);
        assert_eq!(end, total);
    }

    #[test]
    fn live_events_are_never_expired_or_replaced() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let mut view = DownloadView::for_test(&store)?;

        view.append_event(LiveEventLevel::Info, "First notice");
        view.append_event(LiveEventLevel::Warning, "Second notice");
        view.append_event(LiveEventLevel::Error, "Third notice");
        view.refresh_log_view()?;

        assert_eq!(view.logs.len(), 3);
        assert!(view.logs[0].message.contains("First notice"));
        assert!(view.logs[1].message.contains("Second notice"));
        assert!(view.logs[2].message.contains("Third notice"));
        Ok(())
    }

    #[test]
    fn monitor_log_scrolling_pauses_and_resumes_tail_following() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let mut view = DownloadView::for_test(&store)?;
        let event_count = view.current_log_layout().0 + 5;
        for index in 0..event_count {
            view.append_event(LiveEventLevel::Info, format!("Event {index}"));
        }
        view.refresh_log_view()?;

        assert!(view.log_offset > 0);
        assert!(view.follow_log_tail);
        view.scroll_logs(LogScroll::Home)?;
        assert_eq!(view.log_offset, 0);
        assert!(!view.follow_log_tail);

        view.append_event(LiveEventLevel::Info, "While browsing history");
        view.refresh_log_view()?;
        assert_eq!(view.log_offset, 0);
        view.scroll_logs(LogScroll::End)?;
        assert!(view.follow_log_tail);
        let previous_offset = view.log_offset;
        view.append_event(LiveEventLevel::Info, "Follow newest");
        view.refresh_log_view()?;
        assert!(view.log_offset > previous_offset);
        Ok(())
    }

    #[test]
    fn stats_line_contains_the_requested_counters() {
        let stats = DownloadStats::default().line();
        for label in ["QUEUE", "ACTIVE", "DONE", "DUP", "WARN", "ERR"] {
            assert!(stats.contains(label));
        }
        assert!(!stats.contains("LOGS"));
    }

    #[test]
    fn live_stats_do_not_load_historical_done_or_error_counts() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        for (message_id, filename) in [(1, "done.bin"), (2, "failed.bin")] {
            let job = store
                .insert_job(&NewJob {
                    channel_id: -100_123,
                    message_id,
                    remote_key: Some(format!("document:{message_id}")),
                    filename: filename.to_owned(),
                    media_kind: "document".to_owned(),
                    final_path: temp.path().join(filename),
                    part_path: temp.path().join(format!("{filename}.part")),
                    total_size: Some(1024),
                    chunk_size: 1024,
                })?
                .expect("unique test job");
            if message_id == 1 {
                store.mark_completed(job.id)?;
            } else {
                store.mark_failed(job.id, "test failure")?;
            }
        }

        let mut stats = DownloadStats::default();
        stats.refresh_jobs(&store, 0)?;
        assert_eq!(stats.completed, 0);
        assert_eq!(stats.errors, 0);

        stats.completed = 2;
        stats.errors = 1;
        stats.refresh_jobs(&store, 0)?;
        assert_eq!(stats.completed, 2);
        assert_eq!(stats.errors, 1);
        Ok(())
    }
}
