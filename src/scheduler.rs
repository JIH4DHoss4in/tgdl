use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::account::{AccountRuntime, SourceAccessError, contains_revoked_authorization_error};
use crate::config::{AccountConfig, Config, SourceAccess};
use crate::download::{DownloadOptions, DownloadOutcome, DownloadProgress, download_job};
use crate::media::descriptor_from_message;
use crate::monitor::MonitorEvent;
#[cfg(test)]
use crate::store::AccountState;
use crate::store::{Job, Store};

#[derive(Clone, Debug)]
pub enum SchedulerEvent {
    Monitoring {
        account: String,
        sources: usize,
    },
    Assigned {
        job_id: i64,
        account: String,
        filename: String,
        route: String,
    },
    Progress(DownloadProgress),
    Inactive {
        job_id: i64,
    },
    Completed {
        job_id: i64,
        filename: String,
    },
    Duplicate {
        filename: String,
        path: String,
    },
    Cooldown {
        job_id: i64,
        account: String,
        seconds: u32,
        reason: String,
    },
    Failed {
        job_id: i64,
        error: String,
    },
    Warning(String),
    Notice(String),
}

type DownloadTaskResult = (Job, String, std::result::Result<DownloadOutcome, String>);

struct DownloadChannels<'a> {
    progress: &'a mpsc::UnboundedSender<DownloadProgress>,
    shutdown: &'a watch::Receiver<bool>,
    events: &'a mpsc::UnboundedSender<SchedulerEvent>,
}

#[derive(Default)]
struct SchedulerState {
    busy_accounts: HashSet<String>,
    active_jobs: HashSet<i64>,
    unavailable_accounts: HashSet<String>,
    inaccessible_accounts: HashMap<i64, HashSet<String>>,
    retry_after: HashMap<i64, tokio::time::Instant>,
    retry_count: HashMap<i64, u8>,
}

impl SchedulerState {
    fn finish(&mut self, job_id: i64, account: &str) {
        self.busy_accounts.remove(account);
        self.active_jobs.remove(&job_id);
    }

    fn clear_retry_state(&mut self, job_id: i64) {
        self.retry_after.remove(&job_id);
        self.retry_count.remove(&job_id);
        self.inaccessible_accounts.remove(&job_id);
    }

    fn retry_delay(&mut self, job_id: i64) -> Option<Duration> {
        let count = self.retry_count.entry(job_id).or_default();
        *count = count.saturating_add(1);
        (*count < 5).then(|| Duration::from_secs(1_u64 << (*count).min(6)))
    }

    fn defer(&mut self, job_id: i64, delay: Duration) {
        self.retry_after
            .insert(job_id, tokio::time::Instant::now() + delay);
    }
}

pub async fn run_scheduler(
    accounts: Vec<Arc<AccountRuntime>>,
    config: Arc<Config>,
    store: Store,
    mut monitor_rx: mpsc::UnboundedReceiver<MonitorEvent>,
    event_tx: mpsc::UnboundedSender<SchedulerEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    store.reset_interrupted_jobs()?;
    let runtimes: HashMap<String, Arc<AccountRuntime>> = accounts
        .into_iter()
        .map(|account| (account.name.clone(), account))
        .collect();
    let connected_accounts = runtimes.keys().cloned().collect();
    let mut released = 0;
    for job in store.runnable_jobs()? {
        let Some(account) = job.assigned_account.as_deref() else {
            continue;
        };
        if !assignment_account_usable(
            &config.accounts,
            &connected_accounts,
            &config.monitor_primary,
            config.monitor_account_downloads,
            account,
        ) {
            store.release_job(job.id, "assigned account is unavailable")?;
            released += 1;
        }
    }
    if released > 0 {
        let _ = event_tx.send(SchedulerEvent::Notice(format!(
            "Released {released} job(s) from unavailable accounts"
        )));
    }
    if !config.monitor_account_downloads
        && runtimes.keys().all(|name| name == &config.monitor_primary)
    {
        let _ = event_tx.send(SchedulerEvent::Warning(
            "No download account is available".to_owned(),
        ));
    }
    let mut state = SchedulerState::default();
    let mut tasks = JoinSet::new();
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let mut premium_refresh = tokio::time::interval(Duration::from_secs(6 * 60 * 60));
    premium_refresh.tick().await;
    let mut slot_refresh = tokio::time::interval(Duration::from_secs(1));
    slot_refresh.tick().await;
    let mut refill_slots = true;

    loop {
        if refill_slots {
            let channels = DownloadChannels {
                progress: &progress_tx,
                shutdown: &shutdown,
                events: &event_tx,
            };
            fill_download_slots(
                &runtimes, &config, &store, &mut state, &mut tasks, &channels,
            )
            .await?;
            refill_slots = false;
        }

        if *shutdown.borrow() && tasks.is_empty() {
            return Ok(());
        }

        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
            Some(progress) = progress_rx.recv() => {
                let _ = event_tx.send(SchedulerEvent::Progress(progress));
            }
            Some(monitor) = monitor_rx.recv() => {
                match monitor {
                    MonitorEvent::Monitoring { account, sources } => {
                        let _ = event_tx.send(SchedulerEvent::Monitoring { account, sources });
                    }
                    MonitorEvent::JobQueued(id) => {
                        refill_slots = true;
                        let _ = event_tx.send(SchedulerEvent::Notice(format!("Queued job #{id}")));
                    }
                    MonitorEvent::DuplicateSkipped { filename, path } => {
                        let _ = event_tx.send(SchedulerEvent::Duplicate { filename, path });
                    }
                    MonitorEvent::Warning(warning) => {
                        let _ = event_tx.send(SchedulerEvent::Warning(warning));
                    }
                }
            }
            _ = premium_refresh.tick() => {
                for account in runtimes.values() {
                    match account.refresh_premium().await {
                        Ok(premium) => {
                            let tier = if premium { "premium" } else { "normal" };
                            let _ = event_tx.send(SchedulerEvent::Notice(format!(
                                "{} is {tier}", account.name
                            )));
                        }
                        Err(_) => {
                            let _ = event_tx.send(SchedulerEvent::Warning(format!(
                                "Could not refresh {}", account.name
                            )));
                        }
                    }
                }
                refill_slots = true;
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let Some(joined) = joined else { continue };
                let (job, account_name, outcome) = joined.context("download task panicked")?;
                state.finish(job.id, &account_name);
                refill_slots = true;
                let _ = event_tx.send(SchedulerEvent::Inactive { job_id: job.id });
                match outcome {
                    Ok(DownloadOutcome::Completed) => {
                        store.mark_completed(job.id)?;
                        state.clear_retry_state(job.id);
                        let _ = event_tx.send(SchedulerEvent::Completed {
                            job_id: job.id,
                            filename: job.filename,
                        });
                    }
                    Ok(DownloadOutcome::Wait { kind, seconds }) => {
                        let until = Utc::now() + chrono::Duration::seconds(seconds as i64);
                        store.set_cooldown(&account_name, until, &kind)?;
                        let retry_same_account =
                            wait_retry_policy(&kind) == WaitRetryPolicy::SameAccount;
                        if retry_same_account {
                            store.retry_on_account(
                                job.id,
                                &account_name,
                                &format!("{kind} for {seconds} seconds"),
                            )?;
                            state.defer(job.id, Duration::from_secs(seconds as u64));
                        } else {
                            store.release_job(job.id, &format!("{kind} for {seconds} seconds"))?;
                        }
                        let _ = event_tx.send(SchedulerEvent::Cooldown {
                            job_id: job.id,
                            account: account_name,
                            seconds,
                            reason: kind,
                        });
                    }
                    Ok(DownloadOutcome::AuthorizationRevoked) => {
                        state.unavailable_accounts.insert(account_name.clone());
                        store.release_job(job.id, "account authorization was revoked")?;
                        let _ = event_tx.send(SchedulerEvent::Warning(format!(
                            "Sign in again: {account_name} · job #{} waiting", job.id
                        )));
                    }
                    Ok(DownloadOutcome::SourceUnavailable { source, error }) => {
                        state.inaccessible_accounts
                            .entry(job.id)
                            .or_default()
                            .insert(account_name.clone());
                        store.release_job(
                            job.id,
                            &format!("{account_name} cannot access {source}: {error}"),
                        )?;
                        let excluded = state.inaccessible_accounts
                            .get(&job.id)
                            .map(HashSet::len)
                            .unwrap_or_default();
                        let candidate_count = config
                            .accounts
                            .iter()
                            .filter(|account| {
                                account.enabled
                                    && (config.monitor_account_downloads
                                        || account.name != config.monitor_primary)
                                    && runtimes.contains_key(&account.name)
                                    && !state.unavailable_accounts.contains(&account.name)
                            })
                            .count();
                        let message = if excluded >= candidate_count {
                            format!(
                                "No account can access {source} · job #{} waiting", job.id
                            )
                        } else {
                            format!(
                                "{account_name} cannot access {source} · trying another account"
                            )
                        };
                        let _ = event_tx.send(SchedulerEvent::Warning(message));
                    }
                    Ok(DownloadOutcome::RefreshReference) => {
                        store.release_job(job.id, "refreshing expired file reference")?;
                        state.defer(job.id, Duration::from_secs(1));
                        let _ = event_tx.send(SchedulerEvent::Notice(format!(
                            "Refreshing job #{}", job.id
                        )));
                    }
                    Ok(DownloadOutcome::Migrate(dc)) => {
                        store.release_job(job.id, &format!("file migrated to DC {dc}"))?;
                        state.defer(job.id, Duration::from_secs(1));
                        let _ = event_tx.send(SchedulerEvent::Notice(format!(
                            "Moving job #{} to Telegram DC {dc}", job.id
                        )));
                    }
                    Ok(DownloadOutcome::Retry(error)) => {
                        retry_or_fail(&store, &mut state, &event_tx, job.id, error)?;
                    }
                    Err(error) if is_authorization_error(&error) => {
                        state.unavailable_accounts.insert(account_name.clone());
                        store.release_job(job.id, &error)?;
                        let _ = event_tx.send(SchedulerEvent::Warning(format!(
                            "Sign in again: {account_name} · job #{} waiting", job.id
                        )));
                    }
                    Err(error) => {
                        retry_or_fail(&store, &mut state, &event_tx, job.id, error)?;
                    }
                    Ok(DownloadOutcome::Failed(error)) => {
                        store.mark_failed(job.id, &error)?;
                        let _ = event_tx.send(SchedulerEvent::Failed { job_id: job.id, error });
                    }
                    Ok(DownloadOutcome::Interrupted) => {
                        store.mark_retrying(job.id, "interrupted during shutdown")?;
                    }
                }
            }
            _ = slot_refresh.tick() => {
                refill_slots = true;
            }
        }
    }
}

fn assignment_account_usable(
    accounts: &[AccountConfig],
    connected_accounts: &HashSet<String>,
    monitor_primary: &str,
    monitor_account_downloads: bool,
    name: &str,
) -> bool {
    accounts
        .iter()
        .find(|account| account.name == name)
        .is_some_and(|account| {
            account.enabled
                && connected_accounts.contains(name)
                && (monitor_account_downloads || name != monitor_primary)
        })
}

fn retry_or_fail(
    store: &Store,
    state: &mut SchedulerState,
    event_tx: &mpsc::UnboundedSender<SchedulerEvent>,
    job_id: i64,
    error: String,
) -> Result<()> {
    let error = if error.trim().is_empty() {
        "transient download error".to_owned()
    } else {
        error
    };
    if let Some(delay) = state.retry_delay(job_id) {
        store.mark_retrying(job_id, &error)?;
        state.defer(job_id, delay);
        let _ = event_tx.send(SchedulerEvent::Notice(format!(
            "Retrying job #{job_id} in {}s",
            delay.as_secs()
        )));
    } else {
        store.mark_failed(job_id, &error)?;
        let _ = event_tx.send(SchedulerEvent::Failed { job_id, error });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitRetryPolicy {
    SameAccount,
    Reassign,
}

fn wait_retry_policy(kind: &str) -> WaitRetryPolicy {
    if kind == "FLOOD_WAIT" {
        WaitRetryPolicy::SameAccount
    } else {
        WaitRetryPolicy::Reassign
    }
}

async fn fill_download_slots(
    runtimes: &HashMap<String, Arc<AccountRuntime>>,
    config: &Config,
    store: &Store,
    state: &mut SchedulerState,
    tasks: &mut JoinSet<DownloadTaskResult>,
    channels: &DownloadChannels<'_>,
) -> Result<()> {
    if *channels.shutdown.borrow() || tasks.len() >= config.max_active_files {
        return Ok(());
    }
    store.clear_expired_cooldowns()?;
    while tasks.len() < config.max_active_files {
        let now = tokio::time::Instant::now();
        let mut selected = None;
        for job in store.runnable_jobs()? {
            if state.active_jobs.contains(&job.id)
                || state
                    .retry_after
                    .get(&job.id)
                    .is_some_and(|deadline| *deadline > now)
            {
                continue;
            }
            let selector = RuntimeSelector {
                runtimes,
                config,
                store,
                busy: &state.busy_accounts,
                unavailable: &state.unavailable_accounts,
                inaccessible: state.inaccessible_accounts.get(&job.id),
                now: Utc::now(),
            };
            let account = match job.assigned_account.as_deref() {
                Some(preferred) => selector.named(preferred)?,
                None => selector.next()?,
            };
            if let Some(account) = account {
                selected = Some((job, account));
                break;
            }
        }
        let Some((job, account)) = selected else {
            break;
        };
        store.assign_job(job.id, &account.name)?;
        state.busy_accounts.insert(account.name.clone());
        state.active_jobs.insert(job.id);
        let _ = channels.events.send(SchedulerEvent::Assigned {
            job_id: job.id,
            account: account.name.clone(),
            filename: job.filename.clone(),
            route: crate::proxy::proxy_label(account.proxy_url.as_deref()).to_owned(),
        });
        let store = store.clone();
        let config = config.clone();
        let progress_tx = channels.progress.clone();
        let shutdown = channels.shutdown.clone();
        let account_name = account.name.clone();
        tasks.spawn(async move {
            let outcome = run_assigned_job(&store, &job, &account, &config, progress_tx, shutdown)
                .await
                .map_err(|error| format!("{error:#}"));
            (job, account_name, outcome)
        });
    }
    Ok(())
}

struct RuntimeSelector<'a> {
    runtimes: &'a HashMap<String, Arc<AccountRuntime>>,
    config: &'a Config,
    store: &'a Store,
    busy: &'a HashSet<String>,
    unavailable: &'a HashSet<String>,
    inaccessible: Option<&'a HashSet<String>>,
    now: DateTime<Utc>,
}

impl RuntimeSelector<'_> {
    fn named(&self, name: &str) -> Result<Option<Arc<AccountRuntime>>> {
        let Some(account) = self.config.account(name) else {
            return Ok(None);
        };
        if !self.available(account) {
            return Ok(None);
        }
        let Some(runtime) = self.runtimes.get(name) else {
            return Ok(None);
        };
        let state = self.store.account_state(name)?;
        if state.cooldown_until.is_some_and(|until| until > self.now) {
            return Ok(None);
        }
        Ok(Some(runtime.clone()))
    }

    fn next(&self) -> Result<Option<Arc<AccountRuntime>>> {
        let mut eligible = Vec::new();
        for account in &self.config.accounts {
            if !self.available(account) {
                continue;
            }
            let Some(runtime) = self.runtimes.get(&account.name) else {
                continue;
            };
            let state = self.store.account_state(&account.name)?;
            if state.cooldown_until.is_some_and(|until| until > self.now) {
                continue;
            }
            eligible.push((runtime.clone(), state));
        }
        eligible.sort_by_key(|(runtime, state)| {
            (
                !runtime.is_premium(),
                state.last_assigned_at,
                runtime.name.clone(),
            )
        });
        Ok(eligible.into_iter().next().map(|(runtime, _)| runtime))
    }

    fn available(&self, account: &AccountConfig) -> bool {
        (self.config.monitor_account_downloads || account.name != self.config.monitor_primary)
            && account.enabled
            && !self.busy.contains(&account.name)
            && !self.unavailable.contains(&account.name)
            && self
                .inaccessible
                .is_none_or(|accounts| !accounts.contains(&account.name))
    }
}

async fn run_assigned_job(
    store: &Store,
    job: &Job,
    account: &AccountRuntime,
    config: &Config,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    shutdown: watch::Receiver<bool>,
) -> Result<DownloadOutcome> {
    let source = config
        .source_for_peer(job.channel_id)
        .map(|source| source.display_name())
        .unwrap_or_else(|| format!("peer {}", job.channel_id));
    let private_source = config
        .source_for_peer(job.channel_id)
        .is_some_and(|source| source.access == SourceAccess::Private);
    let resolved = match account.resolve_job_source(config, job.channel_id).await {
        Ok(resolved) => resolved,
        Err(error) if error.downcast_ref::<SourceAccessError>().is_some() => {
            return Ok(DownloadOutcome::SourceUnavailable {
                source,
                error: error.to_string(),
            });
        }
        Err(error) => return Err(error),
    };
    let messages = match account
        .client
        .get_messages_by_id(resolved.peer, &[job.message_id])
        .await
    {
        Ok(messages) => messages,
        Err(error) if is_source_access_error(&error.to_string()) => {
            return Ok(DownloadOutcome::SourceUnavailable {
                source,
                error: error.to_string(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let message = messages.into_iter().next().flatten();
    let Some(message) = message else {
        if private_source {
            return Ok(DownloadOutcome::SourceUnavailable {
                source,
                error: "the message is not accessible with this account".to_owned(),
            });
        }
        return Ok(DownloadOutcome::Failed(
            "the source message was deleted or is no longer accessible".to_owned(),
        ));
    };
    let Some(descriptor) = descriptor_from_message(&message) else {
        return Ok(DownloadOutcome::Failed(
            "the source message no longer contains downloadable media".to_owned(),
        ));
    };
    if let (Some(expected), Some(actual)) = (job.total_size, descriptor.total_size)
        && expected != actual
    {
        return Ok(DownloadOutcome::Failed(format!(
            "media size changed from {expected} to {actual} bytes"
        )));
    }
    download_job(
        store,
        job,
        account,
        &descriptor,
        DownloadOptions {
            max_lanes: config.max_download_lanes_per_account,
            verify_file_hashes: config.verify_file_hashes,
        },
        progress_tx,
        shutdown,
    )
    .await
}

fn is_authorization_error(error: &str) -> bool {
    contains_revoked_authorization_error(error)
}

fn is_source_access_error(error: &str) -> bool {
    [
        "CHANNEL_PRIVATE",
        "CHANNEL_INVALID",
        "USER_BANNED_IN_CHANNEL",
        "CHAT_ID_INVALID",
        "PEER_ID_INVALID",
    ]
    .iter()
    .any(|name| error.contains(name))
}

#[cfg(test)]
fn select_account_for_test<'a>(
    accounts: &'a [AccountConfig],
    states: &'a HashMap<String, AccountState>,
    busy: &HashSet<String>,
    inaccessible: &HashSet<String>,
    now: DateTime<Utc>,
    monitor_primary: &str,
    monitor_account_downloads: bool,
) -> Option<&'a AccountConfig> {
    let mut eligible: Vec<_> = accounts
        .iter()
        .filter(|account| {
            account.enabled
                && (monitor_account_downloads || account.name != monitor_primary)
                && !busy.contains(&account.name)
                && !inaccessible.contains(&account.name)
        })
        .filter(|account| {
            states
                .get(&account.name)
                .and_then(|state| state.cooldown_until)
                .is_none_or(|until| until <= now)
        })
        .collect();
    eligible.sort_by_key(|account| {
        let state = states.get(&account.name);
        (
            !account.premium,
            state.and_then(|state| state.last_assigned_at),
            account.name.clone(),
        )
    });
    eligible.into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(name: &str, premium: bool) -> AccountConfig {
        AccountConfig {
            name: name.into(),
            enabled: true,
            monitor_priority: 0,
            user_id: None,
            display_name: None,
            premium,
            premium_checked_at: None,
            proxy_url: None,
        }
    }

    #[test]
    fn selects_premium_then_least_recently_used() {
        let now = Utc::now();
        let accounts = vec![
            account("normal", false),
            account("new", true),
            account("old", true),
        ];
        let states = HashMap::from([
            (
                "new".into(),
                AccountState {
                    last_assigned_at: Some(now),
                    ..Default::default()
                },
            ),
            (
                "old".into(),
                AccountState {
                    last_assigned_at: Some(now - chrono::Duration::hours(1)),
                    ..Default::default()
                },
            ),
        ]);
        let selected = select_account_for_test(
            &accounts,
            &states,
            &HashSet::new(),
            &HashSet::new(),
            now,
            "monitor",
            false,
        )
        .unwrap();
        assert_eq!(selected.name, "old");
    }

    #[test]
    fn skips_cooldown_and_busy_accounts() {
        let now = Utc::now();
        let accounts = vec![account("premium", true), account("normal", false)];
        let states = HashMap::from([(
            "premium".into(),
            AccountState {
                cooldown_until: Some(now + chrono::Duration::minutes(1)),
                ..Default::default()
            },
        )]);
        let selected = select_account_for_test(
            &accounts,
            &states,
            &HashSet::new(),
            &HashSet::new(),
            now,
            "monitor",
            false,
        )
        .unwrap();
        assert_eq!(selected.name, "normal");

        let busy = HashSet::from(["premium".to_owned(), "normal".to_owned()]);
        assert!(
            select_account_for_test(
                &accounts,
                &states,
                &busy,
                &HashSet::new(),
                now,
                "monitor",
                false,
            )
            .is_none()
        );
    }

    #[test]
    fn never_selects_the_primary_monitor_for_downloads() {
        let now = Utc::now();
        let accounts = vec![account("watcher", true), account("premium", true)];
        let states = HashMap::new();
        let busy = HashSet::new();
        let selected = select_account_for_test(
            &accounts,
            &states,
            &busy,
            &HashSet::new(),
            now,
            "watcher",
            false,
        )
        .unwrap();
        assert_eq!(selected.name, "premium");
    }

    #[test]
    fn primary_monitor_is_eligible_when_enabled_for_downloads() {
        let now = Utc::now();
        let accounts = vec![account("watcher", true)];
        let states = HashMap::new();
        let selected = select_account_for_test(
            &accounts,
            &states,
            &HashSet::new(),
            &HashSet::new(),
            now,
            "watcher",
            true,
        )
        .expect("monitor should be eligible");
        assert_eq!(selected.name, "watcher");
    }

    #[test]
    fn skips_an_account_that_cannot_access_the_job_source() {
        let now = Utc::now();
        let accounts = vec![account("premium", true), account("backup", false)];
        let states = HashMap::new();
        let inaccessible = HashSet::from(["premium".to_owned()]);
        let selected = select_account_for_test(
            &accounts,
            &states,
            &HashSet::new(),
            &inaccessible,
            now,
            "watcher",
            false,
        )
        .expect("backup should receive the released job");
        assert_eq!(selected.name, "backup");
    }

    #[test]
    fn generic_flood_wait_keeps_account_affinity() {
        assert_eq!(
            wait_retry_policy("FLOOD_WAIT"),
            WaitRetryPolicy::SameAccount
        );
        assert_eq!(
            wait_retry_policy("FLOOD_PREMIUM_WAIT"),
            WaitRetryPolicy::Reassign
        );
    }

    #[test]
    fn pinned_jobs_release_accounts_that_are_disabled_or_disconnected() {
        let mut accounts = vec![account("watcher", false), account("download", true)];
        let connected = HashSet::from(["watcher".to_owned(), "download".to_owned()]);
        assert!(assignment_account_usable(
            &accounts, &connected, "watcher", false, "download",
        ));
        assert!(!assignment_account_usable(
            &accounts, &connected, "watcher", false, "watcher",
        ));

        accounts[1].enabled = false;
        assert!(!assignment_account_usable(
            &accounts, &connected, "watcher", false, "download",
        ));
        assert!(!assignment_account_usable(
            &accounts,
            &HashSet::new(),
            "watcher",
            true,
            "download",
        ));
    }
}
