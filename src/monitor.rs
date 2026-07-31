use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use grammers_session::types::{PeerKind, PeerRef};
use grammers_session::updates::UpdatesLike;
use grammers_tl_types as tl;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::account::{AccountRuntime, contains_revoked_authorization_error};
use crate::config::Config;
use crate::media::{MessageReservation, descriptor_from_message, reserve_message};
use crate::store::Store;

#[derive(Clone, Debug)]
pub enum MonitorEvent {
    Monitoring { account: String, sources: usize },
    JobQueued(i64),
    DuplicateSkipped { filename: String, path: String },
    Warning(String),
}

#[derive(Clone, Copy, Debug)]
struct SourceCheckpoint {
    baseline_id: i32,
    cursor_id: i32,
    pts: Option<i32>,
}

#[derive(Clone, Debug)]
struct ActiveSource {
    config_index: usize,
    peer: PeerRef,
    label: String,
    next_poll: Instant,
}

pub async fn run_monitor(
    accounts: Vec<Arc<AccountRuntime>>,
    config: Arc<Config>,
    store: Store,
    event_tx: mpsc::UnboundedSender<MonitorEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let ordered = monitor_order(&accounts, &config);
    if ordered.is_empty() {
        bail!("no enabled account is available to monitor the configured sources");
    }

    // Checkpoints are indexed by configuration order and deliberately survive
    // account failover. They are in-memory so messages posted while tgdl is
    // stopped remain outside the no-history baseline.
    let mut checkpoints = vec![None; config.sources.len()];
    for account in ordered {
        if *shutdown.borrow() {
            return Ok(());
        }

        let mut active_sources = Vec::with_capacity(config.sources.len());
        let mut setup_failed = false;
        for (config_index, source) in config.sources.iter().enumerate() {
            let resolved = match account.resolve_source(source).await {
                Ok(resolved) => resolved,
                Err(_) => {
                    let _ = event_tx.send(MonitorEvent::Warning(format!(
                        "{} cannot open {}",
                        account.name,
                        source.display_name()
                    )));
                    setup_failed = true;
                    break;
                }
            };
            let peer = resolved.peer;
            if checkpoints[config_index].is_none() {
                let pts = if peer.id.kind() == PeerKind::Channel {
                    match channel_pts(&account, peer).await {
                        Ok(pts) => Some(pts),
                        Err(_) => {
                            let _ = event_tx.send(MonitorEvent::Warning(format!(
                                "{} cannot watch {}",
                                account.name,
                                source.display_name()
                            )));
                            setup_failed = true;
                            break;
                        }
                    }
                } else {
                    None
                };
                // For channels, capture PTS before the newest message ID. A
                // post racing startup is then either below the baseline or
                // present in the subsequent difference, never silently lost.
                let baseline_id = match latest_message_id(&account, peer).await {
                    Ok(message_id) => message_id,
                    Err(_) => {
                        let _ = event_tx.send(MonitorEvent::Warning(format!(
                            "{} cannot prepare {}",
                            account.name,
                            source.display_name()
                        )));
                        setup_failed = true;
                        break;
                    }
                };
                checkpoints[config_index] = Some(SourceCheckpoint {
                    baseline_id,
                    cursor_id: baseline_id,
                    pts,
                });
                if backfill_media_keys(&account, peer, &store).await.is_err() {
                    let _ = event_tx.send(MonitorEvent::Warning(format!(
                        "Could not index old media in {}",
                        source.display_name()
                    )));
                }
            }
            active_sources.push(ActiveSource {
                config_index,
                peer,
                label: source.display_name(),
                next_poll: Instant::now(),
            });
        }
        if setup_failed {
            continue;
        }

        let mut updates = match account.take_updates().await {
            Ok(updates) => updates,
            Err(_) => {
                let _ = event_tx.send(MonitorEvent::Warning(format!(
                    "{} could not start watching",
                    account.name
                )));
                continue;
            }
        };
        let _ = event_tx.send(MonitorEvent::Monitoring {
            account: account.name.clone(),
            sources: active_sources.len(),
        });
        let source_by_peer: HashMap<_, _> = active_sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| {
                source
                    .peer
                    .id
                    .bot_api_dialog_id()
                    .map(|peer_id| (peer_id, index))
            })
            .collect();
        let mut failure_times = vec![VecDeque::new(); config.sources.len()];
        let mut failover = false;

        loop {
            let next_poll = active_sources
                .iter()
                .map(|source| source.next_poll)
                .min()
                .unwrap_or_else(Instant::now);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                }
                maybe_update = updates.recv() => {
                    match maybe_update {
                        Some(UpdatesLike::ConnectionClosed | UpdatesLike::MalformedUpdates) => {
                            for source in &mut active_sources {
                                source.next_poll = Instant::now();
                            }
                        }
                        Some(update) => {
                            for (peer_id, message_id) in ids_from_updates_like(update) {
                                let Some(source) = source_by_peer
                                    .get(&peer_id)
                                    .and_then(|index| active_sources.get(*index))
                                else {
                                    continue;
                                };
                                let source_index = source.config_index;
                                let source_peer = source.peer;
                                let source_label = source.label.clone();
                                let checkpoint = checkpoints[source_index]
                                    .context("active source is missing its monitor checkpoint")?;
                                if message_id > checkpoint.baseline_id
                                    && queue_message(
                                        &account,
                                        source_peer,
                                        message_id,
                                        &config,
                                        &store,
                                        &event_tx,
                                    )
                                    .await
                                    .is_err()
                                {
                                    let failures =
                                        register_failure(&mut failure_times[source_index]);
                                    let _ = event_tx.send(MonitorEvent::Warning(format!(
                                        "{} could not read a new file from {} · retry {failures}/3",
                                        account.name, source_label
                                    )));
                                    if let Some(source) = active_sources.iter_mut().find(|candidate| {
                                        candidate.config_index == source_index
                                    }) {
                                        source.next_poll = Instant::now();
                                    }
                                    failover = failures >= 3;
                                }
                            }
                        }
                        None => {
                            failover = true;
                        }
                    }
                }
                _ = tokio::time::sleep_until(next_poll) => {
                    let now = Instant::now();
                    for source in active_sources
                        .iter_mut()
                        .filter(|source| source.next_poll <= now)
                    {
                        let checkpoint = checkpoints[source.config_index]
                            .as_mut()
                            .context("active source is missing its monitor checkpoint")?;
                        match poll_source(
                            &account,
                            source,
                            checkpoint,
                            &config,
                            &store,
                            &event_tx,
                        )
                        .await
                        {
                            Ok(delay) => {
                                failure_times[source.config_index].clear();
                                source.next_poll = Instant::now() + delay;
                            }
                            Err(error) => {
                                let failures =
                                    register_failure(&mut failure_times[source.config_index]);
                                if is_authorization_error(&error) {
                                    failover = true;
                                }
                                let _ = event_tx.send(MonitorEvent::Warning(format!(
                                    "{} lost {} · retry {failures}/3",
                                    account.name, source.label
                                )));
                                source.next_poll = Instant::now() + Duration::from_secs(2);
                                failover |= failures >= 3;
                            }
                        }
                        if failover {
                            break;
                        }
                    }
                }
            }
            if failover {
                break;
            }
        }
    }
    bail!(
        "all monitoring accounts failed; ensure a monitor account has access to every private source"
    )
}

async fn poll_source(
    account: &AccountRuntime,
    source: &ActiveSource,
    checkpoint: &mut SourceCheckpoint,
    config: &Config,
    store: &Store,
    event_tx: &mpsc::UnboundedSender<MonitorEvent>,
) -> Result<Duration> {
    if source.peer.id.kind() == PeerKind::Channel {
        let difference = channel_difference(
            account,
            source.peer,
            checkpoint
                .pts
                .context("channel source is missing its PTS checkpoint")?,
        )
        .await?;
        for message_id in difference.message_ids {
            if message_id > checkpoint.baseline_id {
                queue_message(account, source.peer, message_id, config, store, event_tx).await?;
            }
        }
        checkpoint.pts = Some(difference.pts);
        Ok(if difference.final_chunk {
            Duration::from_secs(difference.timeout.unwrap_or(1).max(1) as u64)
        } else {
            Duration::ZERO
        })
    } else {
        let message_ids = messages_since(account, source.peer, checkpoint.cursor_id).await?;
        for message_id in &message_ids {
            if *message_id > checkpoint.baseline_id {
                queue_message(account, source.peer, *message_id, config, store, event_tx).await?;
            }
        }
        if let Some(latest) = message_ids.last() {
            checkpoint.cursor_id = *latest;
        }
        Ok(Duration::from_secs(1))
    }
}

async fn backfill_media_keys(account: &AccountRuntime, peer: PeerRef, store: &Store) -> Result<()> {
    let peer_id = peer
        .id
        .bot_api_dialog_id()
        .context("monitored source has no persistent Telegram ID")?;
    let jobs = store.jobs_without_remote_key_for_source(peer_id)?;
    for batch in jobs.chunks(100) {
        let ids: Vec<_> = batch.iter().map(|job| job.message_id).collect();
        let messages = account.client.get_messages_by_id(peer, &ids).await?;
        for (job, message) in batch.iter().zip(messages) {
            let Some(descriptor) = message.as_ref().and_then(descriptor_from_message) else {
                continue;
            };
            store.set_remote_key(job.id, &descriptor.remote_key)?;
        }
    }
    Ok(())
}

fn monitor_order(accounts: &[Arc<AccountRuntime>], config: &Config) -> Vec<Arc<AccountRuntime>> {
    let mut result: Vec<_> = accounts
        .iter()
        .filter(|runtime| {
            config
                .account(&runtime.name)
                .is_some_and(|account| account.enabled)
        })
        .cloned()
        .collect();
    result.sort_by_key(|runtime| {
        let priority = config
            .account(&runtime.name)
            .map(|account| account.monitor_priority)
            .unwrap_or(u32::MAX);
        (
            runtime.name != config.monitor_primary,
            priority,
            runtime.name.clone(),
        )
    });
    result
}

async fn channel_pts(account: &AccountRuntime, peer: PeerRef) -> Result<i32> {
    let result = account
        .client
        .invoke(&tl::functions::channels::GetFullChannel {
            channel: peer.into(),
        })
        .await?;
    let tl::enums::messages::ChatFull::Full(result) = result;
    let full = match result.full_chat {
        tl::enums::ChatFull::ChannelFull(full) => full,
        _ => bail!("configured peer is not a channel or supergroup"),
    };
    Ok(full.pts)
}

async fn latest_message_id(account: &AccountRuntime, peer: PeerRef) -> Result<i32> {
    let mut messages = account.client.iter_messages(peer).limit(1);
    Ok(messages
        .next()
        .await?
        .map(|message| message.id())
        .unwrap_or(0))
}

async fn messages_since(
    account: &AccountRuntime,
    peer: PeerRef,
    cursor_id: i32,
) -> Result<Vec<i32>> {
    let mut messages = account
        .client
        .iter_messages(peer)
        .offset_id(cursor_id)
        .reverse(true);
    let mut ids = Vec::new();
    while let Some(message) = messages.next().await? {
        ids.push(message.id());
    }
    Ok(ids)
}

struct Difference {
    pts: i32,
    timeout: Option<i32>,
    message_ids: Vec<i32>,
    final_chunk: bool,
}

async fn channel_difference(
    account: &AccountRuntime,
    peer: PeerRef,
    pts: i32,
) -> Result<Difference> {
    let result = account
        .client
        .invoke(&tl::functions::updates::GetChannelDifference {
            force: false,
            channel: peer.into(),
            filter: tl::enums::ChannelMessagesFilter::Empty,
            pts,
            limit: 100,
        })
        .await?;
    match result {
        tl::enums::updates::ChannelDifference::Empty(difference) => Ok(Difference {
            pts: difference.pts,
            timeout: difference.timeout,
            message_ids: Vec::new(),
            final_chunk: difference.r#final,
        }),
        tl::enums::updates::ChannelDifference::Difference(difference) => {
            let mut message_ids: Vec<_> = difference
                .new_messages
                .into_iter()
                .filter_map(message_id)
                .collect();
            message_ids.extend(difference.other_updates.into_iter().filter_map(
                |update| match update {
                    tl::enums::Update::NewChannelMessage(update) => message_id(update.message),
                    _ => None,
                },
            ));
            Ok(Difference {
                pts: difference.pts,
                timeout: difference.timeout,
                message_ids,
                final_chunk: difference.r#final,
            })
        }
        tl::enums::updates::ChannelDifference::TooLong(difference) => {
            let next_pts = match difference.dialog {
                tl::enums::Dialog::Dialog(dialog) => dialog.pts.unwrap_or(pts),
                _ => pts,
            };
            Ok(Difference {
                pts: next_pts,
                timeout: difference.timeout,
                message_ids: difference
                    .messages
                    .into_iter()
                    .filter_map(message_id)
                    .collect(),
                final_chunk: difference.r#final,
            })
        }
    }
}

async fn queue_message(
    account: &AccountRuntime,
    peer: PeerRef,
    message_id: i32,
    config: &Config,
    store: &Store,
    event_tx: &mpsc::UnboundedSender<MonitorEvent>,
) -> Result<()> {
    let Some(message) = account
        .client
        .get_messages_by_id(peer, &[message_id])
        .await?
        .into_iter()
        .next()
        .flatten()
    else {
        return Ok(());
    };
    let peer_id = peer
        .id
        .bot_api_dialog_id()
        .context("monitored source has no persistent Telegram ID")?;
    match reserve_message(store, config, peer_id, &message)? {
        MessageReservation::Queued(job) => {
            let _ = event_tx.send(MonitorEvent::JobQueued(job.id));
        }
        MessageReservation::Duplicate(job) => {
            let _ = event_tx.send(MonitorEvent::DuplicateSkipped {
                filename: job.filename,
                path: job.final_path.display().to_string(),
            });
        }
        MessageReservation::NotMedia
        | MessageReservation::Filtered
        | MessageReservation::AlreadyTracked => {}
    }
    Ok(())
}

fn ids_from_updates_like(update: UpdatesLike) -> Vec<(i64, i32)> {
    let UpdatesLike::Updates(updates) = update else {
        return Vec::new();
    };
    let raw_updates = match updates {
        tl::enums::Updates::Updates(value) => value.updates,
        tl::enums::Updates::Combined(value) => value.updates,
        tl::enums::Updates::UpdateShort(value) => vec![value.update],
        _ => Vec::new(),
    };
    raw_updates
        .into_iter()
        .filter_map(|update| match update {
            tl::enums::Update::NewChannelMessage(update) => message_id_with_peer(update.message),
            tl::enums::Update::NewMessage(update) => message_id_with_peer(update.message),
            _ => None,
        })
        .collect()
}

fn message_id_with_peer(message: tl::enums::Message) -> Option<(i64, i32)> {
    let (id, peer) = match message {
        tl::enums::Message::Message(message) => (message.id, message.peer_id),
        tl::enums::Message::Service(message) => (message.id, message.peer_id),
        tl::enums::Message::Empty(_) => return None,
    };
    Some((peer_bot_id(peer)?, id))
}

fn peer_bot_id(peer: tl::enums::Peer) -> Option<i64> {
    let peer_id = match peer {
        tl::enums::Peer::Channel(channel) => {
            grammers_session::types::PeerId::channel(channel.channel_id)?
        }
        tl::enums::Peer::Chat(chat) => grammers_session::types::PeerId::chat(chat.chat_id)?,
        tl::enums::Peer::User(_) => return None,
    };
    peer_id.bot_api_dialog_id()
}

fn message_id(message: tl::enums::Message) -> Option<i32> {
    match message {
        tl::enums::Message::Message(message) => Some(message.id),
        tl::enums::Message::Service(message) => Some(message.id),
        tl::enums::Message::Empty(_) => None,
    }
}

fn is_authorization_error(error: &anyhow::Error) -> bool {
    contains_revoked_authorization_error(&format!("{error:#}"))
}

fn register_failure(failure_times: &mut VecDeque<Instant>) -> u8 {
    let now = Instant::now();
    failure_times.push_back(now);
    while failure_times
        .front()
        .is_some_and(|time| now.duration_since(*time) > Duration::from_secs(30))
    {
        failure_times.pop_front();
    }
    failure_times.len().min(u8::MAX as usize) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_empty_has_no_identifier() {
        assert_eq!(
            message_id(tl::enums::Message::Empty(tl::types::MessageEmpty {
                id: 1,
                peer_id: None,
            })),
            None
        );
    }

    #[test]
    fn update_peer_ids_distinguish_groups_from_channels() {
        let group = peer_bot_id(tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 123 }));
        let channel = peer_bot_id(tl::enums::Peer::Channel(tl::types::PeerChannel {
            channel_id: 123,
        }));
        assert_eq!(group, Some(-123));
        assert_eq!(channel, Some(-1_000_000_000_123));
    }
}
