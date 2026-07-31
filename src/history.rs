use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, watch};

use crate::account::AccountRuntime;
use crate::config::{Config, SourceConfig};
use crate::media::{MessageReservation, reserve_message};
use crate::store::Store;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoryRange {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

impl HistoryRange {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Self> {
        if start > end {
            bail!("history start time must not be after its end time");
        }
        telegram_timestamp(start)?;
        telegram_timestamp(end)?;
        Ok(Self {
            start: Some(start),
            end: Some(end),
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HistoryProgress {
    pub account: String,
    pub source: String,
    pub source_index: usize,
    pub source_count: usize,
    pub scanned: usize,
    pub media: usize,
    pub queued: usize,
    pub duplicates: usize,
    pub filtered: usize,
    pub existing: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryEvent {
    Progress(HistoryProgress),
    Warning(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HistorySummary {
    pub scanned: usize,
    pub media: usize,
    pub queued: usize,
    pub duplicates: usize,
    pub filtered: usize,
    pub existing: usize,
    pub failed_sources: usize,
    pub cancelled: bool,
}

pub async fn import_history(
    account: Arc<AccountRuntime>,
    config: Arc<Config>,
    sources: Vec<SourceConfig>,
    range: HistoryRange,
    store: Store,
    event_tx: mpsc::UnboundedSender<HistoryEvent>,
    shutdown: watch::Receiver<bool>,
) -> Result<HistorySummary> {
    let mut summary = HistorySummary::default();
    let source_count = sources.len();

    for (source_index, source) in sources.into_iter().enumerate() {
        if *shutdown.borrow() {
            summary.cancelled = true;
            break;
        }
        let label = source.display_name();
        let resolved = match account.resolve_source(&source).await {
            Ok(resolved) => resolved,
            Err(error) => {
                summary.failed_sources += 1;
                let _ = event_tx.send(HistoryEvent::Warning(format!(
                    "Could not open {label}: {error:#}"
                )));
                continue;
            }
        };
        let Some(peer_id) = resolved.peer.id.bot_api_dialog_id() else {
            summary.failed_sources += 1;
            let _ = event_tx.send(HistoryEvent::Warning(format!("Could not identify {label}")));
            continue;
        };
        let mut progress = HistoryProgress {
            account: account.name.clone(),
            source: label.clone(),
            source_index: source_index + 1,
            source_count,
            scanned: summary.scanned,
            media: summary.media,
            queued: summary.queued,
            duplicates: summary.duplicates,
            filtered: summary.filtered,
            existing: summary.existing,
        };
        send_progress(&event_tx, &progress);

        let mut messages = account.client.iter_messages(resolved.peer);
        if let Some(end) = range.end {
            messages = messages.offset_date(telegram_end_offset(end)?);
        }
        let mut last_update = Instant::now();
        loop {
            if *shutdown.borrow() {
                summary.cancelled = true;
                break;
            }
            let message = match messages.next().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => {
                    summary.failed_sources += 1;
                    let _ = event_tx.send(HistoryEvent::Warning(format!(
                        "Stopped scanning {label}: {error}"
                    )));
                    break;
                }
            };
            let date = message.date();
            if range.end.is_some_and(|end| date > end) {
                continue;
            }
            if range.start.is_some_and(|start| date < start) {
                break;
            }

            progress.scanned += 1;
            summary.scanned += 1;
            match reserve_message(&store, &config, peer_id, &message)? {
                MessageReservation::NotMedia => {}
                MessageReservation::Filtered => {
                    progress.media += 1;
                    progress.filtered += 1;
                    summary.media += 1;
                    summary.filtered += 1;
                }
                MessageReservation::AlreadyTracked => {
                    progress.media += 1;
                    progress.existing += 1;
                    summary.media += 1;
                    summary.existing += 1;
                }
                MessageReservation::Queued(_) => {
                    progress.media += 1;
                    progress.queued += 1;
                    summary.media += 1;
                    summary.queued += 1;
                }
                MessageReservation::Duplicate(_) => {
                    progress.media += 1;
                    progress.duplicates += 1;
                    summary.media += 1;
                    summary.duplicates += 1;
                }
            }
            if progress.scanned.is_multiple_of(50)
                || last_update.elapsed() >= Duration::from_millis(250)
            {
                send_progress(&event_tx, &progress);
                last_update = Instant::now();
            }
        }
        send_progress(&event_tx, &progress);
        if summary.cancelled {
            break;
        }
    }
    Ok(summary)
}

fn send_progress(event_tx: &mpsc::UnboundedSender<HistoryEvent>, progress: &HistoryProgress) {
    let _ = event_tx.send(HistoryEvent::Progress(progress.clone()));
}

fn telegram_timestamp(value: DateTime<Utc>) -> Result<i32> {
    i32::try_from(value.timestamp())
        .with_context(|| format!("{} is outside Telegram's supported date range", value))
}

fn telegram_end_offset(value: DateTime<Utc>) -> Result<i32> {
    let timestamp = value
        .timestamp()
        .checked_add(1)
        .context("history end time is outside Telegram's supported date range")?;
    i32::try_from(timestamp)
        .with_context(|| format!("{} is outside Telegram's supported date range", value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn validates_inclusive_history_ranges() -> Result<()> {
        let start = Utc.timestamp_opt(1_700_000_000, 0).single().unwrap();
        let end = Utc.timestamp_opt(1_700_000_100, 0).single().unwrap();
        let range = HistoryRange::new(start, end)?;
        assert_eq!(range.start, Some(start));
        assert_eq!(range.end, Some(end));
        assert!(HistoryRange::new(end, start).is_err());
        Ok(())
    }
}
