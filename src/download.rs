use std::collections::VecDeque;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::stream::{FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{mpsc, watch};

use crate::account::AccountRuntime;
use crate::media::{CHUNK_SIZE, MediaDescriptor};
use crate::store::{Job, Store};
use crate::telegram::{
    FILE_REQUEST_LIMIT, FetchError, LanePool, fetch_file_part, fetch_file_range, fetch_hashes,
};

const CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);
const HASH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const VERIFY_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const INITIAL_LANES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DownloadPhase {
    Downloading,
    Verifying,
    Finalizing,
}

#[derive(Clone, Debug)]
pub struct DownloadProgress {
    pub job_id: i64,
    pub downloaded_bytes: u64,
    pub lanes: usize,
    pub phase: DownloadPhase,
}

#[derive(Clone, Copy, Debug)]
pub struct DownloadOptions {
    pub max_lanes: usize,
    pub verify_file_hashes: bool,
}

#[derive(Debug)]
pub enum DownloadOutcome {
    Completed,
    Wait { kind: String, seconds: u32 },
    RefreshReference,
    Migrate(i32),
    AuthorizationRevoked,
    SourceUnavailable { source: String, error: String },
    Retry(String),
    Failed(String),
    Interrupted,
}

pub async fn download_job(
    store: &Store,
    job: &Job,
    account: &AccountRuntime,
    descriptor: &MediaDescriptor,
    options: DownloadOptions,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<DownloadOutcome> {
    if options.max_lanes == 0 {
        bail!("max_lanes must be positive");
    }
    let mut descriptor = descriptor.clone();
    for _ in 0..3 {
        let current_job = store
            .job(job.id)?
            .with_context(|| format!("job {} disappeared while downloading", job.id))?;
        let lane_pool = LanePool::create(account, &descriptor, options.max_lanes).await?;
        let outcome = if descriptor.total_size.is_none() {
            download_unknown_size(
                &current_job,
                &descriptor,
                &lane_pool,
                progress_tx.clone(),
                shutdown.clone(),
            )
            .await?
        } else {
            download_known_size(
                store,
                &current_job,
                &descriptor,
                &lane_pool,
                options.verify_file_hashes,
                progress_tx.clone(),
                &mut shutdown,
            )
            .await?
        };
        match outcome {
            DownloadOutcome::Migrate(dc) if dc != descriptor.dc_id => {
                account.invalidate_transfer_dc(descriptor.dc_id).await;
                descriptor.dc_id = dc;
                continue;
            }
            DownloadOutcome::Retry(error) => {
                account.invalidate_transfer_dc(descriptor.dc_id).await;
                return Ok(DownloadOutcome::Retry(error));
            }
            other => return Ok(other),
        }
    }
    Ok(DownloadOutcome::Failed(
        "Telegram repeatedly migrated the file between data centers".to_owned(),
    ))
}

async fn download_known_size(
    store: &Store,
    job: &Job,
    descriptor: &MediaDescriptor,
    lanes: &LanePool,
    verify_file_hashes: bool,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<DownloadOutcome> {
    let total_size = descriptor
        .total_size
        .context("known-size download has no size")?;
    let chunk_count = total_size.div_ceil(CHUNK_SIZE as u64) as usize;
    let existing_length = tokio::fs::metadata(&job.part_path)
        .await
        .ok()
        .map(|metadata| metadata.len());
    let mut completed_chunks = job.completed_chunks.clone();
    if existing_length != Some(total_size) && completed_chunks.iter().any(|byte| *byte != 0) {
        let completed: Vec<_> = (0..chunk_count).collect();
        store.clear_chunks(job.id, &completed)?;
        completed_chunks.fill(0);
    }
    let mut missing: VecDeque<usize> = (0..chunk_count)
        .filter(|index| !bitmap_contains(&completed_chunks, *index))
        .collect();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&job.part_path)
        .await
        .with_context(|| format!("failed to open {}", job.part_path.display()))?;
    file.set_len(total_size).await?;

    let mut active_limit = INITIAL_LANES.min(lanes.len()).max(1);
    let mut free_lanes: VecDeque<usize> = (0..lanes.len()).collect();
    let mut in_flight = FuturesUnordered::new();
    let mut retry_counts = vec![0_u8; chunk_count];
    let mut error_times = VecDeque::new();
    let mut success_streak = 0_usize;
    let mut pending_checkpoint = Vec::new();
    let mut checkpoint_bytes = 0_u64;
    let mut last_checkpoint = Instant::now();
    let mut downloaded = job.completed_bytes();
    let hash_client = lanes.client(0);
    let mut hash_future = Box::pin(async {
        if verify_file_hashes {
            collect_file_hashes(hash_client, descriptor, total_size).await
        } else {
            std::future::pending().await
        }
    });
    let mut expected_hashes = None;

    fill_workers(
        &mut in_flight,
        &mut free_lanes,
        &mut missing,
        active_limit,
        WorkerSource {
            lanes,
            descriptor,
            retry_counts: &retry_counts,
        },
    );

    while !in_flight.is_empty() {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
                    return Ok(DownloadOutcome::Interrupted);
                }
            }
            hashes = &mut hash_future, if verify_file_hashes && expected_hashes.is_none() => {
                match hashes {
                    Ok(hashes) => expected_hashes = Some(hashes),
                    Err(error) => {
                        checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
                        return Ok(outcome_from_fetch(error));
                    }
                }
            }
            result = in_flight.next() => {
                let Some((lane, chunk, result)) = result else { break };
                free_lanes.push_back(lane);
                match result {
                    Ok(bytes) => {
                        let offset = chunk as u64 * CHUNK_SIZE as u64;
                        let expected = (total_size - offset).min(CHUNK_SIZE as u64) as usize;
                        if bytes.len() != expected {
                            checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
                            return Ok(DownloadOutcome::Retry(format!(
                                "chunk {chunk} returned {} bytes, expected {expected}", bytes.len()
                            )));
                        }
                        file.seek(SeekFrom::Start(offset)).await?;
                        file.write_all(&bytes).await?;
                        pending_checkpoint.push(chunk);
                        checkpoint_bytes += bytes.len() as u64;
                        downloaded += bytes.len() as u64;
                        success_streak += 1;
                        let _ = progress_tx.send(DownloadProgress {
                            job_id: job.id,
                            downloaded_bytes: downloaded.min(total_size),
                            lanes: active_limit,
                            phase: DownloadPhase::Downloading,
                        });
                        if success_streak >= 8 && active_limit < lanes.len() {
                            active_limit += 1;
                            success_streak = 0;
                        }
                        if checkpoint_bytes >= CHECKPOINT_BYTES
                            || last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL
                        {
                            checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
                            checkpoint_bytes = 0;
                            last_checkpoint = Instant::now();
                        }
                    }
                    Err(FetchError::Transient(error)) => {
                        retry_counts[chunk] = retry_counts[chunk].saturating_add(1);
                        let now = Instant::now();
                        error_times.push_back(now);
                        while error_times.front().is_some_and(|time| now.duration_since(*time) > Duration::from_secs(30)) {
                            error_times.pop_front();
                        }
                        if error_times.len() >= 3 {
                            active_limit = active_limit.saturating_sub(1).max(1);
                            error_times.clear();
                            success_streak = 0;
                        }
                        if retry_counts[chunk] > 5 {
                            checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
                            return Ok(DownloadOutcome::Retry(error));
                        }
                        missing.push_front(chunk);
                    }
                    Err(error) => {
                        checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
                        return Ok(outcome_from_fetch(error));
                    }
                }
                fill_workers(
                    &mut in_flight,
                    &mut free_lanes,
                    &mut missing,
                    active_limit,
                    WorkerSource {
                        lanes,
                        descriptor,
                        retry_counts: &retry_counts,
                    },
                );
            }
        }
    }

    checkpoint(&mut file, store, job.id, &mut pending_checkpoint).await?;
    file.sync_all().await?;
    drop(file);
    if verify_file_hashes {
        let _ = progress_tx.send(DownloadProgress {
            job_id: job.id,
            downloaded_bytes: 0,
            lanes: 0,
            phase: DownloadPhase::Verifying,
        });
        let expected_hashes = match expected_hashes {
            Some(hashes) => hashes,
            None => match hash_future.await {
                Ok(hashes) => hashes,
                Err(error) => return Ok(outcome_from_fetch(error)),
            },
        };
        match verify_file(job, total_size, expected_hashes, progress_tx.clone()).await? {
            Verification::Verified => {}
            Verification::Mismatched(indexes) => {
                store.clear_chunks(job.id, &indexes)?;
                return Ok(DownloadOutcome::Retry(format!(
                    "{} downloaded chunks failed Telegram hash verification",
                    indexes.len()
                )));
            }
        }
    }
    let _ = progress_tx.send(DownloadProgress {
        job_id: job.id,
        downloaded_bytes: total_size,
        lanes: 0,
        phase: DownloadPhase::Finalizing,
    });
    if job.final_path.exists() {
        return Ok(DownloadOutcome::Failed(format!(
            "destination appeared after reservation: {}",
            job.final_path.display()
        )));
    }
    tokio::fs::rename(&job.part_path, &job.final_path)
        .await
        .with_context(|| {
            format!(
                "failed to finalize {} as {}",
                job.part_path.display(),
                job.final_path.display()
            )
        })?;
    Ok(DownloadOutcome::Completed)
}

type ChunkFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = (usize, usize, Result<Vec<u8>, FetchError>)> + Send + 'a>,
>;

#[derive(Clone, Copy)]
struct WorkerSource<'transfer, 'retry> {
    lanes: &'transfer LanePool,
    descriptor: &'transfer MediaDescriptor,
    retry_counts: &'retry [u8],
}

fn fill_workers<'transfer>(
    in_flight: &mut FuturesUnordered<ChunkFuture<'transfer>>,
    free_lanes: &mut VecDeque<usize>,
    missing: &mut VecDeque<usize>,
    active_limit: usize,
    source: WorkerSource<'transfer, '_>,
) {
    while in_flight.len() < active_limit {
        let Some(chunk) = missing.pop_front() else {
            break;
        };
        let Some(lane) = free_lanes.pop_front() else {
            missing.push_front(chunk);
            break;
        };
        let client = source.lanes.client(lane);
        let offset = chunk as u64 * CHUNK_SIZE as u64;
        let delay = if source.retry_counts[chunk] == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs(1_u64 << source.retry_counts[chunk].min(6))
        };
        let descriptor = source.descriptor;
        in_flight.push(Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let result = fetch_file_range(&client, descriptor, offset).await;
            (lane, chunk, result)
        }));
    }
}

fn bitmap_contains(bitmap: &[u8], index: usize) -> bool {
    bitmap
        .get(index / 8)
        .is_some_and(|byte| byte & (1 << (index % 8)) != 0)
}

async fn checkpoint(
    file: &mut File,
    store: &Store,
    job_id: i64,
    pending: &mut Vec<usize>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    file.sync_data().await?;
    store.mark_chunks(job_id, pending)?;
    pending.clear();
    Ok(())
}

enum Verification {
    Verified,
    Mismatched(Vec<usize>),
}

#[derive(Clone, Debug)]
struct ExpectedHash {
    offset: u64,
    length: usize,
    hash: Vec<u8>,
}

async fn collect_file_hashes(
    client: grammers_client::Client,
    descriptor: &MediaDescriptor,
    total: u64,
) -> std::result::Result<Vec<ExpectedHash>, FetchError> {
    let mut query_offset = 0_u64;
    let mut hashes = Vec::new();
    while query_offset < total {
        let response = match tokio::time::timeout(
            HASH_REQUEST_TIMEOUT,
            fetch_hashes(&client, descriptor, query_offset),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(FetchError::Transient(format!(
                    "timed out fetching Telegram file hashes at offset {query_offset}"
                )));
            }
        };
        if response.is_empty() {
            break;
        }
        let mut advanced = query_offset;
        for expected in response {
            let grammers_tl_types::enums::FileHash::Hash(expected) = expected;
            let (hash, declared_end) = normalize_file_hash(expected, total)?;
            if let Some(hash) = hash {
                hashes.push(hash);
            }
            advanced = advanced.max(declared_end);
        }
        if advanced <= query_offset {
            break;
        }
        query_offset = advanced;
    }
    hashes.sort_unstable_by_key(|hash| (hash.offset, hash.length));
    hashes.dedup_by(|left, right| {
        left.offset == right.offset && left.length == right.length && left.hash == right.hash
    });
    Ok(hashes)
}

fn normalize_file_hash(
    expected: grammers_tl_types::types::FileHash,
    total: u64,
) -> std::result::Result<(Option<ExpectedHash>, u64), FetchError> {
    if expected.offset < 0 || expected.limit <= 0 {
        return Ok((None, 0));
    }
    let offset = expected.offset as u64;
    let declared_length = expected.limit as usize;
    let Some(declared_end) = offset.checked_add(declared_length as u64) else {
        return Err(FetchError::Permanent(format!(
            "Telegram returned an invalid file-hash range at offset {offset}"
        )));
    };
    let hash = (offset < total).then(|| ExpectedHash {
        offset,
        length: declared_length.min((total - offset) as usize),
        hash: expected.hash,
    });
    Ok((hash, declared_end))
}

async fn verify_file(
    job: &Job,
    total: u64,
    expected_hashes: Vec<ExpectedHash>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<Verification> {
    if expected_hashes.is_empty() {
        return Ok(Verification::Verified);
    }
    let part_path = job.part_path.clone();
    let job_id = job.id;
    let mismatched = tokio::task::spawn_blocking(move || {
        verify_hashes_on_disk(
            &part_path,
            total,
            &expected_hashes,
            Some((&progress_tx, job_id)),
        )
    })
    .await
    .context("file verification worker panicked")??;
    if mismatched.is_empty() {
        Ok(Verification::Verified)
    } else {
        Ok(Verification::Mismatched(mismatched))
    }
}

fn verify_hashes_on_disk(
    path: &Path,
    total: u64,
    expected_hashes: &[ExpectedHash],
    progress: Option<(&mpsc::UnboundedSender<DownloadProgress>, i64)>,
) -> Result<Vec<usize>> {
    use std::io::{Read, Seek};

    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open {} for verification", path.display()))?;
    let mut reader = std::io::BufReader::with_capacity(VERIFY_BUFFER_SIZE, file);
    let mut buffer = vec![0_u8; CHUNK_SIZE as usize];
    let mut cursor = 0_u64;
    let mut processed = 0_u64;
    let mut last_report = 0_u64;
    let mut mismatched = Vec::new();
    for expected in expected_hashes {
        let end = expected
            .offset
            .checked_add(expected.length as u64)
            .context("Telegram file-hash range overflowed")?;
        if end > total {
            bail!(
                "Telegram file-hash range {}..{} exceeds file size {}",
                expected.offset,
                end,
                total
            );
        }
        if cursor != expected.offset {
            reader.seek(std::io::SeekFrom::Start(expected.offset))?;
        }
        let mut hasher = Sha256::new();
        let mut remaining = expected.length;
        while remaining > 0 {
            let read_length = remaining.min(buffer.len());
            reader.read_exact(&mut buffer[..read_length])?;
            hasher.update(&buffer[..read_length]);
            remaining -= read_length;
        }
        if hasher.finalize()[..] != expected.hash[..] {
            let first = expected.offset / CHUNK_SIZE as u64;
            let last = (end - 1) / CHUNK_SIZE as u64;
            mismatched.extend(first as usize..=last as usize);
        }
        cursor = end;
        processed = processed.saturating_add(expected.length as u64).min(total);
        if processed.saturating_sub(last_report) >= CHECKPOINT_BYTES || processed == total {
            if let Some((progress_tx, job_id)) = progress {
                let _ = progress_tx.send(DownloadProgress {
                    job_id,
                    downloaded_bytes: processed,
                    lanes: 0,
                    phase: DownloadPhase::Verifying,
                });
            }
            last_report = processed;
        }
    }
    mismatched.sort_unstable();
    mismatched.dedup();
    Ok(mismatched)
}

async fn download_unknown_size(
    job: &Job,
    descriptor: &MediaDescriptor,
    lanes: &LanePool,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<DownloadOutcome> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&job.part_path)
        .await?;
    let mut offset = file.metadata().await?.len();
    if offset % FILE_REQUEST_LIMIT as u64 != 0 {
        offset -= offset % FILE_REQUEST_LIMIT as u64;
        file.set_len(offset).await?;
    }
    let client = lanes.client(0);
    let mut unsynced_bytes = 0_u64;
    let mut last_sync = Instant::now();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    file.sync_all().await?;
                    return Ok(DownloadOutcome::Interrupted);
                }
            }
            result = fetch_file_part(&client, descriptor, offset) => {
                match result {
                    Ok(bytes) => {
                        if bytes.is_empty() {
                            break;
                        }
                        file.write_all(&bytes).await?;
                        offset += bytes.len() as u64;
                        unsynced_bytes += bytes.len() as u64;
                        if unsynced_bytes >= CHECKPOINT_BYTES
                            || last_sync.elapsed() >= CHECKPOINT_INTERVAL
                        {
                            file.sync_data().await?;
                            unsynced_bytes = 0;
                            last_sync = Instant::now();
                        }
                        let _ = progress_tx.send(DownloadProgress {
                            job_id: job.id,
                            downloaded_bytes: offset,
                            lanes: 1,
                            phase: DownloadPhase::Downloading,
                        });
                        if bytes.len() < FILE_REQUEST_LIMIT as usize {
                            break;
                        }
                    }
                    Err(error) => {
                        if unsynced_bytes > 0 {
                            file.sync_data().await?;
                        }
                        return Ok(outcome_from_fetch(error));
                    }
                }
            }
        }
    }
    file.sync_all().await?;
    drop(file);
    let _ = progress_tx.send(DownloadProgress {
        job_id: job.id,
        downloaded_bytes: offset,
        lanes: 0,
        phase: DownloadPhase::Finalizing,
    });
    if job.final_path.exists() {
        return Ok(DownloadOutcome::Failed(
            "destination appeared after reservation".to_owned(),
        ));
    }
    tokio::fs::rename(&job.part_path, &job.final_path).await?;
    Ok(DownloadOutcome::Completed)
}

fn outcome_from_fetch(error: FetchError) -> DownloadOutcome {
    match error {
        FetchError::Wait { kind, seconds } => DownloadOutcome::Wait { kind, seconds },
        FetchError::FileReferenceExpired => DownloadOutcome::RefreshReference,
        FetchError::Migrate(dc) => DownloadOutcome::Migrate(dc),
        FetchError::AuthorizationRevoked => DownloadOutcome::AuthorizationRevoked,
        FetchError::Transient(error) => DownloadOutcome::Retry(error),
        FetchError::Permanent(error) => DownloadOutcome::Failed(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_errors_map_to_scheduler_outcomes() {
        assert!(matches!(
            outcome_from_fetch(FetchError::Wait {
                kind: "FLOOD_WAIT".into(),
                seconds: 30
            }),
            DownloadOutcome::Wait { seconds: 30, .. }
        ));
        assert!(matches!(
            outcome_from_fetch(FetchError::FileReferenceExpired),
            DownloadOutcome::RefreshReference
        ));
    }

    #[test]
    fn buffered_verification_reports_only_the_corrupt_local_chunks() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("download.part");
        let bytes: Vec<u8> = (0..(CHUNK_SIZE as usize * 2 + 321))
            .map(|index| (index % 251) as u8)
            .collect();
        std::fs::write(&path, &bytes)?;
        let mut hashes = vec![
            ExpectedHash {
                offset: 0,
                length: CHUNK_SIZE as usize,
                hash: Sha256::digest(&bytes[..CHUNK_SIZE as usize]).to_vec(),
            },
            ExpectedHash {
                offset: CHUNK_SIZE as u64,
                length: CHUNK_SIZE as usize,
                hash: Sha256::digest(&bytes[CHUNK_SIZE as usize..CHUNK_SIZE as usize * 2]).to_vec(),
            },
            ExpectedHash {
                offset: CHUNK_SIZE as u64 * 2,
                length: 321,
                hash: Sha256::digest(&bytes[CHUNK_SIZE as usize * 2..]).to_vec(),
            },
        ];

        assert!(verify_hashes_on_disk(&path, bytes.len() as u64, &hashes, None)?.is_empty());
        hashes[1].hash[0] ^= 0xff;
        assert_eq!(
            verify_hashes_on_disk(&path, bytes.len() as u64, &hashes, None)?,
            vec![1]
        );
        Ok(())
    }

    #[test]
    fn telegrams_final_hash_range_is_truncated_at_the_real_file_size() -> Result<()> {
        let total = CHUNK_SIZE as u64 + 321;
        let expected = grammers_tl_types::types::FileHash {
            offset: CHUNK_SIZE as i64,
            limit: CHUNK_SIZE as i32,
            hash: vec![7; 32],
        };

        let (normalized, declared_end) = normalize_file_hash(expected, total)?;
        let normalized = normalized.context("expected an in-file hash range")?;
        assert_eq!(normalized.offset, CHUNK_SIZE as u64);
        assert_eq!(normalized.length, 321);
        assert_eq!(declared_end, CHUNK_SIZE as u64 * 2);
        Ok(())
    }
}
