use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeZone, Utc};
use libsql::{Connection, Row, params};

use crate::security;

const JOB_COLUMNS: &str = "id, channel_id, message_id, filename, final_path, part_path, total_size, \
     chunk_size, completed_chunks, state, assigned_account, last_error";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Queued,
    Downloading,
    Retrying,
    Failed,
    Completed,
}

impl JobState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Retrying => "retrying",
            Self::Failed => "failed",
            Self::Completed => "completed",
        }
    }
}

impl FromStr for JobState {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "downloading" => Ok(Self::Downloading),
            "retrying" => Ok(Self::Retrying),
            "failed" => Ok(Self::Failed),
            "completed" => Ok(Self::Completed),
            _ => bail!("unknown job state {value}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Job {
    pub id: i64,
    pub channel_id: i64,
    pub message_id: i32,
    pub filename: String,
    pub final_path: PathBuf,
    pub part_path: PathBuf,
    pub total_size: Option<u64>,
    pub chunk_size: u32,
    pub completed_chunks: Vec<u8>,
    pub state: JobState,
    pub assigned_account: Option<String>,
    pub last_error: Option<String>,
}

impl Job {
    pub fn chunk_count(&self) -> Option<usize> {
        if self.chunk_size == 0 {
            return None;
        }
        self.total_size.map(|size| {
            if size == 0 {
                0
            } else {
                size.div_ceil(self.chunk_size as u64) as usize
            }
        })
    }

    pub fn is_chunk_complete(&self, index: usize) -> bool {
        let byte = index / 8;
        let bit = index % 8;
        self.completed_chunks
            .get(byte)
            .is_some_and(|value| value & (1 << bit) != 0)
    }

    pub fn completed_bytes(&self) -> u64 {
        match self.total_size {
            Some(size) => {
                let complete = (0..self.chunk_count().unwrap_or_default())
                    .filter(|index| self.is_chunk_complete(*index))
                    .count() as u64
                    * self.chunk_size as u64;
                complete.min(size)
            }
            None => self
                .final_path
                .metadata()
                .or_else(|_| self.part_path.metadata())
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.len())
                .unwrap_or_default(),
        }
    }

    fn completed_file_is_reusable(&self) -> bool {
        self.final_path
            .metadata()
            .ok()
            .filter(|metadata| metadata.is_file())
            .is_some_and(|metadata| {
                self.total_size
                    .is_none_or(|expected| metadata.len() == expected)
            })
    }
}

#[derive(Clone, Debug)]
pub struct NewJob {
    pub channel_id: i64,
    pub message_id: i32,
    pub remote_key: Option<String>,
    pub filename: String,
    pub media_kind: String,
    pub final_path: PathBuf,
    pub part_path: PathBuf,
    pub total_size: Option<u64>,
    pub chunk_size: u32,
}

#[derive(Clone, Debug, Default)]
pub struct AccountState {
    pub cooldown_until: Option<DateTime<Utc>>,
    pub last_assigned_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            security::create_private_dir(parent)?;
        }
        let connection = wait_for(async {
            let database = libsql::Builder::new_local(&path)
                .build()
                .await
                .with_context(|| format!("failed to open state database {}", path.display()))?;
            let connection = database.connect()?;
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            Result::<Connection>::Ok(connection)
        })?;
        let store = Self { connection };
        wait_for(async {
            let connection = store.connect().await?;
            connection
                .execute_batch(
                    "PRAGMA journal_mode=WAL;
                     PRAGMA foreign_keys=ON;
                     CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        filename TEXT NOT NULL,
                        media_kind TEXT NOT NULL,
                        final_path TEXT NOT NULL UNIQUE,
                        part_path TEXT NOT NULL UNIQUE,
                        total_size INTEGER CHECK(total_size IS NULL OR total_size >= 0),
                        chunk_size INTEGER NOT NULL CHECK(chunk_size > 0),
                        completed_chunks BLOB NOT NULL,
                        state TEXT NOT NULL,
                        assigned_account TEXT,
                        last_error TEXT,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        remote_key TEXT,
                        UNIQUE(channel_id, message_id)
                     );
                     CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs(state, created_at);
                     CREATE TABLE IF NOT EXISTS account_state (
                        name TEXT PRIMARY KEY,
                        cooldown_until INTEGER,
                        last_assigned_at INTEGER,
                        last_error TEXT
                     );",
                )
                .await?;
            let mut columns = connection.query("PRAGMA table_info(jobs)", ()).await?;
            let mut has_remote_key = false;
            while let Some(row) = columns.next().await? {
                let name: String = row.get(1)?;
                has_remote_key |= name == "remote_key";
            }
            drop(columns);
            if !has_remote_key {
                connection
                    .execute("ALTER TABLE jobs ADD COLUMN remote_key TEXT", ())
                    .await?;
            }
            // Older tgdl releases stored a channel's bare positive ID. All
            // monitored groups/channels now use Telegram's Bot API-style peer
            // ID, which embeds the peer kind and is always negative. This
            // one-way conversion preserves existing jobs and prevents a basic
            // group and channel with the same bare ID from colliding.
            connection
                .execute(
                    "UPDATE jobs SET channel_id=-(1000000000000 + channel_id)
                     WHERE channel_id > 0",
                    (),
                )
                .await?;
            connection
                .execute_batch(
                    "CREATE INDEX IF NOT EXISTS jobs_remote_key_idx
                     ON jobs(remote_key);",
                )
                .await?;
            Result::<()>::Ok(())
        })?;
        security::restrict_file(&path)?;
        Ok(store)
    }

    async fn connect(&self) -> Result<Connection> {
        Ok(self.connection.clone())
    }

    pub fn reset_interrupted_jobs(&self) -> Result<usize> {
        let interrupted = self.query_jobs(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE state='downloading' ORDER BY id"
        ))?;
        if interrupted.is_empty() {
            return Ok(0);
        }
        let recovered: Vec<_> = interrupted
            .iter()
            .map(|job| {
                let finalized = !job.part_path.exists()
                    && job
                        .final_path
                        .metadata()
                        .ok()
                        .filter(|metadata| metadata.is_file())
                        .is_some_and(|metadata| {
                            job.total_size.is_none_or(|expected| {
                                metadata.len() == expected && job.completed_bytes() == expected
                            })
                        });
                (job.id, finalized)
            })
            .collect();
        let recovered_count = recovered.len();
        wait_for(async move {
            let connection = self.connect().await?;
            let transaction = connection.transaction().await?;
            let now = Utc::now().timestamp();
            for (job_id, finalized) in recovered {
                if finalized {
                    transaction
                        .execute(
                            "UPDATE jobs SET state='completed', assigned_account=NULL,
                             last_error=NULL, updated_at=?1
                             WHERE id=?2 AND state='downloading'",
                            params![now, job_id],
                        )
                        .await?;
                } else {
                    transaction
                        .execute(
                            "UPDATE jobs SET state='queued', assigned_account=NULL,
                             last_error='interrupted before a checkpointed shutdown',
                             updated_at=?1 WHERE id=?2 AND state='downloading'",
                            params![now, job_id],
                        )
                        .await?;
                }
            }
            transaction.commit().await?;
            Ok(recovered_count)
        })
    }

    pub fn insert_job(&self, new: &NewJob) -> Result<Option<Job>> {
        if new.chunk_size == 0 {
            bail!("job chunk size must be positive");
        }
        let total_size = new
            .total_size
            .map(i64::try_from)
            .transpose()
            .context("job size exceeds SQLite's supported integer range")?;
        let id = wait_for(async {
            let connection = self.connect().await?;
            let now = Utc::now().timestamp();
            let bitmap_size = new
                .total_size
                .map(|size| size.div_ceil(new.chunk_size as u64).div_ceil(8) as usize)
                .unwrap_or_default();
            let changed = connection
                .execute(
                    "INSERT OR IGNORE INTO jobs
                     (channel_id, message_id, filename, media_kind, final_path, part_path,
                      total_size, chunk_size, completed_chunks, state, created_at, updated_at,
                      remote_key)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'queued', ?10, ?10, ?11)",
                    params![
                        new.channel_id,
                        new.message_id,
                        new.filename.clone(),
                        new.media_kind.clone(),
                        path_text(&new.final_path),
                        path_text(&new.part_path),
                        total_size,
                        new.chunk_size as i64,
                        vec![0_u8; bitmap_size],
                        now,
                        new.remote_key.clone(),
                    ],
                )
                .await?;
            Result::<Option<i64>>::Ok(if changed == 0 {
                None
            } else {
                Some(connection.last_insert_rowid())
            })
        })?;
        match id {
            None => Ok(None),
            Some(id) => Ok(Some(
                self.job(id)?
                    .context("new job disappeared from state database")?,
            )),
        }
    }

    pub fn output_reserved(&self, path: &Path) -> Result<bool> {
        let path = path_text(path);
        wait_for(async {
            let connection = self.connect().await?;
            #[cfg(windows)]
            let query = "SELECT 1 FROM jobs WHERE final_path=?1 COLLATE NOCASE LIMIT 1";
            #[cfg(not(windows))]
            let query = "SELECT 1 FROM jobs WHERE final_path=?1 LIMIT 1";
            let mut rows = connection.query(query, [path]).await?;
            Ok(rows.next().await?.is_some())
        })
    }

    pub fn reusable_job_for_media(&self, remote_key: &str) -> Result<Option<Job>> {
        let jobs = wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection
                .query(
                    &format!(
                        "SELECT {JOB_COLUMNS} FROM jobs WHERE remote_key=?1
                         ORDER BY created_at, id"
                    ),
                    [remote_key],
                )
                .await?;
            let mut result = Vec::new();
            while let Some(row) = rows.next().await? {
                result.push(row_to_job(row)?);
            }
            Result::<Vec<Job>>::Ok(result)
        })?;
        Ok(jobs.into_iter().find(|job| match job.state {
            JobState::Completed => job.completed_file_is_reusable(),
            JobState::Queued | JobState::Downloading | JobState::Retrying => true,
            JobState::Failed => false,
        }))
    }

    pub fn jobs_without_remote_key_for_source(&self, source_id: i64) -> Result<Vec<Job>> {
        wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection
                .query(
                    &format!(
                        "SELECT {JOB_COLUMNS} FROM jobs
                         WHERE remote_key IS NULL AND channel_id=?1 ORDER BY id"
                    ),
                    [source_id],
                )
                .await?;
            let mut result = Vec::new();
            while let Some(row) = rows.next().await? {
                result.push(row_to_job(row)?);
            }
            Ok(result)
        })
    }

    pub fn set_remote_key(&self, job_id: i64, remote_key: &str) -> Result<()> {
        wait_for(async {
            let connection = self.connect().await?;
            let changed = connection
                .execute(
                    "UPDATE jobs SET remote_key=?1, updated_at=?2 WHERE id=?3",
                    params![remote_key, Utc::now().timestamp(), job_id],
                )
                .await?;
            if changed != 1 {
                bail!("job {job_id} was not found while saving its media identity");
            }
            Ok(())
        })
    }

    pub fn job(&self, id: i64) -> Result<Option<Job>> {
        wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection
                .query(&format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id=?1"), [id])
                .await?;
            rows.next().await?.map(row_to_job).transpose()
        })
    }

    pub fn job_for_message(&self, channel_id: i64, message_id: i32) -> Result<Option<Job>> {
        wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection
                .query(
                    &format!(
                        "SELECT {JOB_COLUMNS} FROM jobs
                         WHERE channel_id=?1 AND message_id=?2 LIMIT 1"
                    ),
                    params![channel_id, message_id],
                )
                .await?;
            rows.next().await?.map(row_to_job).transpose()
        })
    }

    pub fn jobs(&self) -> Result<Vec<Job>> {
        self.query_jobs(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs ORDER BY created_at, id"
        ))
    }

    pub fn runnable_jobs(&self) -> Result<Vec<Job>> {
        self.query_jobs(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE state IN ('queued', 'retrying') \
             ORDER BY created_at, id"
        ))
    }

    pub fn pending_job_count(&self) -> Result<usize> {
        wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection
                .query(
                    "SELECT COUNT(*) FROM jobs WHERE state IN ('queued', 'retrying')",
                    (),
                )
                .await?;
            let count = rows
                .next()
                .await?
                .context("pending job count query returned no row")?
                .get::<i64>(0)?;
            usize::try_from(count).context("pending job count is outside the supported range")
        })
    }

    fn query_jobs(&self, sql: &str) -> Result<Vec<Job>> {
        wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection.query(sql, ()).await?;
            let mut result = Vec::new();
            while let Some(row) = rows.next().await? {
                result.push(row_to_job(row)?);
            }
            Ok(result)
        })
    }

    pub fn assign_job(&self, job_id: i64, account: &str) -> Result<()> {
        wait_for(async {
            let connection = self.connect().await?;
            let transaction = connection.transaction().await?;
            let now = Utc::now().timestamp();
            let changed = transaction
                .execute(
                    "UPDATE jobs SET state='downloading', assigned_account=?1, last_error=NULL,
                     updated_at=?2 WHERE id=?3 AND state IN ('queued', 'retrying')",
                    params![account, now, job_id],
                )
                .await?;
            if changed != 1 {
                bail!("job {job_id} is missing or no longer ready for assignment");
            }
            transaction
                .execute(
                    "INSERT INTO account_state(name, last_assigned_at) VALUES (?1, ?2)
                     ON CONFLICT(name) DO UPDATE SET last_assigned_at=excluded.last_assigned_at",
                    params![account, now],
                )
                .await?;
            transaction.commit().await?;
            Ok(())
        })
    }

    pub fn mark_chunks(&self, job_id: i64, indexes: &[usize]) -> Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }
        let mut job = self
            .job(job_id)?
            .context("job not found while checkpointing")?;
        let chunk_count = job.chunk_count().unwrap_or_default();
        for index in indexes {
            if *index >= chunk_count {
                bail!("chunk index {index} is outside job {} bitmap", job.id);
            }
            let byte = index / 8;
            let bit = index % 8;
            job.completed_chunks[byte] |= 1 << bit;
        }
        wait_for(async {
            let connection = self.connect().await?;
            let changed = connection
                .execute(
                    "UPDATE jobs SET completed_chunks=?1, updated_at=?2 WHERE id=?3",
                    params![job.completed_chunks, Utc::now().timestamp(), job_id],
                )
                .await?;
            if changed != 1 {
                bail!("job {job_id} was not found while checkpointing chunks");
            }
            Ok(())
        })
    }

    pub fn clear_chunks(&self, job_id: i64, indexes: &[usize]) -> Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }
        let mut job = self
            .job(job_id)?
            .context("job not found while clearing chunks")?;
        let chunk_count = job.chunk_count().unwrap_or_default();
        for index in indexes {
            if *index >= chunk_count {
                bail!("chunk index {index} is outside job {} bitmap", job.id);
            }
            let byte = index / 8;
            let bit = index % 8;
            job.completed_chunks[byte] &= !(1 << bit);
        }
        wait_for(async {
            let connection = self.connect().await?;
            let changed = connection
                .execute(
                    "UPDATE jobs SET completed_chunks=?1, updated_at=?2 WHERE id=?3",
                    params![job.completed_chunks, Utc::now().timestamp(), job_id],
                )
                .await?;
            if changed != 1 {
                bail!("job {job_id} was not found while clearing chunks");
            }
            Ok(())
        })
    }

    pub fn mark_retrying(&self, job_id: i64, error: &str) -> Result<()> {
        self.update_job_state(job_id, JobState::Retrying, Some(error), None)
    }

    pub fn retry_on_account(&self, job_id: i64, account: &str, error: &str) -> Result<()> {
        self.update_job_state(job_id, JobState::Retrying, Some(error), Some(account))
    }

    pub fn mark_failed(&self, job_id: i64, error: &str) -> Result<()> {
        self.update_job_state(job_id, JobState::Failed, Some(error), None)
    }

    pub fn mark_completed(&self, job_id: i64) -> Result<()> {
        self.update_job_state(job_id, JobState::Completed, None, None)
    }

    pub fn release_job(&self, job_id: i64, error: &str) -> Result<()> {
        self.update_job_state(job_id, JobState::Retrying, Some(error), None)
    }

    pub fn release_jobs_for_account(&self, account: &str, error: &str) -> Result<usize> {
        wait_for(async {
            let connection = self.connect().await?;
            let changed = connection
                .execute(
                    "UPDATE jobs SET state='retrying', assigned_account=NULL, last_error=?1,
                     updated_at=?2 WHERE assigned_account=?3
                     AND state IN ('queued', 'downloading', 'retrying')",
                    params![error, Utc::now().timestamp(), account],
                )
                .await?;
            Ok(changed as usize)
        })
    }

    fn update_job_state(
        &self,
        job_id: i64,
        state: JobState,
        error: Option<&str>,
        account: Option<&str>,
    ) -> Result<()> {
        wait_for(async {
            let connection = self.connect().await?;
            let changed = connection
                .execute(
                    "UPDATE jobs SET state=?1, assigned_account=?2, last_error=?3, updated_at=?4 \
                     WHERE id=?5",
                    params![
                        state.as_str(),
                        account,
                        error,
                        Utc::now().timestamp(),
                        job_id
                    ],
                )
                .await?;
            if changed != 1 {
                bail!("job {job_id} was not found while changing its state");
            }
            Ok(())
        })
    }

    pub fn retry_failed(&self) -> Result<usize> {
        wait_for(async {
            let connection = self.connect().await?;
            let changed = connection
                .execute(
                    "UPDATE jobs SET state='queued', assigned_account=NULL, last_error=NULL, \
                     updated_at=?1 WHERE state='failed'",
                    [Utc::now().timestamp()],
                )
                .await?;
            Ok(changed as usize)
        })
    }

    pub fn account_state(&self, name: &str) -> Result<AccountState> {
        wait_for(async {
            let connection = self.connect().await?;
            let mut rows = connection
                .query(
                    "SELECT cooldown_until, last_assigned_at, last_error
                     FROM account_state WHERE name=?1",
                    [name],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(AccountState {
                    cooldown_until: timestamp(row.get(0)?),
                    last_assigned_at: timestamp(row.get(1)?),
                    last_error: row.get(2)?,
                }),
                None => Ok(AccountState::default()),
            }
        })
    }

    pub fn set_cooldown(&self, name: &str, until: DateTime<Utc>, error: &str) -> Result<()> {
        wait_for(async {
            let connection = self.connect().await?;
            connection
                .execute(
                    "INSERT INTO account_state(name, cooldown_until, last_error) VALUES (?1, ?2, ?3)
                     ON CONFLICT(name) DO UPDATE SET cooldown_until=excluded.cooldown_until,
                     last_error=excluded.last_error",
                    params![name, until.timestamp(), error],
                )
                .await?;
            Ok(())
        })
    }

    pub fn clear_expired_cooldowns(&self) -> Result<()> {
        wait_for(async {
            let connection = self.connect().await?;
            connection
                .execute(
                    "UPDATE account_state SET cooldown_until=NULL, last_error=NULL
                     WHERE cooldown_until <= ?1",
                    [Utc::now().timestamp()],
                )
                .await?;
            Ok(())
        })
    }

    pub fn remove_account_state(&self, name: &str) -> Result<()> {
        wait_for(async {
            let connection = self.connect().await?;
            let transaction = connection.transaction().await?;
            transaction
                .execute(
                    "UPDATE jobs SET state='retrying', assigned_account=NULL,
                     last_error='assigned account was removed' WHERE assigned_account=?1",
                    [name],
                )
                .await?;
            transaction
                .execute("DELETE FROM account_state WHERE name=?1", [name])
                .await?;
            transaction.commit().await?;
            Ok(())
        })
    }
}

fn row_to_job(row: Row) -> Result<Job> {
    let state: String = row.get(9)?;
    let total_size = row
        .get::<Option<i64>>(6)?
        .map(|value| {
            u64::try_from(value).context("job total_size is negative in the state database")
        })
        .transpose()?;
    let chunk_size = u32::try_from(row.get::<i64>(7)?)
        .ok()
        .filter(|value| *value > 0)
        .context("job chunk_size is invalid in the state database")?;
    let completed_chunks: Vec<u8> = row.get(8)?;
    if let Some(total_size) = total_size {
        let expected_bitmap = total_size.div_ceil(chunk_size as u64).div_ceil(8) as usize;
        if completed_chunks.len() != expected_bitmap {
            bail!(
                "job chunk bitmap has {} bytes, expected {expected_bitmap}",
                completed_chunks.len()
            );
        }
    } else if !completed_chunks.is_empty() {
        bail!("unknown-size job has a non-empty chunk bitmap");
    }
    Ok(Job {
        id: row.get(0)?,
        channel_id: row.get(1)?,
        message_id: row.get(2)?,
        filename: row.get(3)?,
        final_path: PathBuf::from(row.get::<String>(4)?),
        part_path: PathBuf::from(row.get::<String>(5)?),
        total_size,
        chunk_size,
        completed_chunks,
        state: state.parse()?,
        assigned_account: row.get(10)?,
        last_error: row.get(11)?,
    })
}

fn wait_for<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create the state database runtime")?
            .block_on(future),
    }
}

fn timestamp(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(|value| Utc.timestamp_opt(value, 0).single())
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_jobs_and_chunk_checkpoints() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let new = NewJob {
            channel_id: 7,
            message_id: 9,
            remote_key: Some("document:123".into()),
            filename: "test.bin".into(),
            media_kind: "document".into(),
            final_path: temp.path().join("test.bin"),
            part_path: temp.path().join("test.bin.part"),
            total_size: Some(3 * 1024 * 1024),
            chunk_size: 1024 * 1024,
        };
        let job = store.insert_job(&new).unwrap().unwrap();
        assert_eq!(
            store
                .reusable_job_for_media("document:123")
                .unwrap()
                .unwrap()
                .id,
            job.id
        );
        assert_eq!(job.chunk_count(), Some(3));
        store.mark_chunks(job.id, &[0, 2]).unwrap();
        let job = store.job(job.id).unwrap().unwrap();
        assert!(job.is_chunk_complete(0));
        assert!(!job.is_chunk_complete(1));
        assert!(job.is_chunk_complete(2));
        assert_eq!(store.pending_job_count().unwrap(), 1);
        assert!(store.insert_job(&new).unwrap().is_none());
    }

    #[test]
    fn cooldown_survives_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let until = Utc::now() + chrono::Duration::minutes(5);
        Store::open(&path)
            .unwrap()
            .set_cooldown("normal", until, "FLOOD_PREMIUM_WAIT")
            .unwrap();
        let state = Store::open(&path).unwrap().account_state("normal").unwrap();
        assert!(state.cooldown_until.unwrap() >= until - chrono::Duration::seconds(1));
    }

    #[test]
    fn reuses_active_or_present_completed_remote_media() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let final_path = temp.path().join("cached.bin");
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 10,
                remote_key: Some("document:777".into()),
                filename: "cached.bin".into(),
                media_kind: "document".into(),
                final_path: final_path.clone(),
                part_path: temp.path().join("cached.bin.part"),
                total_size: Some(4),
                chunk_size: 1024 * 1024,
            })
            .unwrap()
            .unwrap();

        assert_eq!(
            store
                .reusable_job_for_media("document:777")
                .unwrap()
                .map(|job| job.id),
            Some(job.id)
        );
        store.mark_completed(job.id).unwrap();
        assert!(
            store
                .reusable_job_for_media("document:777")
                .unwrap()
                .is_none()
        );
        std::fs::write(&final_path, b"bad").unwrap();
        assert!(
            store
                .reusable_job_for_media("document:777")
                .unwrap()
                .is_none(),
            "a truncated completed file must be downloaded again"
        );
        std::fs::write(final_path, b"test").unwrap();
        assert_eq!(
            store
                .reusable_job_for_media("document:777")
                .unwrap()
                .map(|job| job.id),
            Some(job.id)
        );
    }

    #[test]
    fn unknown_size_jobs_report_bytes_from_the_partial_or_final_file() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let final_path = temp.path().join("stream.bin");
        let part_path = temp.path().join("stream.bin.part");
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 11,
                remote_key: Some("document:stream".into()),
                filename: "stream.bin".into(),
                media_kind: "document".into(),
                final_path: final_path.clone(),
                part_path: part_path.clone(),
                total_size: None,
                chunk_size: 1024 * 1024,
            })?
            .context("unknown-size job was not inserted")?;

        std::fs::write(&part_path, b"partial")?;
        assert_eq!(
            store.job(job.id)?.context("job missing")?.completed_bytes(),
            7
        );
        std::fs::rename(part_path, final_path)?;
        assert_eq!(
            store.job(job.id)?.context("job missing")?.completed_bytes(),
            7
        );
        Ok(())
    }

    #[test]
    fn filters_missing_media_keys_by_source() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        for (source_id, message_id) in [(11, 1), (22, 2)] {
            store.insert_job(&NewJob {
                channel_id: source_id,
                message_id,
                remote_key: None,
                filename: format!("{message_id}.bin"),
                media_kind: "document".into(),
                final_path: temp.path().join(format!("{message_id}.bin")),
                part_path: temp.path().join(format!("{message_id}.bin.part")),
                total_size: Some(1),
                chunk_size: 1,
            })?;
        }

        let jobs = store.jobs_without_remote_key_for_source(22)?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].channel_id, 22);
        assert_eq!(jobs[0].message_id, 2);
        Ok(())
    }

    #[test]
    fn releasing_an_account_keeps_checkpointed_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 2,
                remote_key: None,
                filename: "resume.bin".into(),
                media_kind: "document".into(),
                final_path: temp.path().join("resume.bin"),
                part_path: temp.path().join("resume.bin.part"),
                total_size: Some(2 * 1024 * 1024),
                chunk_size: 1024 * 1024,
            })
            .unwrap()
            .unwrap();
        store.assign_job(job.id, "premium").unwrap();
        store.mark_chunks(job.id, &[0]).unwrap();
        store.release_job(job.id, "cooldown").unwrap();
        let resumed = store.job(job.id).unwrap().unwrap();
        assert_eq!(resumed.state, JobState::Retrying);
        assert!(resumed.assigned_account.is_none());
        assert!(resumed.is_chunk_complete(0));
    }

    #[test]
    fn rejects_stale_job_updates_and_invalid_chunks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 3,
                remote_key: None,
                filename: "guarded.bin".into(),
                media_kind: "document".into(),
                final_path: temp.path().join("guarded.bin"),
                part_path: temp.path().join("guarded.bin.part"),
                total_size: Some(1),
                chunk_size: 1,
            })?
            .context("test job was not inserted")?;

        store.assign_job(job.id, "first")?;
        assert!(store.assign_job(job.id, "second").is_err());
        assert!(store.account_state("second")?.last_assigned_at.is_none());
        assert!(store.mark_chunks(job.id, &[1]).is_err());
        assert!(store.clear_chunks(job.id, &[1]).is_err());
        assert!(store.set_remote_key(i64::MAX, "missing").is_err());
        assert!(store.mark_failed(i64::MAX, "missing").is_err());
        Ok(())
    }

    #[test]
    fn same_account_retry_survives_reopen_and_startup_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let store = Store::open(&path).unwrap();
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 3,
                remote_key: None,
                filename: "held.bin".into(),
                media_kind: "document".into(),
                final_path: temp.path().join("held.bin"),
                part_path: temp.path().join("held.bin.part"),
                total_size: Some(1024 * 1024),
                chunk_size: 1024 * 1024,
            })
            .unwrap()
            .unwrap();
        store.assign_job(job.id, "premium-proxy").unwrap();
        store
            .retry_on_account(job.id, "premium-proxy", "FLOOD_WAIT for 2 seconds")
            .unwrap();
        drop(store);

        let reopened = Store::open(&path).unwrap();
        reopened.reset_interrupted_jobs().unwrap();
        let held = reopened.job(job.id).unwrap().unwrap();
        assert_eq!(held.state, JobState::Retrying);
        assert_eq!(held.assigned_account.as_deref(), Some("premium-proxy"));
    }

    #[test]
    fn startup_recovers_a_file_renamed_before_state_commit() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let final_path = temp.path().join("finalized.bin");
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 30,
                remote_key: None,
                filename: "finalized.bin".into(),
                media_kind: "document".into(),
                final_path: final_path.clone(),
                part_path: temp.path().join("finalized.bin.part"),
                total_size: Some(4),
                chunk_size: 1024 * 1024,
            })
            .unwrap()
            .unwrap();
        store.assign_job(job.id, "premium").unwrap();
        store.mark_chunks(job.id, &[0]).unwrap();
        std::fs::write(&final_path, b"done").unwrap();

        assert_eq!(store.reset_interrupted_jobs().unwrap(), 1);
        let recovered = store.job(job.id).unwrap().unwrap();
        assert_eq!(recovered.state, JobState::Completed);
        assert!(recovered.assigned_account.is_none());
    }

    #[test]
    fn startup_requeues_an_unfinished_partial_file() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let part_path = temp.path().join("partial.bin.part");
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 31,
                remote_key: None,
                filename: "partial.bin".into(),
                media_kind: "document".into(),
                final_path: temp.path().join("partial.bin"),
                part_path: part_path.clone(),
                total_size: Some(4),
                chunk_size: 1024 * 1024,
            })
            .unwrap()
            .unwrap();
        store.assign_job(job.id, "premium").unwrap();
        std::fs::write(part_path, b"part").unwrap();

        assert_eq!(store.reset_interrupted_jobs().unwrap(), 1);
        let recovered = store.job(job.id).unwrap().unwrap();
        assert_eq!(recovered.state, JobState::Queued);
        assert!(recovered.assigned_account.is_none());
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("interrupted before a checkpointed shutdown")
        );
    }

    #[test]
    fn releases_monitor_only_account_jobs_without_losing_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let job = store
            .insert_job(&NewJob {
                channel_id: 1,
                message_id: 4,
                remote_key: None,
                filename: "monitor.bin".into(),
                media_kind: "document".into(),
                final_path: temp.path().join("monitor.bin"),
                part_path: temp.path().join("monitor.bin.part"),
                total_size: Some(1024 * 1024),
                chunk_size: 1024 * 1024,
            })
            .unwrap()
            .unwrap();
        store.assign_job(job.id, "watcher").unwrap();
        store.mark_chunks(job.id, &[0]).unwrap();
        assert_eq!(
            store
                .release_jobs_for_account("watcher", "monitor-only account")
                .unwrap(),
            1
        );
        let released = store.job(job.id).unwrap().unwrap();
        assert_eq!(released.state, JobState::Retrying);
        assert!(released.assigned_account.is_none());
        assert!(released.is_chunk_complete(0));
    }

    #[test]
    fn migrates_legacy_bare_channel_ids_to_peer_aware_ids() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("state.sqlite");
        let store = Store::open(&path)?;
        let job = store
            .insert_job(&NewJob {
                channel_id: 123,
                message_id: 9,
                remote_key: Some("document:legacy".into()),
                filename: "legacy.bin".into(),
                media_kind: "document".into(),
                final_path: temp.path().join("legacy.bin"),
                part_path: temp.path().join("legacy.bin.part"),
                total_size: Some(1),
                chunk_size: 1,
            })?
            .context("legacy job was not inserted")?;
        assert_eq!(job.channel_id, 123);

        let reopened = Store::open(&path)?;
        assert_eq!(
            reopened
                .job(job.id)?
                .context("migrated job disappeared")?
                .channel_id,
            -1_000_000_000_123
        );
        Ok(())
    }
}
