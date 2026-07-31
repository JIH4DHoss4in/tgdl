use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use grammers_session::SessionData;
use grammers_session::storages::SqliteSession;
use hermes_tdata::TDesktop;

use crate::account::inspect_authorized_session;
use crate::config::{AccountConfig, Config};
use crate::security;

const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";

struct ImportedCredentials {
    session: SessionData,
    expected_user_id: Option<i64>,
}

pub async fn import_telethon_session(
    name: &str,
    config: &Config,
    source: &Path,
    destination: &Path,
    proxy_override: Option<&str>,
) -> Result<AccountConfig> {
    let credentials = read_telethon_session(source).await?;
    persist_and_validate(name, config, destination, credentials, proxy_override).await
}

pub async fn import_tdata_session(
    name: &str,
    config: &Config,
    source: &Path,
    account_index: Option<usize>,
    passcode: Option<&str>,
    destination: &Path,
    proxy_override: Option<&str>,
) -> Result<AccountConfig> {
    if !source.is_dir() {
        bail!("tdata path is not a directory: {}", source.display());
    }
    let tdesktop = match passcode {
        Some(passcode) => TDesktop::from_path_with_passcode(source, passcode),
        None => TDesktop::from_path(source),
    }
    .with_context(|| {
        if passcode.is_some() {
            format!("failed to decrypt or parse tdata at {}", source.display())
        } else {
            format!(
                "failed to parse tdata at {}; if it uses a local passcode, add --tdata-passcode",
                source.display()
            )
        }
    })?;

    let account = match account_index {
        Some(index) => {
            let index_i32 =
                i32::try_from(index).context("tdata account index is outside the valid range")?;
            tdesktop
                .accounts()
                .iter()
                .find(|account| account.index() == index_i32)
                .with_context(|| {
                    let choices = tdata_choices(&tdesktop);
                    format!(
                        "tdata account index {index} was not found; available accounts: {choices}"
                    )
                })?
        }
        None if tdesktop.accounts().len() == 1 => &tdesktop.accounts()[0],
        None => {
            bail!(
                "tdata contains multiple accounts; select one with --tdata-index. Available accounts: {}",
                tdata_choices(&tdesktop)
            )
        }
    };

    let credentials = credentials_from_key(
        account.dc_id(),
        *account.auth_key_bytes(),
        Some(account.user_id()),
        None,
        None,
    )?;
    persist_and_validate(name, config, destination, credentials, proxy_override).await
}

fn tdata_choices(tdesktop: &TDesktop) -> String {
    tdesktop
        .accounts()
        .iter()
        .map(|account| format!("{} (user {})", account.index(), account.user_id()))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn read_telethon_session(source: &Path) -> Result<ImportedCredentials> {
    validate_sqlite_file(source)?;
    let database = libsql::Builder::new_local(source)
        .build()
        .await
        .with_context(|| format!("failed to open Telethon session {}", source.display()))?;
    let connection = database.connect()?;
    connection.execute("PRAGMA query_only=ON", ()).await?;

    let mut table = connection
        .query(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions' LIMIT 1",
            (),
        )
        .await?;
    if table.next().await?.is_none() {
        bail!(
            "{} does not contain Telethon's sessions table",
            source.display()
        );
    }

    let mut rows = connection
        .query(
            "SELECT dc_id, server_address, port, auth_key
             FROM sessions WHERE auth_key IS NOT NULL",
            (),
        )
        .await
        .context(
            "unsupported Telethon session schema; expected dc_id, server_address, port, and auth_key",
        )?;
    let row = rows
        .next()
        .await?
        .context("the Telethon session has no authorization key")?;
    let dc_id: i64 = row.get(0)?;
    let server_address: String = row.get(1)?;
    let port: i64 = row.get(2)?;
    let auth_key: Vec<u8> = row.get(3)?;
    if rows.next().await?.is_some() {
        bail!("the Telethon session contains more than one authorized session row");
    }
    let auth_key: [u8; 256] = auth_key.try_into().map_err(|value: Vec<u8>| {
        anyhow!(
            "Telethon authorization key must be 256 bytes, found {}",
            value.len()
        )
    })?;
    let dc_id = i32::try_from(dc_id).context("Telethon dc_id is outside the supported range")?;
    let port = u16::try_from(port)
        .ok()
        .filter(|port| *port != 0)
        .context("Telethon session contains an invalid server port")?;
    let ip = server_address
        .trim_matches(['[', ']'])
        .parse::<IpAddr>()
        .ok();
    credentials_from_key(dc_id, auth_key, None, ip, Some(port))
}

fn credentials_from_key(
    dc_id: i32,
    auth_key: [u8; 256],
    expected_user_id: Option<i64>,
    address: Option<IpAddr>,
    port: Option<u16>,
) -> Result<ImportedCredentials> {
    let mut session = SessionData::default();
    let dc = session
        .dc_options
        .get_mut(&dc_id)
        .with_context(|| format!("unsupported Telegram datacenter ID {dc_id}; expected 1-5"))?;
    dc.auth_key = Some(auth_key);
    if let (Some(address), Some(port)) = (address, port) {
        match address {
            IpAddr::V4(address) => dc.ipv4 = SocketAddrV4::new(address, port),
            IpAddr::V6(address) => {
                dc.ipv6 = SocketAddrV6::new(address, port, 0, 0);
            }
        }
    }
    session.home_dc = dc_id;
    Ok(ImportedCredentials {
        session,
        expected_user_id: expected_user_id.filter(|id| *id != 0),
    })
}

async fn persist_and_validate(
    name: &str,
    config: &Config,
    destination: &Path,
    credentials: ImportedCredentials,
    proxy_override: Option<&str>,
) -> Result<AccountConfig> {
    ensure_new_destination(destination)?;
    if let Some(parent) = destination.parent() {
        security::create_private_dir(parent)?;
    }

    let result = async {
        let session = SqliteSession::open(destination)
            .await
            .with_context(|| format!("failed to create {}", destination.display()))?;
        credentials.session.import_to(&session).await?;
        drop(session);
        restrict_session_artifacts(destination)?;

        let account =
            inspect_authorized_session(name, config, destination, proxy_override).await?;
        if let (Some(expected), Some(actual)) = (credentials.expected_user_id, account.user_id)
            && expected != actual
        {
            bail!(
                "imported tdata identified user {expected}, but Telegram authenticated user {actual}"
            );
        }
        Ok(account)
    }
    .await;

    if let Err(error) = result {
        if let Err(cleanup) = remove_session_artifacts(destination) {
            return Err(error.context(format!(
                "also failed to remove the incomplete imported session: {cleanup:#}"
            )));
        }
        return Err(error);
    }
    result
}

fn validate_sqlite_file(path: &Path) -> Result<()> {
    if !path.is_file() {
        bail!("Telethon session is not a file: {}", path.display());
    }
    let mut header = [0_u8; SQLITE_HEADER.len()];
    File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?
        .read_exact(&mut header)
        .with_context(|| format!("{} is too short to be a SQLite database", path.display()))?;
    if &header != SQLITE_HEADER {
        bail!("{} is not a SQLite Telethon session", path.display());
    }
    Ok(())
}

fn ensure_new_destination(path: &Path) -> Result<()> {
    if session_artifacts(path)
        .iter()
        .any(|candidate| candidate.exists())
    {
        bail!(
            "local session files already exist for this alias at {}; choose another alias or remove the stale files",
            path.display()
        );
    }
    Ok(())
}

fn restrict_session_artifacts(path: &Path) -> Result<()> {
    for candidate in session_artifacts(path) {
        if candidate.exists() {
            security::restrict_file(&candidate)?;
        }
    }
    Ok(())
}

fn remove_session_artifacts(path: &Path) -> Result<()> {
    for candidate in session_artifacts(path) {
        if candidate.exists() {
            std::fs::remove_file(&candidate)
                .with_context(|| format!("failed to remove {}", candidate.display()))?;
        }
    }
    Ok(())
}

fn session_artifacts(path: &Path) -> [PathBuf; 3] {
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let mut shm = path.as_os_str().to_os_string();
    shm.push("-shm");
    [path.to_path_buf(), PathBuf::from(wal), PathBuf::from(shm)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_sqlite_input() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bad.session");
        std::fs::write(&path, b"not a sqlite database").unwrap();
        assert!(validate_sqlite_file(&path).is_err());
    }

    #[test]
    fn builds_grammers_session_data_from_imported_key() {
        let key = [7_u8; 256];
        let imported = credentials_from_key(
            2,
            key,
            Some(42),
            Some("149.154.167.51".parse().unwrap()),
            Some(443),
        )
        .unwrap();
        assert_eq!(imported.session.home_dc, 2);
        assert_eq!(
            imported.session.dc_options.get(&2).unwrap().auth_key,
            Some(key)
        );
        assert_eq!(imported.expected_user_id, Some(42));

        let memory = grammers_session::storages::MemorySession::from(imported.session);
        assert_eq!(grammers_session::Session::home_dc_id(&memory).unwrap(), 2);
    }

    #[tokio::test]
    async fn reads_telethon_v1_sqlite_session() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("account.session");
        let database = libsql::Builder::new_local(&path).build().await.unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE sessions (
                    dc_id INTEGER PRIMARY KEY,
                    server_address TEXT,
                    port INTEGER,
                    auth_key BLOB,
                    takeout_id INTEGER
                 );",
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO sessions(dc_id, server_address, port, auth_key)
                 VALUES (?1, ?2, ?3, ?4)",
                libsql::params![2_i64, "149.154.167.51", 443_i64, vec![9_u8; 256]],
            )
            .await
            .unwrap();
        drop(connection);
        drop(database);

        let imported = read_telethon_session(&path).await.unwrap();
        assert_eq!(imported.session.home_dc, 2);
        assert_eq!(
            imported.session.dc_options.get(&2).unwrap().auth_key,
            Some([9_u8; 256])
        );
    }
}
