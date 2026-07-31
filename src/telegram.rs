use std::collections::HashMap;
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result};
use grammers_client::Client;
use grammers_client::client::{ClientConfiguration, NoRetries};
use grammers_mtsender::{InvocationError, SenderPool};
use grammers_session::types::{
    ChannelState, DcOption, PeerId, PeerInfo, UpdateState, UpdatesState,
};
use grammers_session::{BoxFuture, Session, SessionData};
use grammers_tl_types as tl;

use crate::account::{AccountRuntime, is_revoked_authorization_name};
use crate::media::MediaDescriptor;

/// Telegram permits one full 1 MiB precise range per `upload.getFile` call.
/// The vendored grammers-mtproto patch accepts the small TL wrapper overhead
/// beyond that payload without changing its outgoing-container limit.
pub const FILE_REQUEST_LIMIT: u32 = 1024 * 1024;

pub struct LanePool {
    clients: Vec<Client>,
}

impl LanePool {
    pub async fn create(
        account: &AccountRuntime,
        descriptor: &MediaDescriptor,
        count: usize,
    ) -> Result<Self> {
        account.ensure_dc_authorized(descriptor.dc_id).await?;
        let transfer_dc = transfer_dc_option(account, descriptor.dc_id).await?;
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            let memory = Arc::new(snapshot_session(
                account.session.as_ref(),
                transfer_dc.clone(),
            )?);
            let SenderPool {
                runner,
                handle,
                mut updates,
            } = SenderPool::with_configuration(
                memory,
                account.api_id,
                crate::proxy::connection_params(account.proxy_url.as_deref()),
            );
            let client = Client::with_configuration(
                handle,
                ClientConfiguration {
                    retry_policy: Box::new(NoRetries),
                    auto_cache_peers: false,
                },
            );
            tokio::spawn(runner.run());
            tokio::spawn(async move { while updates.recv().await.is_some() {} });
            clients.push(client);
        }
        Ok(Self { clients })
    }

    pub fn client(&self, index: usize) -> Client {
        self.clients[index % self.clients.len()].clone()
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }
}

impl Drop for LanePool {
    fn drop(&mut self) {
        for client in &self.clients {
            client.disconnect();
        }
    }
}

fn snapshot_session(
    session: &grammers_session::storages::SqliteSession,
    transfer_dc: DcOption,
) -> Result<TransferSession> {
    let home_dc = session.home_dc_id()?;
    let mut dc_options = HashMap::new();
    for dc_id in 1..=5 {
        if let Some(option) = session.dc_option(dc_id)? {
            dc_options.insert(dc_id, option);
        }
    }
    dc_options.insert(transfer_dc.id, transfer_dc.clone());
    Ok(TransferSession {
        data: Mutex::new(SessionData {
            home_dc,
            dc_options,
            peer_infos: HashMap::new(),
            updates_state: UpdatesState::default(),
        }),
        transfer_dc,
    })
}

/// Keeps a file-transfer pool pinned to Telegram's media-only address. The
/// sender refreshes DC configuration after connecting, but its generic session
/// implementation otherwise replaces the media address with a main RPC address.
struct TransferSession {
    data: Mutex<SessionData>,
    transfer_dc: DcOption,
}

impl TransferSession {
    fn data(&self) -> Result<MutexGuard<'_, SessionData>, TransferSessionError> {
        self.data.lock().map_err(|_| TransferSessionError)
    }

    fn pin_transfer_address(&self, option: &mut DcOption) {
        if option.id == self.transfer_dc.id {
            option.ipv4 = self.transfer_dc.ipv4;
            option.ipv6 = self.transfer_dc.ipv6;
        }
    }
}

#[derive(Debug)]
struct TransferSessionError;

impl fmt::Display for TransferSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transfer session lock is poisoned")
    }
}

impl std::error::Error for TransferSessionError {}

impl Session for TransferSession {
    type Error = TransferSessionError;

    fn home_dc_id(&self) -> Result<i32, Self::Error> {
        Ok(self.data()?.home_dc)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.data()?.home_dc = dc_id;
            Ok(())
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, Self::Error> {
        let mut option = self.data()?.dc_options.get(&dc_id).cloned();
        if let Some(option) = option.as_mut() {
            self.pin_transfer_address(option);
        }
        Ok(option)
    }

    fn set_dc_option(&self, option: &DcOption) -> BoxFuture<'_, Result<(), Self::Error>> {
        let mut option = option.clone();
        self.pin_transfer_address(&mut option);
        Box::pin(async move {
            self.data()?.dc_options.insert(option.id, option);
            Ok(())
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, Self::Error>> {
        Box::pin(async move { Ok(self.data()?.peer_infos.get(&peer).cloned()) })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), Self::Error>> {
        let peer = peer.clone();
        Box::pin(async move {
            self.data()?
                .peer_infos
                .entry(peer.id())
                .or_insert_with(|| peer.clone())
                .extend_info(&peer);
            Ok(())
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, Self::Error>> {
        Box::pin(async move { Ok(self.data()?.updates_state.clone()) })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut data = self.data()?;
            match update {
                UpdateState::All(updates_state) => data.updates_state = updates_state,
                UpdateState::Primary { pts, date, seq } => {
                    data.updates_state.pts = pts;
                    data.updates_state.date = date;
                    data.updates_state.seq = seq;
                }
                UpdateState::Secondary { qts } => data.updates_state.qts = qts,
                UpdateState::Channel { id, pts } => {
                    data.updates_state
                        .channels
                        .retain(|channel| channel.id != id);
                    data.updates_state.channels.push(ChannelState { id, pts });
                }
            }
            Ok(())
        })
    }
}

async fn transfer_dc_option(account: &AccountRuntime, dc_id: i32) -> Result<DcOption> {
    if let Some(option) = account
        .transfer_dc_options
        .lock()
        .await
        .get(&dc_id)
        .cloned()
    {
        return Ok(option);
    }

    let mut transfer = account
        .session
        .dc_option(dc_id)?
        .with_context(|| format!("Telegram did not provide DC {dc_id}"))?;
    let tl::enums::Config::Config(config) = account
        .client
        .invoke(&tl::functions::help::GetConfig {})
        .await
        .context("failed to refresh Telegram media endpoints")?;
    let (ipv4, ipv6) =
        choose_media_dc_options(&config.dc_options, dc_id, account.proxy_url.is_some());

    if let Some(option) = ipv4 {
        let port = u16::try_from(option.port)
            .ok()
            .filter(|port| *port != 0)
            .context("Telegram returned an invalid media DC port")?;
        transfer.ipv4 = SocketAddrV4::new(
            option
                .ip_address
                .parse::<Ipv4Addr>()
                .context("Telegram returned an invalid media DC IPv4 address")?,
            port,
        );
    }
    if let Some(option) = ipv6 {
        let port = u16::try_from(option.port)
            .ok()
            .filter(|port| *port != 0)
            .context("Telegram returned an invalid media DC port")?;
        transfer.ipv6 = SocketAddrV6::new(
            option
                .ip_address
                .parse::<Ipv6Addr>()
                .context("Telegram returned an invalid media DC IPv6 address")?,
            port,
            0,
            0,
        );
    }

    if ipv4.is_none() {
        tracing::warn!(
            account = %account.name,
            dc_id,
            "Telegram did not advertise an IPv4 media-only endpoint; using the main DC endpoint"
        );
    }

    account
        .transfer_dc_options
        .lock()
        .await
        .insert(dc_id, transfer.clone());
    Ok(transfer)
}

fn choose_media_dc_options(
    options: &[tl::enums::DcOption],
    dc_id: i32,
    proxied: bool,
) -> (Option<&tl::types::DcOption>, Option<&tl::types::DcOption>) {
    let candidates: Vec<_> = options
        .iter()
        .map(|option| {
            let tl::enums::DcOption::Option(option) = option;
            option
        })
        .filter(|option| {
            option.id == dc_id && option.media_only && !option.tcpo_only && !option.cdn
        })
        .collect();
    let rank = |option: &&tl::types::DcOption| u8::from(proxied && !option.r#static);
    let ipv4 = candidates
        .iter()
        .copied()
        .filter(|option| !option.ipv6)
        .min_by_key(rank);
    let ipv6 = candidates
        .iter()
        .copied()
        .filter(|option| option.ipv6)
        .min_by_key(rank);
    (ipv4, ipv6)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FetchError {
    Wait { kind: String, seconds: u32 },
    FileReferenceExpired,
    Migrate(i32),
    AuthorizationRevoked,
    Transient(String),
    Permanent(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wait { kind, seconds } => write!(formatter, "{kind} for {seconds} seconds"),
            Self::FileReferenceExpired => write!(formatter, "file reference expired"),
            Self::Migrate(dc) => write!(formatter, "file migrated to DC {dc}"),
            Self::AuthorizationRevoked => write!(formatter, "account authorization was revoked"),
            Self::Transient(error) | Self::Permanent(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for FetchError {}

pub async fn fetch_file_range(
    client: &Client,
    descriptor: &MediaDescriptor,
    offset: u64,
) -> Result<Vec<u8>, FetchError> {
    let mut result = Vec::with_capacity(crate::media::CHUNK_SIZE as usize);
    for request_offset in file_request_offsets(offset, crate::media::CHUNK_SIZE) {
        let bytes = fetch_file_part(client, descriptor, request_offset).await?;
        let received = bytes.len();
        let remaining = crate::media::CHUNK_SIZE as usize - result.len();
        result.extend_from_slice(&bytes[..received.min(remaining)]);
        if received < FILE_REQUEST_LIMIT as usize
            || result.len() == crate::media::CHUNK_SIZE as usize
        {
            break;
        }
    }
    Ok(result)
}

pub async fn fetch_file_part(
    client: &Client,
    descriptor: &MediaDescriptor,
    offset: u64,
) -> Result<Vec<u8>, FetchError> {
    let request = tl::functions::upload::GetFile {
        precise: true,
        cdn_supported: false,
        location: descriptor.location.clone(),
        offset: offset as i64,
        limit: FILE_REQUEST_LIMIT as i32,
    };
    match client.invoke_in_dc(descriptor.dc_id, &request).await {
        Ok(tl::enums::upload::File::File(file)) => Ok(file.bytes),
        Ok(tl::enums::upload::File::CdnRedirect(_)) => Err(FetchError::Permanent(
            "Telegram returned a CDN redirect despite cdn_supported=false".to_owned(),
        )),
        Err(error) => Err(classify_invocation(error)),
    }
}

fn file_request_offsets(offset: u64, length: u32) -> impl Iterator<Item = u64> {
    let request_count = length.div_ceil(FILE_REQUEST_LIMIT);
    (0..request_count).map(move |index| offset + u64::from(index * FILE_REQUEST_LIMIT))
}

pub async fn fetch_hashes(
    client: &Client,
    descriptor: &MediaDescriptor,
    offset: u64,
) -> Result<Vec<tl::enums::FileHash>, FetchError> {
    client
        .invoke_in_dc(
            descriptor.dc_id,
            &tl::functions::upload::GetFileHashes {
                location: descriptor.location.clone(),
                offset: offset as i64,
            },
        )
        .await
        .map_err(classify_invocation)
}

pub fn classify_invocation(error: InvocationError) -> FetchError {
    match error {
        InvocationError::Rpc(error)
            if matches!(error.name.as_str(), "FLOOD_WAIT" | "FLOOD_PREMIUM_WAIT") =>
        {
            FetchError::Wait {
                kind: error.name,
                seconds: error.value.unwrap_or(1).max(1),
            }
        }
        InvocationError::Rpc(error)
            if matches!(
                error.name.as_str(),
                "FILE_REFERENCE_EXPIRED" | "FILE_REFERENCE_INVALID"
            ) =>
        {
            FetchError::FileReferenceExpired
        }
        InvocationError::Rpc(error) if error.name == "FILE_MIGRATE" => error
            .value
            .and_then(|dc| i32::try_from(dc).ok())
            .filter(|dc| *dc > 0)
            .map(FetchError::Migrate)
            .unwrap_or_else(|| {
                FetchError::Permanent("Telegram returned an invalid FILE_MIGRATE target".to_owned())
            }),
        InvocationError::Rpc(error) if is_revoked_authorization_name(&error.name) => {
            FetchError::AuthorizationRevoked
        }
        InvocationError::Io(error) => FetchError::Transient(error.to_string()),
        InvocationError::Transport(error) => FetchError::Transient(error.to_string()),
        InvocationError::Dropped => FetchError::Transient("connection dropped".to_owned()),
        InvocationError::Rpc(error) if (500..600).contains(&error.code.unsigned_abs()) => {
            FetchError::Transient(error.to_string())
        }
        other => FetchError::Permanent(other.to_string()),
    }
}

impl AccountRuntime {
    pub async fn invalidate_transfer_dc(&self, dc_id: i32) {
        self.transfer_dc_options.lock().await.remove(&dc_id);
    }

    pub async fn ensure_dc_authorized(&self, dc_id: i32) -> Result<()> {
        let home_dc = self.session.home_dc_id()?;
        if dc_id == home_dc {
            return Ok(());
        }
        {
            let authorized = self.authorized_dcs.lock().await;
            if authorized.contains(&dc_id) {
                return Ok(());
            }
        }

        let exported = self
            .client
            .invoke(&tl::functions::auth::ExportAuthorization { dc_id })
            .await
            .with_context(|| format!("failed to export authorization to DC {dc_id}"))?;
        let tl::enums::auth::ExportedAuthorization::Authorization(exported) = exported;
        self.client
            .invoke_in_dc(
                dc_id,
                &tl::functions::auth::ImportAuthorization {
                    id: exported.id,
                    bytes: exported.bytes,
                },
            )
            .await
            .with_context(|| format!("failed to import authorization into DC {dc_id}"))?;
        self.authorized_dcs.lock().await.insert(dc_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grammers_tl_types::Identifiable;

    fn raw_dc_option(
        ip_address: &str,
        media_only: bool,
        static_address: bool,
    ) -> tl::enums::DcOption {
        tl::enums::DcOption::Option(tl::types::DcOption {
            ipv6: false,
            media_only,
            tcpo_only: false,
            cdn: false,
            r#static: static_address,
            this_port_only: false,
            id: 2,
            ip_address: ip_address.to_owned(),
            port: 443,
            secret: None,
        })
    }

    #[test]
    fn requests_each_checkpoint_chunk_in_one_round_trip() {
        assert_eq!(FILE_REQUEST_LIMIT, 1024 * 1024);
        assert_eq!(
            file_request_offsets(4 * 1024 * 1024, crate::media::CHUNK_SIZE).collect::<Vec<_>>(),
            vec![4 * 1024 * 1024]
        );
    }

    #[test]
    fn retries_negative_telegram_server_timeouts() {
        let error = InvocationError::Rpc(grammers_mtsender::RpcError {
            code: -503,
            name: "Timeout".to_owned(),
            value: None,
            caused_by: Some(tl::functions::upload::GetFile::CONSTRUCTOR_ID),
        });

        assert!(matches!(
            classify_invocation(error),
            FetchError::Transient(message)
                if message.contains("-503") && message.contains("upload.getFile")
        ));
    }

    #[test]
    fn recognizes_additional_revoked_session_errors() {
        let error = InvocationError::Rpc(grammers_mtsender::RpcError {
            code: 401,
            name: "SESSION_EXPIRED".to_owned(),
            value: None,
            caused_by: None,
        });

        assert_eq!(classify_invocation(error), FetchError::AuthorizationRevoked);
    }

    #[test]
    fn normalizes_invalid_waits_and_migration_targets() {
        let wait = InvocationError::Rpc(grammers_mtsender::RpcError {
            code: 420,
            name: "FLOOD_WAIT".to_owned(),
            value: Some(0),
            caused_by: None,
        });
        assert_eq!(
            classify_invocation(wait),
            FetchError::Wait {
                kind: "FLOOD_WAIT".to_owned(),
                seconds: 1,
            }
        );

        let migrate = InvocationError::Rpc(grammers_mtsender::RpcError {
            code: 303,
            name: "FILE_MIGRATE".to_owned(),
            value: None,
            caused_by: None,
        });
        assert!(matches!(
            classify_invocation(migrate),
            FetchError::Permanent(message) if message.contains("FILE_MIGRATE")
        ));
    }

    #[test]
    fn proxy_transfers_prefer_static_media_only_endpoints() {
        let options = vec![
            raw_dc_option("149.154.167.50", false, true),
            raw_dc_option("149.154.167.51", true, false),
            raw_dc_option("149.154.167.52", true, true),
        ];

        let (ipv4, _) = choose_media_dc_options(&options, 2, true);
        assert_eq!(ipv4.unwrap().ip_address, "149.154.167.52");
    }

    #[tokio::test]
    async fn transfer_session_keeps_media_address_when_config_refreshes() {
        let mut data = SessionData::default();
        let mut transfer_dc = data.dc_options.get(&2).unwrap().clone();
        transfer_dc.ipv4 = "149.154.167.52:443".parse().unwrap();
        data.dc_options.insert(2, transfer_dc.clone());
        let session = TransferSession {
            data: Mutex::new(data),
            transfer_dc: transfer_dc.clone(),
        };
        let mut refreshed = transfer_dc.clone();
        refreshed.ipv4 = "149.154.167.40:443".parse().unwrap();
        refreshed.auth_key = Some([7; 256]);

        session.set_dc_option(&refreshed).await.unwrap();

        let stored = session.dc_option(2).unwrap().unwrap();
        assert_eq!(stored.ipv4, transfer_dc.ipv4);
        assert_eq!(stored.auth_key, Some([7; 256]));
    }
}
