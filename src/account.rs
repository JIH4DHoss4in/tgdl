use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use grammers_client::client::{ClientConfiguration, NoRetries};
use grammers_client::peer::Peer;
use grammers_client::{Client, SignInError};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use grammers_session::types::{DcOption, PeerRef};
use grammers_session::updates::UpdatesLike;
use tokio::sync::{Mutex, mpsc};

use crate::config::{
    AccountConfig, Config, SourceAccess, SourceConfig, SourcePeerKind, normalize_channel,
};
use crate::security;
use crate::ui::{info_text, print_wrapped};
use crate::util::{prompt, prompt_password};

pub struct AccountRuntime {
    pub name: String,
    pub client: Client,
    pub session: Arc<SqliteSession>,
    premium: AtomicBool,
    pub user_id: i64,
    pub display_name: String,
    pub api_id: i32,
    pub proxy_url: Option<String>,
    pub(crate) authorized_dcs: Mutex<HashSet<i32>>,
    pub(crate) transfer_dc_options: Mutex<HashMap<i32, DcOption>>,
    source_refs: Mutex<HashMap<i64, PeerRef>>,
    updates: Mutex<Option<mpsc::UnboundedReceiver<UpdatesLike>>>,
}

#[derive(Clone, Debug)]
pub struct ResolvedSource {
    pub peer: PeerRef,
    pub title: String,
    pub peer_kind: SourcePeerKind,
}

#[derive(Debug)]
pub struct SourceAccessError {
    account: String,
    source: String,
}

impl std::fmt::Display for SourceAccessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "account '{}' is not joined to or cannot access '{}'",
            self.account, self.source
        )
    }
}

impl std::error::Error for SourceAccessError {}

impl AccountRuntime {
    pub async fn connect(
        account: &AccountConfig,
        config: &Config,
        session_path: &Path,
    ) -> Result<Arc<Self>> {
        let session = Arc::new(
            SqliteSession::open(session_path)
                .await
                .with_context(|| format!("failed to open session for {}", account.name))?,
        );
        let proxy_url = config.proxy_for_account(account).map(str::to_owned);
        let SenderPool {
            runner,
            handle,
            updates,
        } = SenderPool::with_configuration(
            Arc::clone(&session),
            config.api_id,
            crate::proxy::connection_params(proxy_url.as_deref()),
        );
        let client = Client::with_configuration(
            handle,
            ClientConfiguration {
                retry_policy: Box::new(NoRetries),
                auto_cache_peers: true,
            },
        );
        tokio::spawn(runner.run());

        if !client.is_authorized().await? {
            client.disconnect();
            bail!(
                "account '{}' is not authorized; re-add it from the console account manager",
                account.name
            );
        }
        let me = client.get_me().await?;
        let premium = user_is_premium(&me);
        let runtime = Arc::new(Self {
            name: account.name.clone(),
            client,
            session,
            premium: AtomicBool::new(premium),
            user_id: me.id().bare_id_unchecked(),
            display_name: me.full_name(),
            api_id: config.api_id,
            proxy_url,
            authorized_dcs: Mutex::new(HashSet::new()),
            transfer_dc_options: Mutex::new(HashMap::new()),
            source_refs: Mutex::new(HashMap::new()),
            updates: Mutex::new(Some(updates)),
        });
        if session_path.exists() {
            security::restrict_file(session_path)?;
        }
        Ok(runtime)
    }

    pub async fn take_updates(&self) -> Result<mpsc::UnboundedReceiver<UpdatesLike>> {
        self.updates
            .lock()
            .await
            .take()
            .with_context(|| format!("account '{}' update stream is already in use", self.name))
    }

    pub fn is_premium(&self) -> bool {
        self.premium.load(Ordering::Relaxed)
    }

    pub async fn refresh_premium(&self) -> Result<bool> {
        let premium = user_is_premium(&self.client.get_me().await?);
        self.premium.store(premium, Ordering::Relaxed);
        Ok(premium)
    }

    pub async fn resolve_source(&self, source: &SourceConfig) -> Result<ResolvedSource> {
        if let Some(peer_id) = source.peer_id
            && let Some(peer) = self.source_refs.lock().await.get(&peer_id).cloned()
        {
            return Ok(ResolvedSource {
                peer,
                title: source.title.clone(),
                peer_kind: source.peer_kind,
            });
        }

        let resolved = match source.access {
            SourceAccess::Public => {
                let username = source
                    .username
                    .as_deref()
                    .map(normalize_channel)
                    .context("public source is missing its username")?;
                let peer = match self.client.resolve_username(&username).await? {
                    Some(peer) => peer,
                    None if source.peer_id.is_some() => {
                        let expected = source.peer_id.unwrap();
                        let mut dialogs = self.client.iter_dialogs();
                        let mut found = None;
                        while let Some(dialog) = dialogs.next().await? {
                            if dialog.peer_id().bot_api_dialog_id() == Some(expected)
                                && matches!(dialog.peer(), Peer::Group(_) | Peer::Channel(_))
                            {
                                found = Some(dialog.peer().clone());
                                break;
                            }
                        }
                        found.with_context(|| {
                            format!("public channel or group @{username} was not found")
                        })?
                    }
                    None => bail!("public channel or group @{username} was not found"),
                };
                resolved_from_peer(&peer, SourceAccess::Public).await?
            }
            SourceAccess::Private => {
                let expected = source
                    .peer_id
                    .context("private source is missing its peer ID")?;
                let mut dialogs = self.client.iter_dialogs();
                let mut found = None;
                while let Some(dialog) = dialogs.next().await? {
                    if dialog.peer_id().bot_api_dialog_id() == Some(expected)
                        && matches!(dialog.peer(), Peer::Group(_) | Peer::Channel(_))
                    {
                        found = Some(ResolvedSource {
                            peer: dialog.peer_ref(),
                            title: dialog
                                .peer()
                                .name()
                                .unwrap_or(source.title.as_str())
                                .to_owned(),
                            peer_kind: peer_kind(dialog.peer()),
                        });
                        break;
                    }
                }
                found.ok_or_else(|| {
                    anyhow::Error::new(SourceAccessError {
                        account: self.name.clone(),
                        source: source.display_name(),
                    })
                })?
            }
        };

        let peer_id = resolved
            .peer
            .id
            .bot_api_dialog_id()
            .context("source peer does not have a persistent ID")?;
        if let Some(expected) = source.peer_id
            && peer_id != expected
        {
            bail!(
                "source '{}' now resolves to peer {peer_id}, expected {expected}",
                source.display_name()
            );
        }
        self.source_refs.lock().await.insert(peer_id, resolved.peer);
        Ok(resolved)
    }

    pub async fn resolve_job_source(
        &self,
        config: &Config,
        peer_id: i64,
    ) -> Result<ResolvedSource> {
        if let Some(source) = config.source_for_peer(peer_id) {
            return self.resolve_source(source).await;
        }
        // Legacy single-channel configurations initially have no persisted peer
        // ID. Resolve only those public sources and match the accepted job.
        for source in config
            .sources
            .iter()
            .filter(|source| source.peer_id.is_none())
        {
            let resolved = self.resolve_source(source).await?;
            if resolved.peer.id.bot_api_dialog_id() == Some(peer_id) {
                return Ok(resolved);
            }
        }
        bail!("job references a channel or group that is no longer configured")
    }

    pub async fn available_private_sources(&self) -> Result<Vec<SourceConfig>> {
        let mut sources = Vec::new();
        let mut dialogs = self.client.iter_dialogs();
        while let Some(dialog) = dialogs.next().await? {
            if !matches!(dialog.peer(), Peer::Group(_) | Peer::Channel(_)) {
                continue;
            }
            let peer_id = dialog
                .peer_id()
                .bot_api_dialog_id()
                .context("dialog does not have a persistent peer ID")?;
            sources.push(SourceConfig {
                title: dialog
                    .peer()
                    .name()
                    .unwrap_or("Unnamed channel or group")
                    .to_owned(),
                access: SourceAccess::Private,
                peer_kind: peer_kind(dialog.peer()),
                username: None,
                peer_id: Some(peer_id),
            });
        }
        sources.sort_by_key(|source| source.title.to_ascii_lowercase());
        sources.dedup_by_key(|source| source.peer_id);
        Ok(sources)
    }
}

impl Drop for AccountRuntime {
    fn drop(&mut self) {
        self.client.disconnect();
    }
}

pub async fn authorize_account(
    name: &str,
    config: &Config,
    session_path: &Path,
    proxy_override: Option<&str>,
) -> Result<AccountConfig> {
    if let Some(parent) = session_path.parent() {
        security::create_private_dir(parent)?;
    }
    let session = Arc::new(SqliteSession::open(session_path).await?);
    let effective_proxy = proxy_override.or(config.proxy_url.as_deref());
    let SenderPool {
        runner,
        handle,
        updates: _,
    } = SenderPool::with_configuration(
        Arc::clone(&session),
        config.api_id,
        crate::proxy::connection_params(effective_proxy),
    );
    let client = Client::new(handle);
    tokio::spawn(runner.run());

    if !client.is_authorized().await? {
        print_wrapped(&info_text(format!(
            "Authorizing '{name}' through Telegram's user API."
        )));
        let phone = prompt("Phone number in international format: ")?;
        let token = client
            .request_login_code(&phone, &config.api_hash)
            .await
            .context("Telegram rejected the login-code request")?;
        let code = prompt("Login code: ")?;
        match client.sign_in(&token, code.trim()).await {
            Ok(_) => {}
            Err(SignInError::PasswordRequired(password_token)) => {
                let hint = password_token.hint().unwrap_or("no hint");
                let password = prompt_password(&format!("2FA password ({hint}): "))?;
                client
                    .check_password(password_token, password.trim())
                    .await
                    .context("Telegram rejected the 2FA password")?;
            }
            Err(error) => return Err(error).context("Telegram sign-in failed"),
        }
    }

    let me = client.get_me().await?;
    let account = AccountConfig {
        name: name.to_owned(),
        enabled: true,
        monitor_priority: config
            .accounts
            .iter()
            .map(|account| account.monitor_priority)
            .max()
            .unwrap_or(0)
            .saturating_add(1),
        user_id: Some(me.id().bare_id_unchecked()),
        display_name: Some(me.full_name()),
        premium: user_is_premium(&me),
        premium_checked_at: Some(Utc::now()),
        proxy_url: proxy_override.map(str::to_owned),
    };
    client.disconnect();
    security::restrict_file(session_path)?;
    Ok(account)
}

pub async fn inspect_authorized_session(
    name: &str,
    config: &Config,
    session_path: &Path,
    proxy_override: Option<&str>,
) -> Result<AccountConfig> {
    let placeholder = AccountConfig {
        name: name.to_owned(),
        enabled: true,
        monitor_priority: next_monitor_priority(config),
        user_id: None,
        display_name: None,
        premium: false,
        premium_checked_at: None,
        proxy_url: proxy_override.map(str::to_owned),
    };
    let runtime = AccountRuntime::connect(&placeholder, config, session_path)
        .await
        .context("the imported session could not be authorized by Telegram")?;
    let account = AccountConfig {
        name: name.to_owned(),
        enabled: true,
        monitor_priority: placeholder.monitor_priority,
        user_id: Some(runtime.user_id),
        display_name: Some(runtime.display_name.clone()),
        premium: runtime.is_premium(),
        premium_checked_at: Some(Utc::now()),
        proxy_url: proxy_override.map(str::to_owned),
    };
    drop(runtime);
    security::restrict_file(session_path)?;
    Ok(account)
}

fn next_monitor_priority(config: &Config) -> u32 {
    config
        .accounts
        .iter()
        .map(|account| account.monitor_priority)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

pub async fn validate_source(
    runtime: &AccountRuntime,
    source: &SourceConfig,
) -> Result<SourceConfig> {
    let resolved = runtime.resolve_source(source).await?;
    let mut source = source.clone();
    source.peer_id = resolved.peer.id.bot_api_dialog_id();
    source.title = resolved.title;
    source.peer_kind = resolved.peer_kind;
    Ok(source)
}

pub fn user_is_premium(user: &grammers_client::peer::User) -> bool {
    match &user.raw {
        grammers_tl_types::enums::User::User(raw) => raw.premium,
        grammers_tl_types::enums::User::Empty(_) => false,
    }
}

const REVOKED_AUTH_ERRORS: [&str; 7] = [
    "AUTH_KEY_UNREGISTERED",
    "AUTH_KEY_DUPLICATED",
    "AUTH_KEY_INVALID",
    "SESSION_REVOKED",
    "SESSION_EXPIRED",
    "USER_DEACTIVATED",
    "USER_DEACTIVATED_BAN",
];

pub fn is_revoked_authorization_name(name: &str) -> bool {
    REVOKED_AUTH_ERRORS.contains(&name)
}

pub fn contains_revoked_authorization_error(message: &str) -> bool {
    REVOKED_AUTH_ERRORS
        .iter()
        .any(|name| message.contains(name))
}

async fn resolved_from_peer(peer: &Peer, access: SourceAccess) -> Result<ResolvedSource> {
    if !matches!(peer, Peer::Group(_) | Peer::Channel(_)) {
        bail!("source resolves to a user, not a channel or group");
    }
    let reference = peer
        .to_ref()
        .await
        .map_err(|error| anyhow!(error.to_string()))?
        .context("could not create an access reference for the channel or group")?;
    let title = peer
        .name()
        .or_else(|| {
            if access == SourceAccess::Public {
                peer.username()
            } else {
                None
            }
        })
        .unwrap_or("Unnamed channel or group")
        .to_owned();
    Ok(ResolvedSource {
        peer: reference,
        title,
        peer_kind: peer_kind(peer),
    })
}

fn peer_kind(peer: &Peer) -> SourcePeerKind {
    match peer {
        Peer::Channel(_) => SourcePeerKind::Channel,
        Peer::Group(_) => SourcePeerKind::Group,
        Peer::User(_) => unreachable!("users are not valid monitored sources"),
    }
}
