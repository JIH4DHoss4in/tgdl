use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::security;

pub const DEFAULT_MAX_ACTIVE_FILES: usize = 2;
pub const DEFAULT_MAX_LANES: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub api_id: i32,
    pub api_hash: String,
    /// Legacy single-public-channel setting. It is accepted when loading old
    /// configurations and retained as a compatibility mirror when possible.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub media_filter: MediaFilter,
    pub output_dir: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    pub monitor_primary: String,
    #[serde(default)]
    pub monitor_account_downloads: bool,
    #[serde(default = "default_active_files")]
    pub max_active_files: usize,
    #[serde(default = "default_lanes")]
    pub max_download_lanes_per_account: usize,
    /// Perform Telegram SHA-256 range verification after the transfer.
    ///
    /// This requires a second full read of the downloaded file and is disabled
    /// by default because MTProto already authenticates transport messages.
    #[serde(default)]
    pub verify_file_hashes: bool,
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaFilter {
    /// Lowercase filename extensions without a leading dot. An empty list
    /// accepts every extension.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_size: Option<u64>,
}

impl MediaFilter {
    pub fn accepts(&self, filename: &str, size: Option<u64>) -> bool {
        let extension_matches = self.extensions.is_empty() || {
            let filename = filename.to_ascii_lowercase();
            self.extensions.iter().any(|extension| {
                filename.ends_with(&format!(".{}", extension.to_ascii_lowercase()))
            })
        };
        let size_matches = match size {
            Some(size) => {
                self.min_size.is_none_or(|minimum| size >= minimum)
                    && self.max_size.is_none_or(|maximum| size <= maximum)
            }
            None => self.min_size.is_none() && self.max_size.is_none(),
        };
        extension_matches && size_matches
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .min_size
            .zip(self.max_size)
            .is_some_and(|(minimum, maximum)| minimum > maximum)
        {
            bail!("media filter minimum size must not exceed its maximum size");
        }
        let mut unique = HashSet::new();
        for extension in &self.extensions {
            let normalized = normalize_extension(extension)?;
            if !unique.insert(normalized) {
                bail!("duplicate media filter extension: {extension}");
            }
        }
        Ok(())
    }
}

pub fn normalize_extension(extension: &str) -> Result<String> {
    let normalized = extension
        .trim()
        .trim_start_matches("*.")
        .trim_start_matches('.')
        .to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 32
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
    {
        bail!("file extensions must be 1-32 letters, numbers, '.', '_', '+', or '-'");
    }
    Ok(normalized)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceAccess {
    Public,
    Private,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourcePeerKind {
    Channel,
    Group,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceConfig {
    pub title: String,
    pub access: SourceAccess,
    pub peer_kind: SourcePeerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Telegram Bot API-style peer ID. This embeds whether the source is a
    /// channel/supergroup or a legacy small group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<i64>,
}

impl SourceConfig {
    pub fn public(username: impl Into<String>) -> Self {
        let username = normalize_channel(&username.into());
        Self {
            title: format!("@{username}"),
            access: SourceAccess::Public,
            peer_kind: SourcePeerKind::Channel,
            username: Some(username),
            peer_id: None,
        }
    }

    pub fn display_name(&self) -> String {
        self.username
            .as_deref()
            .map(|username| format!("@{username}"))
            .unwrap_or_else(|| self.title.clone())
    }

    pub const fn access_label(&self) -> &'static str {
        match self.access {
            SourceAccess::Public => "PUBLIC",
            SourceAccess::Private => "PRIVATE",
        }
    }

    pub const fn kind_label(&self) -> &'static str {
        match self.peer_kind {
            SourcePeerKind::Channel => "CHANNEL",
            SourcePeerKind::Group => "GROUP",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountConfig {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub monitor_priority: u32,
    #[serde(default)]
    pub user_id: Option<i64>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub premium: bool,
    #[serde(default)]
    pub premium_checked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
}

fn default_active_files() -> usize {
    DEFAULT_MAX_ACTIVE_FILES
}

fn default_lanes() -> usize {
    DEFAULT_MAX_LANES
}

fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut config = Self::read(path)?;
        config.apply_environment_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// Load exactly what is persisted on disk. Configuration editors must use
    /// this method so process-only environment overrides are never written back
    /// to `config.toml` as a side effect of an unrelated change.
    pub fn load_stored(path: &Path) -> Result<Self> {
        let config = Self::read(path)?;
        config.validate()?;
        Ok(config)
    }

    /// Apply process-only environment overrides to an already loaded config.
    pub fn effective(&self) -> Result<Self> {
        let mut config = self.clone();
        config.apply_environment_overrides()?;
        config.validate()?;
        Ok(config)
    }

    fn read(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let mut config: Self =
            toml::from_str(&text).with_context(|| format!("invalid TOML in {}", path.display()))?;
        config.migrate_legacy_channel();
        config.normalize_loaded_values()?;
        Ok(config)
    }

    fn apply_environment_overrides(&mut self) -> Result<()> {
        let api_id = optional_environment_value("TGDL_API_ID")?;
        let api_hash = optional_environment_value("TGDL_API_HASH")?;
        let proxy = std::env::var_os("TGDL_PROXY_URL")
            .is_some()
            .then(proxy_url_from_env)
            .transpose()?;
        self.apply_overrides(api_id.as_deref(), api_hash, proxy)
    }

    fn apply_overrides(
        &mut self,
        api_id: Option<&str>,
        api_hash: Option<String>,
        proxy: Option<Option<String>>,
    ) -> Result<()> {
        if let Some(value) = api_id {
            self.api_id = value.parse().context("TGDL_API_ID must be an integer")?;
        }
        if let Some(value) = api_hash {
            self.api_hash = value;
        }
        if let Some(value) = proxy {
            self.proxy_url = value;
        }
        Ok(())
    }

    fn normalize_loaded_values(&mut self) -> Result<()> {
        self.channel = normalize_channel(&self.channel);
        for source in &mut self.sources {
            source.title = source.title.trim().to_owned();
            if source.access == SourceAccess::Public {
                source.username = source.username.as_deref().map(normalize_channel);
            }
        }
        self.media_filter.extensions = self
            .media_filter
            .extensions
            .iter()
            .map(|extension| normalize_extension(extension))
            .collect::<Result<_>>()?;
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let text = toml::to_string_pretty(self).context("failed to serialize configuration")?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent).with_context(|| {
            format!("failed to create a temporary file in {}", parent.display())
        })?;
        temporary
            .write_all(text.as_bytes())
            .context("failed to write temporary configuration")?;
        temporary
            .as_file()
            .sync_all()
            .context("failed to sync temporary configuration")?;
        security::restrict_file(temporary.path())?;
        temporary.persist(path).map_err(|error| {
            anyhow::Error::new(error.error).context(format!(
                "failed to replace configuration {}",
                path.display()
            ))
        })?;
        security::restrict_file(path)?;
        #[cfg(unix)]
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .with_context(|| format!("failed to sync {}", parent.display()))?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.api_id <= 0 {
            bail!("api_id must be a positive integer");
        }
        if self.api_hash.trim().is_empty() {
            bail!("api_hash must not be empty");
        }
        if self.sources.is_empty() {
            bail!("at least one monitored channel or group must be configured");
        }
        let mut source_peer_ids = HashSet::new();
        let mut source_usernames = HashSet::new();
        for source in &self.sources {
            if source.title.trim().is_empty() {
                bail!("source titles must not be empty");
            }
            match source.access {
                SourceAccess::Public => {
                    let username = source
                        .username
                        .as_deref()
                        .map(normalize_channel)
                        .filter(|username| {
                            !username.is_empty()
                                && username
                                    .bytes()
                                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                        })
                        .context(
                            "public sources must contain a username, not a message or invite link",
                        )?;
                    if !source_usernames.insert(username.to_ascii_lowercase()) {
                        bail!("duplicate public source @{username}");
                    }
                }
                SourceAccess::Private => {
                    if source.peer_id.is_none() {
                        bail!("private source '{}' is missing its peer ID", source.title);
                    }
                }
            }
            if let Some(peer_id) = source.peer_id {
                let peer = grammers_session::types::PeerId::from_bot_api_dialog_id(peer_id)
                    .with_context(|| format!("source '{}' has an invalid peer ID", source.title))?;
                if matches!(peer.kind(), grammers_session::types::PeerKind::User) {
                    bail!(
                        "source '{}' resolves to a user, not a group or channel",
                        source.title
                    );
                }
                if !source_peer_ids.insert(peer_id) {
                    bail!("source '{}' is configured more than once", source.title);
                }
            }
        }
        self.media_filter.validate()?;
        if let Some(proxy_url) = self.proxy_url.as_deref() {
            crate::proxy::validate_proxy_url(proxy_url)?;
        }
        if self.max_active_files == 0 || self.max_active_files > 2 {
            bail!("max_active_files must be between 1 and 2");
        }
        if !(1..=8).contains(&self.max_download_lanes_per_account) {
            bail!("max_download_lanes_per_account must be between 1 and 8");
        }
        if self.accounts.is_empty() {
            bail!("at least one account must be configured");
        }
        let mut names = HashSet::new();
        for account in &self.accounts {
            validate_account_name(&account.name)?;
            if let Some(proxy_url) = account.proxy_url.as_deref() {
                crate::proxy::validate_proxy_url(proxy_url)
                    .with_context(|| format!("invalid proxy for account '{}'", account.name))?;
            }
            if !names.insert(account.name.to_ascii_lowercase()) {
                bail!("duplicate account name: {}", account.name);
            }
        }
        let monitor = self
            .accounts
            .iter()
            .find(|account| account.name == self.monitor_primary)
            .context("monitor_primary does not name a configured account")?;
        if !monitor.enabled {
            bail!("monitor_primary must name an enabled account");
        }
        if !self.accounts.iter().any(|a| a.enabled) {
            bail!("at least one account must be enabled");
        }
        Ok(())
    }

    pub fn account(&self, name: &str) -> Option<&AccountConfig> {
        self.accounts.iter().find(|account| account.name == name)
    }

    pub fn account_mut(&mut self, name: &str) -> Option<&mut AccountConfig> {
        self.accounts
            .iter_mut()
            .find(|account| account.name == name)
    }

    pub fn proxy_for_account<'a>(&'a self, account: &'a AccountConfig) -> Option<&'a str> {
        account.proxy_url.as_deref().or(self.proxy_url.as_deref())
    }

    pub fn source_for_peer(&self, peer_id: i64) -> Option<&SourceConfig> {
        self.sources
            .iter()
            .find(|source| source.peer_id == Some(peer_id))
    }

    fn migrate_legacy_channel(&mut self) {
        if self.sources.is_empty() {
            let username = normalize_channel(&self.channel);
            if !username.is_empty() {
                self.channel = username.clone();
                self.sources.push(SourceConfig::public(username));
            }
        }
    }
}

pub fn proxy_url_from_env() -> Result<Option<String>> {
    let Some(value) = optional_environment_value("TGDL_PROXY_URL")?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if value.contains("://") {
        crate::proxy::validate_proxy_url(&value)?;
        Ok(Some(value.trim_end_matches('/').to_owned()))
    } else {
        Ok(Some(crate::proxy::parse_proxy_input(
            crate::proxy::ProxyKind::Socks5,
            &value,
        )?))
    }
}

fn optional_environment_value(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("{name} must contain valid Unicode text")
        }
    }
}

pub fn normalize_channel(channel: &str) -> String {
    channel
        .trim()
        .trim_start_matches("https://t.me/")
        .trim_start_matches("http://t.me/")
        .trim_start_matches('@')
        .trim_matches('/')
        .to_owned()
}

pub fn validate_account_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 48
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        bail!("account names must be 1-48 ASCII letters, numbers, '-' or '_'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config {
            api_id: 1,
            api_hash: "hash".into(),
            channel: String::new(),
            sources: vec![SourceConfig {
                title: "Drop box".into(),
                access: SourceAccess::Public,
                peer_kind: SourcePeerKind::Channel,
                username: Some("drop_box".into()),
                peer_id: Some(-1_000_000_000_123),
            }],
            media_filter: MediaFilter::default(),
            output_dir: PathBuf::from("downloads"),
            proxy_url: None,
            monitor_primary: "main".into(),
            monitor_account_downloads: false,
            max_active_files: 2,
            max_download_lanes_per_account: 8,
            verify_file_hashes: false,
            accounts: vec![AccountConfig {
                name: "main".into(),
                enabled: true,
                monitor_priority: 1,
                user_id: None,
                display_name: None,
                premium: true,
                premium_checked_at: None,
                proxy_url: None,
            }],
        }
    }

    #[test]
    fn channel_normalization() {
        assert_eq!(normalize_channel("@drop_box"), "drop_box");
        assert_eq!(normalize_channel("https://t.me/drop_box/"), "drop_box");
        let mut config = valid_config();
        config.sources[0].username = Some("https://t.me/drop_box/123".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn legacy_channel_is_migrated_to_sources() -> Result<()> {
        let text = r#"
            api_id = 1
            api_hash = "hash"
            channel = "@legacy_drop"
            output_dir = "downloads"
            monitor_primary = "main"

            [[accounts]]
            name = "main"
            enabled = true
        "#;
        let mut parsed: Config = toml::from_str(text)?;
        parsed.migrate_legacy_channel();
        assert_eq!(parsed.sources, vec![SourceConfig::public("legacy_drop")]);
        assert_eq!(parsed.channel, "legacy_drop");
        assert!(!parsed.monitor_account_downloads);
        Ok(())
    }

    #[test]
    fn file_hash_verification_defaults_to_disabled() -> Result<()> {
        let serialized = toml::to_string(&valid_config())?;
        let without_setting = serialized
            .lines()
            .filter(|line| !line.starts_with("verify_file_hashes"))
            .collect::<Vec<_>>()
            .join("\n");
        let parsed: Config = toml::from_str(&without_setting)?;

        assert!(!parsed.verify_file_hashes);
        Ok(())
    }

    #[test]
    fn media_filters_accept_extensions_and_inclusive_size_ranges() -> Result<()> {
        let filter = MediaFilter {
            extensions: vec![
                normalize_extension("*.TAR.GZ")?,
                normalize_extension(".rar")?,
            ],
            min_size: Some(100),
            max_size: Some(200),
        };
        filter.validate()?;

        assert!(filter.accepts("backup.TAR.GZ", Some(100)));
        assert!(filter.accepts("backup.rar", Some(200)));
        assert!(!filter.accepts("backup.zip", Some(150)));
        assert!(!filter.accepts("backup.rar", Some(99)));
        assert!(!filter.accepts("backup.rar", None));
        assert!(MediaFilter::default().accepts("anything.bin", None));
        Ok(())
    }

    #[test]
    fn media_filter_rejects_invalid_or_reversed_values() {
        assert!(normalize_extension("../zip").is_err());
        assert!(normalize_extension("*").is_err());
        let filter = MediaFilter {
            extensions: vec!["ZIP".into(), "zip".into()],
            min_size: Some(2),
            max_size: Some(1),
        };
        assert!(filter.validate().is_err());
    }

    #[test]
    fn account_proxy_overrides_global_proxy() {
        let mut config = valid_config();
        config.proxy_url = Some("socks5://global.example:1080".into());
        assert_eq!(
            config.proxy_for_account(&config.accounts[0]),
            Some("socks5://global.example:1080")
        );

        config.accounts[0].proxy_url = Some("socks5://account.example:1081".into());
        assert_eq!(
            config.proxy_for_account(&config.accounts[0]),
            Some("socks5://account.example:1081")
        );
    }

    #[test]
    fn account_name_validation() {
        assert!(validate_account_name("premium-main_2").is_ok());
        assert!(validate_account_name("bad name").is_err());
        assert!(validate_account_name("../bad").is_err());
    }

    #[test]
    fn enforces_v1_file_and_lane_limits() {
        let mut config = valid_config();
        assert!(config.validate().is_ok());
        config.max_active_files = 3;
        assert!(config.validate().is_err());
        config.max_active_files = 2;
        config.max_download_lanes_per_account = 9;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validates_multiple_public_and_private_sources() {
        let mut config = valid_config();
        config.sources.push(SourceConfig {
            title: "Private group".into(),
            access: SourceAccess::Private,
            peer_kind: SourcePeerKind::Group,
            username: None,
            peer_id: Some(-456),
        });
        assert!(config.validate().is_ok());

        config.sources.push(config.sources[1].clone());
        assert!(config.validate().is_err());
        config.sources.pop();
        config.sources[1].peer_id = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn primary_monitor_must_be_enabled() {
        let mut config = valid_config();
        config.accounts[0].enabled = false;
        config.accounts.push(AccountConfig {
            name: "backup".into(),
            enabled: true,
            monitor_priority: 2,
            user_id: None,
            display_name: None,
            premium: false,
            premium_checked_at: None,
            proxy_url: None,
        });
        assert!(config.validate().is_err());
        config.monitor_primary = "backup".into();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn save_replaces_configuration_without_leaving_temporary_data() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("config.toml");
        let mut config = valid_config();
        config.save(&path)?;
        config.sources[0] = SourceConfig {
            title: "Second group".into(),
            access: SourceAccess::Private,
            peer_kind: SourcePeerKind::Group,
            username: None,
            peer_id: Some(-123),
        };
        config.save(&path)?;

        let loaded = Config::load_stored(&path)?;
        assert_eq!(loaded.sources[0].title, "Second group");
        assert_eq!(
            std::fs::read_dir(directory.path())?.count(),
            1,
            "temporary configuration should be removed after replacement"
        );
        Ok(())
    }

    #[test]
    fn stored_load_normalizes_user_facing_filter_and_source_values() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
                api_id = 1
                api_hash = "hash"
                output_dir = "downloads"
                monitor_primary = "main"

                [media_filter]
                extensions = ["*.RAR", ".Tar.Gz"]

                [[sources]]
                title = "  Public source  "
                access = "public"
                peer_kind = "channel"
                username = "@Drop_Box"

                [[accounts]]
                name = "main"
                enabled = true
            "#,
        )?;

        let config = Config::load_stored(&path)?;
        assert_eq!(config.media_filter.extensions, ["rar", "tar.gz"]);
        assert_eq!(config.sources[0].title, "Public source");
        assert_eq!(config.sources[0].username.as_deref(), Some("Drop_Box"));
        Ok(())
    }
}
