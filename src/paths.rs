use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;

use crate::security;

pub const DEFAULT_CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Debug)]
pub struct AppPaths {
    pub root: PathBuf,
    pub config: PathBuf,
    pub sessions: PathBuf,
    pub state_db: PathBuf,
    pub lock_file: PathBuf,
}

impl AppPaths {
    pub fn discover(config_override: Option<&Path>) -> Result<Self> {
        let root = ProjectDirs::from("dev", "tgdl", "tgdl")
            .context("could not determine the operating-system application-data directory")?
            .config_dir()
            .to_path_buf();
        let config = config_override
            .map(Path::to_path_buf)
            .unwrap_or_else(|| root.join(DEFAULT_CONFIG_FILE));
        Ok(Self {
            sessions: root.join("sessions"),
            state_db: root.join("state.sqlite"),
            lock_file: root.join("tgdl.lock"),
            root,
            config,
        })
    }

    pub fn ensure(&self) -> Result<()> {
        security::create_private_dir(&self.root)?;
        security::create_private_dir(&self.sessions)?;
        Ok(())
    }

    pub fn session_path(&self, account_name: &str) -> PathBuf {
        self.sessions.join(format!("{account_name}.sqlite"))
    }
}
