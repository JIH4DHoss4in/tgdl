use anyhow::Result;
use tgdl::run_console;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("off")),
        )
        .with_target(false)
        .compact()
        .init();

    if std::env::args_os().len() > 1 {
        eprintln!(
            "tgdl is now an interactive console application; command-line arguments are ignored."
        );
    }
    run_console().await
}
