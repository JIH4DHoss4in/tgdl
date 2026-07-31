use std::fs;
use std::path::Path;

#[cfg(windows)]
use anyhow::bail;
use anyhow::{Context, Result};

pub fn create_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create private directory {}", path.display()))?;
    restrict_dir(path)
}

pub fn restrict_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to restrict {}", path.display()))?;
    }
    #[cfg(windows)]
    restrict_windows(path, false)?;
    Ok(())
}

fn restrict_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to restrict {}", path.display()))?;
    }
    #[cfg(windows)]
    restrict_windows(path, true)?;
    Ok(())
}

#[cfg(windows)]
fn restrict_windows(path: &Path, directory: bool) -> Result<()> {
    use std::process::Command;

    let who = Command::new("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .context("failed to determine the current Windows account SID")?;
    if !who.status.success() {
        bail!("whoami failed while securing {}", path.display());
    }
    let output = String::from_utf8_lossy(&who.stdout);
    let sid =
        windows_sid(&output).context("whoami did not return the current Windows account SID")?;
    // A numeric SID avoids failures when the account name contains characters
    // outside the active Windows console code page.
    let principal = format!("*{sid}");
    let grant = if directory {
        format!("{principal}:(OI)(CI)F")
    } else {
        format!("{principal}:F")
    };
    let result = Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(grant)
        .output()
        .with_context(|| format!("failed to run icacls for {}", path.display()))?;
    if !result.status.success() {
        bail!(
            "icacls could not secure {}: {}",
            path.display(),
            String::from_utf8_lossy(&result.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(windows)]
fn windows_sid(output: &str) -> Option<&str> {
    output
        .split(|character: char| character == ',' || character == '"' || character.is_whitespace())
        .find(|field| field.starts_with("S-1-"))
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn extracts_sid_without_decoding_the_account_name() {
        assert_eq!(
            windows_sid("\"DOMAIN\\Jos\u{fffd}\",\"S-1-5-21-123-456-789-1001\"\r\n"),
            Some("S-1-5-21-123-456-789-1001")
        );
    }
}
