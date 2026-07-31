use std::ffi::OsStr;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use console::Style;

const MAX_FILENAME_BYTES: usize = 240;

pub fn prompt(label: &str) -> Result<String> {
    print!("{}", Style::new().cyan().bright().bold().apply_to(label));
    io::stdout().flush().context("failed to flush stdout")?;
    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .context("failed to read input")?;
    Ok(value.trim().to_owned())
}

pub fn prompt_password(label: &str) -> Result<String> {
    print!("{}", Style::new().cyan().bright().bold().apply_to(label));
    io::stdout().flush().context("failed to flush stdout")?;
    rpassword::read_password().context("failed to read hidden input")
}

pub fn normalize_user_path(input: &str) -> Result<PathBuf> {
    let mut value = input.trim();
    while value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value = value[1..value.len() - 1].trim();
    }
    if value.is_empty() {
        anyhow::bail!("path must not be empty");
    }
    if value.to_ascii_lowercase().starts_with("file://") {
        let url = url::Url::parse(value).context("file path URL is invalid")?;
        return url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("file path URL cannot be converted on this system"));
    }

    let mut value = expand_home(value);
    #[cfg(windows)]
    {
        value = expand_windows_environment(&value);
        value = value.replace('/', "\\");
        value = collapse_windows_separators(&value);
    }
    #[cfg(not(windows))]
    {
        value = value.replace('\\', "/");
        while value.contains("//") {
            value = value.replace("//", "/");
        }
    }
    Ok(PathBuf::from(value))
}

fn expand_home(value: &str) -> String {
    if value == "~" || value.starts_with("~/") || value.starts_with("~\\") {
        let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" });
        if let Some(home) = home {
            let mut expanded = home.to_string_lossy().into_owned();
            expanded.push_str(&value[1..]);
            return expanded;
        }
    }
    value.to_owned()
}

#[cfg(windows)]
fn expand_windows_environment(value: &str) -> String {
    let mut expanded = String::with_capacity(value.len());
    let mut remainder = value;
    while let Some(start) = remainder.find('%') {
        expanded.push_str(&remainder[..start]);
        let after_start = &remainder[start + 1..];
        let Some(end) = after_start.find('%') else {
            expanded.push_str(&remainder[start..]);
            return expanded;
        };
        let name = &after_start[..end];
        if let Some(environment_value) = std::env::var_os(name) {
            expanded.push_str(&environment_value.to_string_lossy());
        } else {
            expanded.push('%');
            expanded.push_str(name);
            expanded.push('%');
        }
        remainder = &after_start[end + 1..];
    }
    expanded.push_str(remainder);
    expanded
}

#[cfg(windows)]
fn collapse_windows_separators(value: &str) -> String {
    if value.starts_with("\\\\?\\") || value.starts_with("\\\\.\\") {
        return value.to_owned();
    }
    let unc = value.starts_with("\\\\");
    let remainder = if unc {
        value.trim_start_matches('\\')
    } else {
        value
    };
    let mut collapsed = String::with_capacity(value.len());
    if unc {
        collapsed.push_str("\\\\");
    }
    let mut previous_separator = false;
    for character in remainder.chars() {
        if character == '\\' {
            if !previous_separator {
                collapsed.push(character);
            }
            previous_separator = true;
        } else {
            collapsed.push(character);
            previous_separator = false;
        }
    }
    collapsed
}

pub fn sanitize_filename(input: &str, fallback: &str) -> String {
    let candidate = Path::new(input)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("")
        .trim()
        .trim_matches('.');
    let mut result = String::with_capacity(candidate.len());
    for ch in candidate.chars() {
        if ch.is_control() || matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') {
            result.push('_');
        } else {
            result.push(ch);
        }
    }
    while result.ends_with([' ', '.']) {
        result.pop();
    }
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let reserved_stem = result.split('.').next().unwrap_or("");
    if result.is_empty()
        || reserved
            .iter()
            .any(|name| reserved_stem.eq_ignore_ascii_case(name))
    {
        truncate_filename(fallback, MAX_FILENAME_BYTES)
    } else {
        truncate_filename(&result, MAX_FILENAME_BYTES)
    }
}

fn truncate_filename(filename: &str, max_bytes: usize) -> String {
    if filename.len() <= max_bytes {
        return filename.to_owned();
    }
    let extension = filename
        .rsplit_once('.')
        .filter(|(stem, extension)| !stem.is_empty() && extension.len() + 1 < max_bytes / 2)
        .map(|(_, extension)| format!(".{extension}"))
        .unwrap_or_default();
    let stem_length = max_bytes.saturating_sub(extension.len());
    let mut end = stem_length.min(filename.len());
    while !filename.is_char_boundary(end) {
        end -= 1;
    }
    let stem = filename[..end].trim_end_matches([' ', '.']);
    format!("{stem}{extension}")
}

pub fn collision_candidate(path: &Path, attempt: u32) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path.file_stem().and_then(OsStr::to_str).unwrap_or("file");
    let extension = path.extension().and_then(OsStr::to_str);
    let mut name = format!("{stem}_{}", attempt + 1);
    if let Some(extension) = extension {
        name.push('.');
        name.push_str(extension);
    }
    parent.join(name)
}

pub fn human_bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut number = value as f64;
    let mut unit = 0;
    while number >= 1024.0 && unit + 1 < UNITS.len() {
        number /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{number:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_path_traversal_and_invalid_characters() {
        assert_eq!(
            sanitize_filename("../../bad:name?.zip", "fallback"),
            "bad_name_.zip"
        );
        assert_eq!(sanitize_filename("CON", "fallback.bin"), "fallback.bin");
        assert_eq!(sanitize_filename("CON.txt", "fallback.bin"), "fallback.bin");
        assert_eq!(sanitize_filename("archive. ", "fallback.bin"), "archive");
    }

    #[test]
    fn truncates_long_names_without_splitting_unicode_or_losing_extension() {
        let filename = format!("{}.zip", "測".repeat(100));
        let sanitized = sanitize_filename(&filename, "fallback.zip");
        assert!(sanitized.len() <= MAX_FILENAME_BYTES);
        assert!(sanitized.ends_with(".zip"));
    }

    #[test]
    fn adds_collision_suffix_before_extension() {
        assert_eq!(
            collision_candidate(Path::new("/tmp/archive.tar"), 0),
            PathBuf::from("/tmp/archive_1.tar")
        );
        assert_eq!(
            collision_candidate(Path::new("/tmp/archive.tar"), 1),
            PathBuf::from("/tmp/archive_2.tar")
        );
    }

    #[test]
    fn normalizes_quoted_and_escaped_user_paths() -> Result<()> {
        #[cfg(windows)]
        {
            assert_eq!(
                normalize_user_path("\"C:/Users/Test Folder/file.session\"")?,
                PathBuf::from(r"C:\Users\Test Folder\file.session")
            );
            assert_eq!(
                normalize_user_path(r"C:\\Users\\Test\\tdata")?,
                PathBuf::from(r"C:\Users\Test\tdata")
            );
            assert_eq!(
                normalize_user_path(r"'\\server\\share\\folder'")?,
                PathBuf::from(r"\\server\share\folder")
            );
        }
        #[cfg(not(windows))]
        {
            assert_eq!(
                normalize_user_path(r#"'/tmp/Test Folder/file.session'"#)?,
                PathBuf::from("/tmp/Test Folder/file.session")
            );
            assert_eq!(
                normalize_user_path(r"\tmp\Test\tdata")?,
                PathBuf::from("/tmp/Test/tdata")
            );
        }
        Ok(())
    }
}
