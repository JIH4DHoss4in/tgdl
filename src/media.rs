use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use grammers_client::media::{Document, Downloadable, Media, Photo};
use grammers_client::message::Message;
use grammers_tl_types as tl;

use crate::config::Config;
use crate::store::{NewJob, Store};
use crate::util::{collision_candidate, sanitize_filename};

pub const CHUNK_SIZE: u32 = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct MediaDescriptor {
    pub kind: String,
    pub filename: String,
    pub remote_key: String,
    pub total_size: Option<u64>,
    pub dc_id: i32,
    pub location: tl::enums::InputFileLocation,
}

pub fn descriptor_from_message(message: &Message) -> Option<MediaDescriptor> {
    let media = message.media()?;
    match media {
        Media::Photo(photo) => photo_descriptor(message.id(), &photo),
        Media::Document(document) => document_descriptor(message.id(), "document", &document),
        Media::Sticker(sticker) => document_descriptor(message.id(), "sticker", &sticker.document),
        _ => None,
    }
}

fn photo_descriptor(message_id: i32, photo: &Photo) -> Option<MediaDescriptor> {
    let raw = match photo.raw.photo.as_ref()? {
        tl::enums::Photo::Photo(raw) => raw,
        tl::enums::Photo::Empty(_) => return None,
    };
    Some(MediaDescriptor {
        kind: "photo".to_owned(),
        filename: format!("photo-{message_id}.jpg"),
        remote_key: format!("photo:{}", raw.id),
        total_size: photo.size().map(|value| value as u64),
        dc_id: raw.dc_id,
        location: photo.to_raw_input_location()?,
    })
}

fn document_descriptor(
    message_id: i32,
    default_kind: &str,
    document: &Document,
) -> Option<MediaDescriptor> {
    let raw = match document.raw.document.as_ref()? {
        tl::enums::Document::Document(raw) => raw,
        tl::enums::Document::Empty(_) => return None,
    };
    let mime = document.mime_type().map(str::to_owned);
    let animated = raw
        .attributes
        .iter()
        .any(|attribute| matches!(attribute, tl::enums::DocumentAttribute::Animated));
    let kind = if default_kind == "sticker" {
        default_kind.to_owned()
    } else if animated {
        "animation".to_owned()
    } else {
        media_kind(mime.as_deref()).to_owned()
    };
    let extension = mime
        .as_deref()
        .and_then(mime_guess::get_mime_extensions_str)
        .and_then(|values| values.first().copied())
        .unwrap_or("bin");
    let fallback = format!("{kind}-{message_id}.{extension}");
    let filename = sanitize_filename(document.name().unwrap_or(""), &fallback);
    Some(MediaDescriptor {
        kind,
        filename,
        remote_key: format!("document:{}", raw.id),
        total_size: document.size().map(|value| value as u64),
        dc_id: raw.dc_id,
        location: document.to_raw_input_location()?,
    })
}

fn media_kind(mime: Option<&str>) -> &'static str {
    match mime {
        Some(value) if value.starts_with("video/") => "video",
        Some(value) if value.starts_with("audio/") => "audio",
        Some("image/gif") => "animation",
        _ => "document",
    }
}

#[derive(Clone, Debug)]
pub enum Reservation {
    Queued(crate::store::Job),
    Duplicate(crate::store::Job),
}

#[derive(Clone, Debug)]
pub enum MessageReservation {
    NotMedia,
    Filtered,
    AlreadyTracked,
    Queued(crate::store::Job),
    Duplicate(crate::store::Job),
}

pub fn reserve_message(
    store: &Store,
    config: &Config,
    channel_id: i64,
    message: &Message,
) -> Result<MessageReservation> {
    let Some(descriptor) = descriptor_from_message(message) else {
        return Ok(MessageReservation::NotMedia);
    };
    if !config
        .media_filter
        .accepts(&descriptor.filename, descriptor.total_size)
    {
        return Ok(MessageReservation::Filtered);
    }
    Ok(
        match reserve_job(
            store,
            &config.output_dir,
            channel_id,
            message.id(),
            &descriptor,
        )? {
            Some(Reservation::Queued(job)) => MessageReservation::Queued(job),
            Some(Reservation::Duplicate(job)) => MessageReservation::Duplicate(job),
            None => MessageReservation::AlreadyTracked,
        },
    )
}

pub fn reserve_job(
    store: &Store,
    output_dir: &Path,
    channel_id: i64,
    message_id: i32,
    descriptor: &MediaDescriptor,
) -> Result<Option<Reservation>> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;
    // Passive updates and channel-difference polling may both observe the same
    // message. It is already handled, not a separate duplicate forward.
    if store.job_for_message(channel_id, message_id)?.is_some() {
        return Ok(None);
    }
    if let Some(job) = store.reusable_job_for_media(&descriptor.remote_key)? {
        return Ok(Some(Reservation::Duplicate(job)));
    }
    let initial = output_dir.join(&descriptor.filename);
    let mut final_path = initial.clone();
    let mut attempt = 0_u32;
    while final_path.exists()
        || part_path_for(&final_path).exists()
        || store.output_reserved(&final_path)?
    {
        final_path = collision_candidate(&initial, attempt);
        attempt += 1;
    }
    let part_path = part_path_for(&final_path);
    let new = NewJob {
        channel_id,
        message_id,
        remote_key: Some(descriptor.remote_key.clone()),
        filename: final_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("download")
            .to_owned(),
        media_kind: descriptor.kind.clone(),
        final_path,
        part_path,
        total_size: descriptor.total_size,
        chunk_size: CHUNK_SIZE,
    };
    Ok(store.insert_job(&new)?.map(Reservation::Queued))
}

pub fn part_path_for(final_path: &Path) -> PathBuf {
    let mut name = final_path.file_name().unwrap_or_default().to_os_string();
    name.push(".part");
    final_path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;

    fn descriptor(filename: &str, remote_key: &str) -> MediaDescriptor {
        MediaDescriptor {
            kind: "document".into(),
            filename: filename.into(),
            remote_key: remote_key.into(),
            total_size: Some(4),
            dc_id: 2,
            location: tl::enums::InputFileLocation::InputDocumentFileLocation(
                tl::types::InputDocumentFileLocation {
                    id: 1,
                    access_hash: 2,
                    file_reference: Vec::new(),
                    thumb_size: String::new(),
                },
            ),
        }
    }

    #[test]
    fn part_is_next_to_final_file() {
        assert_eq!(
            part_path_for(Path::new("/downloads/archive.zip")),
            PathBuf::from("/downloads/archive.zip.part")
        );
    }

    #[test]
    fn skips_same_remote_media_and_indexes_different_filename_collisions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        let first = reserve_job(
            &store,
            temp.path(),
            1,
            10,
            &descriptor("archive.rar", "document:100"),
        )?
        .context("first reservation missing")?;
        let Reservation::Queued(first) = first else {
            bail!("first media was unexpectedly considered a duplicate");
        };

        let duplicate = reserve_job(
            &store,
            temp.path(),
            1,
            11,
            &descriptor("renamed.rar", "document:100"),
        )?
        .context("duplicate result missing")?;
        let Reservation::Duplicate(duplicate) = duplicate else {
            bail!("same remote media was queued twice");
        };
        assert_eq!(duplicate.id, first.id);

        assert!(
            reserve_job(
                &store,
                temp.path(),
                1,
                10,
                &descriptor("archive.rar", "document:100"),
            )?
            .is_none()
        );

        let collision = reserve_job(
            &store,
            temp.path(),
            1,
            12,
            &descriptor("archive.rar", "document:200"),
        )?
        .context("collision reservation missing")?;
        let Reservation::Queued(collision) = collision else {
            bail!("different remote media was unexpectedly deduplicated");
        };
        assert_eq!(collision.filename, "archive_1.rar");
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn reserves_output_names_case_insensitively_on_windows() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = Store::open(temp.path().join("state.sqlite"))?;
        reserve_job(
            &store,
            temp.path(),
            1,
            20,
            &descriptor("Archive.rar", "document:case-1"),
        )?
        .context("first reservation missing")?;
        let reservation = reserve_job(
            &store,
            temp.path(),
            1,
            21,
            &descriptor("archive.rar", "document:case-2"),
        )?
        .context("case-insensitive collision reservation missing")?;
        let Reservation::Queued(job) = reservation else {
            bail!("different media was unexpectedly deduplicated");
        };

        assert_eq!(job.filename, "archive_1.rar");
        Ok(())
    }
}
