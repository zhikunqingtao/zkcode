//! Bounded native Read image producer. Authorization binds its path before execution.
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use image::ImageEncoder as _;
use serde_json::json;

use crate::atomic::sha256_hex;
use crate::file_state::{self, ReadObservation, session_key};
use crate::input::failure;
use crate::{ToolContext, ToolOutput};

const MAX_BYTES: u64 = 10 * 1024 * 1024;
const MAX_PIXELS: u64 = 40_000_000;

pub(super) fn is_image_path(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp"
            )
        })
}

pub(super) async fn read(path: PathBuf, ctx: ToolContext) -> ToolOutput {
    if ctx.cancel.is_cancelled() {
        return failure("FILE_READ_CANCELLED", "Image read cancelled");
    }
    let cancel = ctx.cancel.clone();
    tokio::select! {
        biased;
        () = cancel.cancelled() => failure("FILE_READ_CANCELLED", "Image read cancelled"),
        result = tokio::task::spawn_blocking(move || read_bounded(&path, &ctx)) =>
            result.unwrap_or_else(|_| failure("FILE_IMAGE_INVALID", "Image decoder did not complete")),
    }
}

use crate::safe_file::open_bound_regular as open_bound;

fn reader(bytes: &[u8]) -> image::ImageResult<image::ImageReader<Cursor<&[u8]>>> {
    let mut reader = image::ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(160 * 1024 * 1024);
    limits.max_image_width = Some(40_000_000);
    limits.max_image_height = Some(40_000_000);
    reader.limits(limits);
    Ok(reader)
}

struct BoundedPng(Vec<u8>);

impl std::io::Write for BoundedPng {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) as u64 > MAX_BYTES {
            return Err(std::io::Error::other("converted image exceeds 10 MiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn prepare_payload(
    bytes: Vec<u8>,
) -> Result<(&'static str, Vec<u8>, u32, u32), ImageSnapshotError> {
    let probe = reader(&bytes)
        .map_err(|_| ImageSnapshotError::new("FILE_IMAGE_INVALID", "Unsupported image format"))?;
    let format = probe.format();
    let (width, height) = probe
        .into_dimensions()
        .map_err(|_| ImageSnapshotError::new("FILE_IMAGE_INVALID", "Invalid image header"))?;
    if width == 0 || height == 0 || u64::from(width) * u64::from(height) > MAX_PIXELS {
        return Err(ImageSnapshotError::new(
            "FILE_IMAGE_TOO_MANY_PIXELS",
            "Image exceeds the 40 million pixel limit",
        ));
    }
    let decoded = reader(&bytes)
        .and_then(image::ImageReader::decode)
        .map_err(|_| {
            ImageSnapshotError::new(
                "FILE_IMAGE_INVALID",
                "Image data is invalid or exceeds decoder memory limits",
            )
        })?;
    let (media, payload) = match format {
        Some(image::ImageFormat::Png) => ("image/png", bytes),
        Some(image::ImageFormat::Jpeg) => ("image/jpeg", bytes),
        Some(image::ImageFormat::Gif) => ("image/gif", bytes),
        Some(image::ImageFormat::WebP) => ("image/webp", bytes),
        Some(image::ImageFormat::Bmp) => {
            let mut converted = BoundedPng(Vec::new());
            image::codecs::png::PngEncoder::new(&mut converted)
                .write_image(decoded.as_bytes(), width, height, decoded.color().into())
                .map_err(|_| {
                    ImageSnapshotError::new(
                        "FILE_IMAGE_INVALID",
                        "BMP conversion failed or exceeds the 10 MiB payload limit",
                    )
                })?;
            ("image/png", converted.0)
        }
        _ => {
            return Err(ImageSnapshotError::new(
                "FILE_IMAGE_INVALID",
                "Unsupported image format",
            ));
        }
    };
    Ok((media, payload, width, height))
}

/// Immutable bytes captured from a previously authorized absolute path.
#[derive(Debug)]
pub struct ImageSnapshot {
    /// Native file digest, before any format conversion.
    pub source_digest: String,
    /// Native byte length.
    pub source_bytes: u64,
    /// Validated provider image media type.
    pub media_type: &'static str,
    /// Validated bytes (BMP is converted to PNG).
    pub payload: Vec<u8>,
    /// Digest of the exact provider payload.
    pub payload_digest: String,
    /// Pixel width.
    pub width: u32,
    /// Pixel height.
    pub height: u32,
}

/// Safe diagnostic without embedding file contents or credentials.
#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ImageSnapshotError {
    /// Stable machine-readable failure code.
    pub code: &'static str,
    /// Human-readable bounded failure detail.
    pub message: &'static str,
}

impl ImageSnapshotError {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }
}

/// Capture and fully decode a bounded image through no-follow directory file
/// descriptors. This function grants no permission: callers must first authorize
/// the path for the current session, and run it outside an async executor thread.
///
/// # Errors
/// Rejects aliases, non-regular/changing files, excessive bytes/pixels, or invalid
/// complete image data. The returned bytes never depend on a subsequent reopen.
pub fn prepare_snapshot(path: &Path) -> Result<ImageSnapshot, ImageSnapshotError> {
    let Ok(mut file) = open_bound(path) else {
        return Err(ImageSnapshotError::new(
            "FILE_IMAGE_PATH_CHANGED",
            "Image path is unavailable or no longer matches its authorized target",
        ));
    };
    let Ok(before) = file.metadata() else {
        return Err(ImageSnapshotError::new(
            "FILE_READ_IO_FAILED",
            "Could not inspect image file",
        ));
    };
    if !before.is_file() {
        return Err(ImageSnapshotError::new(
            "FILE_IMAGE_INVALID",
            "Image input must be a regular file",
        ));
    }
    if before.len() > MAX_BYTES {
        return Err(ImageSnapshotError::new(
            "FILE_TOO_LARGE",
            "Image exceeds the 10 MiB read limit",
        ));
    }
    let mut bytes = Vec::new();
    if file
        .by_ref()
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Err(ImageSnapshotError::new(
            "FILE_READ_IO_FAILED",
            "Image read failed",
        ));
    }
    let Ok(after) = file.metadata() else {
        return Err(ImageSnapshotError::new(
            "FILE_READ_IO_FAILED",
            "Could not inspect image after reading",
        ));
    };
    if bytes.len() as u64 > MAX_BYTES
        || bytes.len() as u64 != before.len()
        || before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
    {
        return Err(ImageSnapshotError::new(
            "FILE_IMAGE_CHANGED",
            "Image changed while being read; retry the read",
        ));
    }
    let source_digest = sha256_hex(&bytes);
    let (media_type, payload, width, height) = prepare_payload(bytes)?;
    let payload_digest = sha256_hex(&payload);
    Ok(ImageSnapshot {
        source_digest,
        source_bytes: before.len(),
        media_type,
        payload,
        payload_digest,
        width,
        height,
    })
}

fn read_bounded(path: &Path, ctx: &ToolContext) -> ToolOutput {
    let snapshot = match prepare_snapshot(path) {
        Ok(snapshot) => snapshot,
        Err(error) => return failure(error.code, error.message),
    };
    if ctx.cancel.is_cancelled() {
        return failure("FILE_READ_CANCELLED", "Image read cancelled");
    }
    let ImageSnapshot {
        source_digest: original_hash,
        source_bytes,
        media_type: media,
        payload,
        payload_digest,
        width,
        height,
    } = snapshot;
    let display = path.to_string_lossy();
    file_state::global().mark_read_with_hash(
        session_key(ctx.session_id()),
        &display,
        "",
        ReadObservation {
            offset: None,
            limit: None,
            is_partial: true,
            content_sha256: None,
        },
    );
    let mut output = ToolOutput::ok(format!(
        "Image {display}: {width}x{height}, {source_bytes} source bytes. The original file is unchanged."
    ));
    output.metadata = Some(json!({
        "inlineImages":[{"mediaType":media,"data":base64::engine::general_purpose::STANDARD.encode(&payload),"sourceDigest":original_hash,"payloadDigest":payload_digest}],
        "structuredResult":{"filePath":display,"type":"image","width":width,"height":height,"sizeBytes":source_bytes,"contentSha256":original_hash,"overwriteEligible":false}
    }));
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadFileTool, Tool};

    fn fixture_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!("zk-read-image-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path.canonicalize().unwrap()
    }

    fn fixture(format: image::ImageFormat) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(32, 32)
            .write_to(&mut bytes, format)
            .unwrap();
        bytes.into_inner()
    }

    async fn invoke(path: &Path) -> ToolOutput {
        let (events, _) = tokio::sync::mpsc::unbounded_channel();
        ReadFileTool
            .execute(
                json!({"file_path":path}),
                ToolContext::new(tokio_util::sync::CancellationToken::new(), events),
            )
            .await
    }

    #[tokio::test]
    async fn image_read_preserves_native_formats_and_records_immutable_byte_identity() {
        let root = fixture_dir();
        for (format, extension, media) in [
            (image::ImageFormat::Png, "png", "image/png"),
            (image::ImageFormat::Jpeg, "jpg", "image/jpeg"),
            (image::ImageFormat::Gif, "gif", "image/gif"),
            (image::ImageFormat::WebP, "webp", "image/webp"),
        ] {
            let bytes = fixture(format);
            let path = root.join(format!("input.{extension}"));
            std::fs::write(&path, &bytes).unwrap();
            let output = invoke(&path).await;
            assert!(!output.is_error, "{}", output.content);
            let meta = output.metadata.unwrap();
            let inline = &meta["inlineImages"][0];
            assert_eq!(inline["mediaType"], media);
            assert_eq!(inline["sourceDigest"], sha256_hex(&bytes));
            assert_eq!(inline["payloadDigest"], inline["sourceDigest"]);
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(inline["data"].as_str().unwrap())
                    .unwrap(),
                bytes
            );
            assert_eq!(meta["structuredResult"]["overwriteEligible"], false);
            assert_eq!(std::fs::read(path).unwrap(), bytes);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn different_bmp_sources_keep_distinct_identities_when_png_payloads_match() {
        let root = fixture_dir();
        let bytes = fixture(image::ImageFormat::Bmp);
        let first = root.join("a.bmp");
        let second = root.join("b.bmp");
        std::fs::write(&first, &bytes).unwrap();
        let mut patched = bytes;
        patched[38..42].copy_from_slice(&10_000u32.to_le_bytes());
        std::fs::write(&second, patched).unwrap();
        let left = invoke(&first).await;
        let right = invoke(&second).await;
        assert!(!left.is_error && !right.is_error);
        let left = &left.metadata.as_ref().unwrap()["inlineImages"][0];
        let right = &right.metadata.as_ref().unwrap()["inlineImages"][0];
        assert_eq!(left["mediaType"], "image/png");
        assert_ne!(left["sourceDigest"], right["sourceDigest"]);
        assert_eq!(left["payloadDigest"], right["payloadDigest"]);
        assert_eq!(left["data"], right["data"]);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn oversize_sparse_files_and_dimension_bombs_fail_before_raster_allocation() {
        let root = fixture_dir();
        let path = root.join("large.png");
        std::fs::File::create(&path)
            .unwrap()
            .set_len(MAX_BYTES + 1)
            .unwrap();
        assert!(invoke(&path).await.content.starts_with("FILE_TOO_LARGE:"));
        let path = root.join("bomb.bmp");
        let mut bytes = fixture(image::ImageFormat::Bmp);
        bytes[18..22].copy_from_slice(&10_000u32.to_le_bytes());
        bytes[22..26].copy_from_slice(&10_000u32.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();
        assert!(
            invoke(&path)
                .await
                .content
                .starts_with("FILE_IMAGE_TOO_MANY_PIXELS:")
        );
        std::fs::write(root.join("invalid.png"), b"not an image").unwrap();
        assert!(invoke(&root.join("invalid.png")).await.is_error);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn image_read_rejects_rebound_file_or_parent_symlinks() {
        let root = fixture_dir();
        let actual = root.join("actual");
        std::fs::create_dir(&actual).unwrap();
        std::fs::write(actual.join("image.png"), fixture(image::ImageFormat::Png)).unwrap();
        std::os::unix::fs::symlink(&actual, root.join("parent")).unwrap();
        std::os::unix::fs::symlink(actual.join("image.png"), root.join("file.png")).unwrap();
        for path in [root.join("parent/image.png"), root.join("file.png")] {
            assert!(
                invoke(&path)
                    .await
                    .content
                    .starts_with("FILE_IMAGE_PATH_CHANGED:")
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
