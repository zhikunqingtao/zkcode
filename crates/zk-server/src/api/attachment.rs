//! Local attachments: bounded uploads, atomic publication and exact UUID downloads.
//!
//! Files remain under `~/.zk/uploads`. Display names contain only a basename,
//! stored extensions are bounded ASCII, and symlinks/prefix UUIDs are never read.
//! Uploads retain the existing 201 response and 10 MiB application limit.

use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::body::Body;
use axum::extract::{Multipart, Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::state::AppState;

/// 旧 `AttachmentController.MAX_FILE_SIZE`（10 MiB）。
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// 旧 `AttachmentController.UploadResponse` record（Jackson `NON_NULL`：
/// `fileUuid` / `fileName` / `error` 为空时剥离；`size` 为原语 `long` 恒在）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadResponse {
    /// 成功时的附件 UUID（失败分支为 `None` → 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    file_uuid: Option<String>,
    /// 原始文件名（`multipart` `filename`；缺省 → 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    file_name: Option<String>,
    /// 字节数（原语 `long`，恒序列化）。
    size: u64,
    /// 失败原因（成功分支为 `None` → 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// 附件上传目录（旧 `${user.home}/.zhikun/uploads` → 本仓 `~/.zk/uploads`）。
fn upload_dir() -> PathBuf {
    zk_core::paths::user_config_dir().join("uploads")
}

/// Keep only a bounded alphanumeric suffix, independent of the display name.
fn extension_of(filename: Option<&str>) -> String {
    let Some((_, extension)) = filename.and_then(|name| name.rsplit_once('.')) else {
        return String::new();
    };
    if extension.is_empty()
        || extension.len() > 16
        || !extension.bytes().all(|c| c.is_ascii_alphanumeric())
    {
        return String::new();
    }
    format!(".{extension}")
}

fn safe_filename(name: &str) -> String {
    sanitized_name(name, "attachment")
}

pub(crate) fn sanitized_name(name: &str, fallback: &str) -> String {
    fn cleaned(name: &str) -> String {
        name.rsplit(['/', '\\']).next().unwrap_or("").chars()
            .filter(|c| !c.is_control() && !matches!(*c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
            .collect::<String>().trim().to_owned()
    }
    let mut name = cleaned(name);
    if name.is_empty() || matches!(name.as_str(), "." | "..") {
        name = cleaned(fallback);
    }
    if name.is_empty() || matches!(name.as_str(), "." | "..") {
        name = "file".into();
    }
    if name.len() <= 200 {
        return name;
    }
    let extension = extension_of(Some(&name));
    let suffix = if extension.len() <= 17 {
        extension
    } else {
        String::new()
    };
    let budget = 200 - suffix.len();
    let mut end = budget.min(name.len().saturating_sub(suffix.len()));
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{suffix}", &name[..end])
}

pub(crate) fn content_disposition(disposition: &str, name: &str) -> String {
    use std::fmt::Write as _;
    let name = sanitized_name(name, "file");
    let mut fallback = String::new();
    for c in name.chars() {
        match c {
            '\\' | '"' => {
                fallback.push('\\');
                fallback.push(c);
            }
            c if c.is_ascii() => fallback.push(c),
            _ => fallback.push('?'),
        }
    }
    let mut encoded = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            )
        {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    let disposition = if disposition == "inline" {
        "inline"
    } else {
        "attachment"
    };
    format!("{disposition}; filename=\"{fallback}\"; filename*=UTF-8''{encoded}")
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AttachmentMetadata {
    version: u8,
    uuid: String,
    storage_file_name: String,
    original_name: String,
}

fn save_attachment(
    dir: &Path,
    id: &str,
    storage_name: &str,
    original_name: &str,
    bytes: &[u8],
) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let dir = std::fs::canonicalize(dir)?;
    let metadata_dir = dir.join(".metadata");
    std::fs::create_dir_all(&metadata_dir)?;
    if !std::fs::canonicalize(&metadata_dir)?.starts_with(&dir) {
        return Err(std::io::Error::other(
            "attachment metadata root escapes upload directory",
        ));
    }
    let metadata = serde_json::to_vec(&AttachmentMetadata {
        version: 1,
        uuid: id.into(),
        storage_file_name: storage_name.into(),
        original_name: original_name.into(),
    })?;
    let target = dir.join(storage_name);
    save_atomic(&dir, &target, bytes)?;
    if let Err(error) = save_atomic(
        &metadata_dir,
        &metadata_dir.join(format!("{id}.json")),
        &metadata,
    ) {
        if let Err(cleanup) = std::fs::remove_file(target) {
            tracing::error!(%cleanup, "attachment payload rollback failed");
        }
        return Err(error);
    }
    Ok(())
}

fn save_atomic(dir: &Path, target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let temp = dir.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        // Publishing by hard link is atomic and never replaces an existing UUID.
        std::fs::hard_link(&temp, target)
    })();
    let cleanup = std::fs::remove_file(temp);
    if result.is_ok() && cleanup.is_err() {
        // This invocation alone created the link; roll back only its publication.
        if let Err(error) = std::fs::remove_file(target) {
            tracing::error!(%error, "attachment publication rollback failed");
        }
    }
    result.and(cleanup)
}

/// `POST /api/attachments/upload`——上传附件（旧 `upload`）。
#[utoipa::path(
    post,
    path = "/api/attachments/upload",
    tag = "attachments",
    responses(
        (status = 201, description = "UploadResponse{fileUuid,fileName,size}"),
        (status = 400, description = "File too large (max 10MB)：UploadResponse{fileName,size,error}")
    )
)]
pub(crate) async fn upload(
    State(_state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Response, ApiError> {
    // 旧 `@RequestParam("file") MultipartFile`：取名为 `file` 的分部。缺失时
    // Spring 抛 `MissingServletRequestPartException`（非 `IllegalArgumentException`
    // 子类）→ 落 `handleGeneric` → 500 `INTERNAL_ERROR`；此处同归 500。
    // 读取分部时 body 超 `DefaultBodyLimit`（64 MiB）→ `Multipart` 报错 → 500。
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| ApiError::internal())?
    {
        if field.name() != Some("file") {
            continue;
        }
        let file_name = field.file_name().map(safe_filename);
        let data = field.bytes().await.map_err(|_| ApiError::internal())?;
        let size = data.len() as u64;
        // 旧 `if (file.getSize() > MAX_FILE_SIZE) return badRequest().body(...)`。
        if size > MAX_FILE_SIZE {
            let body = UploadResponse {
                file_uuid: None,
                file_name,
                size,
                error: Some("File too large (max 10MB)".to_owned()),
            };
            return Ok((StatusCode::BAD_REQUEST, Json(body)).into_response());
        }
        // 旧 `UUID.randomUUID()` + `getExtension` + `uploadDir.resolve(uuid+ext)`。
        let file_uuid = uuid::Uuid::new_v4().to_string();
        let ext = extension_of(file_name.as_deref());
        let dir = upload_dir();
        let storage_name = format!("{file_uuid}{ext}");
        let original_name = file_name.clone().unwrap_or_else(|| storage_name.clone());
        let stored_id = file_uuid.clone();
        // 旧 `@PostConstruct createDirectories` → 本实现惰性建目录后写入。
        tokio::task::spawn_blocking(move || {
            save_attachment(&dir, &stored_id, &storage_name, &original_name, &data)
        })
        .await
        .map_err(|_| ApiError::internal())?
        .map_err(|_| ApiError::internal())?;
        let body = UploadResponse {
            file_uuid: Some(file_uuid),
            file_name,
            size,
            error: None,
        };
        return Ok((StatusCode::CREATED, Json(body)).into_response());
    }
    // 未提供名为 `file` 的分部（旧 `MissingServletRequestPartException` → 500）。
    Err(ApiError::validation_with_code(
        "ATTACHMENT_FILE_REQUIRED",
        "A file part is required",
    ))
}

/// Read only a regular file whose complete UUID and safe suffix match.
#[derive(Debug, PartialEq, Eq)]
struct LoadedAttachment {
    bytes: Vec<u8>,
    name: String,
}

fn load_by_uuid(dir: &Path, file_uuid: &str) -> Result<Option<LoadedAttachment>, ApiError> {
    if uuid::Uuid::parse_str(file_uuid)
        .ok()
        .is_none_or(|uuid| uuid.to_string() != file_uuid)
        || !dir.exists()
    {
        return Ok(None);
    }
    let dir = std::fs::canonicalize(dir).map_err(|_| ApiError::internal())?;
    let mut candidate = None;
    for entry in std::fs::read_dir(&dir).map_err(|_| ApiError::internal())? {
        let entry = entry.map_err(|_| ApiError::internal())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let matches = name == file_uuid
            || name.strip_prefix(file_uuid).is_some_and(|suffix| {
                suffix.starts_with('.') && extension_of(Some(&name)) == suffix
            });
        if matches
            && entry
                .file_type()
                .map_err(|_| ApiError::internal())?
                .is_file()
        {
            if candidate.is_some() {
                return Err(ApiError {
                    status: StatusCode::CONFLICT,
                    code: "ATTACHMENT_ID_CONFLICT".into(),
                    message: "Multiple attachments match the UUID".into(),
                });
            }
            candidate = Some((entry.path(), name));
        }
    }
    let Some((path, storage_name)) = candidate else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| ApiError::internal())?
        .take(MAX_FILE_SIZE + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ApiError::internal())?;
    if bytes.len() as u64 > MAX_FILE_SIZE {
        return Ok(None);
    }
    let name = read_attachment_name(&dir, file_uuid, &storage_name).unwrap_or(storage_name);
    Ok(Some(LoadedAttachment { bytes, name }))
}

fn read_attachment_name(dir: &Path, id: &str, storage_name: &str) -> Option<String> {
    let metadata_dir = std::fs::canonicalize(dir.join(".metadata")).ok()?;
    if !metadata_dir.starts_with(dir) {
        return None;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(metadata_dir.join(format!("{id}.json")))
        .ok()?;
    let meta: AttachmentMetadata = serde_json::from_reader(file.take(4096)).ok()?;
    (meta.version == 1 && meta.uuid == id && meta.storage_file_name == storage_name)
        .then(|| sanitized_name(&meta.original_name, storage_name))
}

/// `GET /api/attachments/{fileUuid}`——下载/预览附件（旧 `download`）。
#[utoipa::path(
    get,
    path = "/api/attachments/{fileUuid}",
    tag = "attachments",
    params(("fileUuid" = String, Path, description = "附件 UUID")),
    responses(
        (status = 200, description = "附件字节流（application/octet-stream）"),
        (status = 404, description = "附件不存在（空体）")
    )
)]
pub(crate) async fn download(
    State(_state): State<AppState>,
    AxumPath(file_uuid): AxumPath<String>,
) -> Result<Response, ApiError> {
    let dir = upload_dir();
    let found = tokio::task::spawn_blocking(move || load_by_uuid(&dir, &file_uuid))
        .await
        .map_err(|_| ApiError::internal())??;
    match found {
        // 旧 `ResponseEntity.notFound().build()`（空体）。
        None => Ok(StatusCode::NOT_FOUND.into_response()),
        // 旧 `ResponseEntity.ok().contentType(APPLICATION_OCTET_STREAM).body(resource)`
        //（无 `Content-Disposition`）。
        Some(found) => {
            let disposition = content_disposition("attachment", &found.name);
            let mut response = Response::new(Body::from(found.bytes));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/octet-stream"),
            );
            response.headers_mut().insert(
                header::X_CONTENT_TYPE_OPTIONS,
                header::HeaderValue::from_static("nosniff"),
            );
            response.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                header::HeaderValue::from_str(&disposition).map_err(|_| ApiError::internal())?,
            );
            Ok(response)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_of_matches_java_last_index_of_dot() {
        // 旧 `dot >= 0 ? substring(dot) : ""`（含点）。
        assert_eq!(extension_of(Some("photo.png")), ".png");
        assert_eq!(extension_of(Some("archive.tar.gz")), ".gz");
        // 无扩展名 → 空串。
        assert_eq!(extension_of(Some("README")), "");
        // 首字符即 `.`（dotfile）→ 整名（Java lastIndexOf('.')==0，substring(0)）。
        assert_eq!(extension_of(Some(".bashrc")), ".bashrc");
        // 末尾一个 `.` → `.`（substring(len-1)）。
        assert_eq!(extension_of(Some("trailing.")), "");
        // null → ""。
        assert_eq!(extension_of(None), "");
    }

    #[test]
    fn attachment_names_and_lookup_are_bounded() {
        assert_eq!(safe_filename("../../bad\r\nname.PNG"), "badname.PNG");
        assert_eq!(safe_filename("C:\\path\\report.pdf"), "report.pdf");
        assert_eq!(extension_of(Some("file.x/../secret")), "");
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        save_atomic(&dir, &dir.join(format!("{id}.txt")), b"safe").unwrap();
        assert_eq!(load_by_uuid(&dir, &id).unwrap().unwrap().bytes, b"safe");
        assert!(load_by_uuid(&dir, &id[..8]).unwrap().is_none());
        assert!(save_atomic(&dir, &dir.join(format!("{id}.txt")), b"replace").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn unicode_metadata_survives_reopen_and_corruption_falls_back_to_storage_name() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let id = uuid::Uuid::new_v4().to_string();
        let storage = format!("{id}.txt");
        save_attachment(&dir, &id, &storage, "中文报告.txt", b"payload").unwrap();
        let found = load_by_uuid(&dir, &id).unwrap().unwrap();
        assert_eq!(found.bytes, b"payload");
        assert_eq!(found.name, "中文报告.txt");
        let header = content_disposition("attachment", &found.name);
        assert!(header.contains("filename=\"????.txt\""));
        assert!(header.contains("filename*=UTF-8''%E4%B8%AD%E6%96%87%E6%8A%A5%E5%91%8A.txt"));
        std::fs::write(dir.join(".metadata").join(format!("{id}.json")), b"broken").unwrap();
        assert_eq!(load_by_uuid(&dir, &id).unwrap().unwrap().name, storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn exact_uuid_conflict_and_symlinks_never_return_arbitrary_candidate() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        std::fs::write(dir.join(&id), b"first").unwrap();
        std::fs::write(dir.join(format!("{id}.part")), b"second").unwrap();
        assert_eq!(
            load_by_uuid(&dir, &id).unwrap_err().code,
            "ATTACHMENT_ID_CONFLICT"
        );
        assert!(load_by_uuid(&dir, &id.to_uppercase()).unwrap().is_none());
        std::fs::remove_file(dir.join(format!("{id}.part"))).unwrap();
        std::os::unix::fs::symlink(dir.join(&id), dir.join(format!("{id}.txt"))).unwrap();
        assert_eq!(load_by_uuid(&dir, &id).unwrap().unwrap().bytes, b"first");
        std::fs::remove_file(dir.join(&id)).unwrap();
        assert!(load_by_uuid(&dir, &id).unwrap().is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn metadata_failure_leaves_no_payload_and_symlinked_upload_root_is_supported() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".metadata"), b"sentinel").unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        assert!(save_attachment(&dir, &id, &id, "file", b"payload").is_err());
        assert!(!dir.join(&id).exists());
        assert_eq!(std::fs::read(dir.join(".metadata")).unwrap(), b"sentinel");
        std::fs::remove_file(dir.join(".metadata")).unwrap();
        let linked = dir.with_extension("link");
        std::os::unix::fs::symlink(&dir, &linked).unwrap();
        save_attachment(&linked, &id, &format!("{id}.TXT"), "报告.TXT", b"payload").unwrap();
        assert_eq!(
            load_by_uuid(&dir, &id).unwrap(),
            load_by_uuid(&linked, &id).unwrap()
        );
        std::fs::remove_file(linked).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn display_names_remove_bidi_but_preserve_unicode_and_complete_office_extensions() {
        for control in [
            '\u{061c}', '\u{200e}', '\u{200f}', '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}',
            '\u{202e}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}',
        ] {
            assert_eq!(
                sanitized_name(&format!("gpj{control}exe.txt"), "file"),
                "gpjexe.txt"
            );
        }
        for name in [
            "مرحبا שלום.txt",
            "👩\u{200d}💻.txt",
            "می\u{200c}روم.txt",
            "word\u{2060}join.txt",
        ] {
            assert_eq!(sanitized_name(name, "file"), name);
        }
        for suffix in [".docx", ".xlsx", ".pptx"] {
            let name = sanitized_name(&format!("{}{suffix}", "汉".repeat(70)), "file");
            assert_eq!(name, format!("{}{suffix}", "汉".repeat(65)));
            assert_eq!(name.len(), 200);
        }
        assert_eq!(sanitized_name(&"😀".repeat(100), "file"), "😀".repeat(50));
        assert_eq!(
            sanitized_name(&format!("{}汉", "a".repeat(199)), "file"),
            "a".repeat(199)
        );
        assert_eq!(
            content_disposition("inline", "evil\r\n\0.txt"),
            "inline; filename=\"evil.txt\"; filename*=UTF-8''evil.txt"
        );
    }
}
