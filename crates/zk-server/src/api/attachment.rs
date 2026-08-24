//! Attachment 域 2 端点 handler（Batch 2 Step 2-7，旧 `AttachmentController.java`；
//! 路由注册见 `routes`）。
//!
//! # 端点对照（旧 `@RequestMapping("/api/attachments")`）
//!
//! | 方法 + 路径 | 旧 handler | 命中 |
//! |---|---|---|
//! | `POST /upload` | `upload` | 201 `UploadResponse` / 400 `UploadResponse(error)` |
//! | `GET /{fileUuid}` | `download` | 200 octet-stream / 404 空体 |
//!
//! # 关键行为（逐字对照）
//!
//! - 上传目录：旧 `@Value("${app.upload-dir:${user.home}/.zhikun/uploads}")`。
//!   本仓统一到 `~/.zk`（[`zk_core::paths::user_config_dir`]）下的 `uploads`。
//!   旧 `@PostConstruct` 启动即 `createDirectories`；本实现改为写入前惰性
//!   `create_dir_all`（无 eager 副作用，语义等价，避免为一个可选功能在启动
//!   期强制建目录）。
//! - `upload`：`file.getSize() > 10 MiB` → `ResponseEntity.badRequest().body(
//!   UploadResponse(null, originalFilename, size, "File too large (max 10MB)"))`
//!   （**400 带体**，非错误信封）；否则 `fileUuid = UUID.randomUUID()`、
//!   `ext = getExtension(originalFilename)`、`target = uploadDir.resolve(uuid+ext)`、
//!   `transferTo` → **201** `UploadResponse(uuid, originalFilename, size, null)`。
//!   Jackson `NON_NULL`：`fileUuid`/`error` 为 null 时剥离（`size` 为原语
//!   `long` 恒序列化）。
//! - `getExtension(filename)`：`filename == null → ""`；`dot = lastIndexOf('.')`；
//!   `dot >= 0 ? substring(dot) : ""`（**含点**，如 `.png`；`.bashrc → .bashrc`）。
//! - `download`：`findByUuid` → `!Files.exists(uploadDir)` → null；否则
//!   `Files.list` 取 `filename.startsWith(fileUuid)` 首个 → null → **404 空体**；
//!   命中 → 200 `APPLICATION_OCTET_STREAM`（`FileSystemResource`，无
//!   `Content-Disposition`）。
//!
//! # 体积上限（决策 A-ATT）
//!
//! 旧 `application.yml` 未配 multipart，走 Spring 默认（`max-file-size=1MB`，
//! `max-request-size=10MB`）；但 controller 显式声明 `MAX_FILE_SIZE = 10MB` 并
//! 据此返回 400。若照搬 Spring 默认 1MB 会使该 400 分支永不可达。本实现以
//! controller 的显式 10MB 为权威，upload 路由挂 `DefaultBodyLimit::max(64 MiB)`
//! 作为传输层护栏，使 handler 内的 10MB 判定成为主门、400 分支可达。

use std::path::{Path, PathBuf};

use axum::Json;
use axum::body::Body;
use axum::extract::{Multipart, Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

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

/// 旧 `AttachmentController.getExtension`：`null → ""`；否则末个 `.` 起（含点）
/// 的后缀，无 `.` → `""`。`.bashrc`（首字符即 `.`）→ `.bashrc`。
fn extension_of(filename: Option<&str>) -> String {
    match filename {
        None => String::new(),
        // Java `lastIndexOf('.')` 返回字节位（`.` 为 ASCII，切片落 UTF-8 边界）；
        // `substring(dot)` 含该 `.`，故 `name[dot..]`。
        Some(name) => match name.rfind('.') {
            Some(dot) => name[dot..].to_owned(),
            None => String::new(),
        },
    }
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
        let file_name = field.file_name().map(str::to_owned);
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
        let target = dir.join(format!("{file_uuid}{ext}"));
        // 旧 `@PostConstruct createDirectories` → 本实现惰性建目录后写入。
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;
            std::fs::write(&target, &data)
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
    Err(ApiError::internal())
}

/// 旧 `AttachmentController.findByUuid` + 读取：目录不存在或未命中 → `None`；
/// `Files.list` 失败（IO）/ 读取失败 → `Err`（旧 `IOException` → 500）。
fn load_by_uuid(dir: &Path, file_uuid: &str) -> Result<Option<Vec<u8>>, ApiError> {
    // 旧 `if (!Files.exists(uploadDir)) return null;`。
    if !dir.exists() {
        return Ok(None);
    }
    // 旧 `Files.list(uploadDir)`：读目录失败为 IO 异常 → 500。
    let entries = std::fs::read_dir(dir).map_err(|_| ApiError::internal())?;
    for entry in entries {
        let entry = entry.map_err(|_| ApiError::internal())?;
        // 旧 `p.getFileName().toString().startsWith(fileUuid)`。
        if entry.file_name().to_string_lossy().starts_with(file_uuid) {
            let bytes = std::fs::read(entry.path()).map_err(|_| ApiError::internal())?;
            return Ok(Some(bytes));
        }
    }
    Ok(None)
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
        Some(bytes) => {
            let mut response = Response::new(Body::from(bytes));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/octet-stream"),
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
        assert_eq!(extension_of(Some("trailing.")), ".");
        // null → ""。
        assert_eq!(extension_of(None), "");
    }
}
