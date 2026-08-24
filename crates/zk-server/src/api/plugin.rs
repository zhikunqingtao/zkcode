//! Plugin REST 端点（Batch 8G Step 3，旧 `PluginController.java` 120L）。
//!
//! | Method | Path | Handler |
//! |--------|------|---------|
//! | GET | /api/plugins | `list_plugins` |
//! | POST | /api/plugins/install | `install_plugin` |
//! | DELETE | /api/plugins/{id} | `uninstall_plugin` |
//! | POST | /api/plugins/reload | `reload_plugins` |

use std::path::PathBuf;

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::state::AppState;

/// `POST /api/plugins/install` 请求体。
#[derive(Debug, Deserialize)]
pub(crate) struct InstallRequest {
    /// 插件目录路径。
    pub path: String,
}

/// `GET /api/plugins`——列出全部已加载插件。
pub(crate) async fn list_plugins(State(state): State<AppState>) -> Json<Value> {
    let plugins = state.plugin_manager.list();
    Json(json!({ "plugins": plugins, "count": plugins.len() }))
}

/// `POST /api/plugins/install`——安装插件。
pub(crate) async fn install_plugin(
    State(state): State<AppState>,
    Json(body): Json<InstallRequest>,
) -> Result<Json<Value>, ApiError> {
    let path = PathBuf::from(&body.path);
    match state.plugin_manager.install(&path) {
        Ok(info) => Ok(Json(json!({
            "status": "installed",
            "plugin": info
        }))),
        Err(err) => Err(ApiError::validation(err)),
    }
}

/// `DELETE /api/plugins/{id}`——卸载插件。
pub(crate) async fn uninstall_plugin(
    State(state): State<AppState>,
    AxumPath(plugin_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    if state.plugin_manager.uninstall(&plugin_id) {
        Ok(Json(json!({
            "status": "uninstalled",
            "pluginId": plugin_id
        })))
    } else {
        Err(ApiError::not_found(
            "PLUGIN_NOT_FOUND",
            &format!("Plugin not found: {plugin_id}"),
        ))
    }
}

/// `POST /api/plugins/reload`——全量重载。
pub(crate) async fn reload_plugins(State(state): State<AppState>) -> Json<Value> {
    state.plugin_manager.reload();
    let count = state.plugin_manager.count();
    Json(json!({ "status": "reloaded", "count": count }))
}

#[cfg(test)]
mod tests {
    // 端点集成测试由 routes 层 oneshot 覆盖（与 swarm/admin 端点同模式）。
    // 此处仅验证 DTO 解析。
    use super::InstallRequest;

    #[test]
    fn install_request_deserializes() {
        let json = r#"{"path": "/tmp/my-plugin"}"#;
        let req: InstallRequest = serde_json::from_str(json).expect("valid");
        assert_eq!(req.path, "/tmp/my-plugin");
    }
}
