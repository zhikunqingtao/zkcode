//! `OpenAPI` 文档端点——`GET /api/openapi.json`（S7b）。
//!
//! `#[utoipa::path]` 编译期注解散布在各 handler（sessions 8 + health×3 +
//! auth×2 + models + config×4 + projects×5 + skills×2 + tools×3 +
//! doctor + mcp 10 + mcp 能力注册表 10），本模块以 [`ApiDoc`]
//! 聚合并暴露
//! JSON 文档。新增运维端点（旧系统无对应，同 `/metrics` 定位）；文档本身与
//! `/metrics` 不进 paths（非业务契约面）。

use axum::Json;
use utoipa::OpenApi;

use crate::api::{
    activity, attachment, config, doctor, file, grant, history, interaction, mcp, mcp_capability,
    memory, models, project, run, session, skill, speech, system, tool,
};

/// Phase 1 端点聚合文档（title/version 取 crate 元数据）。
#[derive(OpenApi)]
#[openapi(
    info(
        title = "zkcode REST API",
        description = "Phase 1 REST 契约（响应形状权威：docs/baseline/samples/）"
    ),
    paths(
        session::create_session,
        session::list_sessions,
        session::get_session_detail,
        session::delete_session,
        session::resume_session,
        session::compact_session,
        session::export_session,
        session::list_session_messages,
        system::health,
        system::health_live,
        system::health_ready,
        system::auth_status,
        system::auth_token,
        models::list_models,
        config::get_config,
        config::put_config,
        config::get_project_config,
        config::put_project_config,
        project::list_projects,
        project::create_project,
        project::browse_directories,
        project::pick_directory,
        project::revoke_project,
        interaction::pending,
        interaction::decide,
        grant::list_active,
        grant::revoke,
        skill::list_skills,
        skill::get_skill,
        tool::list_tools,
        tool::get_tool_detail,
        tool::toggle_tool,
        doctor::doctor,
        run::list_runs,
        run::get_run,
        run::get_events,
        file::search_files,
        file::preview,
        file::reveal,
        activity::get_activities,
        attachment::upload,
        attachment::download,
        speech::asr_status,
        speech::recognize,
        speech::tts_status,
        speech::synthesize,
        mcp::list_servers,
        mcp::add_server,
        mcp::delete_server,
        mcp::restart_server,
        mcp::server_logs,
        mcp::reconnect_server,
        mcp::list_resources,
        mcp::read_resource,
        mcp::list_prompts,
        mcp::execute_prompt,
        mcp_capability::list_capabilities,
        mcp_capability::list_domains,
        mcp_capability::get_capability,
        mcp_capability::update_capability,
        mcp_capability::add_capability,
        mcp_capability::delete_capability,
        mcp_capability::toggle_capability,
        mcp_capability::list_server_tools,
        mcp_capability::test_capability,
        mcp_capability::invoke_capability,
        history::list_snapshots,
        history::rewind_to_snapshot,
        history::get_diff_stats,
        memory::get_memories,
        memory::update_memories,
        memory::create_memory,
        memory::get_all_memories,
        memory::delete_memory,
    )
)]
pub(crate) struct ApiDoc;

/// `GET /api/openapi.json`——`OpenAPI` 3.1 文档（每请求即时序列化，
/// 文档为编译期常量结构，无缓存必要）。
pub(crate) async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 聚合文档含全部 62 条业务路径（逐路径互锁在集成测试，
    /// 此处锁 paths 非空与关键八域在位）。
    #[test]
    fn api_doc_aggregates_paths() {
        let doc = ApiDoc::openapi();
        let paths = &doc.paths.paths;
        assert!(paths.contains_key("/api/sessions"));
        assert!(paths.contains_key("/api/health/live"));
        assert!(paths.contains_key("/api/config"));
        assert!(paths.contains_key("/api/projects"));
        assert!(paths.contains_key("/api/interactions/pending"));
        assert!(paths.contains_key("/api/permissions/grants"));
        assert!(paths.contains_key("/api/skills"));
        assert!(paths.contains_key("/api/tools"));
        assert!(paths.contains_key("/api/tools/{toolName}"));
        assert!(paths.contains_key("/api/doctor"));
        // MCP 域（Batch 4B）：服务器 9 路径 + 能力注册表 7 路径。
        assert!(paths.contains_key("/api/mcp/servers"));
        assert!(paths.contains_key("/api/mcp/servers/{name}/restart"));
        assert!(paths.contains_key("/api/mcp/resources/read"));
        assert!(paths.contains_key("/api/mcp/prompts/execute"));
        assert!(paths.contains_key("/api/mcp/reconnect"));
        assert!(paths.contains_key("/api/mcp/capabilities"));
        assert!(paths.contains_key("/api/mcp/capabilities/domains"));
        assert!(paths.contains_key("/api/mcp/capabilities/{id}/toggle"));
        assert!(paths.contains_key("/api/mcp/capabilities/{id}/server-tools"));
        // 记忆与历史域（Batch 5 Step 6）：记忆 3 路径（`/api/memory` 承载
        // GET/PUT/POST 三方法）+ 文件历史 3 路径。
        assert!(paths.contains_key("/api/memory"));
        assert!(paths.contains_key("/api/memory/all"));
        assert!(paths.contains_key("/api/memory/{memoryId}"));
        assert!(paths.contains_key("/api/sessions/{sessionId}/history/snapshots"));
        assert!(paths.contains_key("/api/sessions/{sessionId}/history/rewind"));
        assert!(paths.contains_key("/api/sessions/{sessionId}/history/diff"));
        assert!(paths.contains_key("/api/asr/status"));
        assert!(paths.contains_key("/api/asr/recognize"));
        assert!(paths.contains_key("/api/tts/status"));
        assert!(paths.contains_key("/api/tts/synthesize"));
        assert_eq!(paths.len(), 62);
    }
}
