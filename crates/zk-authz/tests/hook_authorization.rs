//! Hook policy uses production `SQLite` grants and the ordinary authorization gateway.
mod common;

use common::{FakeTool, Harness};
use serde_json::{Value, json};
use zk_authz::model::{GrantKind, PermissionMode, PermissionScope};
use zk_authz::tool_facts::ToolFacts;

struct HostHook(Value);
impl ToolFacts for HostHook {
    fn name(&self) -> &'static str {
        "Hook"
    }
    fn host_hook_facts(&self) -> Option<&Value> {
        Some(&self.0)
    }
}
fn hook(h: &Harness, http: bool) -> HostHook {
    HostHook(
        json!({"workingRoot":h.workspace, "source":h.workspace.join(".zk/hooks.toml"),
        "declaration":{"name":"notify", "event":"RUN_START", "command":if http {Value::Null} else {json!("printf ok")},
        "url":if http {json!("https://example.com/hook")} else {Value::Null}},
        "environment":{"PATH":"/usr/bin:/bin"}}),
    )
}

#[tokio::test]
async fn hook_mode_matrix_for_command_and_http() {
    for http in [false, true] {
        for mode in [
            PermissionMode::Plan,
            PermissionMode::DontAsk,
            PermissionMode::Default,
            PermissionMode::AcceptEdits,
            PermissionMode::AutoApprove,
        ] {
            let h = Harness::new();
            h.seed_run("s", "r").await;
            h.modes.set(mode);
            let tool = hook(&h, http);
            let frozen = h.freeze("Hook", &tool.0);
            let ctx = h.context("r", "host-hook-1", "s");
            let p = h
                .service
                .prepare(&tool, &frozen, &tool.0, &ctx)
                .await
                .unwrap();
            h.gateway.allow_once(&p.descriptor.operation_hash);
            let result = h
                .service
                .authorize(&tool, &frozen, tool.0.clone(), &ctx)
                .await;
            match mode {
                PermissionMode::Plan => {
                    assert_eq!(result.unwrap_err().code, "PLAN_MODE_EFFECT_DENIED");
                }
                PermissionMode::DontAsk => {
                    assert_eq!(result.unwrap_err().code, "PERMISSION_INTERACTION_REQUIRED");
                }
                _ => {
                    let allowed = result.unwrap();
                    h.execution_gateway()
                        .admit(&tool, &allowed, &ctx)
                        .await
                        .unwrap();
                }
            }
            assert_eq!(
                h.gateway.prompt_count(),
                usize::from(matches!(
                    mode,
                    PermissionMode::Default | PermissionMode::AcceptEdits
                ))
            );
            assert!(h.events.of_type("tool_started").is_empty());
            assert_eq!(
                h.events.of_type("hook_admitted").len(),
                usize::from(!matches!(
                    mode,
                    PermissionMode::Plan | PermissionMode::DontAsk
                ))
            );
        }
    }
}

#[tokio::test]
async fn hook_grant_exact_identity_plan_override_and_revocation() {
    let h = Harness::new();
    h.seed_run("s", "r").await;
    let tool = hook(&h, false);
    let frozen = h.freeze("Hook", &tool.0);
    let ctx = h.context("r", "host-hook", "s");
    let prepared = h
        .service
        .prepare(&tool, &frozen, &tool.0, &ctx)
        .await
        .unwrap();
    for scope in [PermissionScope::Run, PermissionScope::Session] {
        let plan = zk_authz::grants::plan(&prepared.descriptor, Some(scope)).unwrap();
        assert_eq!(plan.kind, GrantKind::ExactGuarded);
    }
    assert!(
        zk_authz::grants::plan(&prepared.descriptor, Some(PermissionScope::Workspace)).is_none()
    );
    h.gateway.allow_remember(
        &h.grants,
        &prepared.subject,
        &prepared.descriptor,
        PermissionScope::Session,
    );
    h.service
        .authorize(&tool, &frozen, tool.0.clone(), &ctx)
        .await
        .unwrap();
    h.modes.set(PermissionMode::DontAsk);
    let allowed = h
        .service
        .authorize(&tool, &frozen, tool.0.clone(), &ctx)
        .await
        .unwrap();
    assert!(allowed.grant_id.is_some());
    let mut changed = hook(&h, false);
    changed.0["declaration"]["command"] = json!("printf changed");
    assert!(
        h.service
            .authorize(
                &changed,
                &h.freeze("Hook", &changed.0),
                changed.0.clone(),
                &ctx
            )
            .await
            .is_err()
    );
    let other_root = h.workspace.join("worktree");
    std::fs::create_dir(&other_root).unwrap();
    let mut moved = hook(&h, false);
    moved.0["workingRoot"] = json!(other_root);
    assert!(
        h.service
            .authorize(&moved, &h.freeze("Hook", &moved.0), moved.0.clone(), &ctx)
            .await
            .is_err()
    );
    h.modes.set(PermissionMode::Plan);
    assert_eq!(
        h.service
            .authorize(&tool, &frozen, tool.0.clone(), &ctx)
            .await
            .unwrap_err()
            .code,
        "PLAN_MODE_EFFECT_DENIED"
    );
    assert!(
        h.execution_gateway()
            .admit(&tool, &allowed, &ctx)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn model_hook_name_cannot_select_host_analyzer_and_bash_grant_cannot_authorize_hook() {
    let h = Harness::new();
    h.seed_run("s", "r").await;
    let tool = hook(&h, false);
    let ctx = h.context("r", "host-hook", "s");
    let fake = FakeTool::new("Hook");
    let prepared = h
        .service
        .prepare(&fake, &h.freeze("Hook", &tool.0), &tool.0, &ctx)
        .await
        .unwrap();
    assert_eq!(prepared.descriptor.analyzer_id, "static-or-remote-v1");
    let bash = FakeTool::new("Bash");
    let input = json!({"command":"printf ok"});
    let frozen = h.freeze("Bash", &input);
    let p = h
        .service
        .prepare(&bash, &frozen, &input, &ctx)
        .await
        .unwrap();
    h.gateway.allow_remember(
        &h.grants,
        &p.subject,
        &p.descriptor,
        PermissionScope::Session,
    );
    h.service
        .authorize(&bash, &frozen, input, &ctx)
        .await
        .unwrap();
    h.modes.set(PermissionMode::DontAsk);
    assert_eq!(
        h.service
            .authorize(&tool, &h.freeze("Hook", &tool.0), tool.0.clone(), &ctx)
            .await
            .unwrap_err()
            .code,
        "PERMISSION_INTERACTION_REQUIRED"
    );
}

#[tokio::test]
async fn absolute_command_denial_precedes_auto_approval() {
    let h = Harness::new();
    h.seed_run("s", "r").await;
    h.modes.set(PermissionMode::AutoApprove);
    h.bash
        .set(zk_authz::tool_facts::BashParseOutcome::BlacklistDeny {
            reason: "test deny".into(),
        });
    let tool = hook(&h, false);
    assert_eq!(
        h.service
            .authorize(
                &tool,
                &h.freeze("Hook", &tool.0),
                tool.0.clone(),
                &h.context("r", "h", "s")
            )
            .await
            .unwrap_err()
            .code,
        "COMMAND_ABSOLUTELY_DENIED"
    );
}
