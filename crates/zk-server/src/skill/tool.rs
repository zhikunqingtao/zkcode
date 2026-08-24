//! Model-callable Skill tool backed by the production `SkillRegistry`.

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_db::Db;
use zk_llm::ProviderRegistry;
use zk_tools::{AgentInvocation, AgentToolBackend, Tool, ToolContext, ToolOutput};

use super::SkillRegistry;

const MAX_SKILL_NAME_CHARS: usize = 128;
const MAX_SKILL_ARGUMENT_CHARS: usize = 32 * 1024;
const MAX_SKILL_TOKEN_BUDGET: usize = 32 * 1024;

/// Skill registry adapter exposed to the LLM tool catalog.
pub struct SkillTool {
    skills: Arc<SkillRegistry>,
    providers: Arc<ProviderRegistry>,
    db: Db,
    known_tools: BTreeSet<String>,
    fork_backend: Option<Arc<dyn AgentToolBackend>>,
}

impl std::fmt::Debug for SkillTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SkillTool")
            .field("known_tools", &self.known_tools.len())
            .field("fork_enabled", &self.fork_backend.is_some())
            .finish_non_exhaustive()
    }
}

impl SkillTool {
    /// Builds the tool from production registry/provider/event dependencies.
    #[must_use]
    pub fn new(
        skills: Arc<SkillRegistry>,
        providers: Arc<ProviderRegistry>,
        db: Db,
        known_tools: impl IntoIterator<Item = String>,
        fork_backend: Option<Arc<dyn AgentToolBackend>>,
    ) -> Self {
        Self {
            skills,
            providers,
            db,
            known_tools: known_tools.into_iter().collect(),
            fork_backend,
        }
    }

    fn resolve_model(&self, requested: Option<&str>) -> Result<Option<String>, ToolOutput> {
        let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        if requested.eq_ignore_ascii_case("inherit") {
            return Ok(None);
        }
        let resolved = if matches!(requested, "default" | "premium") {
            self.providers.default_model()
        } else {
            requested
        };
        if !self.providers.models().is_empty()
            && !self
                .providers
                .models()
                .iter()
                .any(|model| model == resolved)
        {
            return Err(error(
                "SKILL_MODEL_INVALID",
                format!("skill requested unsupported model '{requested}'"),
            ));
        }
        Ok(Some(resolved.to_owned()))
    }
}

impl Tool for SkillTool {
    fn name(&self) -> &'static str {
        "Skill"
    }

    fn description(&self) -> &'static str {
        "Execute a registered skill with validated arguments, model, and allowed-tool policy."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["name"],
            "properties": {
                "name": { "type": "string", "minLength": 1, "maxLength": MAX_SKILL_NAME_CHARS },
                "arguments": { "type": "string", "maxLength": MAX_SKILL_ARGUMENT_CHARS },
                "token_budget": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_SKILL_TOKEN_BUDGET
                }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        false
    }

    #[allow(clippy::too_many_lines)] // resolution, policy, prompt, provider and persistence boundary
    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Some(name) = input.get("name").and_then(Value::as_str).map(str::trim) else {
                return error("SKILL_NAME_INVALID", "name is required");
            };
            if name.is_empty()
                || name.chars().count() > MAX_SKILL_NAME_CHARS
                || !name.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
                })
            {
                return error(
                    "SKILL_NAME_INVALID",
                    "skill name may contain only ASCII letters, digits, '-' and '_'",
                );
            }
            let arguments = input
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if arguments.chars().count() > MAX_SKILL_ARGUMENT_CHARS {
                return error("SKILL_ARGUMENTS_TOO_LARGE", "skill arguments exceed 32KiB");
            }
            let token_budget = input
                .get("token_budget")
                .or_else(|| input.get("tokenBudget"))
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(MAX_SKILL_TOKEN_BUDGET);
            if token_budget == 0 || token_budget > MAX_SKILL_TOKEN_BUDGET {
                return error(
                    "SKILL_TOKEN_BUDGET_INVALID",
                    format!("token_budget must be between 1 and {MAX_SKILL_TOKEN_BUDGET}"),
                );
            }
            let Some(skill) = self.skills.resolve(name) else {
                return error("SKILL_NOT_FOUND", format!("Skill not found: {name}"));
            };
            if skill.frontmatter.disable_model_invocation {
                return error(
                    "SKILL_MODEL_INVOCATION_DISABLED",
                    format!("Skill cannot be invoked by the model: {name}"),
                );
            }
            let unknown: Vec<_> = skill
                .frontmatter
                .allowed_tools
                .iter()
                .filter(|tool| !self.known_tools.contains(tool.as_str()))
                .cloned()
                .collect();
            if !unknown.is_empty() {
                return error(
                    "SKILL_ALLOWED_TOOL_UNKNOWN",
                    format!("skill references unknown tools: {}", unknown.join(", ")),
                );
            }
            let model = match self.resolve_model(skill.frontmatter.resolved_model()) {
                Ok(model) => model,
                Err(rejection) => return rejection,
            };
            let params = skill.parse_args(arguments);
            let rendered = skill.render_template(&params);
            let approximate_tokens = rendered.chars().count().div_ceil(4);
            if approximate_tokens > token_budget {
                return error(
                    "SKILL_TOKEN_BUDGET_EXCEEDED",
                    format!(
                        "rendered skill requires approximately {approximate_tokens} tokens, budget is {token_budget}"
                    ),
                );
            }

            let event = json!({
                "skill": skill.effective_name(),
                "version": skill.frontmatter.version,
                "source": skill.source.as_str(),
                "fork": skill.frontmatter.is_fork(),
                "allowedTools": skill.frontmatter.allowed_tools,
                "model": model,
            });
            if let Some(run_id) = ctx.run_id()
                && let Err(db_error) = self
                    .db
                    .append_run_event(run_id, "skill_invoked", ctx.tool_use_id(), &event)
                    .await
            {
                tracing::warn!(run_id, error = %db_error, "failed to persist skill event");
            }

            if skill.frontmatter.is_fork() {
                let Some(backend) = self.fork_backend.as_ref() else {
                    return error(
                        "FEATURE_NOT_READY",
                        "forked skills require the validated Agent capability",
                    );
                };
                let (Some(parent_session_id), Some(parent_run_id), Some(tool_use_id)) =
                    (ctx.session_id(), ctx.run_id(), ctx.tool_use_id())
                else {
                    return error(
                        "SKILL_CONTEXT_INCOMPLETE",
                        "forked skill requires session, run, and tool-use identity",
                    );
                };
                let invocation = AgentInvocation {
                    prompt: rendered,
                    description: format!("skill {}", skill.effective_name()),
                    agent_type: skill.frontmatter.agent.clone(),
                    model_override: model,
                    isolation: "none".to_owned(),
                    run_in_background: false,
                    parent_session_id: parent_session_id.to_owned(),
                    parent_run_id: parent_run_id.to_owned(),
                    working_directory: ctx.working_dir().to_path_buf(),
                    tool_use_id: tool_use_id.to_owned(),
                    allowed_tools: (!skill.frontmatter.allowed_tools.is_empty())
                        .then(|| skill.frontmatter.allowed_tools.iter().cloned().collect()),
                };
                let (status, result, _) =
                    backend.execute_agent(invocation, ctx.cancel.clone()).await;
                return if status == "completed" {
                    ToolOutput::ok(result.unwrap_or_else(|| "Forked skill completed.".to_owned()))
                } else {
                    error(
                        "SKILL_FORK_FAILED",
                        result.unwrap_or_else(|| format!("forked skill ended with {status}")),
                    )
                };
            }

            ToolOutput {
                content: rendered.clone(),
                is_error: false,
                metadata: Some(json!({
                    "skillDirective": {
                        "allowedTools": skill.frontmatter.allowed_tools,
                        "model": model,
                    },
                    "structuredResult": {
                        "skill": skill.effective_name(),
                        "version": skill.frontmatter.version,
                        "source": skill.source.as_str(),
                        "fork": false,
                        "renderedPrompt": rendered,
                    }
                })),
            }
        })
    }
}

fn error(code: &str, message: impl AsRef<str>) -> ToolOutput {
    ToolOutput::error(format!("{code}: {}", message.as_ref()))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::skill::{SkillDefinition, SkillSource};

    fn context() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    fn tool(raw: &str, tools: &[&str]) -> SkillTool {
        let registry = Arc::new(SkillRegistry::new());
        registry.register(SkillDefinition::from_markdown(
            "demo.md",
            raw,
            SkillSource::Project,
            None,
        ));
        SkillTool::new(
            registry,
            Arc::new(ProviderRegistry::new().with_default_model("model-a")),
            Db::open_in_memory().expect("db"),
            tools.iter().map(|tool| (*tool).to_owned()),
            None,
        )
    }

    #[derive(Default)]
    struct RecordingAgentBackend {
        seen: Mutex<Vec<AgentInvocation>>,
    }

    impl AgentToolBackend for RecordingAgentBackend {
        fn execute_agent(
            &self,
            invocation: AgentInvocation,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> BoxFuture<'_, (String, Option<String>, Option<String>)> {
            self.seen.lock().expect("seen").push(invocation);
            Box::pin(futures::future::ready((
                "completed".to_owned(),
                Some("fork result".to_owned()),
                None,
            )))
        }
    }

    #[tokio::test]
    async fn renders_inline_prompt_and_emits_narrowing_directive() {
        let tool = tool(
            "---\narguments:\n  - target\nallowed-tools:\n  - Read\nversion: 2\n---\nInspect {{target}}",
            &["Read", "Skill"],
        );
        let output = tool
            .execute(
                json!({ "name": "demo", "arguments": "src/lib.rs", "token_budget": 100 }),
                context(),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "Inspect src/lib.rs");
        assert_eq!(
            output.metadata.expect("metadata")["skillDirective"]["allowedTools"],
            json!(["Read"])
        );
    }

    #[tokio::test]
    async fn rejects_disabled_unknown_tool_and_over_budget_skills() {
        let disabled = tool(
            "---\ndisable_model_invocation: true\n---\nNo",
            &["Read", "Skill"],
        );
        assert!(
            disabled
                .execute(json!({ "name": "demo" }), context())
                .await
                .content
                .starts_with("SKILL_MODEL_INVOCATION_DISABLED: ")
        );
        let unknown = tool(
            "---\nallowed-tools:\n  - RootShell\n---\nNo",
            &["Read", "Skill"],
        );
        assert!(
            unknown
                .execute(json!({ "name": "demo" }), context())
                .await
                .content
                .starts_with("SKILL_ALLOWED_TOOL_UNKNOWN: ")
        );
        let budget = tool("123456789", &["Skill"]);
        assert!(
            budget
                .execute(json!({ "name": "demo", "token_budget": 1 }), context())
                .await
                .content
                .starts_with("SKILL_TOKEN_BUDGET_EXCEEDED: ")
        );
    }

    #[tokio::test]
    async fn invocation_identity_is_persisted_to_the_parent_run_event_log() {
        let db = Db::open_in_memory().expect("db");
        let session = db.create_session("model-a", "/tmp").await.expect("session");
        db.start_run("run-skill", &session.id, None, Some("query"), "model-a")
            .await
            .expect("run");
        let registry = Arc::new(SkillRegistry::new());
        registry.register(SkillDefinition::from_markdown(
            "demo.md",
            "---\nversion: 7\n---\nInspect",
            SkillSource::Project,
            None,
        ));
        let tool = SkillTool::new(
            registry,
            Arc::new(ProviderRegistry::new().with_default_model("model-a")),
            db.clone(),
            ["Skill".to_owned()],
            None,
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = ToolContext::new(CancellationToken::new(), tx)
            .with_session_id(&session.id)
            .with_run_id("run-skill")
            .with_tool_use_id("skill-call-1")
            .with_working_dir("/tmp");
        let output = tool.execute(json!({ "name": "demo" }), ctx).await;
        assert!(!output.is_error, "{}", output.content);
        let events = db.get_run_events("run-skill", 0, 20).await.expect("events");
        let invoked = events
            .iter()
            .find(|event| event.event_type == "skill_invoked")
            .expect("skill event");
        let event: Value = serde_json::from_str(&invoked.event_data).expect("event JSON");
        assert_eq!(event["toolUseId"], "skill-call-1");
        assert_eq!(event["data"]["skill"], "demo");
        assert_eq!(event["data"]["version"], "7");
        assert_eq!(event["data"]["source"], "PROJECT");
    }

    #[tokio::test]
    async fn fork_passes_allowed_tools_model_and_parent_identity_to_child_backend() {
        let registry = Arc::new(SkillRegistry::new());
        registry.register(SkillDefinition::from_markdown(
            "forked.md",
            "---\ncontext: fork\nallowed-tools:\n  - Read\nmodel: default\nagent: explore\n---\nInspect safely",
            SkillSource::Project,
            None,
        ));
        let backend = Arc::new(RecordingAgentBackend::default());
        let tool = SkillTool::new(
            registry,
            Arc::new(ProviderRegistry::new().with_default_model("model-a")),
            Db::open_in_memory().expect("db"),
            ["Read".to_owned(), "Skill".to_owned()],
            Some(Arc::clone(&backend) as Arc<dyn AgentToolBackend>),
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("parent-session")
            .with_run_id("parent-run")
            .with_tool_use_id("skill-call")
            .with_working_dir("/tmp");
        let output = tool.execute(json!({"name": "forked"}), ctx).await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "fork result");
        let seen = backend.seen.lock().expect("seen");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].model_override.as_deref(), Some("model-a"));
        assert_eq!(seen[0].agent_type.as_deref(), Some("explore"));
        assert_eq!(seen[0].parent_session_id, "parent-session");
        assert_eq!(
            seen[0].allowed_tools,
            Some(std::collections::BTreeSet::from(["Read".to_owned()]))
        );
    }
}
