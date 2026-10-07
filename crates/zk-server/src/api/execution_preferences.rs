//! Session-local fast/effort settings for ordinary chat; Query keeps explicit options.
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zk_db::{Db, SessionExecutionPreferences};
use zk_engine::{ConversationPreferenceSource, ConversationRunOptions};
use zk_llm::{ChatProvider, ChatRequest, ReasoningEffort, SwappableProvider, ThinkingMode};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExecutionPreferencesPatch {
    pub revision: u64,
    pub effort: Option<String>,
    pub fast: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutionPreferencesResponse {
    pub revision: u64,
    pub effort: String,
    pub fast: bool,
    pub effective_model: String,
    pub fast_available: bool,
    pub supported_efforts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_error: Option<String>,
}

pub(crate) struct ChatPreferences {
    pub db: Db,
    pub providers: Arc<SwappableProvider>,
}

fn configured_fast(providers: &zk_llm::ProviderRegistry) -> Option<String> {
    crate::engine_bridge::configured_fast_model()
        .map(|model| model.trim().to_owned())
        .filter(|model| providers.model_owner(model).is_some())
}

fn effort(value: &str) -> Result<Option<ReasoningEffort>, ApiError> {
    if value == "auto" {
        return Ok(None);
    }
    serde_json::from_value(serde_json::Value::String(value.into()))
        .map(Some)
        .map_err(|_| {
            ApiError::validation_with_code(
                "SESSION_EFFORT_INVALID",
                "effort must be auto, low, medium, high, xhigh, or max",
            )
        })
}

fn resolve(
    providers: &zk_llm::ProviderRegistry,
    model: &str,
    preferences: &SessionExecutionPreferences,
) -> Result<(ConversationRunOptions, ExecutionPreferencesResponse), ApiError> {
    resolve_with_fast(providers, model, preferences, configured_fast(providers))
}

fn resolve_with_fast(
    providers: &zk_llm::ProviderRegistry,
    model: &str,
    preferences: &SessionExecutionPreferences,
    configured: Option<String>,
) -> Result<(ConversationRunOptions, ExecutionPreferencesResponse), ApiError> {
    let fast_model = configured.filter(|model| providers.model_owner(model).is_some());
    let effective_model = if preferences.fast {
        fast_model.clone().ok_or_else(|| ApiError::validation_with_code("SESSION_FAST_MODEL_UNAVAILABLE", "Fast mode requires an explicitly configured ZK_FAST_MODEL or LLM_FAST_MODEL registered with a provider"))?
    } else {
        model.to_owned()
    };
    let selected_effort = preferences
        .effort
        .as_deref()
        .map(effort)
        .transpose()?
        .flatten();
    let mut request = ChatRequest::new(&effective_model);
    request.thinking = ThinkingMode::Adaptive;
    request.reasoning_effort = selected_effort;
    // Existing auto/default sessions remain usable with custom model routing.
    // Explicit choices require the actual adapter to prove support.
    if preferences.fast || selected_effort.is_some() {
        providers
            .validate_request_options(&request)
            .map_err(|error| {
                ApiError::validation_with_code(
                    "SESSION_EXECUTION_OPTIONS_UNSUPPORTED",
                    &error.to_string(),
                )
            })?;
    }
    let supported_efforts = [
        ReasoningEffort::Low,
        ReasoningEffort::Medium,
        ReasoningEffort::High,
        ReasoningEffort::XHigh,
        ReasoningEffort::Max,
    ]
    .into_iter()
    .filter(|value| {
        request.reasoning_effort = Some(*value);
        providers.validate_request_options(&request).is_ok()
    })
    .map(|value| value.as_str().to_owned())
    .collect();
    Ok((
        ConversationRunOptions {
            model_override: preferences.fast.then(|| effective_model.clone()),
            reasoning_effort: selected_effort,
            ..ConversationRunOptions::default()
        },
        ExecutionPreferencesResponse {
            revision: preferences.revision,
            effort: preferences.effort.clone().unwrap_or_else(|| "auto".into()),
            fast: preferences.fast,
            effective_model,
            fast_available: fast_model.is_some(),
            supported_efforts,
            validation_error: None,
        },
    ))
}

pub(crate) async fn load_execution_preferences(
    state: &AppState,
    session_id: &str,
) -> Result<ExecutionPreferencesResponse, ApiError> {
    let session = state
        .db
        .get_session(session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(session_id))?;
    let preferences = state.db.session_execution_preferences(session_id).await?;
    let providers = state.providers.load();
    match resolve(&providers, &session.model, &preferences) {
        Ok((_, response)) => Ok(response),
        // Keep the revision and controls reachable after credentials/model
        // configuration changes. Execution still fails until the user fixes it.
        Err(error) => Ok(ExecutionPreferencesResponse {
            revision: preferences.revision,
            effort: preferences.effort.unwrap_or_else(|| "auto".into()),
            fast: preferences.fast,
            effective_model: session.model,
            fast_available: configured_fast(&providers).is_some(),
            supported_efforts: Vec::new(),
            validation_error: Some(error.to_string()),
        }),
    }
}

pub(crate) async fn update_execution_preferences(
    state: &AppState,
    session_id: &str,
    patch: ExecutionPreferencesPatch,
) -> Result<ExecutionPreferencesResponse, ApiError> {
    let session = state
        .db
        .get_session(session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(session_id))?;
    let mut preferences = state.db.session_execution_preferences(session_id).await?;
    if let Some(value) = patch.effort {
        preferences.effort = effort(&value)?.map(|level| level.as_str().to_owned());
    }
    if let Some(value) = patch.fast {
        preferences.fast = value;
    }
    let (_, mut response) = resolve(&state.providers.load(), &session.model, &preferences)?;
    let saved = state
        .db
        .set_session_execution_preferences(session_id, patch.revision, preferences)
        .await
        .map_err(|error| match error {
            zk_db::DbError::Conflict(_) => ApiError {
                status: axum::http::StatusCode::CONFLICT,
                code: "SESSION_EXECUTION_PREFERENCES_CHANGED".into(),
                message: "The session settings changed; reload before saving".into(),
            },
            other => ApiError::from(other),
        })?;
    response.revision = saved.revision;
    Ok(response)
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ExecutionPreferencesResponse>, ApiError> {
    load_execution_preferences(&state, &id).await.map(Json)
}
pub(crate) async fn patch(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<ExecutionPreferencesPatch>,
) -> Result<Json<ExecutionPreferencesResponse>, ApiError> {
    update_execution_preferences(&state, &id, body)
        .await
        .map(Json)
}

impl ConversationPreferenceSource for ChatPreferences {
    fn load<'a>(
        &'a self,
        session_id: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<ConversationRunOptions, String>> {
        Box::pin(async move {
            let session = self
                .db
                .get_session(session_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "Session does not exist".to_owned())?;
            let preferences = self
                .db
                .session_execution_preferences(session_id)
                .await
                .map_err(|error| error.to_string())?;
            resolve(&self.providers.load(), &session.model, &preferences)
                .map(|result| result.0)
                .map_err(|error| error.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_routing_requires_registered_explicit_configuration_and_preserves_default() {
        let providers = zk_llm::ProviderRegistry::from_configs(vec![zk_llm::ProviderConfig::new(
            "deepseek",
            "https://example.invalid/",
            zk_llm::ApiKey::new("fixture"),
            "deepseek-v4-pro",
            vec!["deepseek-v4-pro".into(), "deepseek-flash".into()],
        )])
        .unwrap();
        let preferences = SessionExecutionPreferences {
            revision: 0,
            effort: Some("low".into()),
            fast: true,
        };
        assert!(resolve_with_fast(&providers, "deepseek-v4-pro", &preferences, None).is_err());
        assert!(
            resolve_with_fast(
                &providers,
                "deepseek-v4-pro",
                &preferences,
                Some("typo".into())
            )
            .is_err()
        );
        let (options, response) = resolve_with_fast(
            &providers,
            "deepseek-v4-pro",
            &preferences,
            Some("deepseek-flash".into()),
        )
        .unwrap();
        assert_eq!(options.model_override.as_deref(), Some("deepseek-flash"));
        assert_eq!(options.reasoning_effort, Some(ReasoningEffort::Low));
        assert_eq!(response.effective_model, "deepseek-flash");
        assert_eq!(providers.default_model(), "deepseek-v4-pro");
        let (options, _) = resolve_with_fast(
            &providers,
            "deepseek-v4-pro",
            &SessionExecutionPreferences::default(),
            Some("deepseek-flash".into()),
        )
        .unwrap();
        assert!(options.model_override.is_none());
    }
}
