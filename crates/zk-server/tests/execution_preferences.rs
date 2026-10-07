//! Ordinary chat options use durable session-local preferences and real providers.
mod common;
use futures::{StreamExt, stream};
use serde_json::json;
use std::sync::{Arc, Mutex};
use zk_engine::{ConversationRunOptions, ConversationService};
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry,
    ReasoningEffort,
};
use zk_server::{engine_bridge::wire_engine, routes::build_router, state::AppState};

#[derive(Default)]
struct Provider(Mutex<Vec<ChatRequest>>);
impl ChatProvider for Provider {
    fn provider_name(&self) -> &'static str {
        "deepseek"
    }
    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<futures::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        self.0.lock().unwrap().push(request);
        Ok(stream::iter([
            ProviderEvent::TextDelta {
                text: "answer".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(zk_protocol::Usage::default()),
            },
        ])
        .boxed())
    }
}

#[tokio::test]
async fn chat_effort_is_persistent_validated_and_query_options_stay_isolated() {
    let provider = Arc::new(Provider::default());
    let mut registry = ProviderRegistry::new();
    registry.register("deepseek", provider.clone(), vec!["deepseek-flash".into()]);
    let state = AppState::for_tests().with_providers(registry);
    let db = state.db.clone();
    let session = db
        .create_session("deepseek-flash", "/tmp")
        .await
        .unwrap()
        .id;
    let engine = wire_engine(&state);
    let mut app = build_router(state);
    let endpoint = format!("/api/sessions/{session}/execution-preferences");
    let (status, _, body) = common::call(&mut app, common::local_get(&endpoint)).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(common::json_body(&body)["effort"], "auto");
    let patch = |revision, value| {
        common::local_patch(
            &endpoint,
            Some(json!({"revision":revision,"effort":value}).to_string()),
        )
    };
    let (status, _, _) = common::call(&mut app, patch(0, "medium")).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(
        db.session_execution_preferences(&session)
            .await
            .unwrap()
            .revision,
        0
    );
    let (status, _, _) = common::call(&mut app, patch(0, "low")).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let (status, _, body) = common::call(&mut app, patch(0, "max")).await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT);
    assert_eq!(
        common::json_body(&body)["code"],
        "SESSION_EXECUTION_PREFERENCES_CHANGED"
    );
    engine
        .spawn_user_message(&session, "ordinary chat".into())
        .await
        .unwrap();
    assert_eq!(
        provider.0.lock().unwrap()[0].reasoning_effort,
        Some(ReasoningEffort::Low)
    );
    assert_eq!(
        db.get_session(&session).await.unwrap().unwrap().model,
        "deepseek-flash"
    );
    let service = ConversationService::new(engine, db.clone());
    service
        .execute_with_options(&session, "query".into(), ConversationRunOptions::default())
        .await;
    assert_eq!(provider.0.lock().unwrap()[1].reasoning_effort, None);
    assert_eq!(
        db.session_execution_preferences(&session)
            .await
            .unwrap()
            .effort
            .as_deref(),
        Some("low")
    );
}
