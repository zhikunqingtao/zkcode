//! Untrusted hook output must not be interpolated into diagnostic logs.
#[path = "support/hook_admission.rs"]
mod hook_admission_fixture;
use hook_admission_fixture::FixtureHookAdmission;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use zk_engine::hook::{
    HookConfig, HookContext, HookEvent, HookRegistry, HookRole, HookService, PreHookDecision,
};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<String>>);
struct Fields<'a>(&'a mut String);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, "{}={value:?};", field.name());
    }
}
impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        attributes.record(&mut Fields(&mut self.0.lock().unwrap()));
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, values: &Record<'_>) {
        values.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        event.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_hook_decision_does_not_leak_arbitrary_stdout_to_logs() {
    let capture = Capture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    let root = std::env::temp_dir().join(format!("zk-hook-logs-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let mut registry = HookRegistry::new();
    registry.register(HookConfig {
        name: "fixture".into(),
        event: HookEvent::PreToolExecution,
        role: HookRole::Security,
        matcher: None,
        priority: 0,
        command: Some(format!(
            "printf x >> '{}'; printf '%s' '{{\"decision\":\"PRIVATE_BODY_CANARY\"}}'",
            root.join("entered").display()
        )),
        url: None,
        async_mode: false,
        timeout_secs: 5,
    });
    let admission = Arc::new(FixtureHookAdmission::for_registry(&registry));
    let service = HookService::new(registry).with_admission(admission);
    let context = HookContext::new().with_tool("Read");
    let outcome = service
        .evaluate_pre_tool(&context, &json!({"path":"PRIVATE_PATH_CANARY"}))
        .await;
    assert!(matches!(outcome, PreHookDecision::Deny {code,..} if code == "HOOK_SECURITY_FAILED"));
    assert_eq!(std::fs::read_to_string(root.join("entered")).unwrap(), "x");
    let diagnostic = capture.0.lock().unwrap().clone();
    assert!(diagnostic.contains("HOOK_SECURITY_FAILED"));
    assert!(!diagnostic.contains("PRIVATE_"), "{diagnostic}");
    let outcome = service
        .evaluate_pre_tool(
            &context.with_ephemeral_content(true),
            &json!({"path":"PRIVATE_PATH_CANARY"}),
        )
        .await;
    assert!(
        matches!(outcome, PreHookDecision::Deny {code,..} if code == "HOOK_EPHEMERAL_SECURITY_UNSUPPORTED")
    );
    assert_eq!(std::fs::read_to_string(root.join("entered")).unwrap(), "x");
    std::fs::remove_dir_all(root).unwrap();
}

fn approved_service(command: &str, events: &[(HookEvent, HookRole)]) -> HookService {
    let mut registry = HookRegistry::new();
    for &(event, role) in events {
        registry.register(HookConfig {
            name: event.as_str().into(),
            event,
            role,
            matcher: None,
            priority: 0,
            command: Some(command.into()),
            url: None,
            async_mode: false,
            timeout_secs: 5,
        });
    }
    let admission = Arc::new(FixtureHookAdmission::for_registry(&registry));
    HookService::new(registry).with_admission(admission)
}

#[tokio::test]
async fn external_ceiling_is_enforced_at_all_hook_execution_phases() {
    use zk_engine::hook::{ExternalHookPolicy, StopHookDecision};
    let root = std::env::temp_dir().join(format!("zk-hook-ceiling-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let marker = root.join("effects.txt");
    let command = format!(
        "printf x >> '{}'; printf '%s' '{{\"decision\":\"continue\",\"presentation\":\"visible\"}}'",
        marker.display()
    );
    let service = approved_service(
        &command,
        &[
            (HookEvent::PreToolExecution, HookRole::Transform),
            (HookEvent::PostToolExecution, HookRole::Presentation),
            (HookEvent::Stop, HookRole::Transform),
            (HookEvent::Notification, HookRole::Notification),
        ],
    );
    let denied = HookContext::new()
        .with_tool("Read")
        .with_external_policy(Some(ExternalHookPolicy {
            write: true,
            process: false,
            network: false,
        }));
    let original = json!({"file_path":"original.txt"});
    assert_eq!(
        service.evaluate_pre_tool(&denied, &original).await,
        PreHookDecision::Continue { input: original }
    );
    assert_eq!(service.post_tool_presentation(&denied).await, None);
    assert_eq!(
        service.evaluate_stop(&denied).await,
        StopHookDecision::Accept
    );
    service.fire(HookEvent::Notification, &denied).await;
    assert!(!marker.exists());
    let allowed = denied.with_external_policy(Some(ExternalHookPolicy {
        write: true,
        process: true,
        network: true,
    }));
    assert!(matches!(
        service.evaluate_pre_tool(&allowed, &json!({})).await,
        PreHookDecision::Continue { .. }
    ));
    assert_eq!(
        service.post_tool_presentation(&allowed).await.as_deref(),
        Some("visible")
    );
    assert_eq!(
        service.evaluate_stop(&allowed).await,
        StopHookDecision::Accept
    );
    service.fire(HookEvent::Notification, &allowed).await;
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "xxxx");
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let cancelled = allowed.with_cancellation(&cancel);
    service.fire(HookEvent::Notification, &cancelled).await;
    assert_eq!(
        service.evaluate_stop(&cancelled).await,
        StopHookDecision::Accept
    );
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "xxxx");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn external_ceiling_denies_required_security_hooks_without_executing_them() {
    use zk_engine::hook::{ExternalHookPolicy, StopHookDecision};
    let root = std::env::temp_dir().join(format!("zk-security-ceiling-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let command = format!(
        "printf x >> '{}'; printf '%s' '{{\"decision\":\"continue\"}}'",
        root.join("entered").display()
    );
    let service = approved_service(
        &command,
        &[
            (HookEvent::PreToolExecution, HookRole::Security),
            (HookEvent::Stop, HookRole::Security),
        ],
    );
    let context = HookContext::new()
        .with_tool("Read")
        .with_external_policy(Some(ExternalHookPolicy {
            write: true,
            process: false,
            network: false,
        }));
    assert!(
        matches!(service.evaluate_pre_tool(&context, &json!({})).await,
            PreHookDecision::Deny { code, .. } if code == "HOOK_EXTERNAL_CAPABILITY_DENIED")
    );
    assert_eq!(
        service.evaluate_stop(&context).await,
        StopHookDecision::Prevent
    );
    assert!(!root.join("entered").exists());
    std::fs::remove_dir_all(root).unwrap();
}
