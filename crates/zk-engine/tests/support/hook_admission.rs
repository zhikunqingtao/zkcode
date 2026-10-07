//! Explicit fixture authority for engine-only tests; production authority is
//! exercised by the server integration tests against real permission storage.
use futures::future::BoxFuture;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use zk_engine::hook::{
    HookAdmission, HookConfig, HookContext, HookEvent, HookRegistry, HookStartPermit,
};

#[derive(Debug, Default)]
pub struct FixtureHookAdmission {
    approved: Arc<Mutex<Vec<HookConfig>>>,
}

impl FixtureHookAdmission {
    pub fn for_registry(registry: &HookRegistry) -> Self {
        let authority = Self::default();
        authority.approve_registry(registry);
        authority
    }

    pub fn approve_registry(&self, registry: &HookRegistry) {
        assert!(!registry.has_invalid_security_config());
        *self.approved.lock().unwrap() = HookEvent::ALL
            .into_iter()
            .flat_map(|event| registry.hooks_for(event).iter().cloned())
            .collect();
    }
}

#[derive(Debug)]
struct FixturePermit {
    approved: Arc<Mutex<Vec<HookConfig>>>,
    declaration: HookConfig,
    working_dir: Option<String>,
    session: Option<String>,
    run: Option<String>,
    consumed: AtomicBool,
}

impl HookAdmission for FixtureHookAdmission {
    fn admit<'a>(
        &'a self,
        hook: &'a HookConfig,
        event: HookEvent,
        context: &'a HookContext,
    ) -> BoxFuture<'a, Result<Box<dyn HookStartPermit>, String>> {
        Box::pin(async move {
            if hook.event != event || !self.approved.lock().unwrap().contains(hook) {
                return Err("FIXTURE_HOOK_NOT_APPROVED".into());
            }
            Ok(Box::new(FixturePermit {
                approved: self.approved.clone(),
                declaration: hook.clone(),
                working_dir: context.working_dir.clone(),
                session: context.session_id.clone(),
                run: context.execution_run_id().map(str::to_owned),
                consumed: AtomicBool::new(false),
            }) as Box<dyn HookStartPermit>)
        })
    }
}

impl HookStartPermit for FixturePermit {
    fn recheck<'a>(
        &'a self,
        hook: &'a HookConfig,
        event: HookEvent,
        context: &'a HookContext,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.consumed.swap(true, Ordering::SeqCst) {
                return Err("FIXTURE_HOOK_RECEIPT_ALREADY_CONSUMED".into());
            }
            if hook != &self.declaration
                || hook.event != event
                || self.working_dir != context.working_dir
                || self.session != context.session_id
                || self.run.as_deref() != context.execution_run_id()
                || !self.approved.lock().unwrap().contains(hook)
            {
                return Err("FIXTURE_HOOK_RECEIPT_CHANGED".into());
            }
            Ok(())
        })
    }
}
