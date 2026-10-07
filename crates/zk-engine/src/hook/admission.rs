//! Host-only permission port. Hook declarations never confer execution authority.
use futures::future::BoxFuture;

use super::{HookConfig, HookContext, HookEvent};

/// A separately authorized host operation, never exposed as a model tool.
/// Implementations must recheck the current declaration, permission and live Run
/// immediately before returning. Approval does not mean the Hook succeeded.
pub trait HookAdmission: Send + Sync + std::fmt::Debug {
    /// Admit one physical command or HTTP request through the existing authority.
    fn admit<'a>(
        &'a self,
        hook: &'a HookConfig,
        event: HookEvent,
        context: &'a HookContext,
    ) -> BoxFuture<'a, Result<Box<dyn HookStartPermit>, String>>;
}

/// One admitted physical launch. Rechecking this receipt must not prompt, create
/// another grant or record execution success. The runner consumes it once.
pub trait HookStartPermit: Send + Sync + std::fmt::Debug {
    /// Revalidate immediately after resource binding and before external work begins.
    fn recheck<'a>(
        &'a self,
        hook: &'a HookConfig,
        event: HookEvent,
        context: &'a HookContext,
    ) -> BoxFuture<'a, Result<(), String>>;
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestHookAdmission;
#[cfg(test)]
impl HookAdmission for TestHookAdmission {
    fn admit<'a>(
        &'a self,
        _: &'a HookConfig,
        _: HookEvent,
        _: &'a HookContext,
    ) -> BoxFuture<'a, Result<Box<dyn HookStartPermit>, String>> {
        Box::pin(async { Ok(Box::new(Self) as Box<dyn HookStartPermit>) })
    }
}
#[cfg(test)]
impl HookStartPermit for TestHookAdmission {
    fn recheck<'a>(
        &'a self,
        _: &'a HookConfig,
        _: HookEvent,
        _: &'a HookContext,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
