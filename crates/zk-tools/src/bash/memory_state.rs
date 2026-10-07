//! Run-owned RAM cwd for temporary sessions. No command, environment or cwd file.
use crate::{RunToolScope, RunToolScopeFactory, ToolContext, ToolRegistry};
use futures::future::BoxFuture;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

static STATES: LazyLock<Mutex<HashMap<String, Weak<MemoryShellState>>>> =
    LazyLock::new(Mutex::default);

pub(super) struct MemoryShellState {
    session: String,
    run: Option<String>,
    active: AtomicBool,
    cwd: Mutex<Option<PathBuf>>,
    pub(super) serial: tokio::sync::Mutex<()>,
}

impl MemoryShellState {
    pub(super) fn cwd(&self) -> Result<PathBuf, &'static str> {
        if !self.active.load(Ordering::Acquire) {
            return Err("SHELL_MEMORY_SCOPE_EXPIRED");
        }
        self.cwd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .filter(|path| path.is_dir())
            .ok_or("SHELL_CWD_UNCONFIRMED")
    }

    pub(super) fn update(&self, cwd: Option<PathBuf>) -> Result<(), &'static str> {
        let mut current = self
            .cwd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.active.load(Ordering::Acquire) {
            return Err("SHELL_MEMORY_SCOPE_EXPIRED");
        }
        *current = cwd;
        if current.is_none() {
            Err("SHELL_CWD_UNCONFIRMED")
        } else {
            Ok(())
        }
    }

    fn close(&self) {
        self.active.store(false, Ordering::Release);
        *self
            .cwd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let mut states = STATES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if states
            .get(&self.session)
            .and_then(Weak::upgrade)
            .is_some_and(|state| std::ptr::eq(state.as_ref(), self))
        {
            states.remove(&self.session);
        }
    }
}

fn register(session: &str, run: Option<&str>, cwd: &Path) -> Result<Arc<MemoryShellState>, String> {
    let cwd = cwd.canonicalize().map_err(|_| "SHELL_CWD_INVALID")?;
    if !cwd.is_dir() {
        return Err("SHELL_CWD_INVALID".into());
    }
    let mut states = STATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    states.retain(|_, state| state.strong_count() > 0);
    if states.get(session).and_then(Weak::upgrade).is_some() {
        return Err("SHELL_MEMORY_SCOPE_CONFLICT".into());
    }
    if states.len() >= 4096 {
        return Err("SHELL_MEMORY_SCOPE_LIMIT".into());
    }
    let state = Arc::new(MemoryShellState {
        session: session.into(),
        run: run.map(str::to_owned),
        active: AtomicBool::new(true),
        cwd: Mutex::new(Some(cwd)),
        serial: tokio::sync::Mutex::new(()),
    });
    states.insert(session.into(), Arc::downgrade(&state));
    Ok(state)
}

pub(super) fn acquire(context: &ToolContext) -> Result<Arc<MemoryShellState>, &'static str> {
    let session = context.session_id().ok_or("SHELL_MEMORY_SCOPE_REQUIRED")?;
    let state = STATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(session)
        .and_then(Weak::upgrade)
        .ok_or("SHELL_MEMORY_SCOPE_REQUIRED")?;
    if state.run.as_deref() != context.run_id() || !state.active.load(Ordering::Acquire) {
        return Err("SHELL_MEMORY_SCOPE_EXPIRED");
    }
    Ok(state)
}

/// The exact same RAM fact is read by authorization and physical execution.
pub(super) fn tracked(session: &str) -> Option<String> {
    let state = STATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(session)
        .and_then(Weak::upgrade)?;
    Some(
        state
            .cwd()
            .ok()
            .and_then(|path| path.to_str().map(str::to_owned))
            .unwrap_or_else(|| "\0".into()),
    )
}

pub(super) fn reset(session: &str, path: &str) -> bool {
    let state = STATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(session)
        .and_then(Weak::upgrade);
    if let Some(state) = state {
        let _ = state.update(
            Path::new(path)
                .canonicalize()
                .ok()
                .filter(|path| path.is_dir()),
        );
        true
    } else {
        false
    }
}

/// Lightweight host default: only temporary Runs allocate cwd state.
#[derive(Debug, Default)]
pub struct ShellMemoryScopeFactory;

struct Scope {
    directory: Arc<ToolRegistry>,
    state: Option<Arc<MemoryShellState>>,
}
impl RunToolScope for Scope {
    fn registry(&self) -> Arc<ToolRegistry> {
        self.directory.clone()
    }
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async {
            if let Some(state) = &self.state {
                state.close();
            }
            Ok(())
        })
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.close();
        }
    }
}
impl RunToolScopeFactory for ShellMemoryScopeFactory {
    fn prepare(
        &self,
        context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        Box::pin(async move {
            let state = if context.is_ephemeral() {
                let session = context
                    .session_id()
                    .filter(|id| !id.is_empty())
                    .ok_or("SHELL_SCOPE_OWNER_REQUIRED")?;
                let run = context
                    .run_id()
                    .filter(|id| !id.is_empty())
                    .ok_or("SHELL_SCOPE_OWNER_REQUIRED")?;
                Some(register(session, Some(run), context.working_dir())?)
            } else {
                None
            };
            Ok(Arc::new(Scope {
                directory: base,
                state,
            }) as Arc<dyn RunToolScope>)
        })
    }
}

#[cfg(test)]
pub(super) fn fixture_scope(session: &str, cwd: &Path) -> impl Drop + use<> {
    struct Guard(Arc<MemoryShellState>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    Guard(register(session, None, cwd).unwrap())
}
