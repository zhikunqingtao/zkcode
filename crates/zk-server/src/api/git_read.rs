//! Read-only Git UI queries use the native supervised process lifecycle and saved scope.
//! No shell interpolation, network fetch, hooks, checkout or independent task engine.
use super::code_analysis::{AnalysisScope, Binding, bind, recheck};
use crate::{error::ApiError, state::AppState, workspace::failure};
use axum::{
    Extension, Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Component, Path},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;

#[derive(Default)]
pub(crate) struct Requests(Mutex<HashMap<(String, String), Entry>>);
struct Entry {
    cancel: CancellationToken,
    active: bool,
    expires: Instant,
}
struct Job {
    directory: Arc<Requests>,
    key: (String, String),
    cancel: CancellationToken,
    deadline: Instant,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(entry) = self
            .directory
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&self.key)
        {
            entry.active = false;
        }
    }
}
impl Requests {
    fn register(self: &Arc<Self>, binding: &Binding) -> Result<Job, ApiError> {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|_, entry| entry.active || entry.expires > Instant::now());
        let key = (binding.owner.clone(), binding.request_id.clone());
        if entries.contains_key(&key) {
            return Err(failure(
                StatusCode::CONFLICT,
                "GIT_REQUEST_ALREADY_USED",
                "Request was already used or cancelled",
            ));
        }
        if entries.len() >= 256 || entries.values().filter(|entry| entry.active).count() >= 8 {
            return Err(failure(
                StatusCode::TOO_MANY_REQUESTS,
                "GIT_READ_BUSY",
                "Git read requests are busy",
            ));
        }
        let cancel = CancellationToken::new();
        entries.insert(
            key.clone(),
            Entry {
                cancel: cancel.clone(),
                active: true,
                expires: Instant::now() + Duration::from_mins(2),
            },
        );
        Ok(Job {
            directory: self.clone(),
            key,
            cancel,
            deadline: Instant::now() + Duration::from_secs(30),
        })
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GitRequest {
    #[serde(flatten)]
    scope: AnalysisScope,
    #[serde(default, alias = "repo_path")]
    repo_path: Option<String>,
    #[serde(default, alias = "max_count")]
    max_count: Option<u16>,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    ref1: Option<String>,
    #[serde(default)]
    ref2: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default, rename = "ref")]
    reference: Option<String>,
    #[serde(default, alias = "file_path")]
    file_path: Option<String>,
}

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/git/log", post(log))
        .route("/api/git/diff", post(diff))
        .route("/api/git/blame", post(blame))
        .route("/api/git/cancel", post(cancel))
        .layer(Extension(Arc::new(Requests::default())))
}
async fn scope(
    state: &AppState,
    headers: &HeaderMap,
    request: &mut GitRequest,
) -> Result<Binding, ApiError> {
    if let Some(repo) = request.repo_path.take() {
        if request
            .scope
            .project_root
            .as_ref()
            .is_some_and(|root| root != &repo)
        {
            return Err(ApiError::validation(
                "Conflicting projectRoot and repo_path",
            ));
        }
        request.scope.project_root = Some(repo);
    }
    let binding = bind(state, headers, &request.scope).await?;
    let root = binding.root.clone();
    if !tokio::task::spawn_blocking(move || {
        zk_authz::workspace::WorkspaceIdentityService.is_validated_git_repository_root(&root)
    })
    .await
    .map_err(|_| ApiError::internal())?
    {
        return Err(ApiError::validation_with_code(
            "GIT_REPOSITORY_INVALID",
            "The authorized workspace must be a validated Git repository root",
        ));
    }
    Ok(binding)
}
async fn run(binding: &Binding, job: &Job, args: &[String]) -> Result<String, ApiError> {
    let timeout = job.deadline.saturating_duration_since(Instant::now());
    if timeout.is_zero() || job.cancel.is_cancelled() {
        return Err(failure(
            StatusCode::CONFLICT,
            "GIT_READ_CANCELLED",
            "Git read was cancelled or exceeded its deadline",
        ));
    }
    let (progress, _receiver) = tokio::sync::mpsc::channel(1);
    let context = zk_tools::ToolContext::with_bounded_progress(job.cancel.clone(), progress)
        .with_working_dir(&binding.root);
    let mut safe = vec![
        "--no-pager",
        "--no-lazy-fetch",
        "--no-optional-locks",
        "--no-replace-objects",
        "--literal-pathspecs",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "diff.external=",
        "-c",
        "core.quotePath=false",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    safe.extend_from_slice(args);
    let output = zk_tools::process::run_git_program(&safe, &binding.root, timeout, &context)
        .await
        .map_err(|_| {
            failure(
                StatusCode::BAD_GATEWAY,
                "GIT_READ_FAILED",
                "Git read process failed",
            )
        })?;
    if output.cancelled || output.timed_out || !output.termination_confirmed {
        return Err(failure(
            StatusCode::CONFLICT,
            "GIT_READ_CANCELLED",
            "Git read did not finish with confirmed cleanup",
        ));
    }
    if output.truncated {
        return Err(failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            "GIT_READ_OUTPUT_LIMIT",
            "Git read exceeds the bounded output limit",
        ));
    }
    if output.exit_code != 0 {
        return Err(ApiError::validation_with_code(
            "GIT_READ_FAILED",
            "Git could not read the selected revision or file",
        ));
    }
    Ok(output.stdout)
}
fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}
fn valid_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
async fn revision(binding: &Binding, job: &Job, value: &str) -> Result<String, ApiError> {
    if value.is_empty()
        || value.len() > 256
        || value.starts_with('-')
        || value
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, ':' | '\\'))
    {
        return Err(ApiError::validation("Invalid Git revision"));
    }
    let ref_arg = format!("{value}^{{commit}}");
    let output = run(
        binding,
        job,
        &args(&["rev-parse", "--verify", "--end-of-options", &ref_arg]),
    )
    .await?;
    let sha = output.trim();
    if !valid_sha(sha) {
        return Err(ApiError::internal());
    }
    Ok(sha.to_owned())
}
async fn files(binding: &Binding, job: &Job, sha: &str) -> Result<Vec<String>, ApiError> {
    let output = run(
        binding,
        job,
        &args(&[
            "diff-tree",
            "--root",
            "--no-commit-id",
            "--name-only",
            "-r",
            "-z",
            sha,
            "--",
        ]),
    )
    .await?;
    Ok(output
        .split('\0')
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect())
}

#[utoipa::path(post,path="/api/git/log",tag="git",request_body=GitRequest,responses((status=200,description="Immutable-revision history page from the authorized repository")))]
pub(crate) async fn log(
    State(state): State<AppState>,
    Extension(directory): Extension<Arc<Requests>>,
    headers: HeaderMap,
    Json(mut request): Json<GitRequest>,
) -> Result<Json<Value>, ApiError> {
    let binding = scope(&state, &headers, &mut request).await?;
    let job = directory.register(&binding)?;
    let max = request.max_count.unwrap_or(20);
    if max == 0 || max > 100 || request.offset > 10_000 {
        return Err(ApiError::validation(
            "Use 1–100 commits and an offset no larger than 10000",
        ));
    }
    let head = revision(&binding, &job, request.branch.as_deref().unwrap_or("HEAD")).await?;
    let total = run(&binding, &job, &args(&["rev-list", "--count", &head, "--"]))
        .await?
        .trim()
        .parse::<u64>()
        .map_err(|_| ApiError::internal())?;
    let limit = format!("--max-count={max}");
    let skip = format!("--skip={}", request.offset);
    let raw = run(
        &binding,
        &job,
        &args(&[
            "log",
            "-z",
            "--no-show-signature",
            "--format=%H%x00%an%x00%aI%x00%B",
            &limit,
            &skip,
            &head,
            "--",
        ]),
    )
    .await?;
    let fields = raw
        .strip_suffix('\0')
        .unwrap_or(&raw)
        .split('\0')
        .collect::<Vec<_>>();
    let mut commits = Vec::new();
    if !raw.is_empty() {
        if fields.len() % 4 != 0 {
            return Err(ApiError::internal());
        }
        for record in fields.chunks_exact(4) {
            if !valid_sha(record[0]) {
                return Err(ApiError::internal());
            }
            commits.push(json!({"sha":record[0],"author":record[1],"date":record[2],"message":record[3],"files":files(&binding,&job,record[0]).await?}));
        }
    }
    recheck(&state, &headers, &request.scope, &binding).await?;
    Ok(Json(
        json!({"success":true,"data":{"commits":commits,"total":total,"head":head}}),
    ))
}

#[utoipa::path(post,path="/api/git/diff",tag="git",request_body=GitRequest,responses((status=200,description="Read-only commit or two-revision diff")))]
pub(crate) async fn diff(
    State(state): State<AppState>,
    Extension(directory): Extension<Arc<Requests>>,
    headers: HeaderMap,
    Json(mut request): Json<GitRequest>,
) -> Result<Json<Value>, ApiError> {
    let binding = scope(&state, &headers, &mut request).await?;
    let job = directory.register(&binding)?;
    let (base, changed) = if let Some(commit) = request.commit.as_deref() {
        let sha = revision(&binding, &job, commit).await?;
        let changed = files(&binding, &job, &sha).await?.len();
        (
            args(&["show", "--format=", "--no-ext-diff", "--no-textconv", &sha]),
            changed,
        )
    } else {
        let from = revision(&binding, &job, request.ref1.as_deref().unwrap_or("HEAD~1")).await?;
        let to = revision(&binding, &job, request.ref2.as_deref().unwrap_or("HEAD")).await?;
        let raw = run(
            &binding,
            &job,
            &args(&[
                "diff",
                "--name-only",
                "-z",
                "--no-ext-diff",
                "--no-textconv",
                &from,
                &to,
                "--",
            ]),
        )
        .await?;
        (
            args(&["diff", "--no-ext-diff", "--no-textconv", &from, &to]),
            raw.split('\0').filter(|s| !s.is_empty()).count(),
        )
    };
    let mut stat = base.clone();
    stat.extend(args(&["--stat", "--"]));
    let summary = run(&binding, &job, &stat).await?;
    let mut patch = base;
    patch.extend(args(&["--patch", "--"]));
    let detailed = run(&binding, &job, &patch).await?;
    recheck(&state, &headers, &request.scope, &binding).await?;
    Ok(Json(
        json!({"success":true,"data":{"summary":summary,"detailed":detailed,"files_changed":changed}}),
    ))
}

#[utoipa::path(post,path="/api/git/blame",tag="git",request_body=GitRequest,responses((status=200,description="Exact file/revision line provenance")))]
pub(crate) async fn blame(
    State(state): State<AppState>,
    Extension(directory): Extension<Arc<Requests>>,
    headers: HeaderMap,
    Json(mut request): Json<GitRequest>,
) -> Result<Json<Value>, ApiError> {
    let binding = scope(&state, &headers, &mut request).await?;
    let job = directory.register(&binding)?;
    let file = request
        .file_path
        .as_deref()
        .ok_or_else(|| ApiError::validation("filePath is required"))?;
    if file.is_empty()
        || file.len() > 4096
        || file.contains('\0')
        || Path::new(file).is_absolute()
        || Path::new(file)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(ApiError::validation(
            "Use an exact repository-relative file path",
        ));
    }
    let sha = revision(
        &binding,
        &job,
        request.reference.as_deref().unwrap_or("HEAD"),
    )
    .await?;
    let mode = run(&binding, &job, &args(&["ls-tree", "-z", &sha, "--", file])).await?;
    if !mode.starts_with("100644 ") && !mode.starts_with("100755 ") {
        return Err(ApiError::validation(
            "Blame requires a regular file in the selected revision",
        ));
    }
    let raw = run(
        &binding,
        &job,
        &args(&["blame", "--line-porcelain", &sha, "--", file]),
    )
    .await?;
    let lines = parse_blame(&raw)?;
    recheck(&state, &headers, &request.scope, &binding).await?;
    Ok(Json(
        json!({"success":true,"data":{"file_path":file,"total_lines":lines.len(),"lines":lines}}),
    ))
}
fn parse_blame(raw: &str) -> Result<Vec<Value>, ApiError> {
    let mut lines = Vec::new();
    let mut sha = "";
    let mut line_no = 0u64;
    let mut author = "";
    let mut date = String::new();
    for line in raw.lines() {
        if let Some(content) = line.strip_prefix('\t') {
            if !valid_sha(sha) || line_no == 0 || date.is_empty() {
                return Err(ApiError::internal());
            }
            lines.push(
                json!({"line_no":line_no,"sha":sha,"author":author,"date":date,"content":content}),
            );
            sha = "";
            author = "";
            date.clear();
        } else if let Some(value) = line.strip_prefix("author ") {
            author = value;
        } else if let Some(value) = line.strip_prefix("author-time ") {
            let ms = value
                .parse::<i64>()
                .ok()
                .and_then(|n| n.checked_mul(1000))
                .ok_or_else(ApiError::internal)?;
            date = zk_db::time::format_rfc3339_micros(ms);
        } else {
            let parts = line.split_ascii_whitespace().collect::<Vec<_>>();
            if parts.len() >= 3 && valid_sha(parts[0]) {
                sha = parts[0];
                line_no = parts[2].parse().map_err(|_| ApiError::internal())?;
            }
        }
    }
    if !sha.is_empty() {
        return Err(ApiError::internal());
    }
    Ok(lines)
}
#[utoipa::path(post,path="/api/git/cancel",tag="git",request_body=AnalysisScope,responses((status=200,description="Scope-bound cancellation requested, not a completion assertion")))]
pub(crate) async fn cancel(
    State(state): State<AppState>,
    Extension(directory): Extension<Arc<Requests>>,
    headers: HeaderMap,
    Json(scope): Json<AnalysisScope>,
) -> Result<Json<Value>, ApiError> {
    if scope.request_id.is_none() {
        return Err(ApiError::validation("requestId is required"));
    }
    let binding = bind(&state, &headers, &scope).await?;
    let mut entries = directory
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    entries.retain(|_, entry| entry.active || entry.expires > Instant::now());
    let key = (binding.owner, binding.request_id);
    if !entries.contains_key(&key) && entries.len() >= 256 {
        return Err(failure(
            StatusCode::TOO_MANY_REQUESTS,
            "GIT_READ_BUSY",
            "Git request registry is full",
        ));
    }
    let entry = entries.entry(key).or_insert_with(|| Entry {
        cancel: CancellationToken::new(),
        active: false,
        expires: Instant::now() + Duration::from_mins(2),
    });
    entry.cancel.cancel();
    Ok(Json(
        json!({"cancellationRequested":true,"status":if entry.active{"cancelling"}else{"cancelled"}}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        process::{Command, Stdio},
    };

    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    async fn fixture() -> (Fixture, Binding, Job) {
        let root = std::env::temp_dir().join(format!("zk-git-cancel-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let mut config = crate::config::Config::test_config();
        config.workspace_allowed_roots = vec![root.clone()];
        let state = AppState::new(zk_db::Db::open_in_memory().unwrap(), config);
        let session = state
            .db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap();
        let scope: AnalysisScope = serde_json::from_value(json!({"sessionId":session.id})).unwrap();
        let binding = bind(&state, &HeaderMap::new(), &scope).await.unwrap();
        let job = Arc::new(Requests::default()).register(&binding).unwrap();
        (Fixture(root), binding, job)
    }
    fn process_alive(pid: u32) -> bool {
        Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    }
    async fn started(root: &Path) -> u32 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(root.join("fixture.pid"))
                    && let Ok(pid) = text.trim().parse::<u32>()
                {
                    assert!(
                        process_alive(pid),
                        "fixture must be physically running before cancellation"
                    );
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real supervised Git alias started")
    }
    async fn gone(pid: u32) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while process_alive(pid) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("owned process must be reaped, not just its response future cancelled");
    }
    async fn cancel_case(drop_request: bool) {
        let (fixture, binding, job) = fixture().await;
        let token = job.cancel.clone();
        // This alias is fixture-only, never accepted as a public route parameter.
        let call = tokio::spawn(async move {
            run(
                &binding,
                &job,
                &args(&[
                    "-c",
                    "alias.fixture=!printf '%s' $$ > fixture.pid; exec sleep 30",
                    "fixture",
                ]),
            )
            .await
        });
        let pid = started(&fixture.0).await;
        if drop_request {
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
            assert!(
                token.is_cancelled(),
                "dropping HTTP work must run Job's cancellation guard"
            );
        } else {
            token.cancel();
            assert!(
                tokio::time::timeout(Duration::from_secs(10), call)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
        }
        gone(pid).await;
    }
    #[tokio::test]
    async fn in_flight_cancellation_reaps_the_supervised_git_process() {
        cancel_case(false).await;
    }
    #[tokio::test]
    async fn dropped_request_reaps_the_supervised_git_process() {
        cancel_case(true).await;
    }
}
