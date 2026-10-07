//! Real read-only Git routes preserve authorized scope, revision identity and cancellation ownership.
mod common;

use axum::{
    Router,
    http::{Method, StatusCode},
};
use common::{app_with_config, call, json_body, local_post, local_with_headers};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};
use zk_server::config::Config;

const UNUSUAL_FILE: &str = "目录/含\t空 格中文.txt";

struct Fixture {
    app: Router,
    db: zk_db::Db,
    directory: PathBuf,
    repository: PathBuf,
    project: String,
    commits: Vec<String>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repository)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
        ])
        .args(args)
        .output()
        .expect("fixture Git process");
    assert!(
        output.status.success(),
        "fixture Git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .trim_end()
        .to_owned()
}
fn commit(repository: &Path, message: &str) -> String {
    git(repository, &["add", "--all"]);
    git(repository, &["commit", "--quiet", "-m", message]);
    git(repository, &["rev-parse", "HEAD"])
}
fn init(repository: &Path) {
    std::fs::create_dir_all(repository).unwrap();
    git(repository, &["init", "--quiet"]);
    git(repository, &["config", "user.name", "Git Read Fixture"]);
    git(
        repository,
        &["config", "user.email", "fixture@example.invalid"],
    );
}
async fn post(app: &mut Router, route: &str, body: Value) -> (StatusCode, Value) {
    let (status, _, bytes) = tokio::time::timeout(
        Duration::from_secs(35),
        call(app, local_post(route, Some(body.to_string()))),
    )
    .await
    .expect("bounded Git route");
    (status, json_body(&bytes))
}
async fn add_project(app: &mut Router, root: &Path, name: &str) -> String {
    let (status, body) = post(
        app,
        "/api/projects",
        json!({"name":name,"workspaceRoot":root}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["id"].as_str().unwrap().to_owned()
}
async fn fixture() -> Fixture {
    let directory = std::env::temp_dir().join(format!("zk-git-read-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let directory = directory.canonicalize().unwrap();
    let repository = directory.join("repository");
    init(&repository);
    std::fs::create_dir_all(repository.join("目录")).unwrap();
    std::fs::write(repository.join(UNUSUAL_FILE), "第一行\t内容\n稳定第二行\n").unwrap();
    std::fs::write(repository.join("README.md"), "first\n").unwrap();
    let first = commit(&repository, "root commit\n\nRoot detail with 中文");
    std::fs::write(repository.join(UNUSUAL_FILE), "修改行\t内容\n稳定第二行\n").unwrap();
    let second = commit(&repository, "edit exact unusual path");
    std::fs::write(repository.join("README.md"), "third commit\n").unwrap();
    let third = commit(&repository, "third commit");
    let mut config = Config::test_config();
    config.python_enabled = false; // These routes must have no sidecar dependency.
    config.workspace_allowed_roots = vec![directory.clone()];
    config.workspace_default_root = repository.to_string_lossy().into_owned();
    let (mut app, db) = app_with_config(config);
    let project = add_project(&mut app, &repository, "Git read").await;
    Fixture {
        app,
        db,
        directory,
        repository,
        project,
        commits: vec![first, second, third],
    }
}
impl Fixture {
    fn body(&self, extra: Value) -> Value {
        let mut body =
            json!({"projectId":self.project,"requestId":uuid::Uuid::new_v4().to_string()});
        let Value::Object(extra) = extra else {
            panic!("fixture request fields must be an object");
        };
        body.as_object_mut().unwrap().extend(extra);
        body
    }
    async fn request(&mut self, route: &str, extra: Value) -> (StatusCode, Value) {
        let body = self.body(extra);
        post(&mut self.app, route, body).await
    }
}

#[tokio::test]
async fn history_pages_keep_the_first_resolved_revision_when_head_moves() {
    let mut fixture = fixture().await;
    let (status, first) = fixture.request("/api/git/log", json!({"maxCount":1})).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["data"]["total"], 3);
    assert_eq!(first["data"]["commits"][0]["sha"], fixture.commits[2]);
    let fixed = first["data"]["head"].as_str().unwrap().to_owned();
    std::fs::write(
        fixture.repository.join("README.md"),
        "HEAD changed after first page\n",
    )
    .unwrap();
    let fourth = commit(&fixture.repository, "fourth commit");
    let (status, next) = fixture
        .request(
            "/api/git/log",
            json!({"maxCount":1,"offset":1,"branch":fixed}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{next}");
    assert_eq!(next["data"]["head"], fixed);
    assert_eq!(next["data"]["total"], 3);
    assert_eq!(next["data"]["commits"][0]["sha"], fixture.commits[1]);
    assert_eq!(next["data"]["commits"][0]["files"], json!([UNUSUAL_FILE]));
    let (status, current) = fixture
        .request("/api/git/log", json!({"max_count":1}))
        .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    assert_eq!(current["data"]["head"], fourth);
    assert_eq!(current["data"]["total"], 4);
    assert!(git(&fixture.repository, &["status", "--porcelain"]).is_empty());
}
#[tokio::test]
async fn root_commit_diff_and_two_revision_diff_report_actual_files_without_writes() {
    let mut fixture = fixture().await;
    let initial_head = git(&fixture.repository, &["rev-parse", "HEAD"]);
    let root = fixture.commits[0].clone();
    let (status, diff) = fixture
        .request("/api/git/diff", json!({"commit":root}))
        .await;
    assert_eq!(status, StatusCode::OK, "{diff}");
    assert_eq!(diff["data"]["files_changed"], 2);
    assert!(
        diff["data"]["detailed"]
            .as_str()
            .unwrap()
            .contains("+第一行\t内容")
    );
    let second = fixture.commits[1].clone();
    let (status, pair) = fixture
        .request("/api/git/diff", json!({"ref1":root,"ref2":second}))
        .await;
    assert_eq!(status, StatusCode::OK, "{pair}");
    assert_eq!(pair["data"]["files_changed"], 1);
    let patch = pair["data"]["detailed"].as_str().unwrap();
    assert!(patch.contains("-第一行\t内容") && patch.contains("+修改行\t内容"));
    assert_eq!(
        git(&fixture.repository, &["rev-parse", "HEAD"]),
        initial_head
    );
    assert!(git(&fixture.repository, &["status", "--porcelain"]).is_empty());
}
#[tokio::test]
async fn blame_uses_literal_unicode_tab_paths_and_the_requested_historical_revision() {
    let mut fixture = fixture().await;
    let root = fixture.commits[0].clone();
    let (status, old) = fixture
        .request(
            "/api/git/blame",
            json!({"filePath":UNUSUAL_FILE,"ref":root}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{old}");
    assert_eq!(old["data"]["file_path"], UNUSUAL_FILE);
    assert_eq!(old["data"]["total_lines"], 2);
    assert_eq!(old["data"]["lines"][0]["content"], "第一行\t内容");
    assert_eq!(old["data"]["lines"][0]["sha"], root);
    let (status, current) = fixture
        .request(
            "/api/git/blame",
            json!({"file_path":UNUSUAL_FILE,"ref":"HEAD"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    assert_eq!(current["data"]["lines"][0]["sha"], fixture.commits[1]);
    assert_eq!(current["data"]["lines"][1]["sha"], fixture.commits[0]);
    assert_eq!(current["data"]["lines"][1]["line_no"], 2);
    assert_eq!(current["data"]["lines"][0]["author"], "Git Read Fixture");
    assert!(
        current["data"]["lines"][0]["date"]
            .as_str()
            .unwrap()
            .contains('T')
    );
}
#[tokio::test]
async fn scope_requires_a_saved_project_or_session_and_rejects_conflicting_claims() {
    let mut fixture = fixture().await;
    let (status, _) = post(
        &mut fixture.app,
        "/api/git/log",
        json!({"projectRoot":fixture.repository}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post(
        &mut fixture.app,
        "/api/git/log",
        json!({"projectId":"unknown-project"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let directory = fixture.directory.clone();
    let (status, _) = fixture
        .request("/api/git/log", json!({"projectRoot":directory}))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let saved = fixture
        .db
        .create_session("fixture", fixture.repository.to_str().unwrap())
        .await
        .unwrap();
    let foreign = fixture
        .db
        .create_session("fixture", fixture.directory.to_str().unwrap())
        .await
        .unwrap();
    let body = fixture.body(json!({"sessionId":foreign.id}));
    let (status, _) = post(&mut fixture.app, "/api/git/log", body).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let body = fixture.body(json!({"sessionId":saved.id}));
    let (status, _, _) = call(
        &mut fixture.app,
        local_with_headers(
            "/api/git/log",
            Method::POST,
            Some(body.to_string()),
            &[("x-session-id", &foreign.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _, bytes) = call(
        &mut fixture.app,
        local_with_headers(
            "/api/git/log",
            Method::POST,
            Some(
                json!({"sessionId":saved.id,"requestId":uuid::Uuid::new_v4().to_string()})
                    .to_string(),
            ),
            &[("x-session-id", &saved.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&bytes));
    let repository = fixture.repository.clone();
    let (status, _) = fixture
        .request("/api/git/log", json!({"repo_path":repository}))
        .await;
    assert_eq!(status, StatusCode::OK);
}
#[tokio::test]
async fn invalid_revisions_traversal_and_historical_symlinks_are_rejected() {
    let mut fixture = fixture().await;
    for extra in [
        json!({"branch":"--help"}),
        json!({"branch":"HEAD:README.md"}),
        json!({"maxCount":0}),
        json!({"maxCount":101}),
        json!({"offset":10001}),
    ] {
        let (status, body) = fixture.request("/api/git/log", extra).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    for file in ["../README.md", "/etc/passwd", ":(glob)*", "missing.txt"] {
        let (status, body) = fixture
            .request("/api/git/blame", json!({"filePath":file}))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    std::os::unix::fs::symlink("README.md", fixture.repository.join("symlink.txt")).unwrap();
    let linked = commit(&fixture.repository, "record symlink");
    let (status, body) = fixture
        .request(
            "/api/git/blame",
            json!({"filePath":"symlink.txt","ref":linked}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _) = fixture
        .request("/api/git/unchecked-path", json!({"repo_path":"/etc"}))
        .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "legacy wildcard cannot proxy arbitrary Git reads"
    );
}
#[tokio::test]
async fn pre_cancel_blocks_only_the_exact_owner_and_request_identity() {
    let mut fixture = fixture().await;
    let request_id = uuid::Uuid::new_v4().to_string();
    let (status, cancelled) = fixture
        .request("/api/git/cancel", json!({"requestId":request_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled["cancellationRequested"], true);
    let (status, blocked) = fixture
        .request("/api/git/log", json!({"requestId":request_id}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked}");
    let alternate = fixture.directory.join("alternate");
    init(&alternate);
    let other = add_project(&mut fixture.app, &alternate, "Other owner").await;
    let fresh = uuid::Uuid::new_v4().to_string();
    let (status, _) = post(
        &mut fixture.app,
        "/api/git/cancel",
        json!({"projectId":other,"requestId":fresh}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, actual) = fixture
        .request("/api/git/log", json!({"requestId":fresh}))
        .await;
    assert_eq!(status, StatusCode::OK, "{actual}");
    assert_eq!(actual["data"]["total"], 3);
    let (status, _) = fixture
        .request("/api/git/log", json!({"requestId":fresh}))
        .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "completed request identity is not silently re-executed"
    );
    assert!(git(&fixture.repository, &["status", "--porcelain"]).is_empty());
}
